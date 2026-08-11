import hashlib
import json
import math
import os
import posixpath
import time
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

from app.log import logger
from app.modules.filemanager.storages import transfer_process
from app.schemas import FileItem, StorageUsage


class FlowpanStorageAPI:
    """
    MoviePilot storage adapter backed by Flowpan's native 115 Cookie upload
    bridge. MoviePilot reads the local file and PUTs parts directly to 115 OSS;
    Flowpan only provides 115 upload init/sign/complete APIs.
    """

    def __init__(
        self,
        flowpan_url: str,
        token: str,
        disk_name: str = "Flowpan-115",
        part_size_mb: int = 10,
        list_cache_ttl: int = 300,
    ) -> None:
        self._flowpan_url = (flowpan_url or "").strip().rstrip("/")
        self._token = (token or "").strip()
        self._disk_name = (disk_name or "Flowpan-115").strip()
        self._part_size = max(5, min(128, int(part_size_mb or 10))) * 1024 * 1024
        self._state_dir = (
            Path(os.environ.get("CONFIG_DIR") or "/config")
            / "plugins"
            / "flowpan-storage"
            / "upload-state"
        )
        self._list_cache: Dict[str, Tuple[float, List[FileItem]]] = {}
        self._list_cache_lock = RLock()
        try:
            cache_ttl = int(list_cache_ttl)
        except (TypeError, ValueError):
            cache_ttl = 300
        self._list_cache_ttl = max(0, min(cache_ttl, 86400))
        self.transtype = {"copy": "复制", "move": "移动"}

    def upload(
        self,
        target_dir: FileItem,
        local_path: Path,
        new_name: Optional[str] = None,
    ) -> Optional[FileItem]:
        local_path = Path(local_path)
        if not local_path.exists() or not local_path.is_file():
            logger.error(f"【Flowpan存储】本地文件不存在: {local_path}")
            return None

        progress_callback = transfer_process(local_path.as_posix())
        target_name = new_name or local_path.name
        target_dir_path = self._dir_path(target_dir)
        target_path = posixpath.join(target_dir_path.rstrip("/") or "/", target_name)
        file_size = local_path.stat().st_size

        logger.info(f"【Flowpan存储】开始原生上传: {local_path} -> {target_path}")
        try:
            progress_callback(1)
            logger.info(f"【Flowpan存储】计算文件 hash: {target_name} size={file_size}")
            file_sha1, file_preid = self._sha1_file_pair(
                local_path, progress_callback, base=1, span=7
            )
            progress_callback(8)
            target_cid = self._target_cid(target_dir, target_dir_path)
            if target_cid is None:
                return None
            progress_callback(9)
            state_key = self._state_key(local_path, target_path, file_size, file_sha1)
            session = self._load_state(state_key) or {}
            if session:
                try:
                    session = self._api("/api/mp/storage/115/upload/resume", session)
                    logger.info(f"【Flowpan存储】恢复断点上传: {target_name}")
                except Exception as error:
                    logger.warning(f"【Flowpan存储】断点恢复失败，重新初始化: {error}")
                    session = {}
            if not session:
                session = self._upload_init(
                    name=target_name,
                    size=file_size,
                    sha1=file_sha1,
                    preid=file_preid,
                    target_cid=target_cid,
                )
                if session.get("requires_sign"):
                    logger.info(f"【Flowpan存储】计算 115 秒传二次校验: {target_name}")
                    sign_val = self._sha1_range(
                        local_path,
                        int(session.get("range_start") or 0),
                        int(session.get("range_end") or 0),
                    )
                    session = self._upload_init(
                        name=target_name,
                        size=file_size,
                        sha1=file_sha1,
                        preid=file_preid,
                        target_cid=target_cid,
                        sign_key=session.get("sign_key"),
                        sign_val=sign_val,
                    )

            if session.get("reused"):
                progress_callback(100)
                self._drop_state(state_key)
                self._clear_list_cache()
                logger.info(f"【Flowpan存储】{target_name} 秒传成功")
                return self._file_item_from_session(target_path, target_name, file_size, session)

            session["part_size"] = self._part_size
            if not session.get("upload_id"):
                session = self._api("/api/mp/storage/115/upload/start", session)
            else:
                session = self._api("/api/mp/storage/115/upload/list-parts", session)
            progress_callback(10)
            self._save_state(state_key, session)
            upload_id = session.get("upload_id")
            if not upload_id:
                logger.error(f"【Flowpan存储】初始化分片失败: {session}")
                return None

            part_size = int(session.get("part_size") or self._part_size)
            existing_parts = {
                int(part.get("part_number")): part
                for part in (session.get("parts") or [])
                if part.get("part_number") and part.get("etag")
            }
            parts: List[Dict[str, Any]] = [
                {
                    "part_number": int(part["part_number"]),
                    "etag": str(part["etag"]),
                    "size": int(part.get("size") or 0),
                }
                for part in existing_parts.values()
            ]
            total_parts = int(math.ceil(file_size / part_size))
            uploaded = sum(int(part.get("size") or 0) for part in parts)
            if uploaded:
                progress_callback(self._upload_progress(uploaded, file_size))

            with local_path.open("rb") as fileobj:
                for part_number in range(1, total_parts + 1):
                    offset = (part_number - 1) * part_size
                    chunk_size = min(part_size, file_size - offset)
                    if part_number in existing_parts:
                        continue
                    fileobj.seek(offset)
                    chunk = fileobj.read(chunk_size)
                    if part_number == 1 or part_number == total_parts or part_number % 10 == 0:
                        logger.info(
                            f"【Flowpan存储】上传分片 {part_number}/{total_parts}: {target_name}"
                        )
                    signed = self._api(
                        "/api/mp/storage/115/upload/part-url",
                        {"session": session, "part_number": part_number},
                    )
                    response = requests.put(
                        signed["url"],
                        data=chunk,
                        headers=signed.get("headers") or {},
                        timeout=300,
                    )
                    if response.status_code < 200 or response.status_code >= 300:
                        raise RuntimeError(
                            f"part {part_number} HTTP {response.status_code}: {response.text[:200]}"
                        )
                    etag = response.headers.get("ETag")
                    if not etag:
                        raise RuntimeError(f"part {part_number} missing ETag")
                    parts.append(
                        {
                            "part_number": part_number,
                            "etag": etag,
                            "size": chunk_size,
                        }
                    )
                    session["parts"] = sorted(parts, key=lambda p: p["part_number"])
                    session["uploaded"] = uploaded + chunk_size
                    self._save_state(state_key, session)
                    uploaded += chunk_size
                    progress_callback(self._upload_progress(uploaded, file_size))

            try:
                session = self._api(
                    "/api/mp/storage/115/upload/complete",
                    {"session": session, "parts": sorted(parts, key=lambda p: p["part_number"])},
                )
            except Exception as error:
                if "restart_required" in str(error).lower():
                    self._drop_state(state_key)
                    logger.warning(
                        f"【Flowpan存储】断点会话已失效，已清理状态等待重新初始化: {target_name}"
                    )
                raise
            progress_callback(100)
            self._drop_state(state_key)
            self._clear_list_cache()
            logger.info(f"【Flowpan存储】{target_name} 上传完成")
            return self._file_item_from_session(target_path, target_name, file_size, session)
        except Exception as error:
            logger.error(f"【Flowpan存储】上传失败: {local_path} - {error}")
            return None

    def create_folder(self, fileitem: FileItem, name: str) -> Optional[FileItem]:
        parent = self._dir_path(fileitem)
        folder_path = posixpath.join(parent.rstrip("/") or "/", name.strip())
        item = self.get_folder(Path(folder_path))
        if item is not None:
            self._clear_list_cache()
        return item

    def get_folder(self, path: Path) -> Optional[FileItem]:
        folder_path = self._normalize_dir_path(Path(path).as_posix())
        try:
            data = self._api("/api/mp/storage/115/dir/ensure", {"path": folder_path})
            return FileItem(
                storage=self._disk_name,
                fileid=str(data.get("cid") or 0),
                path=self._ensure_trailing_slash(data.get("path") or folder_path),
                name="/" if folder_path == "/" else posixpath.basename(folder_path.rstrip("/")),
                basename="/" if folder_path == "/" else posixpath.basename(folder_path.rstrip("/")),
                type="dir",
            )
        except Exception as error:
            logger.error(f"【Flowpan存储】创建目录失败 {folder_path}: {error}")
            return None

    def get_item(self, path: Path) -> Optional[FileItem]:
        try:
            data = self._api(
                "/api/mp/storage/115/item",
                {"path": Path(path).as_posix(), "storage": self._disk_name},
            )
            return self._file_item_from_data(data)
        except Exception:
            return None

    def get_item_strict(self, path: Path) -> Optional[FileItem]:
        return self.get_item(path)

    def detail(self, fileitem: FileItem) -> Optional[FileItem]:
        return self.get_item(Path(fileitem.path))

    def exists(self, fileitem: FileItem) -> Optional[bool]:
        return self.get_item(Path(fileitem.path)) is not None

    def list(self, fileitem: FileItem) -> List[FileItem]:
        cache_key = self._list_cache_key(fileitem)
        cached = self._get_list_cache(cache_key)
        if cached is not None:
            return cached
        try:
            payload = {
                "path": getattr(fileitem, "path", "/") or "/",
                "storage": self._disk_name,
            }
            file_id = self._file_id(fileitem)
            if file_id is not None:
                payload["cid"] = file_id
            data = self._api(
                "/api/mp/storage/115/dir/list",
                payload,
            )
            items = [
                item
                for item in (self._file_item_from_data(raw) for raw in data.get("items") or [])
                if item is not None
            ]
            self._set_list_cache(cache_key, items)
            return items
        except Exception as error:
            logger.warning(f"【Flowpan存储】浏览目录失败 {getattr(fileitem, 'path', '/')}: {error}")
            return []

    def search(
        self,
        keyword: str,
        cid: int = 0,
        offset: int = 0,
        limit: int = 100,
    ) -> Dict[str, Any]:
        keyword = str(keyword or "").strip()
        payload = {
            "keyword": keyword,
            "cid": max(int(cid or 0), 0),
            "offset": max(int(offset or 0), 0),
            "limit": max(1, min(int(limit or 100), 200)),
            "storage": self._disk_name,
        }
        data = self._api("/api/mp/storage/115/search", payload)
        items = [
            item
            for item in (self._file_item_from_data(raw) for raw in data.get("items") or [])
            if item is not None
        ]
        return {
            "keyword": keyword,
            "cid": payload["cid"],
            "offset": int(data.get("offset") or payload["offset"]),
            "limit": int(data.get("limit") or payload["limit"]),
            "total": int(data.get("total") or len(items)),
            "items": items,
        }

    def iter_files(self, fileitem: FileItem) -> List[FileItem]:
        result: List[FileItem] = []
        for item in self.list(fileitem):
            if item.type == "dir":
                result.extend(self.iter_files(item))
            else:
                result.append(item)
        return result

    def any_files(self, fileitem: FileItem, extensions: list = None) -> bool:
        for item in self.list(fileitem):
            if item.type == "file":
                if not extensions:
                    return True
                ext = f".{(item.extension or '').lower()}"
                if ext in extensions:
                    return True
            if item.type == "dir" and self.any_files(item, extensions):
                return True
        return False

    def delete(self, fileitem: FileItem) -> bool:
        file_id = self._file_id(fileitem)
        if not file_id:
            return False
        try:
            self._api("/api/mp/storage/115/delete", {"ids": [file_id]})
            self._clear_list_cache()
            return True
        except Exception as error:
            logger.error(f"【Flowpan存储】删除失败 {getattr(fileitem, 'path', '')}: {error}")
            return False

    def rename(self, fileitem: FileItem, name: str) -> bool:
        file_id = self._file_id(fileitem)
        name = (name or "").strip()
        if not file_id or not name:
            return False
        try:
            self._api("/api/mp/storage/115/rename", {"id": file_id, "name": name})
            self._clear_list_cache()
            return True
        except Exception as error:
            logger.error(f"【Flowpan存储】重命名失败 {getattr(fileitem, 'path', '')}: {error}")
            return False

    def copy(self, fileitem: FileItem, path: Path, new_name: str) -> bool:
        return self._copy_or_move(fileitem, path, new_name, action="copy")

    def move(self, fileitem: FileItem, path: Path, new_name: str) -> bool:
        return self._copy_or_move(fileitem, path, new_name, action="move")

    def download(self, fileitem: FileItem, path: Path = None) -> Optional[Path]:
        return None

    def storage_usage(self) -> StorageUsage:
        try:
            data = self.probe_connection()
            return StorageUsage(
                total=float(data.get("total") or 0),
                available=float(data.get("available") or 0),
            )
        except Exception as error:
            logger.warning(f"【Flowpan存储】读取容量失败: {error}")
            return StorageUsage(total=0, available=0)

    def probe_connection(self) -> Dict[str, Any]:
        """
        探测 Flowpan / 115 存储桥连通性与鉴权状态。
        """
        return self._api("/api/mp/storage/115/usage", {})

    def list_cache_stats(self) -> Dict[str, Any]:
        """
        返回当前目录缓存统计信息。
        """
        now = time.time()
        with self._list_cache_lock:
            entries: List[Dict[str, Any]] = []
            expired_keys: List[str] = []
            for key, cached in self._list_cache.items():
                cached_at, items = cached
                age_seconds = max(0.0, now - cached_at)
                if self._list_cache_ttl <= 0:
                    expired_keys.append(key)
                    continue
                if age_seconds >= self._list_cache_ttl:
                    expired_keys.append(key)
                    continue
                entries.append(
                    {
                        "key": key,
                        "cached_at": int(cached_at),
                        "age_seconds": int(age_seconds),
                        "remaining_seconds": int(max(self._list_cache_ttl - age_seconds, 0)),
                        "item_count": len(items),
                    }
                )
            for key in expired_keys:
                self._list_cache.pop(key, None)
            entries.sort(key=lambda item: item["cached_at"], reverse=True)
            latest_cached_at = entries[0]["cached_at"] if entries else 0
            oldest_cached_at = entries[-1]["cached_at"] if entries else 0
            return {
                "enabled": self._list_cache_ttl > 0,
                "ttl_seconds": self._list_cache_ttl,
                "entry_count": len(entries),
                "latest_cached_at": latest_cached_at,
                "oldest_cached_at": oldest_cached_at,
                "entries": entries,
            }

    def clear_list_cache(self) -> None:
        """
        清空目录缓存。
        """
        self._clear_list_cache()

    def support_transtype(self) -> dict:
        return self.transtype

    def is_support_transtype(self, transtype: str) -> bool:
        return transtype in self.transtype

    def _copy_or_move(
        self,
        fileitem: FileItem,
        target_dir: Path,
        new_name: Optional[str],
        action: str,
    ) -> bool:
        file_id = self._file_id(fileitem)
        if not file_id:
            return False
        target_dir_path = self._normalize_dir_path(Path(target_dir).as_posix())
        target_name = (new_name or getattr(fileitem, "name", "") or Path(getattr(fileitem, "path", "")).name).strip()
        if not target_name:
            return False
        try:
            folder = self._api("/api/mp/storage/115/dir/ensure", {"path": target_dir_path})
            target_cid = int(folder.get("cid") or 0)
            endpoint = "/api/mp/storage/115/copy" if action == "copy" else "/api/mp/storage/115/move"
            if action == "move":
                self._api(
                    "/api/mp/storage/115/move/check-conflict",
                    {"ids": [file_id], "parent_id": target_cid},
                )
            self._api(endpoint, {"ids": [file_id], "parent_id": target_cid})
            self._clear_list_cache()
            current_name = (getattr(fileitem, "name", "") or Path(getattr(fileitem, "path", "")).name).strip()
            if target_name and target_name != current_name:
                target_item = self.get_item(Path(posixpath.join(target_dir_path.rstrip("/") or "/", current_name)))
                if target_item is not None:
                    return self.rename(target_item, target_name)
            return True
        except Exception as error:
            logger.error(f"【Flowpan存储】{self.transtype.get(action, action)}失败 {getattr(fileitem, 'path', '')}: {error}")
            return False

    def _list_cache_key(self, fileitem: FileItem) -> str:
        file_id = self._file_id(fileitem)
        if file_id is not None:
            return f"{self._disk_name}:cid:{file_id}"
        path = self._normalize_dir_path(getattr(fileitem, "path", "/") or "/")
        return f"{self._disk_name}:{path}"

    def _get_list_cache(self, key: str) -> Optional[List[FileItem]]:
        if self._list_cache_ttl <= 0:
            return None
        now = time.time()
        with self._list_cache_lock:
            cached = self._list_cache.get(key)
            if not cached:
                return None
            cached_at, items = cached
            if now - cached_at < self._list_cache_ttl:
                return list(items)
            self._list_cache.pop(key, None)
        return None

    def _set_list_cache(self, key: str, items: List[FileItem]) -> None:
        if self._list_cache_ttl <= 0:
            return
        with self._list_cache_lock:
            self._list_cache[key] = (time.time(), list(items))

    def _clear_list_cache(self) -> None:
        with self._list_cache_lock:
            self._list_cache.clear()

    def _upload_init(
        self,
        name: str,
        size: int,
        sha1: str,
        preid: str,
        target_cid: int,
        sign_key: Optional[str] = None,
        sign_val: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "name": name,
            "size": size,
            "sha1": sha1,
            "preid": preid,
            "target_cid": target_cid,
        }
        if sign_key:
            payload["sign_key"] = sign_key
        if sign_val:
            payload["sign_val"] = sign_val
        return self._api("/api/mp/storage/115/upload/init", payload)

    def _api(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = self._flowpan_url + path
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        response = requests.post(url, json=payload, headers=headers, timeout=60)
        try:
            data = response.json()
        except Exception:
            raise RuntimeError(f"Flowpan HTTP {response.status_code}: {response.text[:200]}")
        if response.status_code != 200 or data.get("code") != 200:
            raise RuntimeError(data.get("msg") or f"Flowpan HTTP {response.status_code}")
        return data.get("data") or {}

    def _target_cid(self, target_dir: FileItem, target_dir_path: str) -> Optional[int]:
        try:
            if getattr(target_dir, "fileid", None) not in (None, ""):
                return int(target_dir.fileid)
        except Exception:
            pass
        try:
            data = self._api("/api/mp/storage/115/dir/ensure", {"path": target_dir_path})
            return int(data.get("cid") or 0)
        except Exception as error:
            logger.error(f"【Flowpan存储】解析目标目录失败 {target_dir_path}: {error}")
            return None

    @staticmethod
    def _file_id(fileitem: FileItem) -> Optional[int]:
        try:
            value = getattr(fileitem, "fileid", None)
            if value not in (None, ""):
                parsed = int(value)
                return parsed if parsed > 0 else None
        except Exception:
            return None
        return None

    @staticmethod
    def _dir_path(fileitem: FileItem) -> str:
        value = str(getattr(fileitem, "path", "") or "/").replace("\\", "/")
        if not value.startswith("/"):
            value = "/" + value
        return value if value.endswith("/") else value + "/"

    @staticmethod
    def _normalize_dir_path(value: str) -> str:
        value = str(value or "/").replace("\\", "/").strip()
        if not value.startswith("/"):
            value = "/" + value
        cleaned = posixpath.normpath(value)
        if cleaned in ("", "."):
            cleaned = "/"
        return cleaned

    @staticmethod
    def _ensure_trailing_slash(value: str) -> str:
        return value if value.endswith("/") else value + "/"

    @staticmethod
    def _sha1_file_pair(
        path: Path,
        progress_callback: Optional[Callable[[float], None]] = None,
        base: float = 0,
        span: float = 100,
    ) -> tuple[str, str]:
        full_digest = hashlib.sha1()
        prefix_digest = hashlib.sha1()
        total = path.stat().st_size
        done = 0
        prefix_limit = 128 * 1024 * 1024
        prefix_done = 0
        next_report = 16 * 1024 * 1024
        with path.open("rb") as fileobj:
            for chunk in iter(lambda: fileobj.read(1024 * 1024), b""):
                full_digest.update(chunk)
                if prefix_done < prefix_limit:
                    prefix_remaining = prefix_limit - prefix_done
                    prefix_chunk = chunk[:prefix_remaining]
                    if prefix_chunk:
                        prefix_digest.update(prefix_chunk)
                        prefix_done += len(prefix_chunk)
                done += len(chunk)
                if progress_callback and (done >= next_report or done >= total):
                    progress_callback(FlowpanStorageAPI._scaled_progress(done, total, base, span))
                    next_report = done + 16 * 1024 * 1024
        return full_digest.hexdigest().upper(), prefix_digest.hexdigest().upper()

    @staticmethod
    def _sha1_range(path: Path, start: int, end: int) -> str:
        if end < start:
            raise ValueError(f"invalid range {start}-{end}")
        digest = hashlib.sha1()
        with path.open("rb") as fileobj:
            fileobj.seek(start)
            remaining = end - start + 1
            while remaining > 0:
                chunk = fileobj.read(min(1024 * 1024, remaining))
                if not chunk:
                    break
                digest.update(chunk)
                remaining -= len(chunk)
        if remaining:
            raise IOError(f"range read incomplete, remaining={remaining}")
        return digest.hexdigest().upper()

    @staticmethod
    def _scaled_progress(done: int, total: int, base: float, span: float) -> float:
        if total <= 0:
            return base + span
        return min(base + span, base + (done * span / total))

    @staticmethod
    def _upload_progress(uploaded: int, total: int) -> float:
        return FlowpanStorageAPI._scaled_progress(uploaded, total, 10, 89)

    def _file_item_from_session(
        self,
        target_path: str,
        target_name: str,
        size: int,
        session: Dict[str, Any],
    ) -> FileItem:
        return FileItem(
            storage=self._disk_name,
            fileid=str(session.get("file_id") or ""),
            parent_fileid=str(session.get("target_cid") or ""),
            name=target_name,
            basename=Path(target_name).stem,
            extension=Path(target_name).suffix[1:] or None,
            type="file",
            path=target_path,
            size=size,
            pickcode=session.get("pickcode") or None,
        )

    def _file_item_from_data(self, data: Dict[str, Any]) -> Optional[FileItem]:
        if not data or not data.get("file_id"):
            return None
        item_path = data.get("path") or "/"
        item_type = data.get("type") or "file"
        item_name = data.get("name") or Path(item_path).name
        if item_type == "dir":
            item_path = self._ensure_trailing_slash(item_path)
        return FileItem(
            storage=self._disk_name,
            fileid=str(data.get("file_id") or ""),
            parent_fileid=str(data.get("parent_id") or ""),
            name=item_name,
            basename=Path(item_name).stem if item_type == "file" else item_name,
            extension=(Path(item_name).suffix[1:] or None) if item_type == "file" else None,
            type=item_type,
            path=item_path,
            size=data.get("size") if item_type == "file" else None,
            pickcode=data.get("pickcode") or None,
        )

    def _state_key(self, local_path: Path, target_path: str, size: int, sha1: str) -> str:
        raw = f"{local_path.as_posix()}|{target_path}|{size}|{sha1}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    def _state_file(self, key: str) -> Path:
        return self._state_dir / f"{key}.json"

    def _load_state(self, key: str) -> Optional[Dict[str, Any]]:
        state_file = self._state_file(key)
        if not state_file.exists():
            return None
        try:
            with state_file.open("r", encoding="utf-8") as fileobj:
                data = json.load(fileobj)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def _save_state(self, key: str, session: Dict[str, Any]) -> None:
        try:
            self._state_dir.mkdir(parents=True, exist_ok=True)
            tmp = self._state_file(key).with_suffix(".tmp")
            with tmp.open("w", encoding="utf-8") as fileobj:
                json.dump(session, fileobj, ensure_ascii=False)
            tmp.replace(self._state_file(key))
        except Exception as error:
            logger.warning(f"【Flowpan存储】保存断点状态失败: {error}")

    def _drop_state(self, key: str) -> None:
        try:
            self._state_file(key).unlink(missing_ok=True)
        except Exception:
            pass
