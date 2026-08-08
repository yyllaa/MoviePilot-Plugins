import hashlib
import json
import math
import os
import posixpath
from pathlib import Path
from typing import Any, Dict, List, Optional

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
        self.transtype = {}

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

        target_name = new_name or local_path.name
        target_dir_path = self._dir_path(target_dir)
        target_path = posixpath.join(target_dir_path.rstrip("/") or "/", target_name)
        file_size = local_path.stat().st_size
        file_sha1 = self._sha1_file(local_path)
        target_cid = self._target_cid(target_dir, target_dir_path)
        if target_cid is None:
            return None
        state_key = self._state_key(local_path, target_path, file_size, file_sha1)

        logger.info(f"【Flowpan存储】开始原生上传: {local_path} -> {target_path}")
        progress_callback = transfer_process(local_path.as_posix())
        try:
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
                    target_cid=target_cid,
                )
                if session.get("requires_sign"):
                    sign_val = self._sha1_range(
                        local_path,
                        int(session.get("range_start") or 0),
                        int(session.get("range_end") or 0),
                    )
                    session = self._upload_init(
                        name=target_name,
                        size=file_size,
                        sha1=file_sha1,
                        target_cid=target_cid,
                        sign_key=session.get("sign_key"),
                        sign_val=sign_val,
                    )

            if session.get("reused"):
                progress_callback(100)
                self._drop_state(state_key)
                logger.info(f"【Flowpan存储】{target_name} 秒传成功")
                return self._file_item_from_session(target_path, target_name, file_size, session)

            session["part_size"] = self._part_size
            if not session.get("upload_id"):
                session = self._api("/api/mp/storage/115/upload/start", session)
            else:
                session = self._api("/api/mp/storage/115/upload/list-parts", session)
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
                progress_callback(min(99, uploaded * 100 / file_size))

            with local_path.open("rb") as fileobj:
                for part_number in range(1, total_parts + 1):
                    offset = (part_number - 1) * part_size
                    chunk_size = min(part_size, file_size - offset)
                    if part_number in existing_parts:
                        continue
                    fileobj.seek(offset)
                    chunk = fileobj.read(chunk_size)
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
                    progress_callback(min(99, uploaded * 100 / file_size))

            session = self._api(
                "/api/mp/storage/115/upload/complete",
                {"session": session, "parts": sorted(parts, key=lambda p: p["part_number"])},
            )
            progress_callback(100)
            self._drop_state(state_key)
            logger.info(f"【Flowpan存储】{target_name} 上传完成")
            return self._file_item_from_session(target_path, target_name, file_size, session)
        except Exception as error:
            logger.error(f"【Flowpan存储】上传失败: {local_path} - {error}")
            return None

    def create_folder(self, fileitem: FileItem, name: str) -> Optional[FileItem]:
        parent = self._dir_path(fileitem)
        folder_path = posixpath.join(parent.rstrip("/") or "/", name.strip())
        try:
            data = self._api("/api/mp/storage/115/dir/ensure", {"path": folder_path})
            return FileItem(
                storage=self._disk_name,
                fileid=str(data.get("cid") or 0),
                path=self._ensure_trailing_slash(data.get("path") or folder_path),
                name=name,
                basename=name,
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

    def detail(self, fileitem: FileItem) -> Optional[FileItem]:
        return self.get_item(Path(fileitem.path))

    def exists(self, fileitem: FileItem) -> Optional[bool]:
        return self.get_item(Path(fileitem.path)) is not None

    def list(self, fileitem: FileItem) -> List[FileItem]:
        try:
            data = self._api(
                "/api/mp/storage/115/dir/list",
                {"path": getattr(fileitem, "path", "/") or "/", "storage": self._disk_name},
            )
            return [
                item
                for item in (self._file_item_from_data(raw) for raw in data.get("items") or [])
                if item is not None
            ]
        except Exception as error:
            logger.warning(f"【Flowpan存储】浏览目录失败 {getattr(fileitem, 'path', '/')}: {error}")
            return []

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
        logger.warning("【Flowpan存储】暂不支持从 MoviePilot 删除 115 文件")
        return False

    def rename(self, fileitem: FileItem, name: str) -> bool:
        logger.warning("【Flowpan存储】暂不支持从 MoviePilot 重命名 115 文件")
        return False

    def copy(self, fileitem: FileItem, path: Path, new_name: str) -> bool:
        return False

    def move(self, fileitem: FileItem, path: Path, new_name: str) -> bool:
        return False

    def download(self, fileitem: FileItem, path: Path = None) -> Optional[Path]:
        return None

    def storage_usage(self) -> StorageUsage:
        return StorageUsage(total=0, available=0)

    def support_transtype(self) -> dict:
        return {}

    def is_support_transtype(self, transtype: str) -> bool:
        return False

    def _upload_init(
        self,
        name: str,
        size: int,
        sha1: str,
        target_cid: int,
        sign_key: Optional[str] = None,
        sign_val: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "name": name,
            "size": size,
            "sha1": sha1,
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
    def _dir_path(fileitem: FileItem) -> str:
        value = str(getattr(fileitem, "path", "") or "/").replace("\\", "/")
        if not value.startswith("/"):
            value = "/" + value
        return value if value.endswith("/") else value + "/"

    @staticmethod
    def _ensure_trailing_slash(value: str) -> str:
        return value if value.endswith("/") else value + "/"

    @staticmethod
    def _sha1_file(path: Path) -> str:
        digest = hashlib.sha1()
        with path.open("rb") as fileobj:
            for chunk in iter(lambda: fileobj.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest().upper()

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
