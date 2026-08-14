"""Flowpan 115 存储桥接的 MoviePilot Agent 工具。"""

import json
from pathlib import Path
from typing import Any, List, Optional, Type
from typing import Literal

from pydantic import BaseModel, Field

from app.agent.tools.base import MoviePilotTool, run_agent_blocking
from app.agent.tools.tags import ToolTag
from app.core.plugin import PluginManager
from app.log import logger
from app.schemas import FileItem, StorageUsage


PLUGIN_ID = "FlowpanEventNotify"


def _tool_tags(*names: str) -> list[str]:
    """兼容不同 MoviePilot 版本的 ToolTag。

    MP 的 ToolTag 枚举在不同版本里并不完全一致，插件不能在 import 阶段
    直接引用可能不存在的 tag，否则会导致插件整体加载失败。
    """
    tags: list[str] = []
    for name in names:
        value = getattr(ToolTag, name, None)
        if value is not None:
            tags.append(value)
    return tags


def _to_plain(value: Any) -> Any:
    """递归转换为可序列化的基础类型。"""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, list):
        return [_to_plain(item) for item in value]
    if isinstance(value, tuple):
        return [_to_plain(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _to_plain(item) for key, item in value.items()}
    if hasattr(value, "model_dump"):
        return _to_plain(value.model_dump(mode="json", exclude_none=True))
    if hasattr(value, "__dict__"):
        try:
            return _to_plain(vars(value))
        except Exception:
            return str(value)
    return str(value)


def _dump(payload: Any) -> str:
    return json.dumps(_to_plain(payload), ensure_ascii=False, indent=2, default=str)


def _get_plugin():
    plugin = PluginManager().running_plugins.get(PLUGIN_ID)
    if not plugin:
        return None, "Flowpan 事件通知插件未运行，无法调用存储工具"
    if not getattr(plugin, "_storage_api", None):
        return None, "Flowpan 存储桥未启用或未初始化"
    return plugin, ""


def _storage_name(plugin: Any) -> str:
    return str(getattr(plugin, "_storage_name", "") or "Flowpan-115")


def _resolve_item(plugin: Any, path: str, strict: bool = False):
    path_obj = Path(path or "/")
    if strict and hasattr(plugin, "get_item_strict"):
        return plugin.get_item_strict(_storage_name(plugin), path_obj)
    return plugin.get_item(_storage_name(plugin), path_obj)


def _item_from_file_id(plugin: Any, file_id: Optional[int], path: str = "", name: str = "") -> Optional[FileItem]:
    if not file_id:
        return None
    item_name = (name or Path(path or "").name or str(file_id)).strip()
    return FileItem(
        storage=_storage_name(plugin),
        fileid=str(file_id),
        path=path or item_name,
        name=item_name,
        basename=Path(item_name).stem,
        extension=Path(item_name).suffix[1:] or None,
        type="file",
    )


def _resolve_item_or_id(
    plugin: Any,
    path: str = "",
    file_id: Optional[int] = None,
    strict: bool = False,
    name: str = "",
) -> Optional[FileItem]:
    if file_id:
        return _item_from_file_id(plugin, file_id, path=path, name=name)
    if path:
        return _resolve_item(plugin, path, strict=strict)
    return None


def _serialize_item(item: Optional[FileItem]) -> Optional[dict[str, Any]]:
    if item is None:
        return None
    if hasattr(item, "model_dump"):
        return item.model_dump(mode="json", exclude_none=True)
    return _to_plain(item)


def _normalize_extensions(extensions: Optional[List[str]]) -> set[str]:
    if not extensions:
        return set()
    normalized = set()
    for ext in extensions:
        value = str(ext or "").strip().lower().lstrip(".")
        if value:
            normalized.add(value)
    return normalized


class FlowpanStorageUsageInput(BaseModel):
    """查询存储容量的输入参数。"""


class FlowpanStorageUsageTool(MoviePilotTool):
    name: str = "flowpan_storage_usage"
    tags: list[str] = _tool_tags("Read", "Admin")
    description: str = "查询 Flowpan 115 存储桥的容量、可用空间和支持的传输类型。"
    require_admin: bool = True
    args_schema: Type[BaseModel] = FlowpanStorageUsageInput

    def get_tool_message(self, **kwargs) -> Optional[str]:
        return "查询 Flowpan 存储容量"

    async def run(self, **kwargs) -> str:
        plugin, error = _get_plugin()
        if not plugin:
            return _dump({"success": False, "message": error})

        def _sync():
            usage: StorageUsage = plugin.storage_usage(_storage_name(plugin))
            return {
                "success": True,
                "plugin_id": PLUGIN_ID,
                "storage_name": _storage_name(plugin),
                "storage_backend": str(getattr(plugin, "_storage_backend", "") or "cookie"),
                "bridge_enabled": bool(getattr(plugin, "_storage_bridge_enabled", False)),
                "storage_ready": bool(getattr(plugin, "_storage_api", None)),
                "usage": usage,
                "support_transtype": plugin.support_transtype(_storage_name(plugin)),
            }

        try:
            result = await run_agent_blocking("storage", _sync)
            logger.info("执行工具: %s", self.name)
            return _dump(result)
        except Exception as err:
            logger.error("查询 Flowpan 存储容量失败: %s", err, exc_info=True)
            return _dump({"success": False, "message": str(err)})


class FlowpanStorageListInput(BaseModel):
    """列出目录内容的输入参数。"""

    path: str = Field(default="/", description="要浏览的目录路径")
    recursive: bool = Field(default=False, description="是否递归列出子目录")
    extensions: Optional[List[str]] = Field(
        default=None,
        description="可选的后缀过滤，如 ['mkv', 'ass']，仅返回匹配文件",
    )
    limit: Optional[int] = Field(
        default=500,
        description="最多返回多少条结果，避免结果过大",
    )


class FlowpanStorageListTool(MoviePilotTool):
    name: str = "flowpan_storage_list"
    tags: list[str] = _tool_tags("Read", "Directory", "Admin")
    description: str = "浏览 Flowpan 115 存储目录，支持递归列出和后缀过滤。"
    require_admin: bool = True
    args_schema: Type[BaseModel] = FlowpanStorageListInput

    def get_tool_message(self, **kwargs) -> Optional[str]:
        path = kwargs.get("path", "/")
        return f"浏览 Flowpan 目录: {path}"

    async def run(self, path: str = "/", recursive: bool = False, extensions: Optional[List[str]] = None, limit: Optional[int] = 500, **kwargs) -> str:
        plugin, error = _get_plugin()
        if not plugin:
            return _dump({"success": False, "message": error})

        def _sync():
            folder = _resolve_item(plugin, path or "/", strict=True)
            if not folder or getattr(folder, "type", None) != "dir":
                return {
                    "success": False,
                    "message": f"目录不存在或不是目录: {path}",
                }
            items = plugin.list_files(folder, recursion=bool(recursive)) or []
            ext_filter = _normalize_extensions(extensions)
            if ext_filter:
                items = [
                    item
                    for item in items
                    if str(getattr(item, "extension", "") or "").lower().lstrip(".") in ext_filter
                ]
            max_items = int(limit or 0)
            if max_items > 0:
                items = items[:max_items]
            return {
                "success": True,
                "path": folder.path,
                "recursive": bool(recursive),
                "count": len(items),
                "items": [_serialize_item(item) for item in items],
            }

        try:
            result = await run_agent_blocking("storage", _sync)
            logger.info("执行工具: %s", self.name)
            return _dump(result)
        except Exception as err:
            logger.error("浏览 Flowpan 目录失败: %s", err, exc_info=True)
            return _dump({"success": False, "message": str(err)})


class FlowpanStorageItemInput(BaseModel):
    """查询单个文件或目录的输入参数。"""

    path: str = Field(..., description="要查询的绝对路径")
    mode: Literal["item", "strict", "parent"] = Field(
        default="item",
        description="item: 普通查询；strict: 严格查询；parent: 返回父目录",
    )


class FlowpanStorageItemTool(MoviePilotTool):
    name: str = "flowpan_storage_item"
    tags: list[str] = _tool_tags("Read", "File", "Directory", "Admin")
    description: str = "查询 Flowpan 115 存储中的单个文件/目录，或返回其父目录。"
    require_admin: bool = True
    args_schema: Type[BaseModel] = FlowpanStorageItemInput

    def get_tool_message(self, **kwargs) -> Optional[str]:
        path = kwargs.get("path", "")
        mode = kwargs.get("mode", "item")
        return f"查询 Flowpan 项目: {mode} {path}"

    async def run(self, path: str, mode: Literal["item", "strict", "parent"] = "item", **kwargs) -> str:
        plugin, error = _get_plugin()
        if not plugin:
            return _dump({"success": False, "message": error})

        def _sync():
            item = _resolve_item(plugin, path, strict=(mode == "strict"))
            if mode == "parent":
                if not item:
                    return {"success": False, "message": f"路径不存在: {path}"}
                parent = plugin.get_parent_item(item)
                return {
                    "success": bool(parent),
                    "path": path,
                    "item": _serialize_item(parent),
                }
            return {
                "success": bool(item),
                "path": path,
                "item": _serialize_item(item),
            }

        try:
            result = await run_agent_blocking("storage", _sync)
            logger.info("执行工具: %s", self.name)
            return _dump(result)
        except Exception as err:
            logger.error("查询 Flowpan 项目失败: %s", err, exc_info=True)
            return _dump({"success": False, "message": str(err)})


class FlowpanStorageCheckInput(BaseModel):
    """检查路径与文件匹配情况的输入参数。"""

    path: str = Field(..., description="要检查的绝对路径")
    extensions: Optional[List[str]] = Field(
        default=None,
        description="可选的后缀过滤，如 ['mkv', 'ass']，用于 any_files 检查",
    )


class FlowpanStorageCheckTool(MoviePilotTool):
    name: str = "flowpan_storage_check"
    tags: list[str] = _tool_tags("Read", "File", "Directory", "Admin")
    description: str = "检查 Flowpan 115 存储中的路径是否存在，以及目录中是否有匹配后缀的文件。"
    require_admin: bool = True
    args_schema: Type[BaseModel] = FlowpanStorageCheckInput

    def get_tool_message(self, **kwargs) -> Optional[str]:
        return f"检查 Flowpan 路径: {kwargs.get('path', '')}"

    async def run(self, path: str, extensions: Optional[List[str]] = None, **kwargs) -> str:
        plugin, error = _get_plugin()
        if not plugin:
            return _dump({"success": False, "message": error})

        def _sync():
            item = _resolve_item(plugin, path, strict=False)
            exists = bool(item) if item is not None else False
            any_files = None
            if item is not None:
                ext_filter = _normalize_extensions(extensions)
                if getattr(item, "type", None) == "file":
                    if not ext_filter:
                        any_files = True
                    else:
                        item_ext = str(getattr(item, "extension", "") or "").lower().lstrip(".")
                        any_files = item_ext in ext_filter
                else:
                    any_files = plugin.any_files(
                        item,
                        [f".{ext}" for ext in ext_filter] if ext_filter else None,
                    )
            return {
                "success": True,
                "path": path,
                "exists": exists,
                "any_files": any_files,
                "item": _serialize_item(item),
                "extensions": sorted(_normalize_extensions(extensions)),
            }

        try:
            result = await run_agent_blocking("storage", _sync)
            logger.info("执行工具: %s", self.name)
            return _dump(result)
        except Exception as err:
            logger.error("检查 Flowpan 路径失败: %s", err, exc_info=True)
            return _dump({"success": False, "message": str(err)})


class FlowpanStorageSearchInput(BaseModel):
    """搜索 115 文件的输入参数。"""

    keyword: str = Field(..., description="搜索关键词")
    path: str = Field(default="/", description="搜索范围目录，默认根目录")
    offset: int = Field(default=0, description="分页偏移")
    limit: int = Field(default=100, description="最多返回多少条，范围 1-200")


class FlowpanStorageSearchTool(MoviePilotTool):
    name: str = "flowpan_storage_search"
    tags: list[str] = _tool_tags("Read", "File", "Directory", "Admin")
    description: str = "搜索 Flowpan 115 存储中的文件和目录，返回可用于 rename/delete 的 file_id。"
    require_admin: bool = True
    args_schema: Type[BaseModel] = FlowpanStorageSearchInput

    def get_tool_message(self, **kwargs) -> Optional[str]:
        return f"搜索 Flowpan 115: {kwargs.get('keyword', '')}"

    async def run(self, keyword: str, path: str = "/", offset: int = 0, limit: int = 100, **kwargs) -> str:
        plugin, error = _get_plugin()
        if not plugin:
            return _dump({"success": False, "message": error})

        def _sync():
            keyword_value = (keyword or "").strip()
            if not keyword_value:
                return {"success": False, "message": "搜索关键词不能为空"}
            scope = _resolve_item(plugin, path or "/", strict=False)
            if scope and getattr(scope, "type", None) != "dir":
                scope = plugin.get_parent_item(scope)
            cid = 0
            if scope and getattr(scope, "fileid", None) not in (None, ""):
                try:
                    cid = int(scope.fileid)
                except Exception:
                    cid = 0
            result = plugin._storage_api.search(
                keyword=keyword_value,
                cid=cid,
                offset=max(int(offset or 0), 0),
                limit=max(1, min(int(limit or 100), 200)),
            )
            return {
                "success": True,
                "keyword": keyword_value,
                "path": path or "/",
                "cid": cid,
                "total": result.get("total"),
                "offset": result.get("offset"),
                "limit": result.get("limit"),
                "items": [_serialize_item(item) for item in result.get("items") or []],
            }

        try:
            result = await run_agent_blocking("storage", _sync)
            logger.info("执行工具: %s", self.name)
            return _dump(result)
        except Exception as err:
            logger.error("搜索 Flowpan 115 失败: %s", err, exc_info=True)
            return _dump({"success": False, "message": str(err)})


class FlowpanStorageFolderInput(BaseModel):
    """确保目录存在的输入参数。"""

    path: str = Field(..., description="要确保存在的绝对目录路径")


class FlowpanStorageFolderTool(MoviePilotTool):
    name: str = "flowpan_storage_folder"
    tags: list[str] = _tool_tags("Write", "Directory", "Admin")
    description: str = "按绝对路径确保 Flowpan 115 存储目录存在并返回目录信息。"
    require_admin: bool = True
    args_schema: Type[BaseModel] = FlowpanStorageFolderInput

    def get_tool_message(self, **kwargs) -> Optional[str]:
        return f"确保 Flowpan 目录: {kwargs.get('path', '')}"

    async def run(self, path: str, **kwargs) -> str:
        plugin, error = _get_plugin()
        if not plugin:
            return _dump({"success": False, "message": error})

        def _sync():
            folder = plugin.get_folder(_storage_name(plugin), Path(path or "/"))
            if not folder:
                return {"success": False, "message": f"目录确保失败: {path}"}
            return {"success": True, "path": path, "item": _serialize_item(folder)}

        try:
            result = await run_agent_blocking("storage", _sync)
            logger.info("执行工具: %s", self.name)
            return _dump(result)
        except Exception as err:
            logger.error("确保 Flowpan 目录失败: %s", err, exc_info=True)
            return _dump({"success": False, "message": str(err)})


class FlowpanStorageManageInput(BaseModel):
    """文件管理操作的输入参数。"""

    action: Literal["rename", "delete", "move", "copy", "upload"] = Field(
        ...,
        description="rename: 重命名；delete: 删除；move/copy: 移动或复制文件到指定目录；upload: 上传本地文件到指定目录",
    )
    path: str = Field(
        default="",
        description="rename/delete 时为源文件路径；move/copy/upload 时为目标目录路径。rename/delete/move/copy 可改用 file_id/file_ids",
    )
    file_id: Optional[int] = Field(
        default=None,
        description="115 文件 ID。rename/delete/move/copy 推荐优先传 file_id，避免路径重名或路径解析失败",
    )
    file_ids: Optional[List[int]] = Field(
        default=None,
        description="批量 115 文件 ID。与 file_id 二选一，用于 delete/move/copy 的批量操作",
    )
    name: Optional[str] = Field(
        default=None,
        description="rename 的新文件名；move/copy/upload 时作为可选的新文件名（批量时仅单文件有效）",
    )
    local_path: Optional[str] = Field(
        default=None,
        description="upload 时本地文件路径",
    )


class FlowpanStorageManageTool(MoviePilotTool):
    name: str = "flowpan_storage_manage"
    tags: list[str] = _tool_tags("Write", "File", "Directory", "Admin")
    description: str = "对 Flowpan 115 存储中的文件执行重命名、删除、复制、移动或上传。"
    require_admin: bool = True
    args_schema: Type[BaseModel] = FlowpanStorageManageInput

    def get_tool_message(self, **kwargs) -> Optional[str]:
        action = kwargs.get("action", "")
        path = kwargs.get("path", "")
        return f"Flowpan 存储操作: {action} {path}"

    async def run(
        self,
        action: Literal["rename", "delete", "move", "copy", "upload"],
        path: str = "",
        file_id: Optional[int] = None,
        file_ids: Optional[List[int]] = None,
        name: Optional[str] = None,
        local_path: Optional[str] = None,
        **kwargs,
    ) -> str:
        plugin, error = _get_plugin()
        if not plugin:
            return _dump({"success": False, "message": error})

        def _sync():
            batch_ids: List[int] = []
            seen_ids: set[int] = set()
            for candidate in list(file_ids or []):
                try:
                    value = int(candidate)
                except (TypeError, ValueError):
                    continue
                if value <= 0 or value in seen_ids:
                    continue
                seen_ids.add(value)
                batch_ids.append(value)

            if action == "rename":
                if batch_ids:
                    return {"success": False, "message": "rename 不支持批量 file_ids，请一次处理一个文件"}
                item = _resolve_item_or_id(plugin, path=path, file_id=file_id, strict=True, name=name or "")
                if not item:
                    return {"success": False, "message": "rename 需要提供有效 path 或 file_id"}
                new_name = (name or "").strip()
                if not new_name:
                    return {"success": False, "message": "rename 需要提供新文件名"}
                success = plugin.rename_file(item, new_name)
                return {
                    "success": bool(success),
                    "action": action,
                    "path": path,
                    "file_id": file_id,
                    "name": new_name,
                }

            if action == "move":
                target_path = (path or "").strip()
                if not target_path:
                    return {"success": False, "message": "move 需要提供目标目录路径（path 参数）"}
                if batch_ids:
                    if name:
                        return {"success": False, "message": "批量 move 不支持统一改名，请一次只传一个 file_id"}
                    items = [_item_from_file_id(plugin, file_id=value) for value in batch_ids]
                    items = [item for item in items if item]
                    if not items:
                        return {"success": False, "message": "move 需要提供有效源文件 file_id/file_ids"}
                    success = plugin.move_files(items, Path(target_path))
                    return {
                        "success": bool(success),
                        "action": action,
                        "target_path": target_path,
                        "file_ids": batch_ids,
                        "count": len(batch_ids),
                    }
                if not file_id:
                    return {"success": False, "message": "move 需要提供源文件 file_id"}
                target_name = (name or "").strip() or None
                item = _item_from_file_id(plugin, file_id=file_id)
                if not item:
                    return {"success": False, "message": "move 需要提供有效源文件 file_id"}
                success = plugin.move_file(item, Path(target_path), target_name)
                return {
                    "success": bool(success),
                    "action": action,
                    "target_path": target_path,
                    "file_id": file_id,
                    "name": target_name,
                }

            if action == "delete":
                if batch_ids:
                    items = [_item_from_file_id(plugin, file_id=value) for value in batch_ids]
                    items = [item for item in items if item]
                    if not items:
                        return {"success": False, "message": "delete 需要提供有效 file_id/file_ids"}
                    success = plugin.delete_files(items)
                    return {
                        "success": bool(success),
                        "action": action,
                        "file_ids": batch_ids,
                        "count": len(batch_ids),
                    }
                item = _resolve_item_or_id(plugin, path=path, file_id=file_id, strict=True)
                if not item:
                    return {"success": False, "message": "delete 需要提供有效 path 或 file_id"}
                success = plugin.delete_file(item)
                return {
                    "success": bool(success),
                    "action": action,
                    "path": path,
                    "file_id": file_id,
                }

            if action == "copy":
                target_path = (path or "").strip()
                if not target_path:
                    return {"success": False, "message": "copy 需要提供目标目录路径（path 参数）"}
                if batch_ids:
                    if name:
                        return {"success": False, "message": "批量 copy 不支持统一改名，请一次只传一个 file_id"}
                    items = [_item_from_file_id(plugin, file_id=value) for value in batch_ids]
                    items = [item for item in items if item]
                    if not items:
                        return {"success": False, "message": "copy 需要提供有效源文件 file_id/file_ids"}
                    success = plugin.copy_files(items, Path(target_path))
                    return {
                        "success": bool(success),
                        "action": action,
                        "target_path": target_path,
                        "file_ids": batch_ids,
                        "count": len(batch_ids),
                    }
                if not file_id:
                    return {"success": False, "message": "copy 需要提供源文件 file_id"}
                target_name = (name or "").strip() or None
                item = _item_from_file_id(plugin, file_id=file_id)
                if not item:
                    return {"success": False, "message": "copy 需要提供有效源文件 file_id"}
                success = plugin.copy_file(item, Path(target_path), target_name)
                return {
                    "success": bool(success),
                    "action": action,
                    "target_path": target_path,
                    "file_id": file_id,
                    "name": target_name,
                }

            if action == "upload":
                if not local_path:
                    return {"success": False, "message": "upload 需要提供本地文件路径"}
                folder = plugin.get_folder(_storage_name(plugin), Path(path or "/"))
                if not folder:
                    return {"success": False, "message": f"目标目录不可用: {path}"}
                uploaded = plugin.upload_file(folder, Path(local_path), new_name=name)
                return {
                    "success": bool(uploaded),
                    "action": action,
                    "path": path,
                    "local_path": local_path,
                    "item": _serialize_item(uploaded),
                }

            return {"success": False, "message": f"不支持的操作: {action}"}

        try:
            result = await run_agent_blocking("storage", _sync)
            logger.info("执行工具: %s", self.name)
            return _dump(result)
        except Exception as err:
            logger.error("Flowpan 存储操作失败: %s", err, exc_info=True)
            return _dump({"success": False, "message": str(err)})


__all__ = [
    "FlowpanStorageUsageTool",
    "FlowpanStorageListTool",
    "FlowpanStorageItemTool",
    "FlowpanStorageCheckTool",
    "FlowpanStorageSearchTool",
    "FlowpanStorageFolderTool",
    "FlowpanStorageManageTool",
]
