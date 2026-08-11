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

    action: Literal["rename", "delete", "upload"] = Field(
        ...,
        description="rename: 重命名；delete: 删除；upload: 上传本地文件到指定目录",
    )
    path: str = Field(..., description="目标文件路径，upload 时表示目标目录路径")
    name: Optional[str] = Field(
        default=None,
        description="rename 的新文件名，upload 时作为可选的新文件名",
    )
    local_path: Optional[str] = Field(
        default=None,
        description="upload 时本地文件路径",
    )


class FlowpanStorageManageTool(MoviePilotTool):
    name: str = "flowpan_storage_manage"
    tags: list[str] = _tool_tags("Write", "File", "Directory", "Admin")
    description: str = "对 Flowpan 115 存储中的文件执行重命名、删除或上传。"
    require_admin: bool = True
    args_schema: Type[BaseModel] = FlowpanStorageManageInput

    def get_tool_message(self, **kwargs) -> Optional[str]:
        action = kwargs.get("action", "")
        path = kwargs.get("path", "")
        return f"Flowpan 存储操作: {action} {path}"

    async def run(
        self,
        action: Literal["rename", "delete", "upload"],
        path: str,
        name: Optional[str] = None,
        local_path: Optional[str] = None,
        **kwargs,
    ) -> str:
        plugin, error = _get_plugin()
        if not plugin:
            return _dump({"success": False, "message": error})

        def _sync():
            if action == "rename":
                item = _resolve_item(plugin, path, strict=True)
                if not item:
                    return {"success": False, "message": f"路径不存在: {path}"}
                new_name = (name or "").strip()
                if not new_name:
                    return {"success": False, "message": "rename 需要提供新文件名"}
                success = plugin.rename_file(item, new_name)
                return {
                    "success": bool(success),
                    "action": action,
                    "path": path,
                    "name": new_name,
                }

            if action == "delete":
                item = _resolve_item(plugin, path, strict=True)
                if not item:
                    return {"success": False, "message": f"路径不存在: {path}"}
                success = plugin.delete_file(item)
                return {
                    "success": bool(success),
                    "action": action,
                    "path": path,
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
    "FlowpanStorageFolderTool",
    "FlowpanStorageManageTool",
]
