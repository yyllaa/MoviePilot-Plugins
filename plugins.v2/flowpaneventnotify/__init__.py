import time
from threading import Lock, Thread, Timer
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Dict, List, Optional, Set, Tuple, Type

from app.core.event import Event, eventmanager
from app.log import logger
from app.helper.storage import StorageHelper
from app.plugins import _PluginBase
from app.schemas import FileItem, StorageOperSelectionEventData
from app.schemas.types import ChainEventType, EventType
from app.utils.http import RequestUtils

from .agent_tools import (
    FlowpanStorageCheckTool,
    FlowpanStorageFolderTool,
    FlowpanStorageItemTool,
    FlowpanStorageListTool,
    FlowpanStorageManageTool,
    FlowpanStorageRecycleTool,
    FlowpanStorageSearchTool,
    FlowpanStorageUsageTool,
    FlowpanStorageVideoHistoryTool,
)
from .flowpan_storage import FlowpanStorageAPI


DEFAULT_TARGET_STORAGES = "u115,115网盘Plus"
DEFAULT_QUIET_SECONDS = 180
DEFAULT_MAX_WAIT_SECONDS = 1800
DEFAULT_STORAGE_NAME = "Flowpan-115"
DEFAULT_STORAGE_BACKEND = "cookie"
DEFAULT_STORAGE_CACHE_TTL_SECONDS = 300
UPLOAD_NOTIFY_DEDUPE_SECONDS = 300


class FlowpanEventNotify(_PluginBase):
    """
    聚合 MoviePilot 的 115 转移完成事件并通知 Flowpan 执行事件增量同步。
    启用 Flowpan 原生存储桥后，上传成功时也会主动通知，避免秒传场景只依赖 MP 后续事件。
    """

    plugin_name = "Flowpan事件通知"
    plugin_desc = "聚合115转移完成事件并通知Flowpan更新，支持Cookie/OpenAPI链路选择"
    plugin_icon = (
        "https://raw.githubusercontent.com/jxxghp/MoviePilot-Frontend/"
        "refs/heads/v2/src/assets/images/misc/u115.png"
    )
    plugin_version = "1.1.26"
    plugin_author = "yyllaa"
    author_url = "https://github.com/yyllaa"
    plugin_config_prefix = "flowpaneventnotify_"
    plugin_order = 99
    auth_level = 1

    def __init__(self) -> None:
        """
        初始化批量通知状态
        """
        super().__init__()
        self._enabled = False
        self._flowpan_url = ""
        self._token = ""
        self._quiet_seconds = DEFAULT_QUIET_SECONDS
        self._max_wait_seconds = DEFAULT_MAX_WAIT_SECONDS
        self._target_storages: Set[str] = set()
        self._storage_bridge_enabled = False
        self._storage_name = DEFAULT_STORAGE_NAME
        self._storage_backend = DEFAULT_STORAGE_BACKEND
        self._storage_part_size_mb = 10
        self._storage_cache_ttl_seconds = DEFAULT_STORAGE_CACHE_TTL_SECONDS
        self._storage_api: Optional[FlowpanStorageAPI] = None
        self._lock = Lock()
        self._timer: Optional[Timer] = None
        self._batch_started_at = 0.0
        self._event_count = 0
        self._upload_notify_cache: Dict[str, float] = {}

    def init_plugin(self, config: Optional[Dict[str, Any]] = None) -> None:
        """
        读取配置并重置批量通知计时器

        :param config (dict): 插件配置
        """
        self.stop_service()
        defaults = self.get_form()[1]
        merged = {**defaults, **(config or {})}
        quiet_seconds = self._bounded_int(
            merged.get("quiet_seconds"), DEFAULT_QUIET_SECONDS, 30, 3600
        )
        max_wait_seconds = self._bounded_int(
            merged.get("max_wait_seconds"), DEFAULT_MAX_WAIT_SECONDS, 60, 21600
        )
        if max_wait_seconds < quiet_seconds:
            max_wait_seconds = quiet_seconds
        target_storages = self._parse_target_storages(
            merged.get("target_storages", DEFAULT_TARGET_STORAGES)
        )
        storage_name = str(merged.get("storage_name") or DEFAULT_STORAGE_NAME).strip()
        storage_backend = self._normalize_storage_backend(
            merged.get("storage_backend", DEFAULT_STORAGE_BACKEND)
        )
        storage_part_size_mb = self._bounded_int(
            merged.get("storage_part_size_mb"), 10, 5, 128
        )
        storage_cache_ttl_seconds = self._bounded_int(
            merged.get("storage_cache_ttl_seconds"),
            DEFAULT_STORAGE_CACHE_TTL_SECONDS,
            0,
            86400,
        )
        self._enabled = bool(merged.get("enabled"))
        self._flowpan_url = str(merged.get("flowpan_url") or "").strip()
        self._token = str(merged.get("token") or "").strip()
        self._quiet_seconds = quiet_seconds
        self._max_wait_seconds = max_wait_seconds
        self._target_storages = target_storages
        self._storage_bridge_enabled = bool(merged.get("storage_bridge_enabled"))
        self._storage_name = storage_name or DEFAULT_STORAGE_NAME
        self._storage_backend = storage_backend
        self._storage_part_size_mb = storage_part_size_mb
        self._storage_cache_ttl_seconds = storage_cache_ttl_seconds
        self._storage_api = None
        if self._storage_bridge_enabled and self._storage_name:
            target_storages.add(self._storage_name.casefold())
        elif self._storage_name:
            target_storages.discard(self._storage_name.casefold())
        if self._storage_bridge_enabled:
            try:
                storage_helper = StorageHelper()
                storages = storage_helper.get_storagies()
                storage_conf = self._mp_storage_conf()
                if any(item.type == self._storage_name for item in storages):
                    storage_helper.set_storage(self._storage_name, storage_conf)
                else:
                    storage_helper.add_storage(
                        storage=self._storage_name, name=self._storage_name, conf=storage_conf
                    )
                self._storage_api = FlowpanStorageAPI(
                    flowpan_url=self._flowpan_url,
                    token=self._token,
                    disk_name=self._storage_name,
                    storage_backend=self._storage_backend,
                    part_size_mb=self._storage_part_size_mb,
                    list_cache_ttl=self._storage_cache_ttl_seconds,
                )
                if not self._flowpan_url or not self._token:
                    logger.warning("【Flowpan事件通知】Flowpan 存储已注册；实际上传前请配置 Flowpan 地址和密钥")
            except Exception as error:
                logger.error(f"【Flowpan事件通知】注册 Flowpan 存储失败: {error}")
        normalized = {
            **merged,
            "quiet_seconds": quiet_seconds,
            "max_wait_seconds": max_wait_seconds,
            "target_storages": ",".join(sorted(target_storages)),
            "storage_name": self._storage_name,
            "storage_backend": self._storage_backend,
            "storage_part_size_mb": storage_part_size_mb,
            "storage_cache_ttl_seconds": storage_cache_ttl_seconds,
        }
        if config and normalized != config:
            self.update_config(normalized)
        if self._enabled and (not self._flowpan_url or not self._token):
            logger.warning("【Flowpan事件通知】请先配置 Flowpan 地址和事件通知密钥")

    def get_state(self) -> bool:
        """
        返回插件是否已启用

        :return bool: True 表示插件已启用
        """
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        返回插件命令列表，本插件无远程命令

        :return List: 空命令列表
        """
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """
        返回插件 API 列表

        :return List: 插件 API 列表
        """
        return [
            {
                "path": "/test_connection",
                "endpoint": self.test_connection,
                "auth": "bear",
                "methods": ["POST"],
                "summary": "连接测试",
                "description": "测试 Flowpan 115 存储桥连通性、鉴权与目录缓存状态",
            },
            {
                "path": "/clear_upload_states",
                "endpoint": self.clear_upload_states,
                "auth": "bear",
                "methods": ["POST"],
                "summary": "清理上传断点",
                "description": "清理当前 Flowpan 链路的上传断点状态",
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """
        返回插件服务列表，本插件无定时服务

        :return List: 空服务列表
        """
        return []

    def get_page(self) -> List[Dict[str, Any]]:
        """
        返回插件页面列表

        :return List: 插件页面列表
        """
        connection = self._build_connection_summary()
        cache = self._build_cache_summary()
        upload_state = self._build_upload_state_summary()
        cache_entries = cache.get("entries") or []
        upload_entries = upload_state.get("entries") or []
        backend_text = "OpenAPI" if self._storage_backend == "open" else "Cookie"
        cache_color = "success" if cache.get("enabled_text") == "启用" else "warning"
        metric_chips = [
            ("mdi-lan-connect", "存储桥", connection["bridge"], connection["tone"]),
            ("mdi-source-branch", "链路", backend_text, "primary"),
            ("mdi-folder-clock-outline", "当前链路缓存", cache["enabled_text"], cache_color),
            ("mdi-timer-outline", "TTL", f"{cache['ttl_seconds']} 秒", "secondary"),
            ("mdi-format-list-numbered", "缓存条数", str(cache["entry_count"]), "secondary"),
        ]
        cache_time_items = [
            ("最近缓存", cache["latest_text"]),
            ("最早缓存", cache["oldest_text"]),
            ("缓存策略", cache["time_summary"]),
        ]
        return [
            {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "mb-3"},
                "content": [
                    {
                        "component": "VCardText",
                        "props": {"class": "pa-4 pa-sm-5"},
                        "content": [
                            {
                                "component": "VRow",
                                "props": {"align": "center", "class": "mb-2"},
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "sm": 7},
                                        "content": [
                                            {
                                                "component": "div",
                                                "props": {"class": "text-subtitle-1 font-weight-bold"},
                                                "text": "连接测试",
                                            },
                                            {
                                                "component": "div",
                                                "props": {"class": "text-body-2 text-medium-emphasis mt-1"},
                                                "text": "验证 Flowpan 存储桥、鉴权和目录缓存状态。",
                                            },
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "sm": 5},
                                        "content": [
                                            {
                                                "component": "VBtn",
                                                "props": {
                                                    "color": "primary",
                                                    "variant": "elevated",
                                                    "prepend-icon": "mdi-lan-connect",
                                                    "block": True,
                                                    "size": "large",
                                                },
                                                "text": "连接测试",
                                                "events": {
                                                    "click": {
                                                        "api": "plugin/FlowpanEventNotify/test_connection",
                                                        "method": "post",
                                                    }
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VRow",
                                "props": {"dense": True, "class": "mb-3"},
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 6},
                                        "content": [
                                            {
                                                "component": "VAlert",
                                                "props": {
                                                    "type": connection["tone"],
                                                    "variant": "tonal",
                                                    "density": "compact",
                                                    "class": "h-100",
                                                },
                                                "text": connection["text"],
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 6},
                                        "content": [
                                            {
                                                "component": "VAlert",
                                                "props": {
                                                    "type": cache["tone"],
                                                    "variant": "tonal",
                                                    "density": "compact",
                                                    "class": "h-100",
                                                },
                                                "text": cache["summary"],
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "div",
                                "props": {"class": "d-flex flex-wrap ga-2 mb-4"},
                                "content": [
                                    {
                                        "component": "VChip",
                                        "props": {
                                            "color": color,
                                            "variant": "tonal",
                                            "size": "small",
                                            "prepend-icon": icon,
                                        },
                                        "text": f"{label}: {value}",
                                    }
                                    for icon, label, value, color in metric_chips
                                ],
                            },
                            {
                                "component": "div",
                                "props": {"class": "rounded border pa-3"},
                                "content": [
                                    {
                                        "component": "div",
                                        "props": {"class": "text-subtitle-2 mb-2"},
                                        "text": "缓存目录时间",
                                    },
                                    {
                                        "component": "VRow",
                                        "props": {"dense": True},
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "sm": 4},
                                                "content": [
                                                    {
                                                        "component": "div",
                                                        "props": {"class": "text-caption text-medium-emphasis"},
                                                        "text": label,
                                                    },
                                                    {
                                                        "component": "div",
                                                        "props": {"class": "text-body-2 font-weight-medium"},
                                                        "text": value,
                                                    },
                                                ],
                                            }
                                            for label, value in cache_time_items
                                        ],
                                    },
                                ],
                            },
                            *(
                                [
                                    {
                                        "component": "VDivider",
                                        "props": {"class": "my-3"},
                                    },
                                    {
                                        "component": "div",
                                        "props": {"class": "d-flex align-center justify-space-between mb-2"},
                                        "content": [
                                            {
                                                "component": "div",
                                                "props": {"class": "text-subtitle-2"},
                                                "text": "缓存目录明细",
                                            },
                                            {
                                                "component": "VChip",
                                                "props": {
                                                    "size": "x-small",
                                                    "variant": "tonal",
                                                    "color": "secondary",
                                                },
                                                "text": "最多 10 条",
                                            },
                                        ],
                                    },
                                ]
                                if cache_entries
                                else []
                            ),
                            *[
                                {
                                    "component": "div",
                                    "props": {"class": "rounded border pa-3 mb-2"},
                                    "content": [
                                        {
                                            "component": "div",
                                            "props": {"class": "text-body-2 font-weight-medium text-truncate mb-2"},
                                            "text": entry["path"],
                                        },
                                        {
                                            "component": "div",
                                            "props": {"class": "d-flex flex-wrap ga-2"},
                                            "content": [
                                                {
                                                    "component": "VChip",
                                                    "props": {"size": "x-small", "variant": "tonal"},
                                                    "text": f"{entry['item_count']} 项",
                                                },
                                                {
                                                    "component": "VChip",
                                                    "props": {"size": "x-small", "variant": "tonal"},
                                                    "text": f"已缓存 {entry['age_text']}",
                                                },
                                                {
                                                    "component": "VChip",
                                                    "props": {"size": "x-small", "variant": "tonal"},
                                                    "text": f"剩余 {entry['remaining_text']}",
                                                },
                                            ],
                                        },
                                    ],
                                }
                                for entry in cache_entries[:10]
                            ],
                            {
                                "component": "VDivider",
                                "props": {"class": "my-4"},
                            },
                            {
                                "component": "VRow",
                                "props": {"align": "center", "class": "mb-2"},
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "sm": 7},
                                        "content": [
                                            {
                                                "component": "div",
                                                "props": {"class": "text-subtitle-2 font-weight-bold"},
                                                "text": "上传断点",
                                            },
                                            {
                                                "component": "div",
                                                "props": {"class": "text-body-2 text-medium-emphasis mt-1"},
                                                "text": upload_state["summary"],
                                            },
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "sm": 5},
                                        "content": [
                                            {
                                                "component": "VBtn",
                                                "props": {
                                                    "color": "warning",
                                                    "variant": "tonal",
                                                    "prepend-icon": "mdi-broom",
                                                    "block": True,
                                                    "disabled": upload_state["entry_count"] <= 0,
                                                },
                                                "text": "清理当前链路断点",
                                                "events": {
                                                    "click": {
                                                        "api": "plugin/FlowpanEventNotify/clear_upload_states",
                                                        "method": "post",
                                                    }
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "div",
                                "props": {"class": "d-flex flex-wrap ga-2 mb-3"},
                                "content": [
                                    {
                                        "component": "VChip",
                                        "props": {
                                            "color": upload_state["tone"],
                                            "variant": "tonal",
                                            "size": "small",
                                            "prepend-icon": "mdi-cloud-upload-outline",
                                        },
                                        "text": f"断点数: {upload_state['entry_count']}",
                                    },
                                    {
                                        "component": "VChip",
                                        "props": {
                                            "color": "secondary",
                                            "variant": "tonal",
                                            "size": "small",
                                            "prepend-icon": "mdi-progress-upload",
                                        },
                                        "text": f"已传: {upload_state['uploaded_text']} / {upload_state['size_text']}",
                                    },
                                    {
                                        "component": "VChip",
                                        "props": {
                                            "color": "secondary",
                                            "variant": "tonal",
                                            "size": "small",
                                            "prepend-icon": "mdi-folder-cog-outline",
                                        },
                                        "text": f"全部状态: {upload_state['all_entry_count']}",
                                    },
                                ],
                            },
                            *(
                                [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "error",
                                            "variant": "tonal",
                                            "density": "compact",
                                            "class": "mb-3",
                                        },
                                        "text": (
                                            f"最近上传错误：{upload_state['last_error']['category']} - "
                                            f"{upload_state['last_error']['message']}"
                                        ),
                                    }
                                ]
                                if upload_state.get("last_error")
                                else []
                            ),
                            *[
                                {
                                    "component": "div",
                                    "props": {"class": "rounded border pa-3 mb-2"},
                                    "content": [
                                        {
                                            "component": "div",
                                            "props": {"class": "text-body-2 font-weight-medium text-truncate mb-2"},
                                            "text": entry["name"] or entry["target_path"] or entry["local_path"],
                                        },
                                        {
                                            "component": "div",
                                            "props": {"class": "text-caption text-medium-emphasis text-truncate mb-2"},
                                            "text": entry["target_path"] or entry["local_path"],
                                        },
                                        {
                                            "component": "div",
                                            "props": {"class": "d-flex flex-wrap ga-2"},
                                            "content": [
                                                {
                                                    "component": "VChip",
                                                    "props": {"size": "x-small", "variant": "tonal"},
                                                    "text": f"{entry['percent']}%",
                                                },
                                                {
                                                    "component": "VChip",
                                                    "props": {"size": "x-small", "variant": "tonal"},
                                                    "text": f"{entry['part_count']} 分片",
                                                },
                                                {
                                                    "component": "VChip",
                                                    "props": {"size": "x-small", "variant": "tonal"},
                                                    "text": f"更新 {entry['updated_text']}",
                                                },
                                            ],
                                        },
                                    ],
                                }
                                for entry in upload_entries[:5]
                            ],
                        ],
                    }
                ],
            }
        ]

    def get_module(self) -> Dict[str, Any]:
        """
        让 MoviePilot 文件管理器可以调用 Flowpan 115 存储桥接。
        """
        if not self._storage_api:
            return {}
        return {
            "list_files": self.list_files,
            "search_files": self.search_files,
            "any_files": self.any_files,
            "download_file": self.download_file,
            "upload_file": self.upload_file,
            "delete_file": self.delete_file,
            "delete_files": self.delete_files,
            "rename_file": self.rename_file,
            "copy_file": self.copy_file,
            "move_file": self.move_file,
            "copy_files": self.copy_files,
            "move_files": self.move_files,
            "get_file_item": self.get_file_item,
            "get_parent_item": self.get_parent_item,
            "get_item_strict": self.get_item_strict,
            "storage_usage": self.storage_usage,
            "support_transtype": self.support_transtype,
            "create_folder": self.create_folder,
            "get_folder": self.get_folder,
            "exists": self.exists,
            "get_item": self.get_item,
        }

    def get_agent_tools(self) -> List[Type]:
        """
        让 MoviePilot 智能体可以直接调用 Flowpan 存储桥接能力。
        """
        if not self._storage_api:
            return []
        return [
            FlowpanStorageUsageTool,
            FlowpanStorageListTool,
            FlowpanStorageItemTool,
            FlowpanStorageCheckTool,
            FlowpanStorageSearchTool,
            FlowpanStorageFolderTool,
            FlowpanStorageManageTool,
            FlowpanStorageRecycleTool,
            FlowpanStorageVideoHistoryTool,
        ]

    def get_form(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        返回插件配置表单和默认配置

        :return Tuple: 配置表单和默认配置
        """
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "flowpan_url",
                                            "label": "Flowpan 地址或通知地址",
                                            "placeholder": "http://flowpan:8080",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "token",
                                            "label": "事件通知密钥",
                                            "type": "password",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "quiet_seconds",
                                            "label": "静默等待（秒）",
                                            "type": "number",
                                            "min": 30,
                                            "hint": "最后一个完成事件后等待，默认 180 秒",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_wait_seconds",
                                            "label": "最长合并（秒）",
                                            "type": "number",
                                            "min": 60,
                                            "hint": "持续上传时最多等待，默认 1800 秒",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "storage_backend",
                                            "label": "Flowpan 链路",
                                            "items": [
                                                {"title": "Cookie", "value": "cookie"},
                                                {"title": "OpenAPI", "value": "open"},
                                            ],
                                            "hint": "OpenAPI 需要 Flowpan 已登录并启用 Open",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "target_storages",
                                            "label": "目标存储",
                                            "hint": "多个存储用逗号分隔",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "storage_bridge_enabled",
                                            "label": "启用 Flowpan 115 存储",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "storage_name",
                                            "label": "MP 存储名称",
                                            "hint": "创建后在 MoviePilot 存储选择里使用",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "storage_part_size_mb",
                                            "label": "上传分片 MB",
                                            "type": "number",
                                            "min": 5,
                                            "max": 128,
                                            "hint": "默认 10MB，文件直接 PUT 到 115 OSS",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "storage_cache_ttl_seconds",
                                            "label": "目录缓存 TTL（秒）",
                                            "type": "number",
                                            "min": 0,
                                            "max": 86400,
                                            "hint": "0 表示关闭目录缓存，默认 300 秒",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "text": (
                                "插件只聚合转移完成事件。静默期内有新事件会重新计时，"
                                "达到最长合并时间后强制通知一次；Flowpan 定时增量仍作为兜底。"
                            ),
                        },
                    },
                ],
            }
        ], {
            "enabled": False,
            "flowpan_url": "",
            "token": "",
            "quiet_seconds": DEFAULT_QUIET_SECONDS,
            "max_wait_seconds": DEFAULT_MAX_WAIT_SECONDS,
            "target_storages": DEFAULT_TARGET_STORAGES,
            "storage_bridge_enabled": False,
            "storage_name": DEFAULT_STORAGE_NAME,
            "storage_backend": DEFAULT_STORAGE_BACKEND,
            "storage_part_size_mb": 10,
            "storage_cache_ttl_seconds": DEFAULT_STORAGE_CACHE_TTL_SECONDS,
        }

    def test_connection(self) -> Dict[str, Any]:
        """
        测试 Flowpan 存储桥连通性。
        """
        try:
            if not self._storage_bridge_enabled:
                return {
                    "code": 1,
                    "msg": "请先启用 Flowpan 115 存储桥",
                }
            if not self._storage_api:
                return {
                    "code": 1,
                    "msg": "存储桥未初始化，请检查地址和密钥",
                }
            usage = self._storage_api.probe_connection()
            cache = self._storage_api.list_cache_stats()
            total = self._format_size_text(usage.get("total"))
            available = self._format_size_text(usage.get("available"))
            cache_state = "启用" if cache.get("enabled") else "关闭"
            cache_count = int(cache.get("entry_count") or 0)
            cache_ttl = int(cache.get("ttl_seconds") or 0)
            logger.info(
                "【Flowpan事件通知】连接测试成功，缓存条数=%d，TTL=%d 秒",
                cache_count,
                cache_ttl,
            )
            return {
                "code": 0,
                "msg": (
                    f"连接测试成功：链路 {self._storage_backend}，容量 {total}，可用 {available}；"
                    f"目录缓存{cache_state}，TTL {cache_ttl} 秒，当前 {cache_count} 条"
                ),
                "data": {
                    "backend": self._storage_backend,
                    "usage": usage,
                    "cache": cache,
                },
            }
        except Exception as error:
            logger.error(f"【Flowpan事件通知】连接测试失败: {error}", exc_info=True)
            return {
                "code": 1,
                "msg": f"连接测试失败: {error}",
            }

    def clear_upload_states(self) -> Dict[str, Any]:
        """
        清理当前链路上传断点。
        """
        if not self._storage_api:
            return {
                "code": 1,
                "msg": "存储桥未初始化，无法清理上传断点",
            }
        try:
            removed = self._storage_api.clear_upload_states(current_backend_only=True)
            return {
                "code": 0,
                "msg": f"已清理当前链路上传断点 {removed} 条",
                "data": {"removed": removed, "backend": self._storage_backend},
            }
        except Exception as error:
            logger.error(f"【Flowpan事件通知】清理上传断点失败: {error}", exc_info=True)
            return {
                "code": 1,
                "msg": f"清理上传断点失败: {error}",
            }

    @eventmanager.register(ChainEventType.StorageOperSelection)
    def storage_oper_selection(self, event: Event) -> None:
        """
        MP 选择 Flowpan 存储时，把操作对象切到桥接适配器。
        """
        if not self._storage_bridge_enabled or not self._storage_api:
            return
        event_data: StorageOperSelectionEventData = event.event_data
        if event_data.storage == self._storage_name:
            event_data.storage_oper = self._storage_api  # noqa

    def list_files(self, fileitem: FileItem, recursion: bool = False):
        if not self._storage_item(fileitem):
            return None
        if recursion:
            return self._storage_api.iter_files(fileitem)
        return self._storage_api.list(fileitem)

    def search_files(self, storage: str, keyword: str, path: str = "/", offset: int = 0, limit: int = 100):
        if storage != self._storage_name or not self._storage_api:
            return None
        try:
            scope = self._storage_api.get_item(Path(path or "/"))
            cid = int(scope.fileid) if scope and getattr(scope, "fileid", None) not in (None, "") else 0
        except Exception:
            cid = 0
        return self._storage_api.search(keyword=keyword, cid=cid, offset=offset, limit=limit)

    def any_files(self, fileitem: FileItem, extensions: list = None):
        if not self._storage_item(fileitem):
            return None
        return self._storage_api.any_files(fileitem, extensions)

    def create_folder(self, fileitem: FileItem, name: str):
        if not self._storage_item(fileitem):
            return None
        return self._storage_api.create_folder(fileitem, name)

    def get_folder(self, storage: str, path: Path):
        if storage != self._storage_name or not self._storage_api:
            return None
        return self._storage_api.get_folder(path)

    def upload_file(self, fileitem: FileItem, path: Path, new_name: Optional[str] = None):
        if not self._storage_item(fileitem):
            return None
        uploaded_item = self._storage_api.upload(fileitem, path, new_name)
        if uploaded_item:
            self._notify_after_storage_upload(uploaded_item)
        return uploaded_item

    def download_file(self, fileitem: FileItem, path: Path = None):
        if not self._storage_item(fileitem):
            return None
        return self._storage_api.download(fileitem, path)

    def delete_file(self, fileitem: FileItem):
        if not self._storage_item(fileitem):
            return None
        return self._storage_api.delete(fileitem)

    def delete_files(self, fileitems: List[FileItem]):
        if not fileitems:
            return None
        valid_items = [item for item in fileitems if self._storage_item(item)]
        if not valid_items:
            return None
        return self._storage_api.delete_many(valid_items)

    def rename_file(self, fileitem: FileItem, name: str):
        if not self._storage_item(fileitem):
            return None
        return self._storage_api.rename(fileitem, name)

    def copy_file(self, fileitem: FileItem, path: Path, new_name: Optional[str] = None):
        if not self._storage_item(fileitem):
            return None
        return self._storage_api.copy(fileitem, path, new_name)

    def move_file(self, fileitem: FileItem, path: Path, new_name: Optional[str] = None):
        if not self._storage_item(fileitem):
            return None
        return self._storage_api.move(fileitem, path, new_name)

    def copy_files(self, fileitems: List[FileItem], path: Path):
        if not fileitems:
            return None
        valid_items = [item for item in fileitems if self._storage_item(item)]
        if not valid_items:
            return None
        return self._storage_api.copy_many(valid_items, path)

    def move_files(self, fileitems: List[FileItem], path: Path):
        if not fileitems:
            return None
        valid_items = [item for item in fileitems if self._storage_item(item)]
        if not valid_items:
            return None
        return self._storage_api.move_many(valid_items, path)

    def get_file_item(self, storage: str, path: Path):
        if storage != self._storage_name or not self._storage_api:
            return None
        return self._storage_api.get_item(path)

    def get_item(self, storage: str, path: Path):
        return self.get_file_item(storage, path)

    def get_item_strict(self, storage: str, path: Path):
        if storage != self._storage_name or not self._storage_api:
            return None
        return self._storage_api.get_item_strict(path)

    def get_parent_item(self, fileitem: FileItem):
        if not self._storage_item(fileitem):
            return None
        parent = Path(str(fileitem.path).rstrip("/")).parent
        return self._storage_api.get_item(parent)

    def exists(self, fileitem: FileItem):
        if not self._storage_item(fileitem):
            return None
        return self._storage_api.exists(fileitem)

    def storage_usage(self, storage: str = ""):
        if storage and storage != self._storage_name:
            return None
        if not self._storage_api:
            return None
        return self._storage_api.storage_usage()

    def support_transtype(self, storage: str = ""):
        if storage and storage != self._storage_name:
            return None
        if not self._storage_api:
            return None
        return self._storage_api.support_transtype()

    def recycle_preview(self, days: int = 0, account: str = ""):
        if not self._storage_api:
            return None
        return self._storage_api.recycle_preview(days=days, account=account)

    def recycle_clean(self, days: int = 0, confirm: str = "", password: str = "", account: str = ""):
        if not self._storage_api:
            return None
        return self._storage_api.recycle_clean(
            days=days,
            confirm=confirm,
            password=password,
            account=account,
        )

    def recycle_revert(self, ids: List[str], account: str = ""):
        if not self._storage_api:
            return None
        return self._storage_api.recycle_revert(ids=ids, account=account)

    def video_history(self, pickcode: str):
        if not self._storage_api:
            return None
        return self._storage_api.video_history(pickcode)

    def video_save_history(
        self,
        pickcode: str,
        time_value: int = 0,
        watch_end: int = 0,
        definition: int = 0,
        category: int = 0,
        share_id: str = "",
    ):
        if not self._storage_api:
            return None
        return self._storage_api.video_save_history(
            pickcode=pickcode,
            time_value=time_value,
            watch_end=watch_end,
            definition=definition,
            category=category,
            share_id=share_id,
        )

    def _build_connection_summary(self) -> Dict[str, str]:
        if not self._storage_bridge_enabled:
            return {
                "tone": "warning",
                "text": "存储桥未启用，无法进行连接测试。",
                "bridge": "关闭",
            }
        if not self._storage_api:
            return {
                "tone": "warning",
                "text": "存储桥已启用，但尚未完成初始化，请检查地址、密钥和存储名称。",
                "bridge": "未初始化",
            }
        if not self._flowpan_url or not self._token:
            return {
                "tone": "warning",
                "text": "Flowpan 地址或事件密钥未配置完整。",
                "bridge": "配置不完整",
            }
        return {
            "tone": "info",
            "text": "Flowpan 存储桥已配置，点击连接测试验证连通性。",
            "bridge": "待测试",
        }

    def _build_cache_summary(self) -> Dict[str, Any]:
        if not self._storage_api:
            return {
                "tone": "warning",
                "summary": "目录缓存不可用：存储桥未初始化",
                "enabled_text": "关闭",
                "ttl_seconds": self._storage_cache_ttl_seconds,
                "entry_count": 0,
                "latest_text": "无",
                "oldest_text": "无",
                "time_summary": f"设置 TTL：{self._storage_cache_ttl_seconds} 秒",
                "entries": [],
            }
        stats = self._storage_api.list_cache_stats()
        ttl_seconds = int(stats.get("ttl_seconds") or self._storage_cache_ttl_seconds)
        entry_count = int(stats.get("entry_count") or 0)
        latest_cached_at = int(stats.get("latest_cached_at") or 0)
        oldest_cached_at = int(stats.get("oldest_cached_at") or 0)
        enabled = bool(stats.get("enabled"))
        entries: List[Dict[str, Any]] = []
        for item in stats.get("entries") or []:
            cached_at = int(item.get("cached_at") or 0)
            age_seconds = int(item.get("age_seconds") or 0)
            remaining_seconds = int(item.get("remaining_seconds") or 0)
            entries.append(
                {
                    "path": self._cache_entry_path(item.get("key") or ""),
                    "item_count": int(item.get("item_count") or 0),
                    "age_text": self._seconds_text(age_seconds),
                    "remaining_text": self._seconds_text(remaining_seconds),
                    "cached_at": cached_at,
                }
            )
        return {
            "tone": "success" if enabled else "warning",
            "summary": (
                f"当前链路目录缓存{'已启用' if enabled else '已关闭'}，"
                f"TTL {ttl_seconds} 秒，当前 {entry_count} 条"
            ),
            "enabled_text": "启用" if enabled else "关闭",
            "ttl_seconds": ttl_seconds,
            "entry_count": entry_count,
            "latest_text": self._format_ts(latest_cached_at),
            "oldest_text": self._format_ts(oldest_cached_at),
            "time_summary": f"设置 TTL：{ttl_seconds} 秒",
            "entries": entries,
        }

    def _build_upload_state_summary(self) -> Dict[str, Any]:
        if not self._storage_api:
            return {
                "tone": "warning",
                "summary": "存储桥未初始化，无法读取上传断点。",
                "entry_count": 0,
                "all_entry_count": 0,
                "uploaded_text": "0 B",
                "size_text": "0 B",
                "last_error": {},
                "entries": [],
            }
        stats = self._storage_api.upload_state_stats()
        entry_count = int(stats.get("entry_count") or 0)
        all_entry_count = int(stats.get("all_entry_count") or 0)
        uploaded = int(stats.get("uploaded") or 0)
        size = int(stats.get("size") or 0)
        last_error = dict(stats.get("last_error") or {})
        if last_error:
            last_error["at_text"] = self._format_ts(int(last_error.get("at") or 0))
        entries: List[Dict[str, Any]] = []
        for item in stats.get("entries") or []:
            updated_at = int(item.get("updated_at") or 0)
            entries.append(
                {
                    "name": item.get("name") or "",
                    "target_path": item.get("target_path") or "",
                    "local_path": item.get("local_path") or "",
                    "percent": item.get("percent") or 0,
                    "part_count": int(item.get("part_count") or 0),
                    "updated_text": self._format_ts(updated_at),
                }
            )
        return {
            "tone": "warning" if entry_count else "success",
            "summary": (
                f"当前 {self._storage_backend} 链路保留 {entry_count} 条断点，"
                f"已记录 {self._format_size_text(uploaded)} / {self._format_size_text(size)}。"
            ),
            "entry_count": entry_count,
            "all_entry_count": all_entry_count,
            "uploaded_text": self._format_size_text(uploaded),
            "size_text": self._format_size_text(size),
            "last_error": last_error,
            "entries": entries,
        }

    @staticmethod
    def _format_ts(timestamp: int) -> str:
        if not timestamp:
            return "无"
        try:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
        except Exception:
            return str(timestamp)

    @staticmethod
    def _seconds_text(value: int) -> str:
        seconds = max(0, int(value or 0))
        if seconds < 60:
            return f"{seconds} 秒"
        minutes, sec = divmod(seconds, 60)
        if minutes < 60:
            return f"{minutes} 分 {sec} 秒"
        hours, minutes = divmod(minutes, 60)
        return f"{hours} 小时 {minutes} 分"

    @staticmethod
    def _cache_entry_path(cache_key: str) -> str:
        if not cache_key:
            return "/"
        if ":cid:" in cache_key:
            return f"[CID] {cache_key.rsplit(':cid:', 1)[-1]}"
        if ":path:" in cache_key:
            return cache_key.split(":path:", 1)[-1]
        if ":" in cache_key:
            return cache_key.split(":", 1)[-1]
        return cache_key

    @staticmethod
    def _format_size_text(raw: Any) -> str:
        try:
            size = float(raw or 0)
        except (TypeError, ValueError):
            return "0 B"
        units = ["B", "KB", "MB", "GB", "TB", "PB"]
        index = 0
        while size >= 1024 and index < len(units) - 1:
            size /= 1024.0
            index += 1
        if index == 0:
            return f"{int(size)} {units[index]}"
        return f"{size:.2f} {units[index]}"

    def _storage_item(self, fileitem: FileItem) -> bool:
        return bool(
            self._storage_api
            and fileitem is not None
            and getattr(fileitem, "storage", None) == self._storage_name
        )

    @eventmanager.register(
        [
            EventType.TransferComplete,
            EventType.AudioTransferComplete,
            EventType.SubtitleTransferComplete,
        ]
    )
    def on_transfer_complete(self, event: Event) -> None:
        """
        接收 115 转移完成事件并加入待通知批次

        :param event (Event): MoviePilot 事件
        """
        if not self._enabled or not self._flowpan_url or not self._token:
            return
        target_storage = self._event_target_storage(event)
        if target_storage.casefold() not in self._target_storages:
            return
        target_path = self._event_target_path(event)
        if self._was_storage_upload_notified(target_storage, target_path):
            logger.info(
                "【Flowpan事件通知】跳过重复完成事件，原生存储上传已通知: %s",
                target_path or target_storage,
            )
            return
        now = monotonic()
        flush_count = 0
        with self._lock:
            if self._event_count == 0:
                self._batch_started_at = now
            self._event_count += 1
            deadline = min(
                now + self._quiet_seconds,
                self._batch_started_at + self._max_wait_seconds,
            )
            if deadline <= now:
                if self._timer is not None:
                    self._timer.cancel()
                    self._timer = None
                flush_count = self._event_count
                self._event_count = 0
                self._batch_started_at = 0.0
            else:
                if self._timer is not None:
                    self._timer.cancel()
                self._timer = Timer(deadline - now, self._flush_batch)
                self._timer.daemon = True
                self._timer.start()
            event_count = flush_count or self._event_count
        if flush_count:
            logger.info(
                "【Flowpan事件通知】达到最长合并时间，发送 %d 个完成事件",
                flush_count,
            )
            worker = Thread(target=self._notify_flowpan, args=(flush_count,))
            worker.daemon = True
            worker.start()
            return
        logger.info(
            "【Flowpan事件通知】已聚合 %d 个完成事件，等待后续事件",
            event_count,
        )

    def stop_service(self) -> None:
        """
        停止插件并取消尚未发送的批量通知
        """
        self._enabled = False
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            self._batch_started_at = 0.0
            self._event_count = 0

    def _flush_batch(self) -> None:
        with self._lock:
            self._timer = None
            event_count = self._event_count
            self._event_count = 0
            self._batch_started_at = 0.0
        if event_count <= 0 or not self._enabled:
            return
        self._notify_flowpan(event_count)

    def _notify_flowpan(self, event_count: int) -> None:
        notify_url = self._notify_url(self._flowpan_url)
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        payload = {"source": "moviepilot", "events": event_count}
        for attempt, delay in enumerate((0, 5, 15), start=1):
            if delay:
                sleep(delay)
            response = None
            try:
                response = RequestUtils(headers=headers, timeout=10).post_res(
                    url=notify_url,
                    json=payload,
                )
                status_code = response.status_code if response is not None else 0
                if status_code in {200, 202}:
                    logger.info(
                        "【Flowpan事件通知】已通知 Flowpan，本批共 %d 个完成事件",
                        event_count,
                    )
                    return
                logger.warning(
                    "【Flowpan事件通知】第 %d 次通知失败，HTTP %d",
                    attempt,
                    status_code,
                )
                if status_code in {400, 401, 403, 404}:
                    break
            except Exception as error:
                logger.warning(
                    "【Flowpan事件通知】第 %d 次通知异常: %s",
                    attempt,
                    error,
                )
            finally:
                if response is not None:
                    response.close()
        logger.error(
            "【Flowpan事件通知】通知失败，本批 %d 个事件将由 Flowpan 定时增量兜底",
            event_count,
        )

    def _notify_after_storage_upload(self, uploaded_item: FileItem) -> None:
        if not self._enabled or not self._flowpan_url or not self._token:
            return
        storage = str(getattr(uploaded_item, "storage", "") or self._storage_name).strip()
        path = str(getattr(uploaded_item, "path", "") or "").strip()
        key = self._upload_notify_key(storage, path)
        now = monotonic()
        with self._lock:
            self._prune_upload_notify_cache(now)
            if key and now - self._upload_notify_cache.get(key, 0) < UPLOAD_NOTIFY_DEDUPE_SECONDS:
                return
            if key:
                self._upload_notify_cache[key] = now
        logger.info("【Flowpan事件通知】原生存储上传完成，立即通知 Flowpan 增量: %s", path or storage)
        worker = Thread(target=self._notify_flowpan, args=(1,))
        worker.daemon = True
        worker.start()

    def _was_storage_upload_notified(self, storage: str, path: str) -> bool:
        key = self._upload_notify_key(storage, path)
        if not key:
            return False
        now = monotonic()
        with self._lock:
            self._prune_upload_notify_cache(now)
            return now - self._upload_notify_cache.get(key, 0) < UPLOAD_NOTIFY_DEDUPE_SECONDS

    def _prune_upload_notify_cache(self, now: float) -> None:
        expired = [
            key
            for key, notified_at in self._upload_notify_cache.items()
            if now - notified_at >= UPLOAD_NOTIFY_DEDUPE_SECONDS
        ]
        for key in expired:
            self._upload_notify_cache.pop(key, None)

    @staticmethod
    def _upload_notify_key(storage: str, path: str) -> str:
        storage = str(storage or "").strip().casefold()
        path = str(path or "").strip()
        if not storage and not path:
            return ""
        return storage + "\x00" + path

    @staticmethod
    def _notify_url(raw_url: str) -> str:
        value = raw_url.strip().rstrip("/")
        if value.endswith("/api/strm/events/notify"):
            return value
        return value + "/api/strm/events/notify"

    @staticmethod
    def _event_target_storage(event: Event) -> str:
        target_item = FlowpanEventNotify._event_target_item(event)
        return str(FlowpanEventNotify._item_value(target_item, "storage") or "").strip()

    @staticmethod
    def _event_target_path(event: Event) -> str:
        target_item = FlowpanEventNotify._event_target_item(event)
        return str(FlowpanEventNotify._item_value(target_item, "path") or "").strip()

    @staticmethod
    def _event_target_item(event: Event) -> Any:
        data = event.event_data if event else None
        if isinstance(data, dict):
            transfer_info = data.get("transferinfo")
        else:
            transfer_info = getattr(data, "transferinfo", None)
        if isinstance(transfer_info, dict):
            return transfer_info.get("target_item") or transfer_info.get("target_diritem")
        return getattr(transfer_info, "target_item", None) or getattr(transfer_info, "target_diritem", None)

    @staticmethod
    def _item_value(item: Any, key: str) -> Any:
        if isinstance(item, dict):
            return item.get(key)
        return getattr(item, key, None)

    @staticmethod
    def _parse_target_storages(raw: Any) -> Set[str]:
        values = str(raw or "").replace("，", ",").split(",")
        return {value.strip().casefold() for value in values if value.strip()}

    def _mp_storage_conf(self) -> Dict[str, Any]:
        """
        MoviePilot 存储卡片用 config 是否为空判断是否已配置。
        这里不重复保存密钥，只写入非敏感桥接元数据。
        """
        return {
            "enabled": True,
            "driver": "flowpan_115_bridge",
            "display_name": self._storage_name,
            "flowpan_url": self._flowpan_url,
            "storage_backend": self._storage_backend,
            "part_size_mb": self._storage_part_size_mb,
            "plugin_version": self.plugin_version,
        }

    @staticmethod
    def _normalize_storage_backend(value: Any) -> str:
        value = str(value or DEFAULT_STORAGE_BACKEND).strip().lower()
        return "open" if value == "open" else "cookie"

    @staticmethod
    def _bounded_int(raw: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = default
        return max(minimum, min(maximum, value))
