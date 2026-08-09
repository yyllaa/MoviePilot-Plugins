from base64 import b64encode
from io import BytesIO
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional

from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import ChainEventType
from app.helper.storage import StorageHelper
from app.schemas import StorageOperSelectionEventData, FileItem, StorageUsage

from p115client import P115Client, check_response

from .p115_api import P115Api
from .p115_client import create_client, build_timeout_config


class P115CookieStorage(_PluginBase):
    """
    115 Cookie 存储插件：为 MoviePilot 提供基于 115 Cookie/扫码登录的独立存储模块，支持文件列表、上传下载、快照等功能
    """

    # 插件名称
    plugin_name = "115 Cookie存储"
    # 插件描述
    plugin_desc = "基于115 Cookie扫码登录的MoviePilot独立存储模块。"
    # 插件图标
    plugin_icon = (
        "https://raw.githubusercontent.com/jxxghp/MoviePilot-Frontend/"
        "refs/heads/v2/src/assets/images/misc/u115.png"
    )
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "Flowpan"
    # 作者主页
    author_url = ""
    # 插件配置项ID前缀
    plugin_config_prefix = "p115cookiestorage_"
    # 加载顺序
    plugin_order = 99
    # 可使用的用户级别
    auth_level = 1

    # 是否启用
    _enabled = False
    _client = None
    _disk_name = None
    _p115_api = None
    _cookie = None
    _login_app = "alipaymini"
    _qr_payload = None

    def __init__(self):
        """
        初始化
        """
        super().__init__()

        self._disk_name = "115Cookie"

    def init_plugin(self, config: dict = None):
        """
        初始化插件
        """
        if not config:
            return

        _, form_defaults = self.get_form()
        merged = {**form_defaults, **config}
        if merged != config:
            self.update_config(merged)
        config = merged

        if config:
            storage_helper = StorageHelper()
            storages = storage_helper.get_storagies()
            storage_conf = {
                "enabled": True,
                "driver": "p115_cookie",
                "display_name": self._disk_name,
                "plugin": self.__class__.__name__,
                "plugin_version": self.plugin_version,
            }
            if not any(
                s.type == self._disk_name and s.name == self._disk_name
                for s in storages
            ):
                storage_helper.add_storage(
                    storage=self._disk_name, name=self._disk_name, conf=storage_conf
                )
            else:
                storage_helper.set_storage(self._disk_name, storage_conf)

            self._enabled = config.get("enabled")
            self._cookie = config.get("cookie")
            self._login_app = str(config.get("login_app") or "alipaymini").strip() or "alipaymini"
            if not self._cookie:
                self._client = None
                self._p115_api = None
                return

            try:
                timeout_kwargs = {}
                if config.get("timeout_enabled", True):
                    timeout_kwargs["default_timeout"] = build_timeout_config(
                        timeout_enabled=True,
                        connect=config.get("timeout_default_connect", 30),
                        pool=config.get("timeout_default_pool", 15),
                        read=config.get("timeout_default_read", 60),
                        write=config.get("timeout_default_write", 60),
                    )
                    timeout_kwargs["slow_timeout"] = build_timeout_config(
                        timeout_enabled=True,
                        connect=config.get("timeout_slow_connect", 30),
                        pool=config.get("timeout_slow_pool", 15),
                        read=config.get("timeout_slow_read", 300),
                        write=config.get("timeout_slow_write", 300),
                    )
                self._client = create_client(
                    self._cookie,
                    **timeout_kwargs,
                )
                self._p115_api = P115Api(client=self._client, disk_name=self._disk_name)
            except Exception as e:
                logger.error(f"115 Cookie存储客户端创建失败: {e}")

    def get_state(self) -> bool:
        """
        返回插件启用状态

        :return bool: True 表示插件已启用
        """
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        返回插件远程命令列表，本插件无远程命令

        :return List: 远程命令列表（本插件为空）
        """
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件 API 端点

        :return List: 插件 API 端点列表
        """
        return [
            {
                "path": "/clear_cache",
                "endpoint": self.clear_cache,
                "auth": "bear",
                "methods": ["POST"],
                "summary": "清理缓存",
                "description": "清理115网盘文件路径ID缓存和文件详情ID缓存",
            },
            {
                "path": "/login/qrcode",
                "endpoint": self.login_qrcode,
                "auth": "bear",
                "methods": ["GET"],
                "summary": "生成115扫码登录二维码",
                "description": "生成115 Cookie扫码登录二维码，返回 data-url 图片和轮询参数",
            },
            {
                "path": "/login/check",
                "endpoint": self.login_check,
                "auth": "bear",
                "methods": ["POST"],
                "summary": "检查115扫码登录状态",
                "description": "检查最近一次二维码扫码状态，确认成功后写入 Cookie 配置",
            },
            {
                "path": "/login/status",
                "endpoint": self.login_status,
                "auth": "bear",
                "methods": ["GET"],
                "summary": "检查115 Cookie可用性",
                "description": "用当前 Cookie 检查115登录状态",
            },
            {
                "path": "/login/logout",
                "endpoint": self.login_logout,
                "auth": "bear",
                "methods": ["POST"],
                "summary": "清空115 Cookie",
                "description": "清空插件保存的115 Cookie并重置客户端",
            },
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面

        :return Tuple: 页面配置和数据结构的元组
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "cookie",
                                            "label": "115 Cookie",
                                            "rows": 2,
                                            "auto-grow": True,
                                            "hint": "可手动粘贴 Cookie；也可到插件详情页扫码登录自动写入。",
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
                                        "component": "VSelect",
                                        "props": {
                                            "model": "login_app",
                                            "label": "扫码登录设备",
                                            "items": [
                                                {"title": "支付宝小程序（推荐）", "value": "alipaymini"},
                                                {"title": "iPad", "value": "ipad"},
                                                {"title": "iOS", "value": "ios"},
                                                {"title": "Web", "value": "web"},
                                            ],
                                            "hint": "二维码确认后按该设备类型写入 Cookie；不使用115 OpenAPI。",
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
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "timeout_enabled",
                                            "label": "启用超时控制",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "props": {"v-if": "timeout_enabled"},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 6, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "timeout_default_connect",
                                            "label": "普通-连接超时(秒)",
                                            "hint": "默认30",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 6, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "timeout_default_pool",
                                            "label": "普通-连接池超时(秒)",
                                            "hint": "默认15",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 6, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "timeout_default_read",
                                            "label": "普通-读取超时(秒)",
                                            "hint": "默认60",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 6, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "timeout_default_write",
                                            "label": "普通-写入超时(秒)",
                                            "hint": "默认60",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "props": {"v-if": "timeout_enabled"},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 6, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "timeout_slow_connect",
                                            "label": "慢操作-连接超时(秒)",
                                            "hint": "默认30",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 6, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "timeout_slow_pool",
                                            "label": "慢操作-连接池超时(秒)",
                                            "hint": "默认15",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 6, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "timeout_slow_read",
                                            "label": "慢操作-读取超时(秒)",
                                            "hint": "默认300",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 6, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "timeout_slow_write",
                                            "label": "慢操作-写入超时(秒)",
                                            "hint": "默认300",
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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "density": "compact",
                                            "class": "mt-2",
                                        },
                                        "content": [
                                            {
                                                "component": "div",
                                                "text": "重要提示：",
                                            },
                                            {
                                                "component": "div",
                                                "text": "• 所有操作均为 Cookie 接口调用，请确保 Cookie 有效",
                                            },
                                            {
                                                "component": "div",
                                                "text": "• 可手动粘贴 Cookie，也可在插件详情页扫码登录自动写入 Cookie",
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "cookie": "",
            "login_app": "alipaymini",
            "timeout_enabled": True,
            "timeout_default_connect": 30,
            "timeout_default_pool": 15,
            "timeout_default_read": 60,
            "timeout_default_write": 60,
            "timeout_slow_connect": 30,
            "timeout_slow_pool": 15,
            "timeout_slow_read": 300,
            "timeout_slow_write": 300,
        }

    def get_page(self) -> List[dict]:
        """
        获取插件数据页面：展示 Cookie 状态、扫码二维码、缓存维护入口。

        :return List: 插件数据页面配置列表
        """
        qr_data = (
            {"code": 1, "msg": "当前已配置 Cookie；如需重新扫码，请先清空 Cookie。"}
            if self._cookie
            else self._prepare_qrcode_for_page()
        )
        status_text = "已配置 Cookie" if self._cookie else "未配置 Cookie"
        api_status = self.login_status()
        if api_status.get("code") == 0:
            status_text = api_status.get("msg") or "Cookie 可用"

        content = [
            {
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal", "density": "compact", "class": "mb-4"},
                "text": "这是独立的 MoviePilot 115 Cookie 存储插件，不依赖 Flowpan 服务，也不使用 115 OpenAPI。",
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {"component": "div", "props": {"class": "text-h6 mb-1"}, "text": "登录状态"},
                            {"component": "div", "props": {"class": "text-body-2 text-medium-emphasis"}, "text": status_text},
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 8},
                        "content": [
                            {
                                "component": "VBtn",
                                "props": {"color": "primary", "variant": "tonal", "prepend-icon": "mdi-check-circle-outline", "class": "mr-2 mb-2"},
                                "text": "检测登录",
                                "events": {"click": {"api": "plugin/P115CookieStorage/login/status", "method": "get"}},
                            },
                            {
                                "component": "VBtn",
                                "props": {"color": "warning", "variant": "tonal", "prepend-icon": "mdi-logout", "class": "mr-2 mb-2"},
                                "text": "清空 Cookie",
                                "events": {"click": {"api": "plugin/P115CookieStorage/login/logout", "method": "post"}},
                            },
                            {
                                "component": "VBtn",
                                "props": {"color": "secondary", "variant": "tonal", "prepend-icon": "mdi-delete-sweep", "class": "mb-2"},
                                "text": "清理缓存",
                                "events": {"click": {"api": "plugin/P115CookieStorage/clear_cache", "method": "post"}},
                            },
                        ],
                    },
                ],
            },
        ]

        if qr_data.get("code") == 0:
            data = qr_data.get("data") or {}
            content.append({"component": "VDivider", "props": {"class": "my-4"}})
            content.append(
                {
                    "component": "VRow",
                    "content": [
                        {
                            "component": "VCol",
                            "props": {"cols": 12, "md": 4},
                            "content": [
                                {
                                    "component": "VImg",
                                    "props": {
                                        "src": data.get("qrcode"),
                                        "width": 220,
                                        "height": 220,
                                        "class": "mx-auto border rounded",
                                    },
                                }
                            ],
                        },
                        {
                            "component": "VCol",
                            "props": {"cols": 12, "md": 8},
                            "content": [
                                {"component": "div", "props": {"class": "text-h6 mb-2"}, "text": "扫码登录"},
                                {"component": "div", "props": {"class": "text-body-2 text-medium-emphasis mb-3"}, "text": data.get("tips")},
                                {
                                    "component": "VBtn",
                                    "props": {"color": "primary", "prepend-icon": "mdi-qrcode-scan", "class": "mr-2 mb-2"},
                                    "text": "我已扫码，检查并写入 Cookie",
                                    "events": {"click": {"api": "plugin/P115CookieStorage/login/check", "method": "post"}},
                                },
                                {
                                    "component": "div",
                                    "props": {"class": "text-caption text-medium-emphasis"},
                                    "text": "二维码在页面加载时生成；如过期，请关闭详情页后重新打开生成。",
                                },
                            ],
                        },
                    ],
                }
            )
        else:
            content.append(
                {
                    "component": "VAlert",
                    "props": {"type": "warning", "variant": "tonal", "density": "compact", "class": "mt-4"},
                    "text": qr_data.get("msg") or "二维码生成失败，可先手动填写 Cookie。",
                }
            )

        return [
            {
                "component": "VCard",
                "props": {"variant": "outlined"},
                "content": [
                    {
                        "component": "VCardText",
                        "props": {"class": "pa-5"},
                        "content": content,
                    },
                ],
            },
        ]

    def get_module(self) -> Dict[str, Any]:
        """
        获取插件模块声明，用于胁持系统模块实现

        :return Dict: 模块方法映射字典
        """
        return {
            "list_files": self.list_files,
            "any_files": self.any_files,
            "download_file": self.download_file,
            "upload_file": self.upload_file,
            "delete_file": self.delete_file,
            "rename_file": self.rename_file,
            "get_file_item": self.get_file_item,
            "get_parent_item": self.get_parent_item,
            "get_item_strict": self.get_item_strict,
            "snapshot_storage": self.snapshot_storage,
            "storage_usage": self.storage_usage,
            "support_transtype": self.support_transtype,
            "create_folder": self.create_folder,
            "exists": self.exists,
            "get_item": self.get_item,
        }

    @eventmanager.register(ChainEventType.StorageOperSelection)
    def storage_oper_selection(self, event: Event):
        """
        监听存储选择事件，返回当前类为操作对象

        :param event (Event): 存储选择事件
        """
        if not self._enabled or not self._p115_api:
            return
        event_data: StorageOperSelectionEventData = event.event_data
        if event_data.storage == self._disk_name:
            event_data.storage_oper = self._p115_api  # noqa

    def list_files(
        self, fileitem: FileItem, recursion: bool = False
    ) -> Optional[List[FileItem]]:
        """
        查询当前目录下所有目录和文件

        :param fileitem (FileItem): 目录文件项
        :param recursion (bool): 是否递归查询

        :return List: 文件项列表，如果存储不匹配则返回 None
        """

        if fileitem.storage != self._disk_name:
            return None

        if recursion:
            result = self._p115_api.iter_files(fileitem)
            if result is not None:
                return result

        def __get_files(_item: FileItem, _r: Optional[bool] = False):
            """
            递归处理
            """
            _items = self._p115_api.list(_item)
            if _items:
                if _r:
                    for t in _items:
                        if t.type == "dir":
                            __get_files(t, _r)
                        else:
                            result.append(t)
                else:
                    result.extend(_items)

        result = []
        __get_files(fileitem, recursion)

        return result

    def any_files(self, fileitem: FileItem, extensions: list = None) -> Optional[bool]:
        """
        查询当前目录下是否存在指定扩展名任意文件

        :param fileitem (FileItem): 目录文件项
        :param extensions (List): 扩展名列表，如 [\".mkv\", \".mp4\"]，为 None 表示查询任意文件

        :return bool: 存在返回 True，不存在返回 False，存储不匹配返回 None
        """
        if fileitem.storage != self._disk_name:
            return None

        def __any_file(_item: FileItem):
            """
            递归处理
            """
            _items = self._p115_api.list(_item)
            if _items:
                if not extensions:
                    return True
                for t in _items:
                    if (
                        t.type == "file"
                        and t.extension
                        and f".{t.extension.lower()}" in extensions
                    ):
                        return True
                    elif t.type == "dir":
                        if __any_file(t):
                            return True
            return False

        return __any_file(fileitem)

    def create_folder(self, fileitem: FileItem, name: str) -> Optional[FileItem]:
        """
        创建目录

        :param fileitem (FileItem): 父目录文件项
        :param name (str): 要创建的目录名称

        :return FileItem: 创建成功返回目录文件项，失败或存储不匹配返回 None
        """
        if fileitem.storage != self._disk_name:
            return None

        return self._p115_api.create_folder(fileitem=fileitem, name=name)

    def download_file(self, fileitem: FileItem, path: Path = None) -> Optional[Path]:
        """
        下载文件

        :param fileitem (FileItem): 文件项
        :param path (Path): 本地保存路径

        :return Path: 下载成功返回本地文件路径，失败或存储不匹配返回 None
        """
        if fileitem.storage != self._disk_name:
            return None

        return self._p115_api.download(fileitem, path)

    def upload_file(
        self, fileitem: FileItem, path: Path, new_name: Optional[str] = None
    ) -> Optional[FileItem]:
        """
        上传文件

        :param fileitem (FileItem): 保存目录项
        :param path (Path): 本地文件路径
        :param new_name (str): 新文件名，为 None 则使用本地文件名

        :return FileItem: 上传成功返回文件项，失败或存储不匹配返回 None
        """
        if fileitem.storage != self._disk_name:
            return None

        return self._p115_api.upload(fileitem, path, new_name)

    def delete_file(self, fileitem: FileItem) -> Optional[bool]:
        """
        删除文件或目录

        :param fileitem (FileItem): 要删除的文件项

        :return bool: 删除成功返回 True，失败或存储不匹配返回 None
        """
        if fileitem.storage != self._disk_name:
            return None

        return self._p115_api.delete(fileitem)

    def rename_file(self, fileitem: FileItem, name: str) -> Optional[bool]:
        """
        重命名文件或目录

        :param fileitem (FileItem): 要重命名的文件项
        :param name (str): 新名称

        :return bool: 重命名成功返回 True，失败或存储不匹配返回 None
        """
        if fileitem.storage != self._disk_name:
            return None

        return self._p115_api.rename(fileitem, name)

    def exists(self, fileitem: FileItem) -> Optional[bool]:
        """
        判断文件或目录是否存在

        :param fileitem (FileItem): 文件项

        :return bool: 存在返回 True，不存在返回 False，存储不匹配返回 None
        """
        if fileitem.storage != self._disk_name:
            return None

        return True if self.get_item(fileitem) else False

    def get_item(self, fileitem: FileItem) -> Optional[FileItem]:
        """
        查询目录或文件

        :param fileitem (FileItem): 文件项

        :return FileItem: 查询到的文件项，不存在或存储不匹配返回 None
        """
        if fileitem.storage != self._disk_name:
            return None

        return self.get_file_item(storage=fileitem.storage, path=Path(fileitem.path))

    def get_item_strict(self, path: Path) -> Optional[FileItem]:
        """
        兼容新版 MoviePilot 整理链：直接按路径返回严格文件项。
        """
        return self._p115_api.get_item_strict(path)

    def get_file_item(self, storage: str, path: Path) -> Optional[FileItem]:
        """
        根据路径获取文件项

        :param storage (str): 存储类型
        :param path (Path): 文件路径

        :return FileItem: 文件项，存储不匹配或不存在返回 None
        """
        if storage != self._disk_name:
            return None

        return self._p115_api.get_item(path)

    def get_parent_item(self, fileitem: FileItem) -> Optional[FileItem]:
        """
        获取上级目录项

        :param fileitem (FileItem): 文件项

        :return FileItem: 上级目录文件项，存储不匹配或不存在返回 None
        """
        if fileitem.storage != self._disk_name:
            return None

        return self._p115_api.get_parent(fileitem)

    def snapshot_storage(
        self,
        storage: str,
        path: Path,
        last_snapshot_time: float = None,
        max_depth: int = 5,
    ) -> Optional[Dict[str, Dict]]:
        """
        快照存储

        :param storage (str): 存储类型
        :param path (Path): 路径
        :param last_snapshot_time (float): 上次快照时间，用于增量快照
        :param max_depth (int): 最大递归深度，避免过深遍历

        :return Dict: 文件信息字典，key 为文件路径，value 为文件信息
        """
        if storage != self._disk_name:
            return None

        files_info = {}

        def __snapshot_file(_fileitm: FileItem, current_depth: int = 0):
            """
            递归获取文件信息
            """
            try:
                if _fileitm.type == "dir":
                    if current_depth >= max_depth:
                        return

                    if (
                        self.snapshot_check_folder_modtime  # noqa
                        and last_snapshot_time
                        and _fileitm.modify_time
                        and _fileitm.modify_time <= last_snapshot_time
                    ):
                        return

                    sub_files = self._p115_api.list(_fileitm)
                    for sub_file in sub_files:
                        __snapshot_file(sub_file, current_depth + 1)
                else:
                    if getattr(_fileitm, "modify_time", 0) > last_snapshot_time:
                        files_info[_fileitm.path] = {
                            "size": _fileitm.size or 0,
                            "modify_time": getattr(_fileitm, "modify_time", 0),
                            "type": _fileitm.type,
                        }

            except Exception as e:
                logger.debug(f"Snapshot error for {_fileitm.path}: {e}")

        fileitem = self._p115_api.get_item(path)
        if not fileitem:
            return {}

        __snapshot_file(fileitem)

        return files_info

    def storage_usage(self, storage: str) -> Optional[StorageUsage]:
        """
        存储使用情况

        :param storage (str): 存储类型

        :return StorageUsage: 存储使用情况对象，存储不匹配返回 None
        """
        if storage != self._disk_name:
            return None

        return self._p115_api.usage()

    def support_transtype(self, storage: str) -> Optional[dict]:
        """
        获取支持的整理方式

        :param storage (str): 存储类型

        :return Dict: 支持的整理方式字典，存储不匹配返回 None
        """
        if storage != self._disk_name:
            return None

        return {"move": "移动", "copy": "复制"}

    def login_qrcode(self) -> Dict[str, Any]:
        """
        生成115扫码登录二维码。
        """
        return self._prepare_qrcode_for_page()

    def login_check(self) -> Dict[str, Any]:
        """
        检查最近一次二维码状态，确认成功后把 Cookie 写入插件配置。
        """
        payload = self._qr_payload or {}
        uid = str(payload.get("uid") or "")
        if not uid:
            return {"code": 1, "msg": "没有可检查的二维码，请先打开插件详情页生成二维码"}
        status_payload = {
            "uid": uid,
            "time": str(payload.get("time") or ""),
            "sign": str(payload.get("sign") or ""),
        }
        try:
            resp = P115Client.login_qrcode_scan_status(status_payload)
            if isinstance(resp, dict):
                check_response(resp)
            status_code = (resp.get("data") or {}).get("status") if isinstance(resp, dict) else None
        except Exception as e:
            logger.error(f"【P115CookieStorage】检查扫码状态失败: {e}", exc_info=True)
            return {"code": 1, "msg": f"检查扫码状态失败: {e}"}

        if status_code == 0:
            return {"code": 1, "msg": "等待扫码"}
        if status_code == 1:
            return {"code": 1, "msg": "已扫码，请在手机端确认登录"}
        if status_code == -1 or (status_code is None and isinstance(resp, dict) and resp.get("message") == "key invalid"):
            self._qr_payload = None
            return {"code": 1, "msg": "二维码已过期，请重新打开插件详情页生成"}
        if status_code == -2:
            self._qr_payload = None
            return {"code": 1, "msg": "用户取消登录"}
        if status_code != 2:
            return {"code": 1, "msg": f"未知扫码状态: {status_code}"}

        app = str(payload.get("client_type") or self._login_app or "alipaymini")
        try:
            login_resp = P115Client.login_qrcode_scan_result(uid, app=app)
            if isinstance(login_resp, dict):
                check_response(login_resp)
        except Exception as e:
            logger.error(f"【P115CookieStorage】获取扫码登录结果失败: {e}", exc_info=True)
            return {"code": 1, "msg": f"获取扫码登录结果失败: {e}"}

        cookie_string = self._extract_cookie_string(login_resp)
        if not cookie_string:
            return {"code": 1, "msg": "登录成功但未能解析 Cookie"}

        self._store_cookie(cookie_string)
        self._qr_payload = None
        return {"code": 0, "msg": "扫码登录成功，Cookie 已写入配置"}

    def login_status(self) -> Dict[str, Any]:
        """
        检查当前 Cookie 是否可用。
        """
        if not self._cookie:
            return {"code": 1, "msg": "未配置 Cookie"}
        try:
            client = self._client or create_client(self._cookie)
            info = client.user_info()
            if isinstance(info, dict):
                check_response(info)
            data = info.get("data") if isinstance(info, dict) else None
            uid = ""
            if isinstance(data, dict):
                uid = str(data.get("user_id") or data.get("uid") or "")
            return {"code": 0, "msg": f"Cookie 可用{('，UID=' + uid) if uid else ''}"}
        except Exception as e:
            return {"code": 1, "msg": f"Cookie 检测失败: {e}"}

    def login_logout(self) -> Dict[str, Any]:
        """
        清空插件 Cookie 配置。
        """
        config = self.get_config() or {}
        _, defaults = self.get_form()
        merged = {**defaults, **config, "enabled": False, "cookie": ""}
        self.update_config(merged)
        self._enabled = False
        self._cookie = ""
        self._client = None
        self._p115_api = None
        self._qr_payload = None
        return {"code": 0, "msg": "Cookie 已清空"}

    def _prepare_qrcode_for_page(self) -> Dict[str, Any]:
        """
        页面加载时生成二维码，并缓存 uid/time/sign 供检查接口使用。
        """
        try:
            token_resp = P115Client.login_qrcode_token()
            if isinstance(token_resp, dict):
                check_response(token_resp)
            data = token_resp.get("data") or {}
            uid = str(data.get("uid") or "")
            qr_time = str(data.get("time") or "")
            sign = str(data.get("sign") or "")
            if not uid or not qr_time or not sign:
                return {"code": 1, "msg": "获取二维码失败：返回登录参数不完整"}

            qrcode_bytes = b""
            try:
                qrcode_bytes = P115Client.login_qrcode({"uid": uid})
            except Exception:
                qrcode_content = str(data.get("qrcode") or f"https://115.com/scan/dg-{uid}")
                try:
                    from qrcode import make as qr_make
                    img = qr_make(qrcode_content)
                    buffered = BytesIO()
                    img.save(buffered, format="PNG")
                    qrcode_bytes = buffered.getvalue()
                except Exception as qe:
                    return {"code": 1, "msg": f"二维码图片生成失败: {qe}"}

            app = self._login_app or "alipaymini"
            self._qr_payload = {"uid": uid, "time": qr_time, "sign": sign, "client_type": app}
            return {
                "code": 0,
                "msg": "二维码已生成",
                "data": {
                    "uid": uid,
                    "time": qr_time,
                    "sign": sign,
                    "client_type": app,
                    "qrcode": f"data:image/png;base64,{b64encode(qrcode_bytes).decode('utf-8')}",
                    "tips": f"请使用115手机客户端扫码，并在手机端确认。登录设备：{app}",
                },
            }
        except Exception as e:
            logger.error(f"【P115CookieStorage】生成扫码二维码失败: {e}", exc_info=True)
            return {"code": 1, "msg": f"生成扫码二维码失败: {e}"}

    @staticmethod
    def _extract_cookie_string(resp: Dict[str, Any]) -> str:
        data = resp.get("data") if isinstance(resp, dict) else None
        if not isinstance(data, dict):
            return ""
        cookie = data.get("cookie") or data.get("cookies")
        if isinstance(cookie, dict):
            return "; ".join(f"{k}={v}" for k, v in cookie.items() if k and v)
        if isinstance(cookie, str):
            return cookie.strip()
        return ""

    def _store_cookie(self, cookie: str) -> None:
        config = self.get_config() or {}
        _, defaults = self.get_form()
        merged = {**defaults, **config, "enabled": True, "cookie": cookie}
        self.update_config(merged)
        self.init_plugin(merged)

    def clear_cache(self) -> Dict[str, Any]:
        """
        清理缓存

        :return Dict: 清理结果，包含 code 和 msg
        """
        try:
            if not self._p115_api:
                return {
                    "code": 1,
                    "msg": "插件未启用或未初始化",
                }

            self._p115_api._id_cache.clear()
            self._p115_api._id_item_cache.clear()

            logger.info("【P115CookieStorage】缓存清理成功")
            return {
                "code": 0,
                "msg": "缓存清理成功",
            }
        except Exception as e:
            logger.error(f"【P115CookieStorage】缓存清理失败: {e}", exc_info=True)
            return {
                "code": 1,
                "msg": f"缓存清理失败: {str(e)}",
            }

    def stop_service(self):
        """
        退出插件
        """
        pass
