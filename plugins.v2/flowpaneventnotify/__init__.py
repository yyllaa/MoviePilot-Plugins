from threading import Lock, Thread, Timer
from time import monotonic, sleep
from typing import Any, Dict, List, Optional, Set, Tuple

from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType
from app.utils.http import RequestUtils


DEFAULT_TARGET_STORAGES = "u115,115网盘Plus"
DEFAULT_QUIET_SECONDS = 180
DEFAULT_MAX_WAIT_SECONDS = 1800


class FlowpanEventNotify(_PluginBase):
    """
    聚合 MoviePilot 的 115 转移完成事件并通知 Flowpan 执行事件增量同步
    """

    plugin_name = "Flowpan事件通知"
    plugin_desc = "聚合115转移完成事件并通知Flowpan更新"
    plugin_icon = (
        "https://raw.githubusercontent.com/jxxghp/MoviePilot-Frontend/"
        "refs/heads/v2/src/assets/images/misc/u115.png"
    )
    plugin_version = "1.0.0"
    plugin_author = "Flowpan"
    author_url = ""
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
        self._lock = Lock()
        self._timer: Optional[Timer] = None
        self._batch_started_at = 0.0
        self._event_count = 0

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
        self._enabled = bool(merged.get("enabled"))
        self._flowpan_url = str(merged.get("flowpan_url") or "").strip()
        self._token = str(merged.get("token") or "").strip()
        self._quiet_seconds = quiet_seconds
        self._max_wait_seconds = max_wait_seconds
        self._target_storages = target_storages
        normalized = {
            **merged,
            "quiet_seconds": quiet_seconds,
            "max_wait_seconds": max_wait_seconds,
            "target_storages": ",".join(sorted(target_storages)),
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
        返回插件 API 列表，本插件无自定义 API

        :return List: 空 API 列表
        """
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """
        返回插件服务列表，本插件无定时服务

        :return List: 空服务列表
        """
        return []

    def get_page(self) -> List[Dict[str, Any]]:
        """
        返回插件页面列表，本插件无数据页面

        :return List: 空页面列表
        """
        return []

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
                                "props": {"cols": 12, "md": 4},
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
                                "props": {"cols": 12, "md": 4},
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
        }

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

    @staticmethod
    def _notify_url(raw_url: str) -> str:
        value = raw_url.strip().rstrip("/")
        if value.endswith("/api/strm/events/notify"):
            return value
        return value + "/api/strm/events/notify"

    @staticmethod
    def _event_target_storage(event: Event) -> str:
        data = event.event_data if event else None
        if not isinstance(data, dict):
            return ""
        transfer_info = data.get("transferinfo")
        if isinstance(transfer_info, dict):
            target_item = transfer_info.get("target_item")
        else:
            target_item = getattr(transfer_info, "target_item", None)
        if isinstance(target_item, dict):
            return str(target_item.get("storage") or "").strip()
        return str(getattr(target_item, "storage", "") or "").strip()

    @staticmethod
    def _parse_target_storages(raw: Any) -> Set[str]:
        values = str(raw or "").replace("，", ",").split(",")
        return {value.strip().casefold() for value in values if value.strip()}

    @staticmethod
    def _bounded_int(raw: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = default
        return max(minimum, min(maximum, value))
