import ast
import json
import threading
from pathlib import Path
from unittest.mock import Mock

from pydantic import BaseModel


ROOT = Path(__file__).resolve().parents[3]
PLUGIN = ROOT / "plugins.v3" / "flowpaneventnotify"


def test_v3_index_and_source_are_aligned():
    package = json.loads((ROOT / "package.v3.json").read_text(encoding="utf-8"))
    source = (PLUGIN / "__init__.py").read_text(encoding="utf-8")

    assert package["FlowpanEventNotify"]["version"] == "3.0.3"
    assert 'plugin_version = "3.0.3"' in source
    assert package["FlowpanEventNotify"]["system_version"] == ">=3.0.0"


def test_v3_source_does_not_add_legacy_imports():
    legacy_prefixes = ("from app.core.", "from app.helper.", "from app.utils.", "from app.log ", "from app.plugins ")
    for path in PLUGIN.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert not any(line.startswith(legacy_prefixes) for line in text.splitlines()), path


def test_v3_runtime_capabilities_are_real():
    source = (PLUGIN / "__init__.py").read_text(encoding="utf-8-sig")
    assert '"cmd": "/flowpan_sync"' in source
    assert '"id": "flowpan_sync"' in source
    assert "def action_flowpan_sync" in source
    assert "@eventmanager.register(EventType.PluginAction)" in source


def test_notification_triggers_are_serialized_and_pending_events_are_drained():
    source = ast.parse((PLUGIN / "__init__.py").read_text(encoding="utf-8-sig"))
    plugin = next(node for node in source.body if isinstance(node, ast.ClassDef))
    names = {
        "_queue_notification",
        "_start_notification_worker",
        "_run_notification",
    }
    methods = [node for node in plugin.body if isinstance(node, ast.FunctionDef) and node.name in names]
    scope = {"Thread": threading.Thread, "logger": Mock()}
    module = ast.Module(body=methods, type_ignores=[])
    exec(compile("from __future__ import annotations\n" + ast.unparse(module), "plugin", "exec"), scope)

    class Probe:
        _queue_notification = scope["_queue_notification"]
        _run_notification = scope["_run_notification"]

        def __init__(self):
            self._enabled = True
            self._flowpan_url = "http://flowpan"
            self._token = "token"
            self._lock = threading.Lock()
            self._notification_inflight = False
            self._pending_notification_count = 0
            self._notification_generation = 0
            self.started = []

        def _start_notification_worker(self, event_count, generation):
            self.started.append((event_count, generation))

        def _notify_flowpan(self, event_count):
            self.sent = getattr(self, "sent", []) + [event_count]

    probe = Probe()
    assert probe._queue_notification(2, "自动")
    assert probe._queue_notification(3, "手动")
    assert probe.started == [(2, 0)]
    probe._run_notification(2, 0)
    assert probe.sent == [2]
    assert probe.started == [(2, 0), (3, 0)]
    probe._run_notification(3, 0)
    assert probe.sent == [2, 3]
    assert probe._pending_notification_count == 0


def test_v3_storage_management_contract():
    # Execute the shipped methods without starting MoviePilot or contacting 115.
    source = ast.parse((PLUGIN / "__init__.py").read_text(encoding="utf-8-sig"))
    plugin = next(node for node in source.body if isinstance(node, ast.ClassDef))
    methods = [node for node in plugin.body if isinstance(node, ast.FunctionDef)
               and node.name in {"get_module", "storage_manage"}]

    class StorageUsage(BaseModel):
        total: float = 0
        available: float = 0

    scope = {"StorageUsage": StorageUsage, "logger": Mock()}
    module = ast.Module(body=methods, type_ignores=[])
    exec(compile("from __future__ import annotations\n" + ast.unparse(module),
                 str(PLUGIN / "__init__.py"), "exec"), scope)
    bridge = Mock()
    bridge.support_transtype.return_value = {"copy": "Copy", "move": "Move"}
    bridge.probe_connection.return_value = {"total": "1000", "available": "400", "backend": "cookie"}
    instance = Mock(_storage_name="Flowpan-115", _storage_api=bridge)
    instance.storage_manage = scope["storage_manage"].__get__(instance)
    handler = scope["get_module"](instance)["storage_manage"]

    assert handler("local", "usage") is None
    bridge.probe_connection.assert_not_called()
    assert handler("Flowpan-115", "support_transtype") == {
        "success": True, "data": {"transtype": {"copy": "Copy", "move": "Move"}}}
    assert handler("Flowpan-115", "usage") == {
        "success": True, "data": {"total": 1000.0, "available": 400.0}}
    bridge.probe_connection.side_effect = RuntimeError("connection failed")
    assert handler("Flowpan-115", "usage")["success"] is False
    for action in ("save_config", "reset_config", "generate_qrcode", "unknown"):
        assert handler("Flowpan-115", action)["success"] is False
    instance._storage_api = None
    assert handler("Flowpan-115", "usage")["success"] is False
    assert scope["get_module"](instance) == {}
