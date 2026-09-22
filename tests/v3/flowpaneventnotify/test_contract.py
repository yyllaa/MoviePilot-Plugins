import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PLUGIN = ROOT / "plugins.v3" / "flowpaneventnotify"


def test_v3_index_and_source_are_aligned():
    package = json.loads((ROOT / "package.v3.json").read_text(encoding="utf-8"))
    source = (PLUGIN / "__init__.py").read_text(encoding="utf-8")

    assert package["FlowpanEventNotify"]["version"] == "3.0.0"
    assert 'plugin_version = "3.0.0"' in source
    assert package["FlowpanEventNotify"]["system_version"] == ">=3.0.0"


def test_v3_source_does_not_add_legacy_imports():
    legacy_prefixes = ("from app.core.", "from app.helper.", "from app.utils.", "from app.log ", "from app.plugins ")
    for path in PLUGIN.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert not any(line.startswith(legacy_prefixes) for line in text.splitlines()), path
