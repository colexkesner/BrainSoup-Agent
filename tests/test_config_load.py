from src.utils import load_yaml


def test_config_yaml_loads():
    cfg = load_yaml("config/config.yaml")
    assert isinstance(cfg, dict)
    assert "input_paths" in cfg
    assert cfg["input_paths"]["excel"] == "data/raw/informs.xlsx"
