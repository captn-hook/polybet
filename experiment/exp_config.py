import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from exp_strategy import normalized_strategy_mode


def load_config() -> dict[str, Any]:
    config_path = os.getenv("EXPERIMENT_CONFIG_PATH", "/app/configs/default.yaml")
    path = Path(config_path)
    if not path.exists():
        raise RuntimeError(f"Experiment config not found: {config_path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


@dataclass(frozen=True)
class RuntimeConfig:
    cfg: dict[str, Any]
    config_path: str
    experiment_id: str
    strategy: str
    loop_interval_seconds: int
    horizon_minutes: int
    strategy_mode: str
    strategy_mode_normalized: str
    yes_probability: float
    seed: str | None


def read_runtime_config() -> RuntimeConfig:
    cfg = load_config()
    config_path = os.getenv("EXPERIMENT_CONFIG_PATH", "/app/configs/default.yaml")
    exp_cfg = cfg.get("experiment", {})
    random_cfg = cfg.get("random_baseline", {})

    strategy_mode = str(random_cfg.get("strategy_mode", "random"))
    strategy_mode_normalized = normalized_strategy_mode(strategy_mode)
    env_seed = os.getenv("EXPERIMENT_SEED")
    rng_required = strategy_mode_normalized in {"probabilistic", "random"}
    if rng_required and not env_seed:
        raise RuntimeError(
            "EXPERIMENT_SEED is required for RNG strategies (probabilistic/random)."
        )

    return RuntimeConfig(
        cfg=cfg,
        config_path=config_path,
        experiment_id=(
            f"{exp_cfg.get('id', 'exp-random')}_{env_seed}"
            if env_seed
            else str(exp_cfg.get("id", "exp-random"))
        ),
        strategy=str(exp_cfg.get("name", "random-baseline")),
        loop_interval_seconds=int(random_cfg.get("loop_interval_seconds", 30)),
        horizon_minutes=int(random_cfg.get("horizon_minutes", 5)),
        strategy_mode=strategy_mode,
        strategy_mode_normalized=strategy_mode_normalized,
        yes_probability=float(random_cfg.get("yes_probability", 0.5)),
        seed=env_seed if env_seed else None,
    )
