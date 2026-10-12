#!/usr/bin/env python3
"""Generate OfflineAI-Bench v0.1 synthetic scenarios (SEED=42)."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from offlineaibench.generator import generate_scenarios, write_splits


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    cfg = yaml.safe_load((ROOT / "configs" / "default.yaml").read_text())
    out_dir = ROOT / cfg["paths"]["data_generated"]
    man_dir = ROOT / cfg["paths"]["manifests"]
    man_dir.mkdir(parents=True, exist_ok=True)

    scenarios = generate_scenarios(
        n=int(cfg["n_total"]),
        seed=int(cfg["seed"]),
        train_frac=float(cfg["splits"]["train"]),
        dev_frac=float(cfg["splits"]["dev"]),
    )
    composition = write_splits(scenarios, out_dir)
    checksums = {name: sha256(Path(p)) for name, p in composition["paths"].items()}
    composition["checksums"] = checksums
    composition["seed"] = cfg["seed"]
    (man_dir / "composition.json").write_text(json.dumps(composition, indent=2) + "\n")
    print(json.dumps({
        "n_total": composition["n_total"],
        "n_train": composition["n_train"],
        "n_dev": composition["n_dev"],
        "n_test": composition["n_test"],
        "test_sha256": checksums["test"],
        "fault_counts_test": composition["fault_counts_test"],
    }, indent=2))


if __name__ == "__main__":
    main()
