"""Centralized application settings loaded from YAML with environment overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    hadoop_executable: str
    mapreduce_jar: Path
    runs_directory: Path
    hdfs_root: str
    timeout_seconds: int


def load_settings() -> Settings:
    config_path = Path(os.environ.get("BDA_CONFIG", ROOT / "config" / "config.yaml"))
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) if config_path.is_file() else {}
    app_config = config.get("app", {})
    hadoop_config = config.get("hadoop", {})
    jar_value = os.environ.get(
        "BDA_MAPREDUCE_JAR",
        hadoop_config.get("mapreduce_jar", "hadoop-jobs/target/ecommerce-sales-analytics-1.0.0.jar"),
    )
    runs_value = os.environ.get("BDA_RUNS_DIRECTORY", app_config.get("runs_directory", "runs"))
    jar_path = Path(jar_value)
    runs_path = Path(runs_value)
    return Settings(
        hadoop_executable=os.environ.get("HADOOP_BIN", hadoop_config.get("executable", "hadoop")),
        mapreduce_jar=jar_path if jar_path.is_absolute() else ROOT / jar_path,
        runs_directory=runs_path if runs_path.is_absolute() else ROOT / runs_path,
        hdfs_root=os.environ.get("BDA_HDFS_ROOT", hadoop_config.get("hdfs_root", "/bda_sales/runs")),
        timeout_seconds=int(os.environ.get("BDA_JOB_TIMEOUT_SECONDS", hadoop_config.get("command_timeout_seconds", 600))),
    )