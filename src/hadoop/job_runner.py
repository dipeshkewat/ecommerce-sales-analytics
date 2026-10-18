"""Safe subprocess execution for compiled Hadoop MapReduce jobs."""

from __future__ import annotations

import shutil
import logging
import subprocess
import time
from zipfile import BadZipFile, ZipFile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

LOGGER = logging.getLogger("bda.hadoop")


@dataclass
class JobExecution:
    name: str
    command: list[str]
    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    stdout: str
    stderr: str
    output_path: str
    error: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class HadoopJobRunner:
    def __init__(self, hadoop: str, jar_path: str | Path, timeout_seconds: int = 600):
        self.hadoop = hadoop
        self.jar_path = Path(jar_path)
        self.timeout_seconds = timeout_seconds

    def run(self, name: str, main_class: str, args: list[str], output_path: str) -> JobExecution:
        command = [self.hadoop, "jar", str(self.jar_path.resolve()), main_class, *args]
        started_wall = datetime.now(timezone.utc)
        start = time.monotonic()
        stdout = ""
        stderr = ""
        error = ""
        try:
            if not self.jar_path.is_file():
                raise FileNotFoundError(f"MapReduce jar not found: {self.jar_path}")
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
                check=False,
            )
            stdout = completed.stdout
            stderr = completed.stderr
            if completed.returncode:
                error = f"Hadoop exited with status {completed.returncode}."
        except FileNotFoundError as exception:
            error = str(exception)
            stderr = error
        except subprocess.TimeoutExpired as exception:
            stdout = exception.stdout.decode(errors="replace") if isinstance(exception.stdout, bytes) else (exception.stdout or "")
            stderr = exception.stderr.decode(errors="replace") if isinstance(exception.stderr, bytes) else (exception.stderr or "")
            error = f"Hadoop job exceeded the {self.timeout_seconds}-second timeout."

        finished_wall = datetime.now(timezone.utc)
        LOGGER.info("Job finished name=%s status=%s duration_seconds=%.3f", name, "FAILED" if error else "SUCCEEDED", time.monotonic() - start)
        return JobExecution(
            name=name,
            command=command,
            status="SUCCEEDED" if not error else "FAILED",
            started_at=started_wall.isoformat(),
            finished_at=finished_wall.isoformat(),
            duration_seconds=round(time.monotonic() - start, 3),
            stdout=stdout,
            stderr=stderr,
            output_path=output_path,
            error=error,
        )


def environment_status(hadoop: str, jar_path: str | Path) -> dict[str, object]:
    java = shutil.which("java")
    hadoop_cli = shutil.which(hadoop)
    jar_file = Path(jar_path)
    jar_exists = jar_file.is_file()
    jar_compatible = False
    if jar_exists:
        required_classes = (
            "com/bda/sales/SalesAggregation.class",
            "com/bda/sales/ProductRanking.class",
            "com/bda/sales/ProductJoin.class",
        )
        try:
            with ZipFile(jar_file) as archive:
                jar_compatible = all(
                    name in archive.namelist()
                    and 52 <= int.from_bytes(archive.read(name)[6:8], "big") <= 55
                    for name in required_classes
                )
        except (BadZipFile, OSError, KeyError):
            jar_compatible = False
    return {
        "java_cli": java,
        "hadoop_cli": hadoop_cli,
        "jar_exists": jar_exists,
        "jar_compatible": jar_compatible,
        "ready": java is not None and hadoop_cli is not None and jar_compatible,
    }