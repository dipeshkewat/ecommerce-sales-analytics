"""Small, timeout-bounded wrapper around the Hadoop filesystem CLI."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


class HadoopUnavailableError(RuntimeError):
    pass


class HadoopCommandError(RuntimeError):
    def __init__(self, command: list[str], returncode: int, stderr: str):
        self.command = command
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(f"Command failed ({returncode}): {' '.join(command)}\n{stderr.strip()}")


class HDFSClient:
    def __init__(self, executable: str = "hadoop", timeout_seconds: int = 60):
        self.executable = executable
        self.timeout_seconds = timeout_seconds

    def command(self, args: list[str], *, timeout_seconds: int | None = None) -> subprocess.CompletedProcess[str]:
        command = [self.executable, *args]
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout_seconds or self.timeout_seconds,
                check=False,
            )
        except FileNotFoundError as error:
            raise HadoopUnavailableError(
                f"Hadoop CLI '{self.executable}' was not found. Configure Hadoop on PATH in Linux/WSL."
            ) from error
        except subprocess.TimeoutExpired as error:
            raise HadoopUnavailableError(f"Hadoop command timed out after {error.timeout} seconds.") from error
        if result.returncode:
            raise HadoopCommandError(command, result.returncode, result.stderr)
        return result

    @staticmethod
    def _validate_remote_path(remote_path: str) -> None:
        if not remote_path.startswith("/") or ".." in remote_path.split("/"):
            raise ValueError("HDFS path must be an absolute path without parent traversal.")
        if any(character in remote_path for character in ("\n", "\r", "\t", "\x00")):
            raise ValueError("HDFS path contains an invalid control character.")

    def upload(self, local_path: str | Path, hdfs_directory: str, filename: str) -> str:
        self._validate_remote_path(hdfs_directory)
        if Path(filename).name != filename or filename in {"", ".", ".."}:
            raise ValueError("HDFS upload filename must be a simple file name.")
        local_file = Path(local_path)
        if not local_file.is_file():
            raise FileNotFoundError(local_file)
        self.command(["fs", "-mkdir", "-p", hdfs_directory])
        destination = f"{hdfs_directory.rstrip('/')}/{filename}"
        self.command(["fs", "-put", str(local_file.resolve()), destination])
        self.command(["fs", "-test", "-e", destination])
        return destination

    def read_part_files(self, hdfs_directory: str) -> list[str]:
        self._validate_remote_path(hdfs_directory)
        listing = self.command(["fs", "-ls", hdfs_directory]).stdout
        part_paths = []
        for line in listing.splitlines():
            fields = line.split()
            if fields and fields[-1].rsplit("/", 1)[-1].startswith("part-"):
                part_paths.append(fields[-1])
        if not part_paths:
            raise HadoopCommandError([self.executable, "fs", "-ls", hdfs_directory], 1, "No part-* output files found.")
        result = self.command(["fs", "-cat", *part_paths])
        return [line for line in result.stdout.splitlines() if line]