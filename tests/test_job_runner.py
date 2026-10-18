from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

from src.hadoop.job_runner import HadoopJobRunner, environment_status


def test_job_runner_uses_argument_list_and_captures_success(monkeypatch, tmp_path):
    jar = tmp_path / "jobs.jar"
    jar.touch()
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return SimpleNamespace(returncode=0, stdout="job complete", stderr="")

    monkeypatch.setattr("src.hadoop.job_runner.subprocess.run", fake_run)
    execution = HadoopJobRunner("hadoop", jar).run("aggregation", "com.bda.Sales", ["/in", "/out"], "/out")

    assert execution.status == "SUCCEEDED"
    assert captured["command"][0:2] == ["hadoop", "jar"]
    assert captured["command"][-2:] == ["/in", "/out"]
    assert captured["capture_output"] is True
    assert "shell" not in captured


def test_job_runner_marks_nonzero_exit_failed(monkeypatch, tmp_path):
    jar = tmp_path / "jobs.jar"
    jar.touch()
    monkeypatch.setattr(
        "src.hadoop.job_runner.subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=2, stdout="", stderr="namenode unavailable"),
    )

    execution = HadoopJobRunner("hadoop", jar).run("aggregation", "com.bda.Sales", [], "/out")

    assert execution.status == "FAILED"
    assert execution.stderr == "namenode unavailable"
    assert "status 2" in execution.error


def test_job_runner_marks_missing_cli_and_jar_failed(tmp_path):
    runner = HadoopJobRunner("missing-hadoop", tmp_path / "missing.jar")

    execution = runner.run("aggregation", "com.bda.Sales", [], "/out")

    assert execution.status == "FAILED"
    assert "jar not found" in execution.error


def test_job_runner_records_timeout(monkeypatch, tmp_path):
    jar = tmp_path / "jobs.jar"
    jar.touch()

    def timeout(*_args, **_kwargs):
        raise subprocess.TimeoutExpired("hadoop", 1, output=b"started")

    monkeypatch.setattr("src.hadoop.job_runner.subprocess.run", timeout)
    execution = HadoopJobRunner("hadoop", jar, timeout_seconds=1).run("aggregation", "com.bda.Sales", [], "/out")

    assert execution.status == "FAILED"
    assert execution.stdout == "started"
    assert "timeout" in execution.error


def test_environment_status_requires_all_job_classes_and_java_11_bytecode(monkeypatch, tmp_path):
    jar = tmp_path / "jobs.jar"
    class_names = (
        "com/bda/sales/SalesAggregation.class",
        "com/bda/sales/ProductRanking.class",
        "com/bda/sales/ProductJoin.class",
    )
    with ZipFile(jar, "w") as archive:
        for name in class_names:
            archive.writestr(name, b"\xca\xfe\xba\xbe\x00\x00\x00\x37")
    monkeypatch.setattr("src.hadoop.job_runner.shutil.which", lambda _command: "available")

    assert environment_status("hadoop", jar)["ready"] is True

    with ZipFile(jar, "w") as archive:
        for name in class_names:
            archive.writestr(name, b"\xca\xfe\xba\xbe\x00\x00\x00\x45")
    status = environment_status("hadoop", jar)
    assert status["jar_exists"] is True
    assert status["jar_compatible"] is False
    assert status["ready"] is False