"""容器内在线备份与按清单复核的测试。

覆盖:
- online backup API 一致性快照(备份期间业务请求持续写入,快照可打开、校验通过);
- 临时目录 -> 校验 -> 哈希清单 -> 原子发布的完整流程与唯一目录;
- 失败清理(源不存在 / integrity_check 不过 / 外键违例),不留半成品;
- 复核:哈希篡改、缺文件、旁挂事务文件、非法清单均判失败;
- CLI(create / verify / verify --all、退出码)。
"""
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from app import backup as bk
from app.db import connect, init_db

MANIFEST = bk.MANIFEST_NAME


# ---------------------------------------------------------------- 辅助


def _make_source(path: Path) -> sqlite3.Connection:
    conn = connect(str(path))
    init_db(conn)
    conn.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, is_root)"
        " VALUES ('S0', '血清', 'mL', 100, 100, 1)"
    )
    conn.execute(
        "INSERT INTO operations (op_no, op_type, payload, effect, seq_index)"
        " VALUES ('OP-1', 'sampling', '{}', '{}', 0)"
    )
    conn.execute(
        "INSERT INTO lineage_edges (parent_id, child_id, op_id, amount, unit, ref_no)"
        " VALUES (NULL, 'S0', 1, 100, 'mL', 'OP-1')"
    )
    conn.commit()
    return conn


def _all_dirs(root: Path) -> list[str]:
    return sorted(p.name for p in root.iterdir()) if root.exists() else []


# ---------------------------------------------------------------- 创建/复核


def test_create_backup_publishes_manifest_and_verifies(tmp_path):
    src = tmp_path / "data" / "samples.db"
    root = tmp_path / "backups"
    conn = _make_source(src)
    conn.close()

    result = bk.create_backup(src, root)

    assert result.backup_dir.parent == root
    assert result.db_filename == "samples.db"
    assert result.backup_dir.is_dir()
    # 唯一备份目录,且没有临时目录残留
    names = _all_dirs(root)
    assert names == [result.backup_dir.name]
    assert not any(n.startswith(".backup-tmp-") for n in names)

    db_copy = result.backup_dir / "samples.db"
    manifest_path = result.backup_dir / MANIFEST
    assert db_copy.is_file()
    # 副本自包含:无 WAL/journal 旁挂文件
    assert not (result.backup_dir / "samples.db-wal").exists()
    assert not (result.backup_dir / "samples.db-shm").exists()
    assert not (result.backup_dir / "samples.db-journal").exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert set(manifest) == {"created_at", "database_file", "sha256"}
    assert manifest["database_file"] == "samples.db"
    assert manifest["sha256"] == bk._sha256_file(db_copy)
    assert manifest["created_at"].endswith("Z")
    assert result.checks.integrity == ["ok"]
    assert result.checks.foreign_key_violations == []

    # 副本内容与源一致且可独立打开
    chk = sqlite3.connect(str(db_copy))
    try:
        assert chk.execute("SELECT remaining FROM samples WHERE sample_id='S0'").fetchone()[0] == 100
        assert chk.execute("PRAGMA journal_mode").fetchone()[0].lower() == "delete"
    finally:
        chk.close()

    report = bk.verify_backup(result.backup_dir)
    assert report.ok is True, report.problems
    assert report.actual_sha256 == manifest["sha256"]


def test_backup_snapshot_while_business_writes_continue(tmp_path):
    """备份过程中持续有业务写入:快照必须是某一时刻的一致状态且校验通过。"""
    src = tmp_path / "data" / "samples.db"
    root = tmp_path / "backups"
    conn = _make_source(src)

    stop = False

    def writer():
        # 模拟业务请求:独立连接持续插入不可变审计行(WAL 允许与备份并发)
        w = sqlite3.connect(str(src), timeout=30)
        i = 0
        while not stop:
            w.execute(
                "INSERT INTO reservation_events (reservation_no, action, detail)"
                " VALUES (NULL, 'query', ?)",
                (f"heartbeat-{i}",),
            )
            w.commit()
            i += 1
            time.sleep(0.0005)

    t = threading.Thread(target=writer)
    t.start()
    try:
        time.sleep(0.02)
        result = bk.create_backup(src, root)
        time.sleep(0.02)
    finally:
        stop = True
        t.join()

    source_rows = conn.execute("SELECT COUNT(*) FROM reservation_events").fetchone()[0]
    conn.close()
    assert source_rows > 0

    snap = sqlite3.connect(str(result.backup_dir / "samples.db"))
    try:
        # 快照来自某个已提交时刻:行数连续无撕裂,且通过两项校验
        n = snap.execute("SELECT COUNT(*) FROM reservation_events").fetchone()[0]
        assert 0 < n <= source_rows
        assert snap.execute(
            "SELECT COUNT(*) FROM reservation_events WHERE detail GLOB 'heartbeat-*'"
        ).fetchone()[0] == n
        assert snap.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert snap.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        snap.close()

    report = bk.verify_backup(result.backup_dir)
    assert report.ok is True, report.problems


def test_multiple_backups_get_unique_directories_and_latest_sorting(tmp_path):
    src = tmp_path / "data" / "samples.db"
    root = tmp_path / "backups"
    conn = _make_source(src)
    conn.close()

    r1 = bk.create_backup(src, root)
    time.sleep(1.05)  # 目录名含秒级时间戳,确保不同名
    r2 = bk.create_backup(src, root)
    assert r1.backup_dir != r2.backup_dir
    assert bk.list_backups(root) == [r1.backup_dir, r2.backup_dir]
    assert bk.list_backups(root)[-1] == r2.backup_dir
    # 没有临时目录残留
    assert all(not n.startswith(".") for n in _all_dirs(root))


# ---------------------------------------------------------------- 失败清理


def test_missing_source_raises_and_leaves_nothing(tmp_path):
    root = tmp_path / "backups"
    with pytest.raises(bk.BackupError, match="源数据库文件不存在"):
        bk.create_backup(tmp_path / "nope.db", root)
    assert not root.exists() or _all_dirs(root) == []


def test_corrupt_copy_fails_and_tmp_cleaned(tmp_path, monkeypatch):
    """integrity_check 不过:备份失败、临时目录被清理、备份根下无成品。"""
    src = tmp_path / "data" / "samples.db"
    root = tmp_path / "backups"
    conn = _make_source(src)
    conn.close()

    real_run_checks = bk._run_checks

    def fake_run_checks(db_path):
        return bk.CheckResult(integrity=["*** in database disk image is malformed"],
                              foreign_key_violations=[])

    monkeypatch.setattr(bk, "_run_checks", fake_run_checks)
    with pytest.raises(bk.BackupError, match="integrity_check"):
        bk.create_backup(src, root)
    assert _all_dirs(root) == []


def test_foreign_key_violation_fails_and_tmp_cleaned(tmp_path, monkeypatch):
    src = tmp_path / "data" / "samples.db"
    root = tmp_path / "backups"
    conn = _make_source(src)
    conn.close()

    def fake_run_checks(db_path):
        return bk.CheckResult(
            integrity=["ok"],
            foreign_key_violations=[{"table": "x", "rowid": 1, "parent": "y", "fkid": 0}],
        )

    monkeypatch.setattr(bk, "_run_checks", fake_run_checks)
    with pytest.raises(bk.BackupError, match="foreign_key_check"):
        bk.create_backup(src, root)
    assert _all_dirs(root) == []


# ---------------------------------------------------------------- 复核失败


def test_verify_detects_tampered_hash(tmp_path):
    src = tmp_path / "data" / "samples.db"
    root = tmp_path / "backups"
    conn = _make_source(src)
    conn.close()
    result = bk.create_backup(src, root)

    # 篡改数据库文件内容(在文件尾部追加字节,SQLite 仍可能打开但哈希已变)
    db_file = result.backup_dir / "samples.db"
    with db_file.open("ab") as f:
        f.write(b"tampered")
    report = bk.verify_backup(result.backup_dir)
    assert report.ok is False
    assert any("SHA-256 不一致" in p for p in report.problems)


def test_verify_detects_missing_db_file(tmp_path):
    src = tmp_path / "data" / "samples.db"
    root = tmp_path / "backups"
    conn = _make_source(src)
    conn.close()
    result = bk.create_backup(src, root)
    (result.backup_dir / "samples.db").unlink()
    report = bk.verify_backup(result.backup_dir)
    assert report.ok is False
    assert any("缺少备份数据库文件" in p for p in report.problems)


def test_verify_rejects_wal_sidecar(tmp_path):
    src = tmp_path / "data" / "samples.db"
    root = tmp_path / "backups"
    conn = _make_source(src)
    conn.close()
    result = bk.create_backup(src, root)
    (result.backup_dir / "samples.db-wal").write_bytes(b"")
    report = bk.verify_backup(result.backup_dir)
    assert report.ok is False
    assert any("旁挂事务文件" in p for p in report.problems)


def test_verify_rejects_broken_manifest_and_path_traversal(tmp_path):
    src = tmp_path / "data" / "samples.db"
    root = tmp_path / "backups"
    conn = _make_source(src)
    conn.close()
    result = bk.create_backup(src, root)

    # 清单指向目录之外的文件名
    manifest = result.backup_dir / MANIFEST
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["database_file"] = "../../data/samples.db"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    report = bk.verify_backup(result.backup_dir)
    assert report.ok is False
    assert any("数据库文件名非法" in p for p in report.problems)

    # 清单不是合法 JSON
    manifest.write_text("{not json", encoding="utf-8")
    report = bk.verify_backup(result.backup_dir)
    assert report.ok is False
    assert any("清单无法解析" in p for p in report.problems)


def test_verify_missing_directory(tmp_path):
    report = bk.verify_backup(tmp_path / "no-such-backup")
    assert report.ok is False
    assert any("备份目录不存在" in p for p in report.problems)


# ---------------------------------------------------------------- CLI


def _run_cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "app.backup", *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_cli_create_and_verify(tmp_path):
    src = tmp_path / "data" / "samples.db"
    root = tmp_path / "backups"
    conn = _make_source(src)
    conn.close()

    r = _run_cli("create", "--db", str(src), "--backup-dir", str(root))
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out["ok"] is True
    assert out["database_file"] == "samples.db"
    assert out["integrity_check"] == ["ok"]
    assert out["foreign_key_check_violations"] == []
    backup_dir = out["backup_dir"]

    r = _run_cli("verify", backup_dir)
    assert r.returncode == 0, r.stderr
    body = json.loads(r.stdout)
    assert body["ok"] is True
    assert body["expected_sha256"] == body["actual_sha256"]

    # 不带路径:复核最新一次备份
    r = _run_cli("verify", "--backup-dir", str(root))
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout)["ok"] is True

    # --all:复核全部
    r = _run_cli("verify", "--all", "--backup-dir", str(root))
    assert r.returncode == 0, r.stderr
    body = json.loads(r.stdout)
    assert body["ok"] is True and body["checked"] == 1

    # 篡改后复核:退出码非 0
    db_file = Path(backup_dir) / "samples.db"
    with db_file.open("ab") as f:
        f.write(b"x")
    r = _run_cli("verify", backup_dir)
    assert r.returncode == 1
    assert json.loads(r.stdout)["ok"] is False


def test_cli_create_missing_db_exit_code(tmp_path):
    r = _run_cli("create", "--db", str(tmp_path / "missing.db"),
                 "--backup-dir", str(tmp_path / "backups"))
    assert r.returncode == 1
    assert json.loads(r.stdout)["ok"] is False
