"""在线备份与按清单复核的端到端测试。

覆盖:SQLite backup API 在线复制(WAL、复制期间并发写入)、
integrity_check / foreign_key_check 门禁、SHA-256 与 manifest、
原子发布(唯一目录、无临时残留)、失败清理与篡改/损坏复核失败。
"""
from __future__ import annotations

import json
import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

from app import backup


def _make_source(db_path: Path) -> sqlite3.Connection:
    """建一个带外键约束和少量数据的 WAL 库,模拟运行中的业务库。"""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(
        """
        CREATE TABLE parents (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE children (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER NOT NULL,
            label TEXT,
            FOREIGN KEY (parent_id) REFERENCES parents(id)
        );
        INSERT INTO parents (id, name) VALUES (1, '父样品'), (2, '备用父');
        INSERT INTO children (id, parent_id, label) VALUES (1, 1, '子样品-1');
        """
    )
    conn.commit()
    return conn


@pytest.fixture()
def env(tmp_path):
    db_path = tmp_path / "data" / "samples.db"
    backup_root = tmp_path / "backups"
    source = _make_source(db_path)
    yield db_path, backup_root, source
    source.close()


# ---------------------------------------------------------------- 创建与复核

def test_create_backup_writes_copy_manifest_and_verifies_ok(env):
    db_path, backup_root, source = env
    result = backup.create_backup(db_path, backup_root)

    # 原子发布:备份根目录下只有一个已发布目录,无 .staging 残留
    entries = sorted(backup_root.iterdir())
    assert entries == [result.directory]
    assert not any(p.name.startswith(".staging") for p in backup_root.iterdir())
    assert result.directory.name.startswith("samples-")

    db_file = result.directory / "samples.db"
    assert db_file.is_file()
    # 备份目录只含数据库副本与清单(WAL 旁车已在检查点后清除)
    assert sorted(p.name for p in result.directory.iterdir()) == [
        "manifest.json",
        "samples.db",
    ]

    manifest = json.loads((result.directory / "manifest.json").read_text("utf-8"))
    assert manifest["database_file"] == "samples.db"
    assert manifest["sha256"] == backup._sha256_file(db_file)
    assert len(manifest["sha256"]) == 64
    assert manifest["database_size_bytes"] == db_file.stat().st_size
    assert manifest["integrity_check"] == "ok"
    assert manifest["foreign_key_check"] == {"status": "ok", "violations": 0}
    assert "created_at" in manifest and manifest["created_at"].endswith("Z")

    # 副本可独立打开,数据与源一致
    ro = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)
    try:
        assert ro.execute("SELECT name FROM parents ORDER BY id").fetchall() == [
            ("父样品",),
            ("备用父",),
        ]
        assert ro.execute("PRAGMA journal_mode").fetchone()[0].lower() == "delete"
    finally:
        ro.close()

    verify = backup.verify_backup(result.directory)
    assert verify.ok is True, verify.problems
    assert verify.actual_sha256 == manifest["sha256"]
    assert backup.list_backups(backup_root) == [result.directory]


def test_backup_is_consistent_under_concurrent_writes(env):
    db_path, backup_root, source = env
    source.execute("PRAGMA busy_timeout = 30000")

    stop = threading.Event()

    def writer():
        # 备份进行中持续写入业务数据并提交
        i = 0
        while not stop.is_set():
            source.execute(
                "INSERT INTO children (parent_id, label) VALUES (2, ?)",
                (f"并发写入-{i}",),
            )
            source.commit()
            i += 1
            time.sleep(0.001)

    t = threading.Thread(target=writer)
    t.start()
    time.sleep(0.05)  # 确保备份开始前业务写入已经在持续提交
    try:
        result = backup.create_backup(db_path, backup_root)
    finally:
        stop.set()
        t.join()

    verify = backup.verify_backup(result.directory)
    assert verify.ok is True, verify.problems

    # backup API 给出的是某个提交点的一致性快照:子表外键全部有效、无撕裂
    ro = sqlite3.connect(
        f"file:{result.directory / 'samples.db'}?mode=ro", uri=True
    )
    try:
        violations = ro.execute("PRAGMA foreign_key_check").fetchall()
        assert violations == []
        count = ro.execute("SELECT COUNT(*) FROM children").fetchone()[0]
        # 至少有一些并发写入落进了快照(证明备份期间业务确实在写)
        assert count >= 1
    finally:
        ro.close()

    # 源库备份后仍可正常写入(在线备份未影响业务)
    source.execute("INSERT INTO children (parent_id, label) VALUES (1, '备份后写入')")
    source.commit()
    assert source.execute(
        "SELECT label FROM children ORDER BY id DESC LIMIT 1"
    ).fetchone() == ("备份后写入",)


def test_each_backup_gets_unique_directory_and_lists_newest_first(env):
    db_path, backup_root, _ = env
    first = backup.create_backup(db_path, backup_root)
    time.sleep(1.01)  # 时间戳精确到秒,靠 UUID + 时间戳保证唯一
    second = backup.create_backup(db_path, backup_root)
    assert first.directory != second.directory
    assert backup.list_backups(backup_root) == [second.directory, first.directory]


# ---------------------------------------------------------------- 失败清理

def test_foreign_key_violation_aborts_and_cleans_staging(env, tmp_path):
    db_path, backup_root, source = env
    # 关闭外键强制写入一条悬空外键(foreign_key_check 必须报出)
    source.execute("PRAGMA foreign_keys = OFF")
    source.execute("INSERT INTO children (id, parent_id, label) VALUES (99, 999, '悬空')")
    source.commit()
    source.execute("PRAGMA foreign_keys = ON")

    with pytest.raises(backup.BackupError, match="foreign_key_check"):
        backup.create_backup(db_path, backup_root)

    # 失败后备份根目录为空:临时目录已清理,没有半成品成品
    assert list(backup_root.iterdir()) == []
    assert backup.list_backups(backup_root) == []


def test_corrupt_database_file_aborts_and_cleans_staging(tmp_path):
    db_path = tmp_path / "data" / "broken.db"
    backup_root = tmp_path / "backups"
    db_path.parent.mkdir(parents=True)
    db_path.write_bytes(b"this is definitely not a sqlite database")

    with pytest.raises(backup.BackupError, match="在线复制失败"):
        backup.create_backup(db_path, backup_root)

    assert backup_root.exists()
    assert list(backup_root.iterdir()) == []


def test_missing_source_database_raises(tmp_path):
    with pytest.raises(backup.BackupError, match="源数据库不存在"):
        backup.create_backup(tmp_path / "nope.db", tmp_path / "backups")
    # 连根目录都未创建失败产物(此处会建空根目录,但无任何备份内容)
    assert not (tmp_path / "nope.db").exists()


# ---------------------------------------------------------------- 复核失败

def test_verify_detects_tampered_database(env):
    db_path, backup_root, _ = env
    result = backup.create_backup(db_path, backup_root)
    assert backup.verify_backup(result.directory).ok

    # 篡改备份库:哈希与清单不符,且 journal_mode 等不受影响(纯内容变更)
    db_file = result.directory / "samples.db"
    data = bytearray(db_file.read_bytes())
    # 翻转靠后某个非零页字节,尽量避开文件头
    for idx in range(len(data) - 1, 4096, -1):
        if data[idx] != 0:
            data[idx] ^= 0xFF
            break
    db_file.write_bytes(bytes(data))

    verify = backup.verify_backup(result.directory)
    assert verify.ok is False
    assert any("SHA-256" in p for p in verify.problems)
    assert verify.actual_sha256 != verify.expected_sha256


def test_verify_reports_missing_manifest_and_database(env):
    db_path, backup_root, _ = env
    result = backup.create_backup(db_path, backup_root)
    (result.directory / "manifest.json").unlink()
    verify = backup.verify_backup(result.directory)
    assert verify.ok is False
    assert any("缺少清单" in p for p in verify.problems)

    # 重新造一份,删掉数据库文件
    result2 = backup.create_backup(db_path, backup_root)
    (result2.directory / "samples.db").unlink()
    verify2 = backup.verify_backup(result2.directory)
    assert verify2.ok is False
    assert any("数据库文件不存在" in p for p in verify2.problems)


def test_verify_rejects_path_traversal_in_manifest(env):
    db_path, backup_root, _ = env
    result = backup.create_backup(db_path, backup_root)
    manifest_path = result.directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text("utf-8"))
    manifest["database_file"] = "../../data/samples.db"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verify = backup.verify_backup(result.directory)
    assert verify.ok is False
    assert any("逃逸备份目录" in p for p in verify.problems)


def test_verify_all_reports_each_backup(env):
    db_path, backup_root, _ = env
    ok_dir = backup.create_backup(db_path, backup_root).directory
    bad_dir = backup.create_backup(db_path, backup_root).directory
    # 破坏坏备份的数据库文件(保留清单,否则不会出现在已发布备份列表中)
    (bad_dir / "samples.db").unlink()

    results = {r.backup_dir: r for r in backup.verify_all(backup_root)}
    assert results[ok_dir].ok is True
    assert results[bad_dir].ok is False


# ---------------------------------------------------------------- CLI

def test_cli_create_verify_list_and_failure_exit_codes(env, capsys):
    db_path, backup_root, _ = env

    assert backup.main(["create", "--db", str(db_path),
                        "--backup-dir", str(backup_root)]) == 0
    out = capsys.readouterr().out
    assert "integrity_check: ok" in out
    assert "已原子发布" in out

    assert backup.main(["list", "--backup-dir", str(backup_root)]) == 0
    listed = capsys.readouterr().out.strip().splitlines()
    target = listed[0]

    # 默认复核最新一份
    assert backup.main(["verify", "--backup-dir", str(backup_root)]) == 0
    assert target in capsys.readouterr().out

    # --all 复核全部
    assert backup.main(["verify", "--all", "--backup-dir", str(backup_root)]) == 0

    # 破坏唯一备份后,复核返回非零退出码
    (Path(target) / "samples.db").unlink()
    assert backup.main(["verify", "--all", "--backup-dir", str(backup_root)]) == 1

    # 空备份目录复核 / 列表
    empty = backup_root.parent / "empty-backups"
    assert backup.main(["verify", "--all", "--backup-dir", str(empty)]) == 1
    assert backup.main(["list", "--backup-dir", str(empty)]) == 0


def test_module_entrypoint_exits_nonzero_on_failure(monkeypatch, tmp_path):
    # `python -m app.backup verify --all` 在空目录时以退出码 1 结束
    import runpy

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "app.backup",
            "verify",
            "--all",
            "--backup-dir",
            str(tmp_path / "empty"),
        ],
    )
    with pytest.raises(SystemExit) as exc:
        runpy.run_module("app.backup", run_name="__main__")
    assert exc.value.code == 1
