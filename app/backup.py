"""容器内 SQLite 在线备份与复核。

备份流程(见 `create_backup`):
1. 在备份根目录下创建隐藏临时目录(与发布目录同一文件系统,保证 rename 原子);
2. 用 SQLite online backup API 把**运行中**的数据库一致性快照拷到临时目录,
   业务请求可同时写入(分页拷贝,遇写入自动重试,快照取自同一时刻);
3. 对副本执行 `PRAGMA integrity_check` 与 `PRAGMA foreign_key_check`;
4. 通过后计算副本 SHA-256,写入含创建时间 / 哈希 / 数据库文件名的 JSON 清单;
5. `os.replace` 原子发布为唯一备份目录(时间戳命名)。
任一步失败都删除整个临时目录,不会留下可能被误用的半成品。

`verify_backup` 按清单复核:文件齐备、哈希一致、integrity_check 与
foreign_key_check 通过。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .config import BACKUP_DIR, DB_PATH

MANIFEST_NAME = "backup_manifest.json"
_TMP_PREFIX = ".backup-tmp-"

# online backup 每批拷贝的页数与批次间休眠:分页拷贝允许业务请求同时写入,
# 检测到源库在两批之间被写入时 SQLite 自动从已变更页重新拷贝
_BACKUP_PAGES = 200
_BACKUP_SLEEP = 0.05
_SHA256_CHUNK = 1024 * 1024


class BackupError(Exception):
    """备份失败:副本未通过校验或前置条件不满足;临时目录已被清理。"""


class VerificationError(Exception):
    """备份复核失败:文件缺失/哈希不符/SQLite 校验不通过/清单非法。"""


@dataclass
class CheckResult:
    integrity: list[str]          # PRAGMA integrity_check 的全部输出
    foreign_key_violations: list[dict]  # PRAGMA foreign_key_check 的违例行


@dataclass
class BackupResult:
    backup_dir: Path
    db_filename: str
    sha256: str
    created_at: str
    pages: int
    checks: CheckResult
    progress: list[str] = field(default_factory=list)


@dataclass
class VerificationReport:
    backup_dir: Path
    ok: bool
    db_filename: str | None = None
    expected_sha256: str | None = None
    actual_sha256: str | None = None
    created_at: str | None = None
    checks: CheckResult | None = None
    problems: list[str] = field(default_factory=list)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(_SHA256_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def _run_checks(db_path: Path) -> CheckResult:
    """对备份副本执行 integrity_check 与 foreign_key_check。

    foreign_keys 对 foreign_key_check 无影响(该 PRAGMA 总是按表定义检查),
    但显式开启以与业务连接保持同一口径。
    """
    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        integrity = [r[0] for r in conn.execute("PRAGMA integrity_check")]
        fk_rows = [
            {"table": r[0], "rowid": r[1], "parent": r[2], "fkid": r[3]}
            for r in conn.execute("PRAGMA foreign_key_check")
        ]
    finally:
        conn.close()
    return CheckResult(integrity=integrity, foreign_key_violations=fk_rows)


def _checks_ok(checks: CheckResult) -> list[str]:
    """返回校验问题列表;空列表表示通过。"""
    problems: list[str] = []
    if checks.integrity != ["ok"]:
        problems.append(
            "integrity_check 未返回 ok: " + "; ".join(checks.integrity[:20])
        )
    if checks.foreign_key_violations:
        problems.append(
            "foreign_key_check 发现 "
            f"{len(checks.foreign_key_violations)} 条外键违例"
        )
    return problems


def create_backup(
    db_path: str | Path | None = None,
    backup_root: str | Path | None = None,
) -> BackupResult:
    """对运行中的数据库生成一致性快照备份并原子发布。

    备份期间业务请求可继续写入;任何失败都清理临时目录并抛 `BackupError`,
    备份根目录下不会残留半成品。
    """
    src_path = Path(db_path or DB_PATH)
    root = Path(backup_root or BACKUP_DIR)
    if not src_path.is_file():
        raise BackupError(f"源数据库文件不存在: {src_path}")
    root.mkdir(parents=True, exist_ok=True)

    db_filename = src_path.name  # 仅取文件名,清单中不泄露源路径
    created_at = _utc_now()
    stamp = created_at.strftime("%Y%m%dT%H%M%S")
    unique = f"{stamp}Z-{os.getpid()}-{uuid.uuid4().hex[:12]}"
    final_dir = root / f"backup-{unique}"
    tmp_dir = root / f"{_TMP_PREFIX}{unique}"
    tmp_dir.mkdir(mode=0o755)

    try:
        tmp_db = tmp_dir / db_filename
        progress_log: list[str] = []

        # --- 第 1 步:online backup API 拷贝一致性快照(允许并发写入)---
        source = sqlite3.connect(str(src_path), timeout=30)
        destination = sqlite3.connect(str(tmp_db), timeout=30)
        try:
            def _log_progress(status: int, remaining: int, total: int) -> None:
                progress_log.append(
                    f"backup status={status} remaining={remaining} total={total}"
                )

            source.backup(
                destination,
                pages=_BACKUP_PAGES,
                progress=_log_progress,
                sleep=_BACKUP_SLEEP,
            )
        finally:
            destination.close()
            source.close()

        # 副本固定为 rollback journal 模式并执行 WAL 检查点:
        # 保证发布物只有单个自包含 .db 文件(无 -wal/-shm/-journal 旁挂文件),
        # 哈希口径唯一,复核/恢复时不会误读旁挂文件
        chk_conn = sqlite3.connect(str(tmp_db), timeout=30)
        try:
            mode = chk_conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            if str(mode).lower() != "delete":
                raise BackupError(f"副本无法切换到 delete 日志模式: {mode}")
            chk_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            chk_conn.close()
        _assert_no_sidecars(tmp_db)

        # --- 第 2 步:对副本执行完整性与外键校验 ---
        checks = _run_checks(tmp_db)
        problems = _checks_ok(checks)
        if problems:
            raise BackupError("备份副本校验失败: " + "; ".join(problems))

        # --- 第 3 步:SHA-256 与 JSON 清单 ---
        digest = _sha256_file(tmp_db)
        manifest = {
            "created_at": created_at.strftime("%Y-%m-%dT%H:%M:%S.")
            + f"{created_at.microsecond:06d}Z",
            "database_file": db_filename,
            "sha256": digest,
        }
        manifest_path = tmp_dir / MANIFEST_NAME
        _atomic_write_json(manifest_path, manifest)

        # --- 第 4 步:原子发布(临时目录与发布目录在同一文件系统)---
        if final_dir.exists():  # 极端情况下名字撞车,交由重试而不是覆盖
            raise BackupError(f"目标备份目录已存在: {final_dir}")
        os.replace(tmp_dir, final_dir)
    except BaseException:
        # 失败即清理:不留下可误用的成品(临时目录也一并消失)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    return BackupResult(
        backup_dir=final_dir,
        db_filename=db_filename,
        sha256=digest,
        created_at=manifest["created_at"],
        pages=len(progress_log),
        checks=checks,
        progress=progress_log,
    )


def _assert_no_sidecars(db_file: Path) -> None:
    leftovers = [
        db_file.with_name(db_file.name + suffix)
        for suffix in ("-wal", "-shm", "-journal")
        if db_file.with_name(db_file.name + suffix).exists()
    ]
    if leftovers:
        raise BackupError(
            "副本切换日志模式后仍存在旁挂事务文件: "
            + ", ".join(p.name for p in leftovers)
        )


def _atomic_write_json(path: Path, payload: dict) -> None:
    """先写同目录临时文件再 rename,保证清单本身也是原子落盘。"""
    fd, tmp_name = tempfile.mkstemp(
        prefix=".manifest-", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def verify_backup(backup_dir: str | Path) -> VerificationReport:
    """按清单复核一个备份目录:清单合法、文件齐备、哈希一致、SQLite 校验通过。"""
    directory = Path(backup_dir)
    report = VerificationReport(backup_dir=directory, ok=False)

    if not directory.is_dir():
        report.problems.append(f"备份目录不存在或不是目录: {directory}")
        return report

    manifest_path = directory / MANIFEST_NAME
    if not manifest_path.is_file():
        report.problems.append(f"缺少清单文件: {manifest_path.name}")
        return report

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        report.problems.append(f"清单无法解析: {exc}")
        return report

    for key in ("created_at", "database_file", "sha256"):
        if not isinstance(manifest.get(key), str) or not manifest[key]:
            report.problems.append(f"清单字段缺失或非法: {key}")
    if report.problems:
        return report

    db_filename = manifest["database_file"]
    expected_hash = manifest["sha256"]
    report.db_filename = db_filename
    report.expected_sha256 = expected_hash
    report.created_at = manifest["created_at"]

    # 数据库文件名必须是纯文件名,防止清单把复核指向目录之外
    if Path(db_filename).name != db_filename or db_filename in ("", ".", ".."):
        report.problems.append(f"清单中的数据库文件名非法: {db_filename!r}")
        return report

    db_file = directory / db_filename
    if not db_file.is_file():
        report.problems.append(f"缺少备份数据库文件: {db_filename}")
        return report

    # 旁挂文件意味着备份并非自包含(哈希只覆盖主库文件),拒绝判定为完好
    for suffix in ("-wal", "-shm", "-journal"):
        side = directory / (db_filename + suffix)
        if side.exists():
            report.problems.append(f"备份目录存在旁挂事务文件: {side.name}")

    actual_hash = _sha256_file(db_file)
    report.actual_sha256 = actual_hash
    if actual_hash != expected_hash:
        report.problems.append(
            f"SHA-256 不一致:清单记录 {expected_hash},实际 {actual_hash}"
        )

    # 哈希通过才对副本做 SQLite 校验(损坏文件可能无法打开)
    if not report.problems:
        try:
            checks = _run_checks(db_file)
        except sqlite3.DatabaseError as exc:
            report.problems.append(f"备份数据库无法打开: {exc}")
            return report
        report.checks = checks
        report.problems.extend(_checks_ok(checks))

    report.ok = not report.problems
    return report


def list_backups(backup_root: str | Path | None = None) -> list[Path]:
    """列出备份根目录下已发布(非临时)的备份目录,按创建时间正序。"""
    root = Path(backup_root or BACKUP_DIR)
    if not root.is_dir():
        return []
    dirs = [
        p
        for p in root.iterdir()
        if p.is_dir() and p.name.startswith("backup-") and (p / MANIFEST_NAME).is_file()
    ]
    return sorted(dirs, key=lambda p: p.name)


def _print_json(payload: dict) -> None:
    json.dump(payload, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m app.backup",
        description="SQLite 样品追溯库在线备份与按清单复核",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_create = sub.add_parser("create", help="在线生成一致性快照备份并原子发布")
    p_create.add_argument("--db", default=None, help=f"源数据库路径(默认 {DB_PATH})")
    p_create.add_argument(
        "--backup-dir", default=None, help=f"备份根目录(默认 {BACKUP_DIR})"
    )

    p_verify = sub.add_parser("verify", help="按清单复核备份完整性与哈希")
    p_verify.add_argument(
        "path",
        nargs="?",
        default=None,
        help="备份目录;省略则复核备份根目录下最新一次备份",
    )
    p_verify.add_argument(
        "--backup-dir", default=None, help=f"备份根目录(默认 {BACKUP_DIR})"
    )
    p_verify.add_argument(
        "--all", action="store_true", help="复核备份根目录下的全部备份"
    )

    args = parser.parse_args(argv)

    if args.command == "create":
        try:
            result = create_backup(args.db, args.backup_dir)
        except BackupError as exc:
            _print_json({"ok": False, "error": str(exc)})
            return 1
        _print_json(
            {
                "ok": True,
                "backup_dir": str(result.backup_dir),
                "database_file": result.db_filename,
                "sha256": result.sha256,
                "created_at": result.created_at,
                "integrity_check": result.checks.integrity,
                "foreign_key_check_violations": result.checks.foreign_key_violations,
            }
        )
        return 0

    # verify
    root = args.backup_dir or BACKUP_DIR
    if args.all:
        targets = list_backups(root)
        if not targets:
            _print_json({"ok": False, "error": f"备份根目录下没有备份: {root}"})
            return 1
        reports = [verify_backup(t) for t in targets]
        _print_json(
            {
                "ok": all(r.ok for r in reports),
                "checked": len(reports),
                "results": [
                    {
                        "backup_dir": str(r.backup_dir),
                        "ok": r.ok,
                        "database_file": r.db_filename,
                        "created_at": r.created_at,
                        "expected_sha256": r.expected_sha256,
                        "actual_sha256": r.actual_sha256,
                        "problems": r.problems,
                    }
                    for r in reports
                ],
            }
        )
        return 0 if all(r.ok for r in reports) else 1

    target = args.path
    if target is None:
        published = list_backups(root)
        if not published:
            _print_json({"ok": False, "error": f"备份根目录下没有备份: {root}"})
            return 1
        target = published[-1]

    report = verify_backup(target)
    payload = {
        "ok": report.ok,
        "backup_dir": str(report.backup_dir),
        "database_file": report.db_filename,
        "created_at": report.created_at,
        "expected_sha256": report.expected_sha256,
        "actual_sha256": report.actual_sha256,
        "integrity_check": report.checks.integrity if report.checks else None,
        "foreign_key_check_violations": (
            report.checks.foreign_key_violations if report.checks else None
        ),
        "problems": report.problems,
    }
    _print_json(payload)
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
