"""SQLite 在线备份与备份复核(容器内命令行)。

备份(create):
- 使用 SQLite 在线 backup API 把**正在运行**的数据库分页复制到临时目录,
  复制期间业务请求可以继续读写(分页之间让出锁,遇 BUSY 重试);
- 对副本执行 PRAGMA integrity_check 与 PRAGMA foreign_key_check;
- 全部通过后计算副本 SHA-256,写入 manifest.json(创建时间、哈希、数据库文件名等);
- 最后用一次原子 rename 把临时目录发布为唯一备份目录;
- 任一步失败都清理整个临时目录,不会在备份根目录留下半成品或可误用的成品。

复核(verify):按 manifest.json 重新计算 SHA-256 并对备份库再跑
integrity_check / foreign_key_check,确认备份完整且哈希与清单一致。

容器内用法:
    python -m app.backup create
    python -m app.backup verify            # 复核最新一份
    python -m app.backup verify --all      # 复核全部备份
    python -m app.backup list
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
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .config import BACKUP_DIR, DB_PATH

MANIFEST_NAME = "manifest.json"
_MANIFEST_TMP_NAME = ".manifest.tmp"
_FORMAT_VERSION = 1
# 每次 backup_step 复制的页数;分页复制并在页批之间休眠,使业务写请求可插入提交
_BACKUP_PAGES = 256
_BACKUP_SLEEP_SECONDS = 0.05
# SHA-256 分块(1 MiB)
_HASH_CHUNK = 1024 * 1024


class BackupError(Exception):
    """备份失败:副本未通过校验或复制出错(临时目录已被清理)。"""


@dataclass
class BackupResult:
    directory: Path
    created_at: str
    database_file: str
    sha256: str
    size_bytes: int


@dataclass
class VerifyResult:
    backup_dir: Path
    ok: bool
    created_at: str | None = None
    database_file: str | None = None
    expected_sha256: str | None = None
    actual_sha256: str | None = None
    problems: list[str] = field(default_factory=list)


# ---------------------------------------------------------------- 时间 / 哈希

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_z(dt: datetime) -> str:
    """ISO 8601 UTC,固定 6 位微秒、Z 结尾(与库内时间戳风格一致)。"""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond:06d}Z"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(_HASH_CHUNK)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------- 在线复制

def _online_copy(source_path: Path, dest_path: Path) -> None:
    """用 SQLite backup API 分页复制在线数据库到 dest_path。

    源连接以只读方式打开(不触碰运行库的 WAL/日志),页批之间休眠让出锁,
    遇 BUSY/LOCKED 休眠重试——复制全过程中其他连接(业务请求)可以正常提交写入。
    兼容两种绑定:
    - Python 3.11:Connection.backup(target, pages=N, sleep=T) 内建分页循环
      并对 SQLITE_BUSY/LOCKED 自动重试,调用即完成;
    - Python 3.12+:Connection.backup(target) 返回 Backup 对象,需自行 step()
      分页推进(在 step 之间休眠,busy 时重试同一步)。
    """
    source_uri = source_path.resolve().as_uri() + "?mode=ro"
    source = sqlite3.connect(source_uri, uri=True, timeout=30)
    try:
        dest = sqlite3.connect(str(dest_path), timeout=30)
        try:
            if hasattr(sqlite3, "Backup"):  # Python 3.12+
                backup_obj = source.backup(dest)
                try:
                    while not backup_obj.done:
                        while True:
                            try:
                                backup_obj.step(_BACKUP_PAGES)
                                break
                            except sqlite3.OperationalError as exc:
                                # BUSY/LOCKED 可重试:备份对象仍有效,休眠后重试同一步
                                message = str(exc).lower()
                                if "locked" in message or "busy" in message:
                                    time.sleep(_BACKUP_SLEEP_SECONDS)
                                    continue
                                raise
                        if not backup_obj.done:
                            # 页批之间主动让出,给业务写请求留出提交窗口
                            time.sleep(_BACKUP_SLEEP_SECONDS)
                finally:
                    backup_obj.finish()
            else:  # Python 3.11:内建分页推进 + busy 自动重试
                source.backup(dest, pages=_BACKUP_PAGES, sleep=_BACKUP_SLEEP_SECONDS)
        except sqlite3.Error as exc:
            # 源库无法识别 / 复制中断等统一转为 BackupError,由上层清理临时目录
            raise BackupError(f"在线复制失败(SQLite backup API): {exc}") from exc
        finally:
            dest.close()
    finally:
        source.close()


def _check_copy(db_path: Path) -> None:
    """对备份副本执行 integrity_check 与 foreign_key_check,不通过即抛 BackupError。

    同时把副本切换为 DELETE 日志模式并做一次检查点,保证备份目录中只有
    单一 .db 文件(无 -wal/-shm 旁车),哈希覆盖的就是完整数据库。
    """
    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        mode = conn.execute("PRAGMA journal_mode = DELETE").fetchone()[0]
        if str(mode).lower() != "delete":
            raise BackupError(
                f"备份副本无法切换为 DELETE 日志模式(实际 {mode}),目录可能残留 WAL 旁车"
            )
        integrity_rows = [
            r[0] for r in conn.execute("PRAGMA integrity_check").fetchall()
        ]
        if integrity_rows != ["ok"]:
            raise BackupError(
                "integrity_check 未通过:" + "; ".join(map(str, integrity_rows[:20]))
            )
        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if fk_violations:
            detail = "; ".join(
                "表 {0} rowid={1} 父表 {2} fkid={3}".format(*row)
                for row in fk_violations[:20]
            )
            raise BackupError(
                f"foreign_key_check 发现 {len(fk_violations)} 项外键违例:{detail}"
            )
    finally:
        conn.close()


# ---------------------------------------------------------------- 创建备份

def create_backup(
    db_path: str | Path | None = None,
    backup_root: str | Path | None = None,
) -> BackupResult:
    """生成一份一致性备份并原子发布,返回备份结果;失败清理临时目录并抛 BackupError。"""
    source_path = Path(db_path or DB_PATH)
    root = Path(backup_root or BACKUP_DIR)
    if not source_path.is_file():
        raise BackupError(f"源数据库不存在: {source_path}")
    root.mkdir(parents=True, exist_ok=True)

    database_file = source_path.name
    # 临时目录与最终目录在同一文件系统(备份根目录)内,rename 才是原子的
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=str(root)))
    try:
        staging.chmod(0o755)
        dest_db = staging / database_file

        _online_copy(source_path, dest_db)
        _check_copy(dest_db)

        sha = _sha256_file(dest_db)
        size_bytes = dest_db.stat().st_size
        created_at = _isoformat_z(_utc_now())
        stamp = _utc_now().strftime("%Y%m%dT%H%M%SZ")
        backup_name = f"{source_path.stem}-{stamp}-{uuid.uuid4().hex[:8]}"

        manifest = {
            "format_version": _FORMAT_VERSION,
            "created_at": created_at,
            "database_file": database_file,
            "database_size_bytes": size_bytes,
            "sha256": sha,
            "integrity_check": "ok",
            "foreign_key_check": {"status": "ok", "violations": 0},
            "source_database_path": str(source_path),
        }
        # 清单先写临时文件再原子改名,避免发布瞬间出现半个清单
        manifest_tmp = staging / _MANIFEST_TMP_NAME
        with manifest_tmp.open("w", encoding="utf-8") as fh:
            json.dump(manifest, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(manifest_tmp, staging / MANIFEST_NAME)

        final_dir = root / backup_name
        os.replace(staging, final_dir)  # 同文件系统原子发布;UUID 保证名称唯一
        return BackupResult(
            directory=final_dir,
            created_at=created_at,
            database_file=database_file,
            sha256=sha,
            size_bytes=size_bytes,
        )
    except BaseException:
        # 任何失败都彻底清理临时目录,绝不留下可被误用的半成品
        shutil.rmtree(staging, ignore_errors=True)
        raise


# ---------------------------------------------------------------- 复核

def _read_manifest(backup_dir: Path) -> tuple[dict | None, list[str]]:
    problems: list[str] = []
    manifest_path = backup_dir / MANIFEST_NAME
    if not manifest_path.is_file():
        return None, [f"缺少清单文件 {MANIFEST_NAME}"]
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, [f"清单无法解析: {exc}"]
    if not isinstance(manifest, dict):
        return None, ["清单格式错误:顶层不是 JSON 对象"]
    for key in ("created_at", "database_file", "sha256"):
        if key not in manifest:
            problems.append(f"清单缺少必需字段: {key}")
    return manifest, problems


def _safe_database_path(backup_dir: Path, database_file: object) -> Path | None:
    """清单中的 database_file 只允许是备份目录内的普通文件名,禁止任何路径逃逸。"""
    if not isinstance(database_file, str) or not database_file:
        return None
    candidate = Path(database_file)
    if candidate.is_absolute() or len(candidate.parts) != 1 or candidate.parts[0] in (
        ".",
        "..",
    ):
        return None
    resolved = (backup_dir / candidate).resolve()
    try:
        resolved.relative_to(backup_dir.resolve())
    except ValueError:
        return None
    return resolved


def verify_backup(backup_dir: str | Path) -> VerifyResult:
    """按清单复核单份备份:哈希一致且 integrity_check / foreign_key_check 通过。"""
    backup_dir = Path(backup_dir)
    result = VerifyResult(backup_dir=backup_dir, ok=False)
    if not backup_dir.is_dir():
        result.problems.append("备份目录不存在或不是目录")
        return result

    manifest, problems = _read_manifest(backup_dir)
    result.problems.extend(problems)
    if manifest is None:
        return result

    result.created_at = manifest.get("created_at")
    result.database_file = manifest.get("database_file")
    expected = manifest.get("sha256")
    result.expected_sha256 = expected if isinstance(expected, str) else None
    if not isinstance(expected, str) or len(expected) != 64:
        result.problems.append("清单中的 sha256 不是 64 位十六进制字符串")
        expected = None

    db_path = _safe_database_path(backup_dir, manifest.get("database_file"))
    if db_path is None:
        result.problems.append(
            f"清单中的 database_file 非法或试图逃逸备份目录: {manifest.get('database_file')!r}"
        )
        return result
    if not db_path.is_file():
        result.problems.append(f"清单指向的数据库文件不存在: {db_path.name}")
        return result

    actual = _sha256_file(db_path)
    result.actual_sha256 = actual
    if expected is not None and actual != expected:
        result.problems.append(
            f"SHA-256 与清单不一致:清单 {expected},实际 {actual}(文件可能已损坏或被篡改)"
        )

    size_manifest = manifest.get("database_size_bytes")
    if isinstance(size_manifest, int) and size_manifest != db_path.stat().st_size:
        result.problems.append(
            f"文件大小与清单不一致:清单 {size_manifest} 字节,实际 {db_path.stat().st_size} 字节"
        )

    # 哈希之外再独立跑一遍完整性 / 外键检查(只读打开,绝不修改备份)
    try:
        ro = sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            integrity_rows = [
                r[0] for r in ro.execute("PRAGMA integrity_check").fetchall()
            ]
            if integrity_rows != ["ok"]:
                result.problems.append(
                    "integrity_check 未通过:" + "; ".join(map(str, integrity_rows[:20]))
                )
            fk_violations = ro.execute("PRAGMA foreign_key_check").fetchall()
            if fk_violations:
                result.problems.append(
                    f"foreign_key_check 发现 {len(fk_violations)} 项外键违例"
                )
        finally:
            ro.close()
    except sqlite3.DatabaseError as exc:
        result.problems.append(f"备份库无法打开: {exc}")

    result.ok = not result.problems
    return result


def list_backups(backup_root: str | Path | None = None) -> list[Path]:
    """按备份名(时间戳)倒序列出全部已发布备份目录。"""
    root = Path(backup_root or BACKUP_DIR)
    if not root.is_dir():
        return []
    return sorted(
        (p for p in root.iterdir() if (p / MANIFEST_NAME).is_file()),
        reverse=True,
    )


def verify_all(backup_root: str | Path | None = None) -> list[VerifyResult]:
    return [verify_backup(p) for p in list_backups(backup_root)]


# ---------------------------------------------------------------- CLI

def _print_result(result: BackupResult) -> None:
    print(f"[backup] 在线备份(SQLite backup API,允许并发写入): {result.database_file}")
    print("[backup] integrity_check: ok;foreign_key_check: 0 项违例")
    print(f"[backup] SHA-256: {result.sha256}")
    print(f"[backup] 大小: {result.size_bytes} 字节;创建时间: {result.created_at}")
    print(f"[backup] 已原子发布: {result.directory}")


def _print_verify(result: VerifyResult) -> None:
    if result.ok:
        print(
            f"[verify] OK   {result.backup_dir}"
            f"  数据库={result.database_file}"
            f"  创建时间={result.created_at}"
            f"  sha256={result.actual_sha256}"
        )
    else:
        print(f"[verify] FAIL {result.backup_dir}", file=sys.stderr)
        for problem in result.problems:
            print(f"[verify]   - {problem}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--backup-dir",
        default=BACKUP_DIR,
        help=f"备份根目录(默认 {BACKUP_DIR},或环境变量 SAMPLE_BACKUP_DIR)",
    )

    parser = argparse.ArgumentParser(
        prog="python -m app.backup",
        description="SQLite 在线一致性备份与按清单复核(容器内运行)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_create = sub.add_parser("create", parents=[common], help="创建一份在线一致性备份")
    p_create.add_argument(
        "--db",
        default=DB_PATH,
        help=f"源数据库路径(默认 {DB_PATH},或环境变量 SAMPLE_DB_PATH)",
    )

    p_verify = sub.add_parser(
        "verify", parents=[common], help="按清单复核备份的完整性与哈希"
    )
    p_verify.add_argument(
        "path",
        nargs="?",
        default=None,
        help="备份目录;省略时复核最新一份(--all 时复核全部)",
    )
    p_verify.add_argument("--all", action="store_true", help="复核备份根目录下全部备份")

    sub.add_parser("list", parents=[common], help="列出全部备份(新的在前)")

    args = parser.parse_args(argv)

    if args.command == "create":
        try:
            result = create_backup(args.db, args.backup_dir)
        except BackupError as exc:
            print(f"[backup] 失败,临时目录已清理: {exc}", file=sys.stderr)
            return 1
        _print_result(result)
        return 0

    if args.command == "list":
        backups = list_backups(args.backup_dir)
        if not backups:
            print(f"[backup] {args.backup_dir} 下暂无备份")
        for path in backups:
            print(path)
        return 0

    # verify
    if args.all:
        results = verify_all(args.backup_dir)
        if not results:
            print(f"[verify] {args.backup_dir} 下没有可复核的备份", file=sys.stderr)
            return 1
    elif args.path is not None:
        results = [verify_backup(args.path)]
    else:
        backups = list_backups(args.backup_dir)
        if not backups:
            print(f"[verify] {args.backup_dir} 下没有可复核的备份", file=sys.stderr)
            return 1
        results = [verify_backup(backups[0])]

    for result in results:
        _print_verify(result)
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
