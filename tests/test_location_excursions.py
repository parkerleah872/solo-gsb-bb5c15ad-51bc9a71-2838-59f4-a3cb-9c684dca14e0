"""带保管位置的越界区间查询端到端测试:

GET /api/v1/samples/{id}/temperature/location-excursions

- 保管位置重建:变更时刻起取新位置;首笔变更前取其原位置;没有变更取当前位置
  (含 null);事件与读数同刻时先应用事件
- 同刻事件先交接后冲正;同类事件按明细编号(item_id)升序
- 相邻同向越界读数除间隔 <= 配置秒数外,期间发生位置变更也截断(归为两段)
- 乱序补录读数后实时重算(合并/拆分);样品或配置不存在 404
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import temperatures as tp
from app.db import connect, init_db
from app.main import app, get_conn


@pytest.fixture()
def client():
    mem = connect(":memory:")
    init_db(mem)

    def _override():
        yield mem

    app.dependency_overrides[get_conn] = _override
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
    mem.close()


@pytest.fixture()
def conn():
    return next(app.dependency_overrides[get_conn]())


def _sample(client, sid, location="冷库A-1"):
    r = client.post("/api/v1/operations/sampling", json={
        "type": "sampling", "op_no": f"OP-{sid}", "sample_id": sid,
        "material": "血清", "amount": 100, "unit": "mL",
        **({"location": location} if location is not None else {}),
    })
    assert r.status_code == 200, r.text


def _profile(client, sid, lo=2.0, hi=8.0, interval=600):
    r = client.post("/api/v1/temperature-profiles", json={
        "sample_id": sid, "min_temp": lo, "max_temp": hi,
        "max_interval_seconds": interval,
    })
    assert r.status_code == 200, r.text


def _read(client, sid, at, temp):
    r = client.post(
        f"/api/v1/samples/{sid}/temperature-readings",
        json={"measured_at": at, "temperature": temp},
    )
    assert r.status_code == 200, r.text


def _loc_excursions(client, sid):
    return client.get(
        f"/api/v1/samples/{sid}/temperature/location-excursions"
    )


def _transfer_event(
    conn, transfer_no, sid, at, frm, to, *, item_id=None, seq=0,
    header_at=None,
):
    """直接写不可变交接审计(显式 created_at,用于与读数时刻确定性对齐)。"""
    conn.execute(
        "INSERT OR IGNORE INTO location_transfers"
        " (transfer_no, operator, payload, note, created_at)"
        " VALUES (?, '交接员', '{}', NULL, ?)",
        (transfer_no, header_at or at),
    )
    if item_id is None:
        conn.execute(
            "INSERT INTO location_transfer_items"
            " (transfer_no, sample_id, seq_index, from_location, to_location,"
            "  operator, created_at) VALUES (?, ?, ?, ?, ?, '交接员', ?)",
            (transfer_no, sid, seq, frm, to, at),
        )
    else:
        conn.execute(
            "INSERT INTO location_transfer_items"
            " (item_id, transfer_no, sample_id, seq_index, from_location,"
            "  to_location, operator, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, '交接员', ?)",
            (item_id, transfer_no, sid, seq, frm, to, at),
        )
    conn.execute(
        "UPDATE samples SET location = ? WHERE sample_id = ?", (to, sid)
    )


def _reversal_event(
    conn, reversal_no, transfer_no, sid, at, frm, to, *, item_id=None, seq=0,
):
    conn.execute(
        "INSERT INTO location_transfer_reversals"
        " (reversal_no, transfer_no, operator, payload, note, created_at)"
        " VALUES (?, ?, '冲正员', '{}', NULL, ?)",
        (reversal_no, transfer_no, at),
    )
    cols = ("reversal_no, transfer_no, sample_id, seq_index, from_location,"
            " to_location, operator, created_at")
    vals = (reversal_no, transfer_no, sid, seq, frm, to, "冲正员", at)
    if item_id is None:
        conn.execute(
            f"INSERT INTO location_transfer_reversal_items ({cols})"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)", vals,
        )
    else:
        conn.execute(
            f"INSERT INTO location_transfer_reversal_items"
            f" (item_id, {cols}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (item_id, *vals),
        )
    conn.execute(
        "UPDATE samples SET location = ? WHERE sample_id = ?", (to, sid)
    )


# ---------------------------------------------------------------- 404

def test_requires_existing_sample_and_profile(client):
    r = _loc_excursions(client, "GHOST")
    assert r.status_code == 404
    assert r.json()["error"] == "not_found"

    _sample(client, "S1")
    r = _loc_excursions(client, "S1")
    assert r.status_code == 404
    assert r.json()["error"] == "temp_profile_not_found"

    _profile(client, "S1")
    r = _loc_excursions(client, "S1")
    assert r.status_code == 200
    assert r.json()["intervals"] == []


# ---------------------------------------------------------------- 当前位置 / 原位置

def test_no_events_uses_current_location(client, conn):
    _sample(client, "S1", location="冷库B-2")
    _profile(client, "S1", interval=600)
    _read(client, "S1", "2026-09-24T10:00:00Z", 9.0)
    _read(client, "S1", "2026-09-24T10:05:00Z", 10.0)
    body = _loc_excursions(client, "S1").json()
    assert len(body["intervals"]) == 1
    iv = body["intervals"][0]
    assert iv["location"] == "冷库B-2"
    assert iv["direction"] == "above"
    assert iv["reading_count"] == 2
    assert iv["extreme_value"] == 10.0


def test_no_events_current_location_null(client, conn):
    _sample(client, "S1", location=None)
    _profile(client, "S1")
    _read(client, "S1", "2026-09-24T10:00:00Z", 1.0)
    iv = _loc_excursions(client, "S1").json()["intervals"][0]
    assert iv["location"] is None
    assert iv["direction"] == "below"


def test_before_first_change_uses_original_location(client, conn):
    """首笔变更前的读数取该变更登记的原位置;变更后取新位置。"""
    _sample(client, "S1", location="冷库B-1")  # 与历史审计无关,仅当前位置
    _profile(client, "S1", interval=600)
    _transfer_event(
        conn, "TRF-1", "S1", "2026-09-24T10:10:00.000000Z",
        "冷库A-1", "冷库B-1",
    )
    _read(client, "S1", "2026-09-24T10:00:00Z", 9.0)   # 变更前 -> A-1
    _read(client, "S1", "2026-09-24T10:05:00Z", 9.5)   # 变更前 -> A-1
    _read(client, "S1", "2026-09-24T10:15:00Z", 9.0)   # 变更后 -> B-1
    _read(client, "S1", "2026-09-24T10:20:00Z", 9.5)   # 变更后 -> B-1

    intervals = _loc_excursions(client, "S1").json()["intervals"]
    assert [(i["location"], i["start_at"][11:19], i["end_at"][11:19],
             i["reading_count"]) for i in intervals] == [
        ("冷库A-1", "10:00:00", "10:05:00", 2),
        ("冷库B-1", "10:15:00", "10:20:00", 2),
    ]


# ---------------------------------------------------------------- 期间变更截断

def test_event_between_readings_splits_even_within_interval(client, conn):
    """两读数间隔恰好 <= 配置秒数,但期间发生位置变更 -> 必须截成两段。"""
    _sample(client, "S1", location="冷库B-1")
    _profile(client, "S1", interval=600)
    _read(client, "S1", "2026-09-24T10:05:00Z", 9.0)
    _transfer_event(
        conn, "TRF-1", "S1", "2026-09-24T10:10:00.000000Z",
        "冷库A-1", "冷库B-1",
    )
    _read(client, "S1", "2026-09-24T10:15:00Z", 9.5)  # 与上笔间隔恰 600 秒

    intervals = _loc_excursions(client, "S1").json()["intervals"]
    assert [(i["location"], i["reading_count"]) for i in intervals] == [
        ("冷库A-1", 1), ("冷库B-1", 1),
    ]


def test_same_instant_event_and_reading_applies_event_first(client, conn):
    """读数时刻恰为变更时刻:先应用事件,读数归属新位置且与前序读数截断。"""
    _sample(client, "S1", location="冷库B-1")
    _profile(client, "S1", interval=600)
    _read(client, "S1", "2026-09-24T10:05:00Z", 9.0)
    _transfer_event(
        conn, "TRF-1", "S1", "2026-09-24T10:10:00.000000Z",
        "冷库A-1", "冷库B-1",
    )
    _read(client, "S1", "2026-09-24T10:10:00Z", 9.0)  # 同刻 -> B-1
    _read(client, "S1", "2026-09-24T10:15:00Z", 9.5)  # 无新事件 -> 延续

    intervals = _loc_excursions(client, "S1").json()["intervals"]
    assert [(i["location"], i["start_at"][11:19], i["end_at"][11:19])
            for i in intervals] == [
        ("冷库A-1", "10:05:00", "10:05:00"),
        ("冷库B-1", "10:10:00", "10:15:00"),
    ]


# ---------------------------------------------------------------- 同刻顺序

def test_same_instant_transfer_then_reversal(client, conn):
    """同刻先交接后冲正:同刻读数取冲正后的位置;冲正恢复 null 时为 null。"""
    _sample(client, "S1", location=None)
    _profile(client, "S1", interval=600)
    at = "2026-09-24T10:10:00.000000Z"
    _transfer_event(conn, "TRF-1", "S1", at, None, "冷库B-1")
    _reversal_event(conn, "RVS-1", "TRF-1", "S1", at, "冷库B-1", None)

    _read(client, "S1", "2026-09-24T10:05:00Z", 9.0)   # 首笔变更前:原位置 null
    _read(client, "S1", "2026-09-24T10:10:00Z", 9.0)   # 同刻:冲正后 null
    _read(client, "S1", "2026-09-24T10:15:00Z", 9.5)   # 无新事件,延续同段

    intervals = _loc_excursions(client, "S1").json()["intervals"]
    # 10:05 一段(期间有同刻交接+冲正事件,即使净位置不变也截断);
    # 10:10~10:15 一段,两段位置均为 null
    assert [(i["location"], i["start_at"][11:19], i["end_at"][11:19],
             i["reading_count"]) for i in intervals] == [
        (None, "10:05:00", "10:05:00", 1),
        (None, "10:10:00", "10:15:00", 2),
    ]


def test_same_kind_same_instant_events_ordered_by_item_id(client, conn):
    """同类同刻事件按明细编号升序应用(与插入先后无关)。"""
    _sample(client, "S1", location="冷库C-1")
    _profile(client, "S1")
    at = "2026-09-24T10:10:00.000000Z"
    # 先插入 item_id 较大的(B->C),再插入较小的(A->B):
    # 按 item_id 升序应用应为 A->B 再 B->C,同刻读数落在 C-1
    _transfer_event(
        conn, "TRF-2", "S1", at, "冷库B-1", "冷库C-1", item_id=200, seq=1,
        header_at=at,
    )
    _transfer_event(
        conn, "TRF-1", "S1", at, "冷库A-1", "冷库B-1", item_id=100, seq=0,
        header_at=at,
    )
    _read(client, "S1", "2026-09-24T10:10:00Z", 9.0)

    intervals = _loc_excursions(client, "S1").json()["intervals"]
    assert len(intervals) == 1
    assert intervals[0]["location"] == "冷库C-1"


# ---------------------------------------------------------------- 截断规则组合

def test_normal_direction_gap_truncate_keeps_locations(client, conn):
    """正常读数、方向改变、超间隔截断(位置不变时),各段带同一位置。"""
    _sample(client, "S1", location=None)
    _profile(client, "S1", interval=600)
    _transfer_event(
        conn, "TRF-1", "S1", "2026-09-24T09:00:00.000000Z",
        None, "冷库A-1",
    )
    _read(client, "S1", "2026-09-24T10:00:00Z", 9.0)   # above
    _read(client, "S1", "2026-09-24T10:05:00Z", 5.0)   # 正常截断
    _read(client, "S1", "2026-09-24T10:10:00Z", 1.0)   # below
    _read(client, "S1", "2026-09-24T10:15:00Z", 0.5)   # below 延续
    _read(client, "S1", "2026-09-24T10:40:00Z", 9.0)   # 超间隔 + 方向改变

    intervals = _loc_excursions(client, "S1").json()["intervals"]
    assert [(i["direction"], i["location"], i["reading_count"],
             i["extreme_value"]) for i in intervals] == [
        ("above", "冷库A-1", 1, 9.0),
        ("below", "冷库A-1", 2, 0.5),
        ("above", "冷库A-1", 1, 9.0),
    ]


# ---------------------------------------------------------------- 乱序补录重算

def test_out_of_order_backfill_recalculates_locations(client, conn):
    """先无事件(取当前位置),补录读数/审计后查询实时反映新结果。"""
    _sample(client, "S1", location="冷库B-1")
    _profile(client, "S1", interval=600)
    _read(client, "S1", "2026-09-24T10:00:00Z", 9.0)
    _read(client, "S1", "2026-09-24T10:05:00Z", 9.5)
    before = _loc_excursions(client, "S1").json()["intervals"]
    assert [(i["location"], i["reading_count"]) for i in before] == [
        ("冷库B-1", 2),
    ]

    # 补录一笔落在两读数之间的交接审计:前段取原位置 A-1、后段取 B-1
    _transfer_event(
        conn, "TRF-1", "S1", "2026-09-24T10:03:00.000000Z",
        "冷库A-1", "冷库B-1",
    )
    after = _loc_excursions(client, "S1").json()["intervals"]
    assert [(i["location"], i["start_at"][11:19], i["end_at"][11:19])
            for i in after] == [
        ("冷库A-1", "10:00:00", "10:00:00"),
        ("冷库B-1", "10:05:00", "10:05:00"),
    ]

    # 再乱序补录一条 10:03 之前的正常读数 -> 前段(above)被拆,结果只剩两段 above
    _read(client, "S1", "2026-09-24T10:02:00Z", 5.0)
    final = _loc_excursions(client, "S1").json()["intervals"]
    assert [(i["location"], i["start_at"][11:19]) for i in final] == [
        ("冷库A-1", "10:00:00"),
        ("冷库B-1", "10:05:00"),
    ]


# ---------------------------------------------------------------- 并发快照

def test_concurrent_writes_during_query_use_single_snapshot(
    tmp_path, monkeypatch,
):
    """查询读到读数之后、读到位置事件/当前位置之前,另一连接提交新读数与
    位置交接:整次响应必须取自同一只读快照(旧版本),不得拼接跨版本数据。

    旧实现(无事务)会拿到:旧读数 + 新事件 + 新当前位置 -> 把一笔 above
    读数按「首笔变更前原位置」归到 A-1 并与事件后语义错配;快照实现则只
    看到提交前的状态(无事件,当前位置 B-1,一段 2 读数)。
    """
    db_path = str(tmp_path / "snap.db")
    setup = connect(db_path)
    init_db(setup)
    setup.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, version, location, is_root) VALUES"
        " ('S1', '血清', 'mL', 100, 100, 0, '冷库B-1', 1)"
    )
    setup.execute(
        "INSERT INTO sample_temp_profiles (sample_id, min_temp, max_temp,"
        " max_interval_seconds, payload) VALUES ('S1', 2.0, 8.0, 600, '{}')"
    )
    setup.executemany(
        "INSERT INTO temperature_readings (sample_id, measured_at_utc,"
        " temperature) VALUES ('S1', ?, ?)",
        [("2026-09-24T10:00:00.000000Z", 9.0),
         ("2026-09-24T10:05:00.000000Z", 9.5)],
    )
    setup.commit()
    setup.close()

    reader = connect(db_path)
    writer = connect(db_path)

    # 在「读数已读出、事件与当前位置尚未读取」的窗口由另一连接并发提交;
    # 只触发一次:下一次查询(重取快照)时不再重复提交同一批写入
    fired = {"done": False}

    def _concurrent_commit():
        if fired["done"]:
            return
        fired["done"] = True
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "INSERT INTO location_transfers (transfer_no, operator, payload)"
            " VALUES ('TRF-X', '交接员', '{}')"
        )
        writer.execute(
            "INSERT INTO location_transfer_items (transfer_no, sample_id,"
            " seq_index, from_location, to_location, operator, created_at)"
            " VALUES ('TRF-X', 'S1', 0, '冷库A-1', '冷库B-1', '交接员', ?)",
            ("2026-09-24T10:03:00.000000Z",),
        )
        writer.execute(
            "INSERT INTO temperature_readings (sample_id, measured_at_utc,"
            " temperature) VALUES ('S1', ?, 9.8)",
            ("2026-09-24T10:07:00.000000Z",),
        )
        writer.execute(
            "UPDATE samples SET location = '冷库C-1' WHERE sample_id = 'S1'"
        )
        writer.execute("COMMIT")

    # reader 的快照在事务内首次读(profile)时建立;包装事件加载函数,
    # 在其执行前(读数已读出后)先完成并发提交
    original_load = tp._load_location_events

    def patched_load(conn, sample_id):
        _concurrent_commit()
        return original_load(conn, sample_id)

    monkeypatch.setattr(tp, "_load_location_events", patched_load)

    try:
        resp = tp.get_location_excursions(reader, "S1")
    finally:
        # 事务必须已结束(reader 不再处于事务中,可直接开启写事务)
        reader.execute("BEGIN IMMEDIATE")
        reader.execute("ROLLBACK")

    # 快照视角:提交前无任何事件 -> 当前位置 冷库B-1;只有两笔读数 -> 一段
    assert [(i.location, i.reading_count) for i in resp.intervals] == [
        ("冷库B-1", 2),
    ]

    # 下一次查询重新取快照:已提交的补录全部可见,实时重算为两段
    # (10:00 在交接前取 A-1;10:05、10:07 在交接后取 B-1,间隔 120s 延续)
    resp2 = tp.get_location_excursions(reader, "S1")
    assert [(i.location, i.start_at[11:19], i.end_at[11:19], i.reading_count)
            for i in resp2.intervals] == [
        ("冷库A-1", "10:00:00", "10:00:00", 1),
        ("冷库B-1", "10:05:00", "10:07:00", 2),
    ]

    reader.close()
    writer.close()


def test_snapshot_rolled_back_on_error(tmp_path, monkeypatch):
    """查询中途异常时只读事务必须回滚结束,连接恢复可写状态且 query_only 复位。"""
    db_path = str(tmp_path / "rollback.db")
    setup = connect(db_path)
    init_db(setup)
    setup.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, version, location, is_root) VALUES"
        " ('S1', '血清', 'mL', 100, 100, 0, '冷库B-1', 1)"
    )
    setup.execute(
        "INSERT INTO sample_temp_profiles (sample_id, min_temp, max_temp,"
        " max_interval_seconds, payload) VALUES ('S1', 2.0, 8.0, 600, '{}')"
    )
    setup.commit()
    setup.close()

    conn = connect(db_path)

    def boom(*_a, **_k):
        raise RuntimeError("snapshot boom")

    monkeypatch.setattr(tp, "_load_location_events", boom)
    with pytest.raises(RuntimeError, match="snapshot boom"):
        tp.get_location_excursions(conn, "S1")

    # 事务已结束:可立即开启 IMMEDIATE 写事务;query_only 已复位为 0
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("ROLLBACK")
    assert conn.execute("PRAGMA query_only").fetchone()[0] == 0
    conn.close()

