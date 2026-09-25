"""分装 / 合并不再绕过预留锁定量的端到端测试。

规则:分装父样品与合并各输入按「当前余量 − 活跃预留未核销锁定量」校验,
已释放预留与已核销部分不再占用;冲突携带样品、请求量与可用量,
整次操作不写谱系与流水。批量提交与 dry-run 同口径、顺序执行,
任一冲突整批不落库;既有操作号重放仍返回原结果。
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

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
        c.db = mem  # type: ignore[attr-defined]
        yield c
    app.dependency_overrides.clear()
    mem.close()


def _sampling(client, op_no, sid, amount, unit="mL"):
    r = client.post("/api/v1/operations/sampling", json={
        "type": "sampling", "op_no": op_no, "sample_id": sid,
        "material": "血清", "amount": amount, "unit": unit,
    })
    assert r.status_code == 200, r.text


def _reserve(client, no, items):
    r = client.post("/api/v1/reservations",
                    json={"reservation_no": no, "items": items})
    assert r.status_code == 200, r.text
    return r.json()


def _consume(client, no, reservation_no, items):
    return client.post("/api/v1/consumptions", json={
        "consumption_no": no, "reservation_no": reservation_no,
        "items": items,
    })


# ---------------------------------------------------------------- 分装

def test_aliquot_respects_active_reservation_and_carries_conflict(client):
    _sampling(client, "OP-1", "S1", 100)
    _reserve(client, "RSV-1", [{"sample_id": "S1", "amount": 80}])

    # 余量 100、活跃预留 80,可用量仅 20:分装 30 被拒
    r = client.post("/api/v1/operations/aliquot", json={
        "type": "aliquot", "op_no": "OP-BAD", "parent_id": "S1",
        "outputs": [{"sample_id": "A1", "amount": 30}],
    })
    assert r.status_code == 409
    c = r.json()["conflicts"][0]
    assert c["code"] == "insufficient_available"
    assert c["op_no"] == "OP-BAD"
    assert c["sample_id"] == "S1"
    assert c["requested"] == 30.0
    assert c["available"] == 20.0
    assert "余量 100" in c["message"] and "预留 80" in c["message"]

    # 整次操作不写谱系与流水:无新样品、余量不变、操作号不存在
    assert client.get("/api/v1/operations/OP-BAD").status_code == 404
    samples = {i["sample_id"] for i in
               client.get("/api/v1/samples").json()["items"]}
    assert samples == {"S1"}
    bal = client.get("/api/v1/samples/S1/balance").json()["samples"][0]
    assert bal["stored_remaining"] == 100.0
    assert bal["total_out"] == 0.0

    # 不超过可用量 20 的分装放行;分装后余量 80、锁定 80、可用 0
    r = client.post("/api/v1/operations/aliquot", json={
        "type": "aliquot", "op_no": "OP-OK", "parent_id": "S1",
        "outputs": [
            {"sample_id": "A1", "amount": 12},
            {"sample_id": "A2", "amount": 8},
        ],
    })
    assert r.status_code == 200, r.text
    assert r.json()["remaining"]["S1"] == 80.0
    av = client.get("/api/v1/reservations?sample_id=S1").json()["samples"][0]
    assert av == {"sample_id": "S1", "unit": "mL",
                  "remaining": 80.0, "reserved": 80.0, "available": 0.0}

    # 可用量为 0 后再分装任意正数都失败,预留核销不受影响
    r = client.post("/api/v1/operations/aliquot", json={
        "type": "aliquot", "op_no": "OP-NOPE", "parent_id": "S1",
        "outputs": [{"sample_id": "A3", "amount": 1}],
    })
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["code"] == "insufficient_available"

    r = _consume(client, "CV-1", "RSV-1", [{"sample_id": "S1", "amount": 80}])
    assert r.status_code == 200, r.text


def test_aliquot_unlocks_after_reservation_consumed_or_released(client):
    _sampling(client, "OP-1", "S1", 100)
    _reserve(client, "RSV-1", [{"sample_id": "S1", "amount": 80}])

    # 核销 30:余量 70、未核销锁定 50,可用量恢复到 20;
    # 已核销部分不再占用,分装 20 放行
    r = _consume(client, "CV-1", "RSV-1", [{"sample_id": "S1", "amount": 30}])
    assert r.status_code == 200, r.text
    r = client.post("/api/v1/operations/aliquot", json={
        "type": "aliquot", "op_no": "OP-A", "parent_id": "S1",
        "outputs": [{"sample_id": "A1", "amount": 20}],
    })
    assert r.status_code == 200, r.text
    assert r.json()["remaining"]["S1"] == 50.0

    # 未核销锁定仍在(余量 50、锁定 50、可用 0),再分装被拒
    r = client.post("/api/v1/operations/aliquot", json={
        "type": "aliquot", "op_no": "OP-LOCKED", "parent_id": "S1",
        "outputs": [{"sample_id": "A2", "amount": 1}],
    })
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["available"] == 0.0

    # 整单释放 RSV-1:未核销锁定全部归还,分装可使用全部余量
    assert client.delete("/api/v1/reservations/RSV-1").status_code == 200
    r = client.post("/api/v1/operations/aliquot", json={
        "type": "aliquot", "op_no": "OP-B", "parent_id": "S1",
        "outputs": [{"sample_id": "A2", "amount": 20}],
    })
    assert r.status_code == 200, r.text
    assert r.json()["remaining"]["S1"] == 30.0

    # 新建预留 20 后可用量仅剩 10;再释放后分装 20 恢复可行
    _reserve(client, "RSV-2", [{"sample_id": "S1", "amount": 20}])
    r = client.post("/api/v1/operations/aliquot", json={
        "type": "aliquot", "op_no": "OP-C", "parent_id": "S1",
        "outputs": [{"sample_id": "A3", "amount": 20}],
    })
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["available"] == 10.0
    assert client.delete("/api/v1/reservations/RSV-2").status_code == 200
    r = client.post("/api/v1/operations/aliquot", json={
        "type": "aliquot", "op_no": "OP-C", "parent_id": "S1",
        "outputs": [{"sample_id": "A3", "amount": 20}],
    })
    assert r.status_code == 200, r.text
    assert r.json()["remaining"]["S1"] == 10.0


# ---------------------------------------------------------------- 合并

def test_merge_each_input_checked_against_available(client):
    _sampling(client, "OP-1", "S1", 100)
    _sampling(client, "OP-2", "S2", 50)
    _reserve(client, "RSV-1", [
        {"sample_id": "S1", "amount": 80},
        {"sample_id": "S2", "amount": 10},
    ])

    # S1 可用 20 却要 30、S2 可用 40 却要 45:两条冲突一次返回
    r = client.post("/api/v1/operations/merge", json={
        "type": "merge", "op_no": "OP-MERGE-BAD",
        "inputs": [
            {"sample_id": "S1", "amount": 30},
            {"sample_id": "S2", "amount": 45},
        ],
        "output_id": "M1", "material": "混合血清",
    })
    assert r.status_code == 409
    conflicts = r.json()["conflicts"]
    assert {c["code"] for c in conflicts} == {"insufficient_available"}
    by_sample = {c["sample_id"]: c for c in conflicts}
    assert by_sample["S1"]["requested"] == 30.0
    assert by_sample["S1"]["available"] == 20.0
    assert by_sample["S2"]["requested"] == 45.0
    assert by_sample["S2"]["available"] == 40.0

    # 整次操作不写谱系与流水:输出样品未创建、余量不变、操作号不存在
    assert client.get("/api/v1/operations/OP-MERGE-BAD").status_code == 404
    samples = {i["sample_id"] for i in
               client.get("/api/v1/samples").json()["items"]}
    assert samples == {"S1", "S2"}
    for sid, rem in (("S1", 100.0), ("S2", 50.0)):
        bal = client.get(f"/api/v1/samples/{sid}/balance").json()["samples"][0]
        assert bal["stored_remaining"] == rem
        assert bal["total_out"] == 0.0

    # 各输入只取可用量之内即可成功
    r = client.post("/api/v1/operations/merge", json={
        "type": "merge", "op_no": "OP-MERGE-OK",
        "inputs": [
            {"sample_id": "S1", "amount": 20},
            {"sample_id": "S2", "amount": 40},
        ],
        "output_id": "M1", "material": "混合血清",
    })
    assert r.status_code == 200, r.text
    assert r.json()["remaining"] == {"S1": 80.0, "S2": 10.0, "M1": 60.0}


# ---------------------------------------------------------------- 批量 / dry-run

def test_batch_uses_same_rule_sequentially_and_rolls_back_all(client):
    _sampling(client, "OP-1", "S1", 100)
    _reserve(client, "RSV-1", [{"sample_id": "S1", "amount": 80}])

    # 第一条分装取走可用量 20 后,第二条再取 1 即不足(顺序同口径)
    r = client.post("/api/v1/batch", json={"operations": [
        {"type": "aliquot", "op_no": "OP-B1", "parent_id": "S1",
         "outputs": [{"sample_id": "A1", "amount": 20}]},
        {"type": "aliquot", "op_no": "OP-B2", "parent_id": "S1",
         "outputs": [{"sample_id": "A2", "amount": 1}]},
    ]})
    assert r.status_code == 409
    conflicts = r.json()["conflicts"]
    assert len(conflicts) == 1
    assert conflicts[0]["batch_index"] == 1
    assert conflicts[0]["code"] == "insufficient_available"
    assert conflicts[0]["available"] == 0.0

    # 任一冲突整批不落库:第一条也回滚
    samples = {i["sample_id"] for i in
               client.get("/api/v1/samples").json()["items"]}
    assert samples == {"S1"}
    bal = client.get("/api/v1/samples/S1/balance").json()["samples"][0]
    assert bal["stored_remaining"] == 100.0

    # 批量 dry-run 同一口径,且不写主库
    r = client.post("/api/v1/batch/validate", json={"operations": [
        {"type": "aliquot", "op_no": "OP-V1", "parent_id": "S1",
         "outputs": [{"sample_id": "V1", "amount": 30}]},
    ]})
    assert r.status_code == 200
    data = r.json()
    assert data["valid"] is False
    c = data["conflicts"][0]
    assert c["code"] == "insufficient_available"
    assert c["requested"] == 30.0
    assert c["available"] == 20.0
    assert client.get("/api/v1/operations/OP-V1").status_code == 404

    # 单操作 dry-run:合法操作 valid=True 但不写库
    r = client.post("/api/v1/validate", json={
        "type": "merge", "op_no": "OP-V2",
        "inputs": [{"sample_id": "S1", "amount": 20}],
        "output_id": "V2", "material": "M",
    })
    body = r.json()
    assert body["valid"] is True
    assert body["results"][0]["remaining"] == {"S1": 80.0, "V2": 20.0}
    assert client.get("/api/v1/operations/OP-V2").status_code == 404
    bal = client.get("/api/v1/samples/S1/balance").json()["samples"][0]
    assert bal["stored_remaining"] == 100.0


def test_replay_returns_original_result_without_rechecking(client):
    """历史合法操作号重放只回放原结果,不受此后预留变化影响。"""
    _sampling(client, "OP-1", "S1", 100)
    body = {"type": "aliquot", "op_no": "OP-H", "parent_id": "S1",
            "outputs": [{"sample_id": "H1", "amount": 90}]}
    r = client.post("/api/v1/operations/aliquot", json=body)
    assert r.status_code == 200, r.text
    assert r.json()["remaining"]["S1"] == 10.0

    # 此后把仅剩的 10 全部预留(余量 10、锁定 10、可用 0):
    # 同号重放仍返回原结果,不重新校验、不二次出库
    _reserve(client, "RSV-1", [{"sample_id": "S1", "amount": 10}])
    r = client.post("/api/v1/operations/aliquot", json=body)
    assert r.status_code == 200
    out = r.json()
    assert out["replayed"] is True
    assert out["remaining"] == {"S1": 10.0, "H1": 90.0}
    bal = client.get("/api/v1/samples/S1/balance").json()["samples"][0]
    assert bal["stored_remaining"] == 10.0  # 重放未二次扣减
