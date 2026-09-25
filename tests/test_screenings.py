"""对照样品独立来源筛选接口测试。

覆盖:取样根反查(分装/合并/合流去重)、与参考共享根不可选、候选间共享根时
最大集合求解与同数量下标字典序最小 tie-break、未入选样品的共享根说明、
输入校验(候选数量 1~16/不重复/均须存在)、分装合并后按当前谱系实时重算。
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


def _sampling(client, op_no, sid, amount=100, unit="mL"):
    r = client.post("/api/v1/operations/sampling", json={
        "type": "sampling", "op_no": op_no, "sample_id": sid,
        "material": "血清", "amount": amount, "unit": unit,
    })
    assert r.status_code == 200, r.text


def _aliquot(client, op_no, parent, outputs):
    r = client.post("/api/v1/operations/aliquot", json={
        "type": "aliquot", "op_no": op_no, "parent_id": parent,
        "outputs": [{"sample_id": s, "amount": a} for s, a in outputs],
    })
    assert r.status_code == 200, r.text


def _merge(client, op_no, inputs, output, material=None):
    body = {
        "type": "merge", "op_no": op_no,
        "inputs": [{"sample_id": s, "amount": a} for s, a in inputs],
        "output_id": output,
    }
    if material:
        body["material"] = material
    r = client.post("/api/v1/operations/merge", json=body)
    assert r.status_code == 200, r.text


def _screen(client, reference, candidates):
    return client.post("/api/v1/screenings/independent-sources", json={
        "reference_sample_id": reference,
        "candidate_sample_ids": candidates,
    })


def _roots_lineage(client):
    """R0/R1/R2/R3 四个取样根;REF 来自 R0。"""
    for i in range(4):
        _sampling(client, f"OP-R{i}", f"R{i}", 100)
    _aliquot(client, "OP-REF", "R0", [("REF", 50)])


# ---------------------------------------------------------------- 基本筛选

def test_all_independent_candidates_selected(client):
    _roots_lineage(client)
    _aliquot(client, "OP-1", "R1", [("C1", 10)])
    _aliquot(client, "OP-2", "R2", [("C2", 10)])
    _aliquot(client, "OP-3", "R3", [("C3", 10)])

    r = _screen(client, "REF", ["C1", "C2", "C3"])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["reference_sample_id"] == "REF"
    assert body["reference_roots"] == ["R0"]
    assert body["selected_count"] == 3
    assert [c["sample_id"] for c in body["selected"]] == ["C1", "C2", "C3"]
    assert [c["input_index"] for c in body["selected"]] == [0, 1, 2]
    assert [c["roots"] for c in body["selected"]] == [["R1"], ["R2"], ["R3"]]
    assert all(c["selected"] for c in body["selected"])
    assert body["excluded"] == []


def test_candidate_sharing_root_with_reference_excluded(client):
    _roots_lineage(client)
    _aliquot(client, "OP-1", "R0", [("C1", 10)])  # 与 REF 共享 R0
    _aliquot(client, "OP-2", "R1", [("C2", 10)])

    r = _screen(client, "REF", ["C1", "C2"])
    assert r.status_code == 200, r.text
    body = r.json()
    assert [c["sample_id"] for c in body["selected"]] == ["C2"]
    assert body["selected_count"] == 1
    assert len(body["excluded"]) == 1
    ex = body["excluded"][0]
    assert ex["sample_id"] == "C1"
    assert ex["selected"] is False
    assert ex["roots"] == ["R0"]
    assert ex["shared_with_reference"] == ["R0"]
    assert ex["shared_with_selected"] == []


def test_reference_can_be_merged_sample_with_multiple_roots(client):
    _roots_lineage(client)
    _aliquot(client, "OP-1", "R0", [("A0", 20)])
    _aliquot(client, "OP-2", "R1", [("A1", 20)])
    _merge(client, "OP-3", [("A0", 10), ("A1", 10)], "REFM", material="混合")
    _aliquot(client, "OP-4", "R1", [("C1", 5)])   # 与 REFM 共享 R1
    _aliquot(client, "OP-5", "R2", [("C2", 5)])

    r = _screen(client, "REFM", ["C1", "C2"])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["reference_roots"] == ["R0", "R1"]
    assert [c["sample_id"] for c in body["selected"]] == ["C2"]
    assert body["excluded"][0]["shared_with_reference"] == ["R1"]


def test_merge_convergence_roots_deduped(client):
    """合流路径去重:同一根经多条路径到达只计一次。"""
    _roots_lineage(client)
    _aliquot(client, "OP-1", "R1", [("A1", 20), ("A2", 20)])
    _merge(client, "OP-2", [("A1", 10), ("A2", 10)], "M1", material="合流")
    _aliquot(client, "OP-3", "M1", [("C1", 5)])

    r = _screen(client, "REF", ["C1"])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["selected_count"] == 1
    assert body["selected"][0]["roots"] == ["R1"]  # 去重后只出现一次


def test_root_sample_as_candidate(client):
    """候选本身是取样根时,其根集合就是自身。"""
    _roots_lineage(client)
    r = _screen(client, "REF", ["R1", "R0"])
    assert r.status_code == 200, r.text
    body = r.json()
    assert [c["sample_id"] for c in body["selected"]] == ["R1"]
    assert body["selected"][0]["roots"] == ["R1"]
    ex = body["excluded"][0]
    assert ex["sample_id"] == "R0"
    assert ex["shared_with_reference"] == ["R0"]


def test_candidate_equal_to_reference_is_excluded(client):
    _roots_lineage(client)
    _aliquot(client, "OP-1", "R1", [("C1", 10)])
    r = _screen(client, "REF", ["REF", "C1"])
    assert r.status_code == 200, r.text
    body = r.json()
    assert [c["sample_id"] for c in body["selected"]] == ["C1"]
    ex = body["excluded"][0]
    assert ex["sample_id"] == "REF"
    assert ex["shared_with_reference"] == ["R0"]


# ---------------------------------------------------------------- 最大集合与字典序

def test_max_set_beats_greedy_input_order(client):
    """C0 与 C1、C2 都冲突:贪心先取 C0 只得 1 个,最大集合为 {C1, C2}。"""
    _roots_lineage(client)
    _aliquot(client, "OP-1", "R1", [("A1", 20)])
    _aliquot(client, "OP-2", "R2", [("A2", 20)])
    _merge(client, "OP-3", [("A1", 10), ("A2", 10)], "C0", material="混合")
    _aliquot(client, "OP-4", "R1", [("C1", 5)])
    _aliquot(client, "OP-5", "R2", [("C2", 5)])
    _aliquot(client, "OP-6", "R3", [("C3", 5)])

    r = _screen(client, "REF", ["C0", "C1", "C2", "C3"])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["selected_count"] == 3
    assert [c["sample_id"] for c in body["selected"]] == ["C1", "C2", "C3"]
    assert [c["input_index"] for c in body["selected"]] == [1, 2, 3]
    ex = body["excluded"][0]
    assert ex["sample_id"] == "C0"
    assert ex["shared_with_reference"] == []
    # 与入选的 C1 共享 R1、与 C2 共享 R2(按入选样品输入下标升序)
    assert ex["shared_with_selected"] == [
        {"sample_id": "C1", "shared_roots": ["R1"]},
        {"sample_id": "C2", "shared_roots": ["R2"]},
    ]


def test_same_size_lexicographically_smallest_indices(client):
    """C0:{R1} C1:{R2} C2:{R1} C3:{R2}:最大大小为 2,
    候选集合 {C0,C1}/{C0,C3}/{C1,C2}/{C2,C3} 中 [0,1] 字典序最小。"""
    _roots_lineage(client)
    _aliquot(client, "OP-1", "R1", [("C0", 5), ("C2", 5)])
    _aliquot(client, "OP-2", "R2", [("C1", 5), ("C3", 5)])

    r = _screen(client, "REF", ["C0", "C1", "C2", "C3"])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["selected_count"] == 2
    assert [c["sample_id"] for c in body["selected"]] == ["C0", "C1"]
    assert [c["input_index"] for c in body["selected"]] == [0, 1]
    excluded = {c["sample_id"]: c for c in body["excluded"]}
    assert excluded["C2"]["shared_with_selected"] == [
        {"sample_id": "C0", "shared_roots": ["R1"]}
    ]
    assert excluded["C3"]["shared_with_selected"] == [
        {"sample_id": "C1", "shared_roots": ["R2"]}
    ]


def test_single_candidate_sharing_reference_yields_empty_selection(client):
    _roots_lineage(client)
    _aliquot(client, "OP-1", "R0", [("C1", 10)])
    r = _screen(client, "REF", ["C1"])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["selected_count"] == 0
    assert body["selected"] == []
    assert body["excluded"][0]["shared_with_reference"] == ["R0"]


# ---------------------------------------------------------------- 输入校验

def test_reference_not_found(client):
    _roots_lineage(client)
    r = _screen(client, "GHOST", ["R1"])
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["code"] == "sample_not_found"


def test_candidate_not_found(client):
    _roots_lineage(client)
    r = _screen(client, "REF", ["R1", "GHOST"])
    assert r.status_code == 409
    conflict = r.json()["conflicts"][0]
    assert conflict["code"] == "sample_not_found"
    assert "GHOST" in conflict["message"]


def test_candidates_must_be_between_1_and_16(client):
    _roots_lineage(client)
    assert _screen(client, "REF", []).status_code == 422
    assert _screen(client, "REF", [f"X{i}" for i in range(17)]).status_code == 422


def test_candidates_must_be_unique(client):
    _roots_lineage(client)
    r = _screen(client, "REF", ["R1", "R1"])
    assert r.status_code == 422


def test_sixteen_candidates_accepted(client):
    for i in range(17):
        _sampling(client, f"OP-R{i}", f"R{i}", 10)
    _aliquot(client, "OP-REF", "R0", [("REF", 5)])
    candidates = [f"R{i}" for i in range(1, 17)]
    r = _screen(client, "REF", candidates)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["selected_count"] == 16
    assert [c["sample_id"] for c in body["selected"]] == candidates


# ---------------------------------------------------------------- 实时重算

def test_requery_reflects_current_lineage_after_merge(client):
    """合并到已有候选后,其根集合扩大,重新查询结果随之变化。"""
    _roots_lineage(client)
    _aliquot(client, "OP-1", "R1", [("C1", 20)])
    _aliquot(client, "OP-2", "R2", [("C2", 20)])

    body = _screen(client, "REF", ["C1", "C2"]).json()
    assert [c["sample_id"] for c in body["selected"]] == ["C1", "C2"]

    # 把 R1 的下游材料并入已存在的 C2 -> C2 的根变为 {R1, R2}
    _aliquot(client, "OP-3", "R1", [("A1", 10)])
    _merge(client, "OP-4", [("A1", 5)], "C2")

    body = _screen(client, "REF", ["C1", "C2"]).json()
    assert body["selected_count"] == 1
    # 两个最大集合 {C1}/{C2} 同大小,取下标列表字典序最小 [0]
    assert [c["sample_id"] for c in body["selected"]] == ["C1"]
    ex = body["excluded"][0]
    assert ex["sample_id"] == "C2"
    assert ex["roots"] == ["R1", "R2"]
    assert ex["shared_with_selected"] == [
        {"sample_id": "C1", "shared_roots": ["R1"]}
    ]


def test_requery_reflects_current_lineage_after_aliquot(client):
    """新分装出的子样品继承父样品的根,可作为新候选参与筛选。"""
    _roots_lineage(client)
    _aliquot(client, "OP-1", "R1", [("C1", 20)])

    body = _screen(client, "REF", ["C1"]).json()
    assert body["selected_count"] == 1

    _aliquot(client, "OP-2", "C1", [("D1", 5)])  # D1 的根也是 R1
    body = _screen(client, "REF", ["C1", "D1"]).json()
    assert body["selected_count"] == 1
    assert [c["sample_id"] for c in body["selected"]] == ["C1"]
    ex = body["excluded"][0]
    assert ex["sample_id"] == "D1"
    assert ex["roots"] == ["R1"]
    assert ex["shared_with_selected"] == [
        {"sample_id": "C1", "shared_roots": ["R1"]}
    ]
