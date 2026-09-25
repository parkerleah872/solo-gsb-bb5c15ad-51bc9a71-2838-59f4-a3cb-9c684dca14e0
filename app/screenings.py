"""对照样品独立来源筛选:以一个参考样品为对照,从候选样品中筛出
取样根互不共享(且与参考样品也不共享)的最大集合。

规则
----
- 输入一个参考样品与 1~16 个不重复的候选样品号,均须为已有样品。
- 沿当前谱系边(`lineage_edges` 子 -> 父)反查各样品的全部取样根
  (`is_root` 样品),合并(合流)路径去重。
- 与参考样品共享任一根的候选不可选;其余候选之间也不得共享根。
- 在可选候选中求数量最多的集合;同数量时按候选输入顺序的下标列表
  字典序最小确定结果。
- 响应返回入选样品,并为每个未入选样品列出与参考或入选样品共享的根。
- 不持久化:每次查询按当前谱系实时重算,后续分装/合并立即反映到结果中。
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from . import schemas as s
from . import service

_CYCLE_LIMIT = 100000  # 谱系遍历节点上限,防御性保护(谱系本身为 DAG)


def _conflict(
    code: str,
    message: str,
    *,
    sample_id: Optional[str] = None,
) -> s.Conflict:
    return s.Conflict(code=code, message=message, sample_id=sample_id)


# ---------------------------------------------------------------- 谱系反查

def _parents_map(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """当前全部谱系边的 子 -> [父] 邻接表。"""
    by_child: dict[str, list[str]] = {}
    for row in conn.execute(
        "SELECT parent_id, child_id FROM lineage_edges WHERE parent_id IS NOT NULL"
    ):
        by_child.setdefault(row["child_id"], []).append(row["parent_id"])
    return by_child


def _root_ids(conn: sqlite3.Connection) -> set[str]:
    return {
        row["sample_id"]
        for row in conn.execute("SELECT sample_id FROM samples WHERE is_root = 1")
    }


def _sampling_roots(
    parents: dict[str, list[str]], root_ids: set[str], start: str
) -> set[str]:
    """从 start 沿 子 -> 父 方向反查全部取样根(含 start 自身若为根)。

    合流(多条路径到达同一祖先)只计一次,返回去重后的根集合。
    """
    found: set[str] = set()
    seen = {start}
    stack = [start]
    while stack:
        node = stack.pop()
        if node in root_ids:
            found.add(node)
        for parent in parents.get(node, ()):
            if parent in seen:
                continue
            seen.add(parent)
            if len(seen) > _CYCLE_LIMIT:
                raise service.ConflictError([
                    _conflict(
                        "lineage_too_large",
                        "谱系规模超过遍历上限,拒绝筛选",
                        sample_id=start,
                    )
                ])
            stack.append(parent)
    return found


# ---------------------------------------------------------------- 最大独立集合

def _max_disjoint_indices(root_sets: list[set[str]]) -> tuple[int, ...]:
    """求两两不共享根的最大下标集合。

    数量最多者优先;同数量时取下标列表(升序)字典序最小者。
    候选不超过 16 个,直接枚举全部子集(2^16)。
    """
    n = len(root_sets)
    best: tuple[int, ...] = ()
    for mask in range(1 << n):
        size = bin(mask).count("1")
        if size < len(best):
            continue
        idxs = tuple(i for i in range(n) if mask & (1 << i))
        if size == len(best) and idxs >= best:
            continue
        seen: set[str] = set()
        for i in idxs:
            if seen & root_sets[i]:
                break
            seen |= root_sets[i]
        else:
            best = idxs
    return best


# ---------------------------------------------------------------- 筛选

def screen_independent_sources(
    conn: sqlite3.Connection, req: s.IndependentSourceScreenRequest
) -> s.IndependentSourceScreenResponse:
    """单事务只读快照:按当前谱系实时计算,不写库。"""
    conn.execute("BEGIN")
    try:
        ids = [req.reference_sample_id, *req.candidate_sample_ids]
        qmarks = ",".join("?" * len(ids))
        found = {
            row["sample_id"]
            for row in conn.execute(
                f"SELECT sample_id FROM samples WHERE sample_id IN ({qmarks})",
                ids,
            )
        }
        missing = [sid for sid in ids if sid not in found]
        if missing:
            raise service.ConflictError([
                _conflict(
                    "sample_not_found",
                    f"样品 {', '.join(missing)} 不存在,无法进行对照筛选",
                    sample_id=missing[0],
                )
            ])

        parents = _parents_map(conn)
        root_ids = _root_ids(conn)
        ref_roots = _sampling_roots(parents, root_ids, req.reference_sample_id)
        cand_roots = [
            _sampling_roots(parents, root_ids, sid)
            for sid in req.candidate_sample_ids
        ]

        # 与参考样品共享任一根的候选不可选,其余进入最大独立集合求解
        eligible = [
            i for i, roots in enumerate(cand_roots) if not (roots & ref_roots)
        ]
        chosen = _max_disjoint_indices([cand_roots[i] for i in eligible])
        selected_idx = sorted(eligible[j] for j in chosen)
        selected_set = set(selected_idx)

        items: list[s.ScreenCandidateOut] = []
        for i, sid in enumerate(req.candidate_sample_ids):
            items.append(s.ScreenCandidateOut(
                sample_id=sid,
                input_index=i,
                roots=sorted(cand_roots[i]),
                selected=i in selected_set,
                shared_with_reference=sorted(cand_roots[i] & ref_roots),
                shared_with_selected=[
                    s.ScreenSharedRootOut(
                        sample_id=req.candidate_sample_ids[j],
                        shared_roots=sorted(cand_roots[i] & cand_roots[j]),
                    )
                    for j in selected_idx
                    if j != i and cand_roots[i] & cand_roots[j]
                ],
            ))

        result = s.IndependentSourceScreenResponse(
            reference_sample_id=req.reference_sample_id,
            reference_roots=sorted(ref_roots),
            selected_count=len(selected_idx),
            selected=[items[i] for i in selected_idx],
            excluded=[it for i, it in enumerate(items) if i not in selected_set],
        )
        conn.execute("COMMIT")
        return result
    except Exception:
        conn.execute("ROLLBACK")
        raise
