"""样方复核/核准功能端到端测试。

运行：.venv/bin/python -m pytest tests/ -q
覆盖：
- 初始均待复核；核准绑定完整成员与值
- 复核期间编辑 -> 提交时拒绝旧核准；其他样方变化不影响
- 人工增删/改值/迁入迁出 -> 受影响样方立即转待复核
- 改回原值、同 ID 重建不恢复旧核准；同值保存不撤销
- 记录变化与核准失效同一事务，失败均不生效
- 核准 CSV 只含已核准样方，网页列出被排除的待复核样方；状态与记录同读一致
- 两个共库进程（两个 DB 连接视角）并发核准/维护/合并不放行未复核数据
- 重启（重新 init_db）保留状态
- 原合并与普通导出保留可用
"""

import os
import tempfile

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(monkeypatch):
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "survey.db")
    monkeypatch.setenv("DB_PATH", db_path)
    # 延迟导入，确保模块读到测试用 DB_PATH
    import importlib

    from app import db as db_mod
    importlib.reload(db_mod)
    from app import main as main_mod
    importlib.reload(main_mod)
    db_mod.init_db(os.path.join(os.path.dirname(__file__), "..", "data", "initial_records.csv"))
    main_mod.db = db_mod  # 路由模块通过 reload 已指向同一模块
    with TestClient(main_mod.app) as c:
        yield c, db_mod


def approve(client, db_mod, quadrat):
    d = db_mod.get_quadrat(quadrat)
    r = client.post(f"/reviews/{quadrat}/approve", data={"expected_hash": d["current_hash"]},
                    follow_redirects=False)
    assert r.status_code == 303
    return r


def test_initial_all_pending(client):
    c, db_mod = client
    qs = db_mod.list_quadrats()
    assert len(qs) == 4
    assert all(q["approved"] is False for q in qs)
    r = c.get("/reviews")
    assert r.status_code == 200
    assert "待复核" in r.text
    # 初始无任何核准 -> 核准 CSV 为空（仅表头），网页列出 4 个被排除样方
    r = c.get("/approved-export.csv")
    lines = [x for x in r.text.strip().splitlines() if x]
    assert lines == ["id,quadrat,species,count"]
    page = c.get("/reviews").text
    for q in ("Q1", "Q2", "Q3", "Q4"):
        assert q in page


def test_approve_binds_full_membership_and_values(client):
    c, db_mod = client
    approve(c, db_mod, "Q1")
    d = db_mod.get_quadrat("Q1")
    assert d["approval"] is not None and d["approval"]["valid"] is True
    assert len(d["approval"]["snapshot"]) == 3
    # 导出只含 Q1 的 3 条
    r = c.get("/approved-export.csv")
    assert r.status_code == 200
    body = r.text.strip().splitlines()
    assert len(body) == 4  # 表头 + 3 条
    assert all(",Q1," in line for line in body[1:])
    _, pending = db_mod.get_approved_export()
    assert {p["quadrat"] for p in pending} == {"Q2", "Q3", "Q4"}


def test_stale_approval_rejected_when_quadrat_changed(client):
    c, db_mod = client
    d = db_mod.get_quadrat("Q1")
    stale_hash = d["current_hash"]
    # 复核期间 Q1 发生改值
    db_mod.update_record("R001", "Q1", "羊草", 50)
    r = c.post("/reviews/Q1/approve", data={"expected_hash": stale_hash}, follow_redirects=False)
    assert r.status_code == 303
    assert db_mod.get_quadrat("Q1")["approval"] is None
    # 按最新值重新核准 -> 成功
    approve(c, db_mod, "Q1")
    assert db_mod.get_quadrat("Q1")["approval"]["valid"] is True


def test_other_quadrat_changes_do_not_block_approval(client):
    c, db_mod = client
    d = db_mod.get_quadrat("Q1")
    h = d["current_hash"]
    db_mod.update_record("R004", "Q2", "羊草", 1)  # 与 Q1 无关
    r = c.post("/reviews/Q1/approve", data={"expected_hash": h}, follow_redirects=False)
    assert r.status_code == 303
    assert db_mod.get_quadrat("Q1")["approval"]["valid"] is True
    # Q2 仍是待复核
    assert db_mod.get_quadrat("Q2")["approval"] is None


def test_add_delete_change_revoke_approved(client):
    c, db_mod = client
    approve(c, db_mod, "Q1")
    # 新增记录进 Q1 -> 失效
    db_mod.add_record("R900", "Q1", "新物种", 2)
    assert db_mod.get_quadrat("Q1")["approval"] is None
    assert db_mod.get_quadrat("Q1")["events"][0]["reason"] == "add"

    approve(c, db_mod, "Q1")
    db_mod.delete_record("R900")
    assert db_mod.get_quadrat("Q1")["approval"] is None
    assert db_mod.get_quadrat("Q1")["events"][0]["reason"] == "delete"

    approve(c, db_mod, "Q1")
    db_mod.update_record("R002", "Q1", "针茅", 88)
    assert db_mod.get_quadrat("Q1")["approval"] is None
    assert db_mod.get_quadrat("Q1")["events"][0]["reason"] == "change"


def test_revert_and_same_id_rebuild_do_not_restore(client):
    c, db_mod = client
    approve(c, db_mod, "Q1")
    db_mod.update_record("R001", "Q1", "羊草", 50)
    assert db_mod.get_quadrat("Q1")["approval"] is None
    # 改回原值 -> 仍待复核
    db_mod.update_record("R001", "Q1", "羊草", 12)
    assert db_mod.get_quadrat("Q1")["approval"] is None

    approve(c, db_mod, "Q1")
    # 删除后同 ID 重建（值相同）-> 不恢复
    db_mod.delete_record("R001")
    db_mod.add_record("R001", "Q1", "羊草", 12)
    assert db_mod.get_quadrat("Q1")["approval"] is None
    ev = db_mod.get_quadrat("Q1")["events"][0]
    assert ev["reason"] in ("add", "delete")


def test_same_value_save_keeps_approval(client):
    c, db_mod = client
    approve(c, db_mod, "Q2")
    ok = db_mod.update_record("R004", "Q2", "羊草", 9)  # 与现值完全相同
    assert ok is True
    d = db_mod.get_quadrat("Q2")
    assert d["approval"] is not None and d["approval"]["valid"] is True


def test_move_in_out_revokes_both_quadrats(client):
    c, db_mod = client
    approve(c, db_mod, "Q1")
    approve(c, db_mod, "Q2")
    # R001 从 Q1 迁入 Q2（改样方字段）
    db_mod.update_record("R001", "Q2", "羊草", 12)
    assert db_mod.get_quadrat("Q1")["approval"] is None
    assert db_mod.get_quadrat("Q2")["approval"] is None
    e1 = db_mod.get_quadrat("Q1")["events"][0]
    e2 = db_mod.get_quadrat("Q2")["events"][0]
    assert e1["reason"] == "move_out"
    assert e2["reason"] == "move_in"
    # 未涉及的 Q3/Q4 不受影响
    approve(c, db_mod, "Q3")
    assert db_mod.get_quadrat("Q3")["approval"]["valid"] is True


def test_transaction_rollback_leaves_data_and_approval(client):
    c, db_mod = client
    approve(c, db_mod, "Q1")
    # 构造必失败的动作批次：重复主键 -> 整批回滚，核准不失效
    bad_actions = [
        {"action": "insert", "id": "R777", "record": {"quadrat": "Q1", "species": "x", "count": 1}},
        {"action": "insert", "id": "R001", "record": {"quadrat": "Q1", "species": "dup", "count": 1}},
    ]
    with pytest.raises(Exception):
        db_mod.apply_actions(bad_actions)
    # 数据未变、核准仍有效
    recs = db_mod.get_records()
    assert "R777" not in recs
    assert recs["R001"]["species"] == "羊草"
    assert db_mod.get_quadrat("Q1")["approval"]["valid"] is True


def test_merge_invalidates_affected_quadrats_only(client):
    c, db_mod = client
    approve(c, db_mod, "Q1")
    approve(c, db_mod, "Q3")
    # 走完整网页合并流程：基线 -> 上传修订（改 Q1 的 R002、删 Q3 的 R008）
    c.post("/baselines", data={"name": "b1"})
    revision = (
        "id,quadrat,species,count\n"
        "R001,Q1,羊草,12\n"
        "R002,Q1,针茅,10\n"   # 改值，影响 Q1
        "R003,Q1,冷蒿,5\n"
        "R004,Q2,羊草,9\n"
        "R005,Q2,冰草,14\n"
        "R006,Q2,糙隐子草,6\n"
        "R007,Q3,针茅,11\n"
        # R008 缺行 = 删除，影响 Q3
        "R009,Q3,羊草,7\n"
        "R010,Q4,冰草,10\n"
        "R011,Q4,糙隐子草,4\n"
        "R012,Q4,针茅,15\n"
    )
    r = c.post(
        "/merge/upload",
        files={"file": ("rev.csv", revision.encode(), "text/csv")},
        data={"baseline_id": 1},
        follow_redirects=False,
    )
    token = r.headers["location"].split("/")[-1]
    r = c.post(f"/previews/{token}/confirm", data={}, follow_redirects=False)
    assert r.status_code == 303
    assert db_mod.get_quadrat("Q1")["approval"] is None
    assert db_mod.get_quadrat("Q3")["approval"] is None
    # Q2/Q4 未被触及，仍待复核/未核准状态不受影响
    assert db_mod.get_records()["R002"]["count"] == 10
    assert "R008" not in db_mod.get_records()
    # 合并动作与失效已一起提交：事件可查
    assert db_mod.get_quadrat("Q1")["events"][0]["reason"] == "merge"


def test_approved_export_consistency_and_exclusion_list(client):
    c, db_mod = client
    approve(c, db_mod, "Q1")
    approve(c, db_mod, "Q4")
    r = c.get("/approved-export.csv")
    assert r.status_code == 200
    rows = [ln for ln in r.text.strip().splitlines()[1:]]
    assert len(rows) == 6  # Q1 3 + Q4 3
    assert all((",Q1," in ln) or (",Q4," in ln) for ln in rows)
    page = c.get("/reviews").text
    assert "Q2" in page and "Q3" in page  # 被排除的待复核样方列出
    # 改 Q1 一条 -> 立即失效 -> 下一次导出不再含 Q1
    db_mod.update_record("R001", "Q1", "羊草", 33)
    r = c.get("/approved-export.csv")
    rows = [ln for ln in r.text.strip().splitlines()[1:]]
    assert all(",Q1," not in ln for ln in rows)
    assert len(rows) == 3  # 只剩 Q4
    _, pending = db_mod.get_approved_export()
    assert {p["quadrat"] for p in pending} == {"Q1", "Q2", "Q3"}


def test_restart_persists_state(client, tmp_path, monkeypatch):
    c, db_mod = client
    approve(c, db_mod, "Q1")
    approve(c, db_mod, "Q2")
    db_mod.update_record("R004", "Q2", "羊草", 99)  # Q2 失效
    path = db_mod.DB_PATH
    # 模拟重启：重新 import 模块并 init_db（同一文件，不重新播种）
    import importlib
    fresh = importlib.reload(db_mod)
    assert fresh.DB_PATH == path
    fresh.init_db(None)
    assert fresh.get_quadrat("Q1")["approval"]["valid"] is True
    assert fresh.get_quadrat("Q2")["approval"] is None
    assert len(fresh.get_records()) == 12


def test_concurrent_approval_and_maintenance_never_exports_unreviewed(client):
    """两个共库连接（模拟两个 Web 进程）并发：维护修改必须让核准失效，
    或核准在修改之后发生（绑定新值）。任何时刻导出的记录都属于有效核准。"""
    import threading
    c, db_mod = client
    errors = []

    def worker_approve():
        try:
            for _ in range(20):
                d = db_mod.get_quadrat("Q1")
                try:
                    db_mod.approve_quadrat("Q1", d["current_hash"])
                except db_mod.StaleApprovalError:
                    pass
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    def worker_edit():
        try:
            for i in range(20):
                db_mod.update_record("R001", "Q1", "羊草", 12 + (i % 3))
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    t1 = threading.Thread(target=worker_approve)
    t2 = threading.Thread(target=worker_edit)
    t1.start(); t2.start(); t1.join(); t2.join()
    assert not errors
    # 最终不变量：若显示已核准，其绑定哈希必须等于当前内容哈希
    d = db_mod.get_quadrat("Q1")
    if d["approval"] is not None:
        assert d["approval"]["snapshot_hash"] == d["current_hash"]
    # 导出内容里的 Q1 记录（若有）必然与当前库一致且核准有效
    recs, _ = db_mod.get_approved_export()
    if "R001" in recs:
        assert d["approval"] is not None
        assert recs["R001"] == db_mod.get_records()["R001"]


def test_normal_export_and_merge_still_available(client):
    c, db_mod = client
    r = c.get("/export.csv")
    assert r.status_code == 200
    assert r.text.startswith("id,quadrat,species,count")
    assert len(r.text.strip().splitlines()) == 13
    # 首页与合并预览页仍可访问
    assert c.get("/").status_code == 200
    c.post("/baselines", data={"name": "keep"})
    assert c.get("/baselines/1").status_code == 200
