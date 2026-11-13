"""SQLite 数据访问层。单文件库。

表：
- records            当前样方记录
- baselines          基线快照
- previews           离线合并预览
- quadrat_approvals  样方核准（绑定核准时所见的完整成员与值的快照哈希）
- approval_events    核准/失效历史（失效原因可在网页查看）

写入均使用 BEGIN IMMEDIATE 显式事务：
- 同一进程内另用 LOCK 串行化；
- 多个 Web 进程共库时由 SQLite 文件锁（busy_timeout 等待）串行化；
- 记录变化与核准失效在同一事务内提交，任何失败整体回滚。
"""

import hashlib
import json
import os
import sqlite3
import threading
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "survey.db"))

# 同进程写操作与确认合并共用同一把锁；跨进程靠 BEGIN IMMEDIATE 文件锁
LOCK = threading.RLock()

# 失效原因枚举
REVOKE_ADD = "add"          # 样方内新增记录
REVOKE_DELETE = "delete"    # 样方内记录被删除（含同 ID 重建前的删除）
REVOKE_CHANGE = "change"    # 样方内记录值被修改
REVOKE_MOVE_IN = "move_in"  # 记录迁入该样方
REVOKE_MOVE_OUT = "move_out"  # 记录迁出该样方
REVOKE_MERGE = "merge"      # 离线合并导致的增删/改值/迁移

REASON_TEXT = {
    REVOKE_ADD: "样方内新增记录",
    REVOKE_DELETE: "样方内记录被删除",
    REVOKE_CHANGE: "样方内记录值被修改",
    REVOKE_MOVE_IN: "有记录迁入该样方",
    REVOKE_MOVE_OUT: "有记录迁出该样方",
    REVOKE_MERGE: "离线合并改动了样方记录",
}


class StaleApprovalError(Exception):
    """提交核准时样方已发生增删或改值，旧核准请求被拒绝。"""


def connect() -> sqlite3.Connection:
    # isolation_level=None：自动提交模式，事务边界由我们显式 BEGIN/COMMIT 控制，
    # 这样多进程共库时可以用 BEGIN IMMEDIATE 立即拿写锁。
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def init_db(seed_csv: str | None = None):
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    with LOCK, connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                id      TEXT PRIMARY KEY,
                quadrat TEXT NOT NULL,
                species TEXT NOT NULL,
                count   INTEGER NOT NULL CHECK (count >= 0)
            );
            CREATE TABLE IF NOT EXISTS baselines (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL,
                created_at TEXT NOT NULL,
                data       TEXT NOT NULL          -- JSON: {id: record}
            );
            CREATE TABLE IF NOT EXISTS previews (
                token       TEXT PRIMARY KEY,
                baseline_id INTEGER NOT NULL,
                created_at  TEXT NOT NULL,
                status      TEXT NOT NULL,        -- pending/applied/discarded
                message     TEXT NOT NULL DEFAULT '',
                revision    TEXT NOT NULL,        -- JSON: {id: record}
                snapshot    TEXT NOT NULL,        -- JSON: 预览时涉及记录的当前值 {id: record|null}
                plan        TEXT NOT NULL         -- JSON: compute_plan 的结果
            );
            CREATE TABLE IF NOT EXISTS quadrat_approvals (
                quadrat       TEXT PRIMARY KEY,
                approved_at   TEXT NOT NULL,
                snapshot      TEXT NOT NULL,      -- JSON: 核准时该样方的完整记录 {id: record}
                snapshot_hash TEXT NOT NULL       -- 快照内容哈希
            );
            CREATE TABLE IF NOT EXISTS approval_events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                quadrat       TEXT NOT NULL,
                event_at      TEXT NOT NULL,
                kind          TEXT NOT NULL,      -- approved/revoked
                reason        TEXT NOT NULL DEFAULT '',
                detail        TEXT NOT NULL DEFAULT '',
                snapshot_hash TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_approval_events_quadrat
                ON approval_events (quadrat, id DESC);
            """
        )
        empty = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"] == 0
    if empty and seed_csv and os.path.exists(seed_csv):
        from .csvio import parse_csv

        with open(seed_csv, encoding="utf-8") as f:
            seed = parse_csv(f.read())
        with LOCK, connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for rid, r in seed.items():
                    conn.execute(
                        "INSERT OR IGNORE INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
                        (rid, r["quadrat"], r["species"], r["count"]),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise


# ---------- 核准快照与失效 ----------


def canonical_snapshot(records: dict) -> str:
    """样方记录的规范化 JSON（作为核准绑定内容）。"""
    return json.dumps(records, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def snapshot_hash(records: dict) -> str:
    return hashlib.sha256(canonical_snapshot(records).encode("utf-8")).hexdigest()


def _revoke_if_approved(conn: sqlite3.Connection, quadrat: str, reason: str, detail: str) -> bool:
    """在当前事务内：若样方已核准则立即失效并留痕；否则不动。返回是否发生失效。"""
    row = conn.execute(
        "SELECT snapshot_hash FROM quadrat_approvals WHERE quadrat=?", (quadrat,)
    ).fetchone()
    if row is None:
        return False
    conn.execute("DELETE FROM quadrat_approvals WHERE quadrat=?", (quadrat,))
    conn.execute(
        "INSERT INTO approval_events (quadrat, event_at, kind, reason, detail, snapshot_hash)"
        " VALUES (?,?,?,?,?,?)",
        (quadrat, now(), "revoked", reason, detail, row["snapshot_hash"]),
    )
    return True


def _revoke_many(conn: sqlite3.Connection, affected: dict) -> None:
    """affected: {quadrat: (reason, detail)}。"""
    for quadrat, (reason, detail) in sorted(affected.items()):
        _revoke_if_approved(conn, quadrat, reason, detail)


def _quadrat_records(conn: sqlite3.Connection, quadrat: str) -> dict:
    rows = conn.execute(
        "SELECT id, quadrat, species, count FROM records WHERE quadrat=? ORDER BY id",
        (quadrat,),
    ).fetchall()
    return {
        row["id"]: {"quadrat": row["quadrat"], "species": row["species"], "count": row["count"]}
        for row in rows
    }


# ---------- 当前记录 ----------


def get_records() -> dict:
    with connect() as conn:
        rows = conn.execute("SELECT * FROM records ORDER BY id").fetchall()
    return {row["id"]: {"quadrat": row["quadrat"], "species": row["species"], "count": row["count"]} for row in rows}


def get_records_by_ids(ids) -> dict:
    """返回 {id: record|None}，不存在的 ID 值为 None。"""
    out = {}
    with connect() as conn:
        for rid in ids:
            row = conn.execute("SELECT * FROM records WHERE id = ?", (rid,)).fetchone()
            out[rid] = (
                {"quadrat": row["quadrat"], "species": row["species"], "count": row["count"]}
                if row
                else None
            )
    return out


def add_record(rid, quadrat, species, count):
    """新增记录；样方若已核准则在同一事务内立即转待复核。"""
    with LOCK, connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
                (rid, quadrat, species, count),
            )
            _revoke_if_approved(conn, quadrat, REVOKE_ADD, f"记录 {rid} 新增入样方")
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def update_record(rid, quadrat, species, count) -> bool:
    """改值/迁移记录。

    - 同值保存不撤销核准；
    - 改值、迁入、迁出均使受影响样方（旧样方与新样方）立即转待复核；
    - 记录变化与核准失效同一事务提交。
    """
    with LOCK, connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            old = conn.execute("SELECT * FROM records WHERE id=?", (rid,)).fetchone()
            if old is None:
                conn.rollback()
                return False
            new_vals = (quadrat, species, count, rid)
            if (old["quadrat"], old["species"], old["count"]) == (quadrat, species, count):
                # 同值保存：不产生记录变化，也不撤销核准
                conn.rollback()
                return True
            conn.execute(
                "UPDATE records SET quadrat=?, species=?, count=? WHERE id=?", new_vals
            )
            affected = {}
            if old["quadrat"] == quadrat:
                affected[quadrat] = (REVOKE_CHANGE, f"记录 {rid} 的值被人工修改")
            else:
                affected[old["quadrat"]] = (REVOKE_MOVE_OUT, f"记录 {rid} 迁出至样方 {quadrat}")
                affected[quadrat] = (REVOKE_MOVE_IN, f"记录 {rid} 自样方 {old['quadrat']} 迁入")
            _revoke_many(conn, affected)
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise


def delete_record(rid) -> bool:
    """删除记录；其所在样方若已核准则在同一事务内立即转待复核。"""
    with LOCK, connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            old = conn.execute("SELECT quadrat FROM records WHERE id=?", (rid,)).fetchone()
            if old is None:
                conn.rollback()
                return False
            conn.execute("DELETE FROM records WHERE id=?", (rid,))
            _revoke_if_approved(conn, old["quadrat"], REVOKE_DELETE, f"记录 {rid} 被删除")
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise


# ---------- 样方复核 ----------


def list_quadrats() -> list:
    """全部样方及其复核状态（按样方名排序）。"""
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT q.quadrat AS quadrat, q.n AS n,
                   a.approved_at AS approved_at, a.snapshot_hash AS snapshot_hash
            FROM (SELECT quadrat, COUNT(*) AS n FROM records GROUP BY quadrat) q
            LEFT JOIN quadrat_approvals a ON a.quadrat = q.quadrat
            ORDER BY q.quadrat
            """
        ).fetchall()
    return [
        {
            "quadrat": r["quadrat"],
            "record_count": r["n"],
            "approved": r["snapshot_hash"] is not None,
            "approved_at": r["approved_at"],
            "snapshot_hash": r["snapshot_hash"],
        }
        for r in rows
    ]


def get_quadrat(quadrat: str):
    """返回样方详情（全部记录 + 核准状态 + 历史），样方无记录时返回 None。"""
    with connect() as conn:
        records = _quadrat_records(conn, quadrat)
        if not records:
            return None
        ap = conn.execute(
            "SELECT * FROM quadrat_approvals WHERE quadrat=?", (quadrat,)
        ).fetchone()
        events = conn.execute(
            "SELECT * FROM approval_events WHERE quadrat=? ORDER BY id DESC LIMIT 20",
            (quadrat,),
        ).fetchall()
    current_hash = snapshot_hash(records)
    return {
        "quadrat": quadrat,
        "records": records,
        "current_hash": current_hash,
        "approval": (
            {
                "approved_at": ap["approved_at"],
                "snapshot": json.loads(ap["snapshot"]),
                "snapshot_hash": ap["snapshot_hash"],
                "valid": ap["snapshot_hash"] == current_hash,
            }
            if ap
            else None
        ),
        "events": [dict(e) for e in events],
    }


def approve_quadrat(quadrat: str, expected_hash: str) -> dict:
    """核准样方，绑定当前所见的完整成员和值。

    expected_hash 为复核页展示时的快照哈希：提交时若样方已发生增删或改值
    （含删除后同 ID 重建、改回原值），哈希不一致 -> 拒绝旧核准。
    其他样方的变化不影响本核准。
    """
    with LOCK, connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            records = _quadrat_records(conn, quadrat)
            if not records:
                conn.rollback()
                raise ValueError(f"样方 {quadrat} 没有记录，无法核准")
            h = snapshot_hash(records)
            if h != expected_hash:
                conn.rollback()
                raise StaleApprovalError(
                    f"样方 {quadrat} 在复核期间发生过增删或改值，旧核准已被拒绝，请按最新记录重新复核"
                )
            snap = canonical_snapshot(records)
            conn.execute(
                "INSERT INTO quadrat_approvals (quadrat, approved_at, snapshot, snapshot_hash)"
                " VALUES (?,?,?,?) ON CONFLICT(quadrat) DO UPDATE SET"
                " approved_at=excluded.approved_at, snapshot=excluded.snapshot,"
                " snapshot_hash=excluded.snapshot_hash",
                (quadrat, now(), snap, h),
            )
            conn.execute(
                "INSERT INTO approval_events (quadrat, event_at, kind, reason, detail, snapshot_hash)"
                " VALUES (?,?,?,?,?,?)",
                (quadrat, now(), "approved", "", "复核员核准样方，绑定当前全部成员与值", h),
            )
            conn.commit()
            return {"quadrat": quadrat, "snapshot_hash": h, "record_count": len(records)}
        except Exception:
            conn.rollback()
            raise


def list_approval_events(limit: int = 30) -> list:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM approval_events ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_approved_export() -> tuple:
    """核准导出：在同一个读事务内取状态与记录，保证二者来自同一已提交状态。

    返回 (records, pending_quadrats)：
    - records 仅含已核准样方的全部记录；
    - pending_quadrats 为被排除的待复核样方（有记录但无有效核准）。
    """
    with LOCK, connect() as conn:
        conn.execute("BEGIN")
        try:
            approvals = {
                r["quadrat"]: r["snapshot_hash"]
                for r in conn.execute("SELECT quadrat, snapshot_hash FROM quadrat_approvals").fetchall()
            }
            rows = conn.execute(
                "SELECT id, quadrat, species, count FROM records ORDER BY quadrat, id"
            ).fetchall()
            all_quadrats = {
                r["quadrat"]: r["n"]
                for r in conn.execute(
                    "SELECT quadrat, COUNT(*) AS n FROM records GROUP BY quadrat"
                ).fetchall()
            }
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    records = {}
    pending = []
    for quadrat in sorted(all_quadrats):
        if quadrat not in approvals:
            pending.append({"quadrat": quadrat, "record_count": all_quadrats[quadrat]})
    for row in rows:
        if row["quadrat"] in approvals:
            records[row["id"]] = {
                "quadrat": row["quadrat"],
                "species": row["species"],
                "count": row["count"],
            }
    return records, pending


# ---------- 基线 ----------


def save_baseline(name: str, data: dict) -> int:
    with LOCK, connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "INSERT INTO baselines (name, created_at, data) VALUES (?,?,?)",
                (name, now(), json.dumps(data, ensure_ascii=False)),
            )
            bid = cur.lastrowid
            conn.commit()
            return bid
        except Exception:
            conn.rollback()
            raise


def list_baselines() -> list:
    with connect() as conn:
        rows = conn.execute("SELECT id, name, created_at, data FROM baselines ORDER BY id DESC").fetchall()
    return [
        {"id": r["id"], "name": r["name"], "created_at": r["created_at"], "size": len(json.loads(r["data"]))}
        for r in rows
    ]


def get_baseline(bid: int):
    with connect() as conn:
        row = conn.execute("SELECT * FROM baselines WHERE id=?", (bid,)).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "name": row["name"],
        "created_at": row["created_at"],
        "data": json.loads(row["data"]),
    }


# ---------- 合并预览 ----------


def save_preview(token, baseline_id, revision, snapshot, plan):
    with LOCK, connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO previews (token, baseline_id, created_at, status, message, revision, snapshot, plan)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (
                    token,
                    baseline_id,
                    now(),
                    "pending",
                    "",
                    json.dumps(revision, ensure_ascii=False),
                    json.dumps(snapshot, ensure_ascii=False),
                    json.dumps(plan, ensure_ascii=False),
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def get_preview(token):
    with connect() as conn:
        row = conn.execute("SELECT * FROM previews WHERE token=?", (token,)).fetchone()
    if not row:
        return None
    return {
        "token": row["token"],
        "baseline_id": row["baseline_id"],
        "created_at": row["created_at"],
        "status": row["status"],
        "message": row["message"],
        "revision": json.loads(row["revision"]),
        "snapshot": json.loads(row["snapshot"]),
        "plan": json.loads(row["plan"]),
    }


def list_previews() -> list:
    with connect() as conn:
        rows = conn.execute(
            "SELECT token, baseline_id, created_at, status, message FROM previews ORDER BY created_at DESC, token DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def update_preview_plan(token, snapshot, plan, message):
    """过期预览被重新比较后，用新计划与新快照替换旧内容。"""
    with LOCK, connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "UPDATE previews SET snapshot=?, plan=?, message=?, status='pending', created_at=? WHERE token=?",
                (json.dumps(snapshot, ensure_ascii=False), json.dumps(plan, ensure_ascii=False), message, now(), token),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def set_preview_status(token, status, message=""):
    with LOCK, connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("UPDATE previews SET status=?, message=? WHERE token=?", (status, message, token))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def apply_actions(actions) -> None:
    """单事务应用最终合并动作，并在同一事务内使受影响样方的核准失效。

    受影响样方 = 动作涉及记录的旧样方与新样方（覆盖增删、改值、迁入迁出）。
    任何失败整体回滚：记录变化与核准失效都不生效。
    """
    with LOCK, connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            # quadrat -> 涉及的记录 ID 列表（用于失效说明）
            affected: dict[str, list] = {}

            def touch(quadrat: str, rid: str):
                affected.setdefault(quadrat, []).append(rid)

            for a in actions:
                if a["action"] == "insert":
                    r = a["record"]
                    conn.execute(
                        "INSERT INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
                        (a["id"], r["quadrat"], r["species"], r["count"]),
                    )
                    touch(r["quadrat"], a["id"])
                elif a["action"] == "update":
                    old = conn.execute(
                        "SELECT quadrat FROM records WHERE id=?", (a["id"],)
                    ).fetchone()
                    r = a["to"]
                    conn.execute(
                        "UPDATE records SET quadrat=?, species=?, count=? WHERE id=?",
                        (r["quadrat"], r["species"], r["count"], a["id"]),
                    )
                    if old is not None:
                        touch(old["quadrat"], a["id"])
                        if old["quadrat"] != r["quadrat"]:
                            touch(r["quadrat"], a["id"])
                    else:
                        touch(r["quadrat"], a["id"])
                elif a["action"] == "delete":
                    old = conn.execute(
                        "SELECT quadrat FROM records WHERE id=?", (a["id"],)
                    ).fetchone()
                    conn.execute("DELETE FROM records WHERE id=?", (a["id"],))
                    if old is not None:
                        touch(old["quadrat"], a["id"])
                else:
                    raise ValueError(f"未知动作：{a['action']}")

            for quadrat, ids in sorted(affected.items()):
                detail = "离线合并改动记录：" + ", ".join(sorted(set(ids)))
                _revoke_if_approved(conn, quadrat, REVOKE_MERGE, detail)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
