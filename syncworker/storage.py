"""SQLite persistence with a single transactional boundary per page/publish.

Public tables (fixed names/columns, extra columns allowed):
  sync_state, sync_items, sync_applied, sync_quarantine.
Internal tables: sync_staging (unpublished generation), sync_rebuild
(resumable rebuild progress), sync_meta (per-source generation counter).

A single connection is guarded by one lock so transactions from the two
source workers can never interleave inside one SQLite connection.
"""
from __future__ import annotations

import datetime
import json
import sqlite3
import threading
import time


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


SCHEMA = """
CREATE TABLE IF NOT EXISTS sync_state (
    source TEXT PRIMARY KEY,
    epoch TEXT,
    generation INTEGER,
    checkpoint INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    degraded INTEGER NOT NULL DEFAULT 0,
    mode TEXT NOT NULL DEFAULT 'initializing',
    backoff_until REAL,
    last_error TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_items (
    source TEXT NOT NULL,
    epoch TEXT NOT NULL,
    generation INTEGER NOT NULL,
    item_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
    payload_json TEXT,
    event_id TEXT,
    updated_at TEXT,
    PRIMARY KEY (source, generation, item_id)
);
CREATE TABLE IF NOT EXISTS sync_applied (
    source TEXT NOT NULL,
    epoch TEXT NOT NULL,
    event_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    seq INTEGER,
    applied_at TEXT NOT NULL,
    PRIMARY KEY (source, epoch, event_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS sync_quarantine (
    source TEXT NOT NULL,
    epoch TEXT NOT NULL,
    event_id TEXT NOT NULL,
    item_id TEXT,
    revision INTEGER,
    seq INTEGER,
    raw_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    resolved INTEGER NOT NULL DEFAULT 0 CHECK (resolved IN (0, 1)),
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    PRIMARY KEY (source, epoch, event_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS sync_staging (
    source TEXT NOT NULL,
    generation INTEGER NOT NULL,
    epoch TEXT NOT NULL,
    item_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    deleted INTEGER NOT NULL,
    payload_json TEXT,
    event_id TEXT,
    updated_at TEXT,
    PRIMARY KEY (source, generation, item_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS sync_rebuild (
    source TEXT PRIMARY KEY,
    generation INTEGER NOT NULL,
    epoch TEXT NOT NULL,
    fence INTEGER NOT NULL,
    target_head INTEGER NOT NULL,
    phase TEXT NOT NULL,
    cursor TEXT,
    after_key TEXT,
    catchup_after INTEGER NOT NULL,
    target_until INTEGER NOT NULL,
    window_cursor TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Storage:
    def __init__(self, path: str, source_names: list[str]):
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, timeout=30,
            isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(SCHEMA)
            for name in source_names:
                self._conn.execute(
                    "INSERT OR IGNORE INTO sync_state(source, epoch, generation, "
                    "checkpoint, status, degraded, mode, updated_at) "
                    "VALUES (?, NULL, NULL, 0, 'initializing', 0, 'initializing', ?)",
                    (name, _now()),
                )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- explicit transactions (crash hooks run inside these) --------------

    def begin(self) -> None:
        self._lock.acquire()
        self._conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self._conn.execute("COMMIT")
        self._lock.release()

    def rollback(self) -> None:
        try:
            self._conn.execute("ROLLBACK")
        finally:
            self._lock.release()

    # -- public state -------------------------------------------------------

    def get_state(self, source: str) -> dict:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM sync_state WHERE source=?", (source,)
            ).fetchone()
            return dict(row)

    def has_published(self, source: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT epoch FROM sync_state WHERE source=?", (source,)
            ).fetchone()
            return row is not None and row["epoch"] is not None

    def _recompute_degraded(self, source: str) -> int:
        row = self._conn.execute(
            "SELECT 1 FROM sync_quarantine WHERE source=? AND resolved=0 LIMIT 1",
            (source,),
        ).fetchone()
        return 1 if row else 0

    def set_status(self, source: str, *, mode: str | None = None,
                   backoff_until: float | None = None,
                   last_error: str | None = None) -> None:
        """Persist the effective public status.

        Priority: error > backoff > rebuilding > initializing > degraded >
        running. `degraded` is derived from unresolved quarantine rows so it
        survives restarts and combines with other modes."""
        with self._lock:
            current = self._conn.execute(
                "SELECT mode, backoff_until, last_error FROM sync_state "
                "WHERE source=?", (source,),
            ).fetchone()
            new_mode = mode if mode is not None else current["mode"]
            new_backoff = (backoff_until if backoff_until is not None
                           else current["backoff_until"])
            new_error = (last_error if last_error is not None
                         else current["last_error"])
            degraded = self._recompute_degraded(source)
            if new_mode == "error":
                status = "error"
            elif new_backoff is not None and new_backoff > time.monotonic():
                status = "backoff"
            elif new_mode == "rebuilding":
                status = "rebuilding"
            elif new_mode == "initializing":
                status = "initializing"
            elif degraded:
                status = "degraded"
            else:
                status = "running"
            self._conn.execute(
                "UPDATE sync_state SET mode=?, backoff_until=?, last_error=?, "
                "degraded=?, status=?, updated_at=? WHERE source=?",
                (new_mode, new_backoff, new_error, degraded, status,
                 _now(), source),
            )

    def clear_backoff(self, source: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE sync_state SET backoff_until=NULL, last_error=NULL, "
                "updated_at=? WHERE source=?", (_now(), source),
            )

    # -- generation counter -------------------------------------------------

    def next_generation(self, source: str) -> int:
        with self._lock:
            key = f"generation:{source}"
            row = self._conn.execute(
                "SELECT value FROM sync_meta WHERE key=?", (key,)
            ).fetchone()
            generation = 1 if row is None else int(row["value"]) + 1
            self._conn.execute(
                "INSERT INTO sync_meta(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(generation)),
            )
            return generation

    # -- event helpers ------------------------------------------------------

    @staticmethod
    def _payload_json(event: dict) -> str | None:
        if event["deleted"]:
            return None
        return json.dumps(event["payload"], ensure_ascii=False)

    def _insert_quarantine(self, source: str, epoch: str, event: dict,
                           reason: str, seq: int | None) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO sync_quarantine(source, epoch, event_id, "
            "item_id, revision, seq, raw_json, reason, resolved, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
            (source, epoch, event["event_id"], event["item_id"],
             event["revision"], seq,
             json.dumps(event, ensure_ascii=False), reason, _now()),
        )

    def _resolve_quarantine(self, source: str, epoch: str, item_id: str,
                            revision: int) -> None:
        self._conn.execute(
            "UPDATE sync_quarantine SET resolved=1, resolved_at=? "
            "WHERE source=? AND epoch=? AND item_id=? AND revision < ? "
            "AND resolved=0",
            (_now(), source, epoch, item_id, revision),
        )

    def _merge_item(self, table: str, source: str, epoch: str,
                    generation: int, event: dict) -> None:
        existing = self._conn.execute(
            f"SELECT revision FROM {table} WHERE source=? AND generation=? "
            f"AND item_id=?", (source, generation, event["item_id"]),
        ).fetchone()
        if existing is not None and event["revision"] <= existing["revision"]:
            return
        self._conn.execute(
            f"INSERT INTO {table}(source, generation, epoch, item_id, revision, "
            f"deleted, payload_json, event_id, updated_at) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            f"ON CONFLICT(source, generation, item_id) DO UPDATE SET "
            f"revision=excluded.revision, deleted=excluded.deleted, "
            f"payload_json=excluded.payload_json, event_id=excluded.event_id, "
            f"updated_at=excluded.updated_at, epoch=excluded.epoch",
            (source, generation, epoch, event["item_id"], event["revision"],
             1 if event["deleted"] else 0, self._payload_json(event),
             event["event_id"], event["updated_at"]),
        )

    # -- incremental page against the published generation -----------------

    def apply_incremental_page(self, source: str, epoch: str, generation: int,
                               through: int, events: list[dict],
                               quarantine: list[tuple[dict, str]]) -> dict:
        """Caller must hold an open transaction. Materialized rows, applied
        dedup, quarantine and the checkpoint move in that one transaction."""
        applied_new = 0
        for event in events:
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO sync_applied(source, epoch, event_id, "
                "item_id, revision, seq, applied_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (source, epoch, event["event_id"], event["item_id"],
                 event["revision"], event["seq"], _now()),
            )
            if cursor.rowcount == 1:
                applied_new += 1
                self._resolve_quarantine(
                    source, epoch, event["item_id"], event["revision"]
                )
                self._merge_item(
                    "sync_items", source, epoch, generation, event
                )
        for event, reason in quarantine:
            self._insert_quarantine(
                source, epoch, event, reason, event.get("seq")
            )
        self._conn.execute(
            "UPDATE sync_state SET checkpoint=?, updated_at=? WHERE source=?",
            (through, _now(), source),
        )
        return {"applied_new": applied_new,
                "quarantined": len(quarantine)}

    # -- rebuild staging and resumption ------------------------------------

    def save_rebuild(self, row: dict) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO sync_rebuild(source, generation, epoch, fence, "
                "target_head, phase, cursor, after_key, catchup_after, "
                "target_until, window_cursor, created_at, updated_at) "
                "VALUES (:source, :generation, :epoch, :fence, :target_head, "
                ":phase, :cursor, :after_key, :catchup_after, :target_until, "
                ":window_cursor, :created_at, :updated_at) "
                "ON CONFLICT(source) DO UPDATE SET generation=excluded.generation, "
                "epoch=excluded.epoch, fence=excluded.fence, "
                "target_head=excluded.target_head, phase=excluded.phase, "
                "cursor=excluded.cursor, after_key=excluded.after_key, "
                "catchup_after=excluded.catchup_after, "
                "target_until=excluded.target_until, "
                "window_cursor=excluded.window_cursor, "
                "updated_at=excluded.updated_at",
                {**row, "updated_at": _now()},
            )

    def get_rebuild(self, source: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM sync_rebuild WHERE source=?", (source,)
            ).fetchone()
            return dict(row) if row else None

    def discard_rebuild(self, source: str, generation: int) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM sync_staging WHERE source=? AND generation=?",
                (source, generation),
            )
            self._conn.execute(
                "DELETE FROM sync_rebuild WHERE source=? AND generation=?",
                (source, generation),
            )

    def discard_staging(self, source: str, generation: int) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM sync_staging WHERE source=? AND generation=?",
                (source, generation),
            )

    def stage_rows(self, source: str, generation: int, epoch: str,
                   rows: list[dict], quarantine: list[tuple[dict, str]]) -> None:
        """Persist one fetched snapshot/scan page plus any bad payloads."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for event in rows:
                    self._merge_item(
                        "sync_staging", source, epoch, generation, event
                    )
                for event, reason in quarantine:
                    self._insert_quarantine(
                        source, epoch, event, reason, event.get("seq")
                    )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def apply_catchup_page(self, source: str, generation: int, epoch: str,
                           through: int, events: list[dict],
                           quarantine: list[tuple[dict, str]]) -> dict:
        """Caller must hold an open transaction. Catch-up events are written
        to staging materialization plus the shared applied/quarantine audit."""
        applied_new = 0
        for event in events:
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO sync_applied(source, epoch, event_id, "
                "item_id, revision, seq, applied_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (source, epoch, event["event_id"], event["item_id"],
                 event["revision"], event["seq"], _now()),
            )
            if cursor.rowcount == 1:
                applied_new += 1
                self._resolve_quarantine(
                    source, epoch, event["item_id"], event["revision"]
                )
                self._merge_item(
                    "sync_staging", source, epoch, generation, event
                )
        for event, reason in quarantine:
            self._insert_quarantine(
                source, epoch, event, reason, event.get("seq")
            )
        return {"applied_new": applied_new,
                "quarantined": len(quarantine)}

    def publish_generation(self, source: str, generation: int, epoch: str,
                           checkpoint: int) -> None:
        """Atomically replace the source's public data, epoch/generation and
        checkpoint. Old generation rows are removed in the same transaction;
        readers never see an empty or mixed generation."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "DELETE FROM sync_items WHERE source=? AND generation<>?",
                    (source, generation),
                )
                self._conn.execute(
                    "INSERT INTO sync_items(source, generation, epoch, item_id, "
                    "revision, deleted, payload_json, event_id, updated_at) "
                    "SELECT source, generation, epoch, item_id, revision, deleted, "
                    "payload_json, event_id, updated_at FROM sync_staging "
                    "WHERE source=? AND generation=?",
                    (source, generation),
                )
                self._conn.execute(
                    "DELETE FROM sync_staging WHERE source=? AND generation=?",
                    (source, generation),
                )
                self._conn.execute(
                    "DELETE FROM sync_rebuild WHERE source=?", (source,)
                )
                degraded = self._recompute_degraded(source)
                self._conn.execute(
                    "UPDATE sync_state SET epoch=?, generation=?, checkpoint=?, "
                    "mode='running', backoff_until=NULL, last_error=NULL, "
                    "degraded=?, status=CASE WHEN ?=1 THEN 'degraded' ELSE 'running' END, "
                    "updated_at=? WHERE source=?",
                    (epoch, generation, checkpoint, degraded, degraded,
                     _now(), source),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
