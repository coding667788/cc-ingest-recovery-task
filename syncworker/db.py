"""SQLite persistence layer.

Consistency boundaries (all multi-row writes happen inside one BEGIN IMMEDIATE
transaction on a per-thread connection):

- page commit: item upserts + sync_applied dedup rows + sync_quarantine rows +
  sync_state checkpoint advance are committed together, so a checkpoint never
  moves ahead of the data it covers.
- publish: sync_items replacement + sync_state (epoch/generation/checkpoint)
  switch + internal phase flip are committed together, so readers in a single
  read transaction only ever see a complete old or complete new generation.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS sync_state (
    source TEXT PRIMARY KEY,
    epoch TEXT,
    generation INTEGER,
    checkpoint INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    phase TEXT NOT NULL,
    last_error TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_items (
    source TEXT NOT NULL,
    epoch TEXT,
    generation INTEGER,
    item_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    deleted INTEGER NOT NULL,
    payload_json TEXT,
    PRIMARY KEY (source, item_id)
);
CREATE TABLE IF NOT EXISTS sync_items_staging (
    source TEXT NOT NULL,
    epoch TEXT,
    generation INTEGER,
    item_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    deleted INTEGER NOT NULL,
    payload_json TEXT,
    PRIMARY KEY (source, item_id)
);
CREATE TABLE IF NOT EXISTS sync_applied (
    source TEXT NOT NULL,
    epoch TEXT NOT NULL,
    event_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    PRIMARY KEY (source, epoch, event_id)
);
CREATE TABLE IF NOT EXISTS sync_quarantine (
    source TEXT NOT NULL,
    epoch TEXT NOT NULL,
    event_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    raw_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    resolved INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, epoch, event_id)
);
CREATE TABLE IF NOT EXISTS sync_internal (
    source TEXT PRIMARY KEY,
    phase TEXT NOT NULL,
    build_epoch TEXT,
    build_generation INTEGER,
    last_error TEXT
);
"""


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Store:
    def __init__(self, path):
        self.path = str(path)
        connection = self.connect()
        try:
            connection.executescript(SCHEMA)
        finally:
            connection.close()

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection


def initialize(connection, sources):
    """Create per-source rows on first run; recover internal phase after a crash."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        for source in sources:
            connection.execute(
                "INSERT OR IGNORE INTO sync_state"
                " (source, epoch, generation, checkpoint, status, phase, last_error, updated_at)"
                " VALUES (?, NULL, NULL, 0, 'initializing', 'initializing', NULL, ?)",
                (source, utcnow()))
            connection.execute(
                "INSERT OR IGNORE INTO sync_internal (source, phase) VALUES (?, 'building')",
                (source,))
            phase = connection.execute(
                "SELECT phase FROM sync_internal WHERE source=?", (source,)).fetchone()[0]
            generation = connection.execute(
                "SELECT generation FROM sync_state WHERE source=?", (source,)).fetchone()[0]
            if phase == "building":
                # A previous process died mid-build: discard the incomplete
                # unpublished generation; the build restarts from scratch.
                connection.execute(
                    "DELETE FROM sync_items_staging WHERE source=?", (source,))
            elif phase == "error" or (phase == "running" and generation is None):
                connection.execute(
                    "UPDATE sync_internal SET phase=? WHERE source=?",
                    ("building" if generation is None else "running", source))
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def get_phase(connection, source):
    return connection.execute(
        "SELECT phase FROM sync_internal WHERE source=?", (source,)).fetchone()[0]


def get_state(connection, source):
    row = connection.execute(
        "SELECT epoch, generation, checkpoint, status FROM sync_state WHERE source=?",
        (source,)).fetchone()
    return {"epoch": row[0], "generation": row[1], "checkpoint": row[2], "status": row[3]}


def _unresolved_quarantine(connection, source):
    return connection.execute(
        "SELECT COUNT(*) FROM sync_quarantine WHERE source=? AND resolved=0",
        (source,)).fetchone()[0]


def _status_for(connection, source, phase):
    return "degraded" if _unresolved_quarantine(connection, source) else phase


def update_status(connection, source, phase, last_error=None):
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            "UPDATE sync_state SET status=?, phase=?, last_error=?, updated_at=? WHERE source=?",
            (_status_for(connection, source, phase), phase, last_error, utcnow(), source))
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def set_error(connection, source, message):
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            "UPDATE sync_state SET status='error', phase='error', last_error=?, updated_at=?"
            " WHERE source=?", (message, utcnow(), source))
        connection.execute(
            "UPDATE sync_internal SET phase='error', last_error=? WHERE source=?",
            (message, source))
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def begin_build(connection, source):
    """Discard any pending generation and mark the source as building.

    Returns the generation number the new build should use.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        published = connection.execute(
            "SELECT generation FROM sync_state WHERE source=?", (source,)).fetchone()[0]
        generation = (published or 0) + 1
        connection.execute("DELETE FROM sync_items_staging WHERE source=?", (source,))
        connection.execute(
            "UPDATE sync_internal SET phase='building', build_epoch=NULL,"
            " build_generation=?, last_error=NULL WHERE source=?", (generation, source))
        connection.execute("COMMIT")
        return generation
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def set_build_epoch(connection, source, epoch):
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            "UPDATE sync_internal SET build_epoch=? WHERE source=?", (epoch, source))
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def commit_page_state(connection, source, checkpoint):
    """Advance the published checkpoint inside the caller's transaction."""
    connection.execute(
        "UPDATE sync_state SET checkpoint=?, status=?, phase='running', last_error=NULL,"
        " updated_at=? WHERE source=?",
        (checkpoint, _status_for(connection, source, "running"), utcnow(), source))


def publish(connection, source, epoch, generation, checkpoint):
    """Atomically replace the published generation for one source."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute("DELETE FROM sync_items WHERE source=?", (source,))
        connection.execute(
            "INSERT INTO sync_items"
            " (source, epoch, generation, item_id, revision, deleted, payload_json)"
            " SELECT source, epoch, generation, item_id, revision, deleted, payload_json"
            " FROM sync_items_staging WHERE source=?", (source,))
        connection.execute("DELETE FROM sync_items_staging WHERE source=?", (source,))
        connection.execute(
            "UPDATE sync_state SET epoch=?, generation=?, checkpoint=?, status=?,"
            " phase='running', last_error=NULL, updated_at=? WHERE source=?",
            (epoch, generation, checkpoint,
             _status_for(connection, source, "running"), utcnow(), source))
        connection.execute(
            "UPDATE sync_internal SET phase='running', build_epoch=NULL WHERE source=?",
            (source,))
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise
