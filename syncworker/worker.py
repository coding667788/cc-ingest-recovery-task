"""Per-source state machine: bootstrap, incremental tailing, safe rebuild.

One worker thread per source. A worker has at most one in-flight HTTP
request at any moment; BoundedHttpClient additionally enforces the global
and per-source concurrency ceilings. All durable writes happen through
Storage transactions.
"""
from __future__ import annotations

import datetime
import threading
import time

from .backoff import Backoff
from .errors import (
    BadEnvelope,
    CursorExpired,
    EpochChanged,
    HistoryGap,
    IncompletePage,
    PermanentUpstreamError,
    RateLimited,
    TransientUpstreamError,
)
from .validation import validate_payload


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


class SourceWorker:
    def __init__(self, name: str, client, protocol, storage, config,
                 logger, shutdown: threading.Event, crash):
        self.name = name
        self._client = client
        self._api = protocol
        self._storage = storage
        self._page_size = config.page_size
        self._poll = config.poll_interval
        self._logger = logger
        self._shutdown = shutdown
        self._crash = crash
        self._backoff = Backoff(shutdown)
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name=f"syncworker-{self.name}", daemon=True,
        )
        self._thread.start()

    def join(self, timeout: float | None = None) -> None:
        if self._thread is not None:
            self._thread.join(timeout)

    # -- main loop ----------------------------------------------------------

    def _run(self) -> None:
        self._logger.log("worker_start", source=self.name)
        while not self._shutdown.is_set():
            try:
                state = self._storage.get_state(self.name)
                if state["epoch"] is None:
                    self._bootstrap()
                    continue
                rebuild = self._storage.get_rebuild(self.name)
                if rebuild is not None:
                    self._resume_or_restart_rebuild(rebuild)
                    continue
                self._run_incremental(state)
            except PermanentUpstreamError as exc:
                delay = self._backoff.permanent()
                self._storage.set_status(
                    self.name, mode="error",
                    backoff_until=time.monotonic() + delay,
                    last_error=str(exc),
                )
                self._logger.log("permanent_error", source=self.name,
                                 error=str(exc), retry_after=delay)
                if not self._backoff.wait(delay):
                    break
            except (TransientUpstreamError, IncompletePage, BadEnvelope) as exc:
                delay = self._backoff.failure()
                self._storage.set_status(
                    self.name,
                    backoff_until=time.monotonic() + delay,
                    last_error=str(exc),
                )
                self._logger.log("retry", source=self.name, error=str(exc),
                                 retry_after=round(delay, 3))
                if not self._backoff.wait(delay):
                    break
            except RateLimited as exc:
                delay = self._backoff.rate_limited(exc.retry_after)
                self._storage.set_status(
                    self.name,
                    backoff_until=time.monotonic() + delay,
                    last_error=str(exc),
                )
                self._logger.log("retry", source=self.name,
                                 error="rate_limited", retry_after=delay)
                if not self._backoff.wait(delay):
                    break
            except (EpochChanged, HistoryGap) as exc:
                self._logger.log(
                    type(exc).__name__, source=self.name,
                    epoch=getattr(exc, "code", None),
                )
                self._start_rebuild()
        self._logger.log("worker_stop", source=self.name)

    # -- incremental tailing ------------------------------------------------

    def _run_incremental(self, state: dict) -> None:
        head = self._api.head()
        if head["epoch"] != state["epoch"]:
            self._logger.log("epoch_changed", source=self.name,
                             epoch=head["epoch"], old_epoch=state["epoch"])
            self._start_rebuild()
            return
        if state["checkpoint"] < head["floor"]:
            self._logger.log("history_gap", source=self.name,
                             epoch=head["epoch"],
                             checkpoint=state["checkpoint"],
                             floor=head["floor"])
            self._start_rebuild()
            return
        if self._backoff.backoff_until is None:
            self._backoff.reset()
            self._storage.clear_backoff(self.name)
        self._storage.set_status(self.name, mode="running")
        if head["head"] == state["checkpoint"]:
            self._shutdown.wait(self._poll)
            return
        if self._crash.crash_source is not None \
                and self.name != self._crash.crash_source:
            # During a crash run only the configured source may process
            # incremental pages, so its first non-empty page is the trigger.
            self._shutdown.wait(self._poll)
            return
        self._drain_window(head["epoch"], state["checkpoint"], head["head"],
                           state["generation"], is_incremental=True)

    def _first_page(self, epoch: str, after: int, until: int) -> dict:
        if self.name == "alpha":
            page = self._api.changes_window(epoch, after, until)
            return {"epoch": page["epoch"], "until": page["until"],
                    "through": page["through"], "more": bool(page["next_cursor"]),
                    "events": page["events"], "cursor": page["next_cursor"]}
        page = self._api.events_window(epoch, after, until)
        return {**page, "cursor": None}

    def _next_page(self, cursor: str) -> dict:
        page = self._api.changes_cursor(cursor)
        return {"epoch": page["epoch"], "until": page["until"],
                "through": page["through"], "more": bool(page["next_cursor"]),
                "events": page["events"], "cursor": page["next_cursor"]}

    def _drain_window(self, epoch: str, after: int, target: int,
                      generation: int, *, is_incremental: bool) -> None:
        """Read one finite window (after, target] page by page and commit each
        fully parsed page. Numeric progress is the only restart anchor; an
        opaque cursor is used only inside this window attempt."""
        position = after
        cursor = None
        cursor_failures = 0
        while not self._shutdown.is_set() and position < target:
            try:
                if cursor is None:
                    page = self._first_page(epoch, position, target)
                else:
                    page = self._next_page(cursor)
            except CursorExpired:
                cursor_failures += 1
                self._logger.log("cursor_expired", source=self.name,
                                 epoch=epoch, checkpoint=position)
                if cursor_failures > 5:
                    raise TransientUpstreamError(
                        "repeated cursor expiry; replaying from numeric checkpoint"
                    )
                cursor = None
                continue
            page = self._check_window(page, epoch, position, target)
            valid, quarantine = self._classify(page["events"])
            self._commit_page(
                epoch, generation, page, valid, quarantine,
                is_incremental=is_incremental,
            )
            position = page["through"]
            cursor = page["cursor"] if page["more"] else None

    @staticmethod
    def _check_window(page: dict, epoch: str, after: int, until: int) -> dict:
        if page.get("epoch") != epoch:
            raise EpochChanged("window epoch mismatch")
        if page.get("until") != until:
            raise IncompletePage("window until changed mid-window")
        through = page.get("through")
        if not isinstance(through, int) or through <= after or through > until:
            raise IncompletePage("window through outside (after, until]")
        if not isinstance(page.get("more"), bool):
            raise IncompletePage("window more must be boolean")
        return page

    def _classify(self, events: list[dict]):
        valid, quarantine = [], []
        for event in events:
            reason = validate_payload(event)
            if reason is None:
                valid.append(event)
            else:
                quarantine.append((event, reason))
        return valid, quarantine

    def _commit_page(self, epoch: str, generation: int, page: dict,
                     valid_events: list[dict],
                     quarantine: list[tuple[dict, str]],
                     *, is_incremental: bool) -> None:
        eligible = self._crash.eligible(
            self.name, is_incremental=is_incremental,
            page_events=len(page["events"]),
        )
        if eligible:
            self._crash.after_receive(self.name, epoch)
        self._storage.begin()
        try:
            if is_incremental:
                result = self._storage.apply_incremental_page(
                    self.name, epoch, generation, page["through"],
                    valid_events, quarantine,
                )
            else:
                result = self._storage.apply_catchup_page(
                    self.name, generation, epoch, page["through"],
                    valid_events, quarantine,
                )
                rebuild = self._storage.get_rebuild(self.name)
                rebuild["catchup_after"] = page["through"]
                rebuild["window_cursor"] = page["cursor"]
                rebuild["target_until"] = page["until"]
                rebuild["phase"] = "catchup"
                self._storage.save_rebuild(rebuild)
            if eligible:
                self._crash.before_commit(self.name, epoch)
            self._storage.commit()
        except Exception:
            self._storage.rollback()
            raise
        if eligible:
            self._crash.after_commit(self.name, epoch)
        for event, reason in quarantine:
            self._logger.log("quarantine", source=self.name, epoch=epoch,
                             checkpoint=page["through"],
                             event_id=event["event_id"], reason=reason)
        self._logger.log("page_commit", source=self.name, epoch=epoch,
                         checkpoint=page["through"],
                         applied_new=result["applied_new"],
                         quarantined=result["quarantined"])

    # -- first bootstrap and safe rebuild ----------------------------------

    def _new_rebuild_row(self, epoch: str) -> dict:
        generation = self._storage.next_generation(self.name)
        return {
            "source": self.name, "generation": generation, "epoch": epoch,
            "fence": 0, "target_head": 0, "phase": "snapshot",
            "cursor": None, "after_key": "", "catchup_after": 0,
            "target_until": 0, "window_cursor": None,
            "created_at": _now_iso(),
        }

    def _bootstrap(self) -> None:
        self._storage.set_status(self.name, mode="initializing")
        rebuild = self._storage.get_rebuild(self.name)
        if rebuild is None:
            head = self._api.head()
            rebuild = self._new_rebuild_row(head["epoch"])
            self._storage.save_rebuild(rebuild)
        self._logger.log("bootstrap", source=self.name, epoch=rebuild["epoch"],
                         generation=rebuild["generation"])
        self._build_generation(rebuild)

    def _start_rebuild(self) -> None:
        head = self._api.head()
        old = self._storage.get_rebuild(self.name)
        if old is not None:
            self._storage.discard_rebuild(self.name, old["generation"])
        rebuild = self._new_rebuild_row(head["epoch"])
        self._storage.save_rebuild(rebuild)
        self._storage.set_status(self.name, mode="rebuilding")
        self._logger.log("rebuild_start", source=self.name, epoch=head["epoch"],
                         generation=rebuild["generation"])
        self._build_generation(rebuild)

    def _resume_or_restart_rebuild(self, rebuild: dict) -> None:
        self._storage.set_status(self.name, mode="rebuilding")
        head = self._api.head()
        if head["epoch"] != rebuild["epoch"]:
            self._logger.log("epoch_changed", source=self.name,
                             epoch=head["epoch"], old_epoch=rebuild["epoch"],
                             generation=rebuild["generation"])
            self._storage.discard_rebuild(self.name, rebuild["generation"])
            self._start_rebuild()
            return
        self._build_generation(rebuild)

    def _build_generation(self, rebuild: dict) -> None:
        if self.name == "alpha":
            self._build_alpha(rebuild)
        else:
            self._build_beta(rebuild)

    # -- alpha: frozen snapshot paging then change-log catch-up -------------

    def _build_alpha(self, rebuild: dict) -> None:
        epoch = rebuild["epoch"]
        generation = rebuild["generation"]
        # A restart cannot reuse the in-memory export id; taking a fresh
        # frozen export and restarting the snapshot is always safe.
        export_id = self._fresh_alpha_export(rebuild)
        cursor = None
        while True:
            try:
                page = self._api.snapshot_page(export_id, cursor)
            except CursorExpired:
                self._logger.log("cursor_expired", source=self.name,
                                 epoch=epoch, checkpoint=rebuild["fence"])
                export_id = self._fresh_alpha_export(rebuild)
                cursor = None
                continue
            if page["epoch"] != epoch:
                raise EpochChanged("snapshot epoch mismatch")
            valid, quarantine = self._classify(page["items"])
            self._storage.stage_rows(self.name, generation, epoch,
                                     valid, quarantine)
            cursor = page["next_cursor"]
            rebuild["cursor"] = cursor
            rebuild["phase"] = "snapshot"
            self._storage.save_rebuild(rebuild)
            if cursor is None:
                break
        self._catch_up(rebuild)

    def _fresh_alpha_export(self, rebuild: dict) -> str:
        export = self._api.create_export()
        if export["epoch"] != rebuild["epoch"]:
            raise EpochChanged("export epoch differs from rebuild epoch")
        self._storage.discard_staging(self.name, rebuild["generation"])
        rebuild["fence"] = export["fence"]
        rebuild["catchup_after"] = export["fence"]
        rebuild["target_until"] = export["fence"]
        rebuild["cursor"] = None
        rebuild["window_cursor"] = None
        rebuild["phase"] = "snapshot"
        self._storage.save_rebuild(rebuild)
        return export["export_id"]

    # -- beta: real-time key scan then finite change-log catch-up -----------

    def _build_beta(self, rebuild: dict) -> None:
        epoch = rebuild["epoch"]
        generation = rebuild["generation"]
        # Tickets are not persisted and the list is not frozen; every (re)
        # start of the build takes a fresh ticket and scans from the first key.
        ticket = self._fresh_beta_ticket(rebuild)
        after_key = ""
        while True:
            page = self._api.scan_page(ticket, after_key)
            if page["epoch"] != epoch:
                raise EpochChanged("scan epoch mismatch")
            valid, quarantine = self._classify(page["rows"])
            self._storage.stage_rows(self.name, generation, epoch,
                                     valid, quarantine)
            next_key = page["next_key"]
            after_key = next_key or ""
            rebuild["after_key"] = after_key
            rebuild["phase"] = "scan"
            self._storage.save_rebuild(rebuild)
            if next_key is None:
                break
        self._catch_up(rebuild)

    def _fresh_beta_ticket(self, rebuild: dict) -> str:
        ticket_info = self._api.bootstrap()
        if ticket_info["epoch"] != rebuild["epoch"]:
            raise EpochChanged("bootstrap epoch mismatch")
        self._storage.discard_staging(self.name, rebuild["generation"])
        rebuild["fence"] = ticket_info["fence"]
        rebuild["catchup_after"] = ticket_info["fence"]
        rebuild["target_until"] = ticket_info["fence"]
        rebuild["after_key"] = ""
        rebuild["window_cursor"] = None
        rebuild["phase"] = "scan"
        self._storage.save_rebuild(rebuild)
        return ticket_info["ticket"]

    # -- finite catch-up and atomic publish ---------------------------------

    def _catch_up(self, rebuild: dict) -> None:
        epoch = rebuild["epoch"]
        head = self._api.head()
        if head["epoch"] != epoch:
            raise EpochChanged("epoch changed before catch-up")
        if head["floor"] > head["head"]:
            raise HistoryGap("upstream floor exceeds its head")
        target = head["head"]
        rebuild["target_head"] = target
        rebuild["target_until"] = target
        rebuild["phase"] = "catchup"
        position = max(rebuild["catchup_after"], rebuild["fence"])
        self._storage.save_rebuild(rebuild)
        while position < target:
            page = self._first_page(epoch, position, target)
            page = self._check_window(page, epoch, position, target)
            valid, quarantine = self._classify(page["events"])
            self._commit_page(epoch, rebuild["generation"], page, valid,
                              quarantine, is_incremental=False)
            position = page["through"]
            # The in-transaction save_rebuild inside _commit_page is the only
            # durable progress update; keep the in-memory row aligned with it.
            rebuild["catchup_after"] = position
        self._storage.publish_generation(
            self.name, rebuild["generation"], epoch, target
        )
        self._backoff.reset()
        self._logger.log("publish", source=self.name, epoch=epoch,
                         checkpoint=target, generation=rebuild["generation"])
