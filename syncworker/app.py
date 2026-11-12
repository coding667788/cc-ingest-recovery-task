"""Application assembly: one bounded client, one storage, one worker/source."""
from __future__ import annotations

import os
import threading

from .config import Config
from .crash import CrashController
from .httpclient import BoundedHttpClient
from .jsonlog import JsonLineLogger
from .protocol import AlphaClient, BetaClient
from .storage import Storage
from .worker import SourceWorker


class Application:
    def __init__(self, config: Config, db_path: str, logger: JsonLineLogger,
                 crash_at: str | None = None, crash_source: str | None = None):
        directory = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(directory, exist_ok=True)
        self._logger = logger
        self._shutdown = threading.Event()
        self._storage = Storage(db_path, [source.name for source in config.sources])
        self._client = BoundedHttpClient(config, logger)
        self._crash = CrashController(crash_at, crash_source, logger)
        protocols = {
            "alpha": lambda: AlphaClient(
                self._client,
                next(s.base_url for s in config.sources if s.name == "alpha"),
                config.page_size,
            ),
            "beta": lambda: BetaClient(
                self._client,
                next(s.base_url for s in config.sources if s.name == "beta"),
                config.page_size,
            ),
        }
        self._workers = []
        for source in config.sources:
            protocol = protocols[source.name]()
            self._workers.append(SourceWorker(
                source.name, self._client, protocol, self._storage,
                config, logger, self._shutdown, self._crash,
            ))

    def run(self) -> int:
        for worker in self._workers:
            worker.start()
        try:
            while not self._shutdown.is_set():
                if self._shutdown.wait(timeout=0.5):
                    break
        finally:
            for worker in self._workers:
                worker.join(timeout=30)
            self._storage.close()
            self._logger.log("shutdown")
        return 0

    def request_shutdown(self) -> None:
        self._shutdown.set()
