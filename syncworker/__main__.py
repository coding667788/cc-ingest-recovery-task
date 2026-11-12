"""Command line entry point: python -m syncworker --config ... --db ..."""
from __future__ import annotations

import argparse
import signal
import sys

from .app import Application
from .config import load_config
from .jsonlog import JsonLineLogger


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Multi-source inventory collector")
    parser.add_argument("--config", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--crash-at", choices=["after_receive", "before_commit",
                                                "after_commit"], default=None)
    parser.add_argument("--crash-source", choices=["alpha", "beta"], default=None)
    args = parser.parse_args(argv)
    if args.crash_at is not None and args.crash_source is None:
        parser.error("--crash-source is required when --crash-at is used")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    logger = JsonLineLogger()
    try:
        config = load_config(args.config)
    except (OSError, ValueError) as exc:
        logger.log("config_error", error=str(exc))
        return 2
    logger.log("startup", config=args.config, db=args.db,
               crash_at=args.crash_at, crash_source=args.crash_source)
    application = Application(
        config, args.db, logger,
        crash_at=args.crash_at, crash_source=args.crash_source,
    )

    def handle_signal(signum, frame):
        logger.log("shutdown_requested", signal=signum)
        application.request_shutdown()

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)
    return application.run()


if __name__ == "__main__":
    sys.exit(main())
