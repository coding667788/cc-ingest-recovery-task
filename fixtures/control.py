"""Control synthetic upstreams for tests; never import this from a collector."""
import argparse
import json
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["status", "expected", "advance", "poison", "repair",
                                          "trim", "rotate", "expire", "mutate-on-scan",
                                          "expire-on-page", "fault", "clear-faults"])
    parser.add_argument("--base", default="http://127.0.0.1:8765")
    parser.add_argument("--source", choices=["all", "alpha", "beta"], default="all")
    parser.add_argument("--mode", choices=["throttle", "unavailable", "truncate", "delay"])
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--seconds", type=float, default=3)
    parser.add_argument("--path", default=None)
    args = parser.parse_args()
    if args.action in ("status", "expected"):
        req = Request(args.base.rstrip("/") + "/admin/" + args.action)
    else:
        body = {"action": args.action, "source": args.source, "mode": args.mode,
                "count": args.count, "seconds": args.seconds}
        if args.path is not None:
            body["path"] = args.path
        req = Request(args.base.rstrip("/") + "/admin/control",
                      data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    opener = build_opener(ProxyHandler({}))
    try:
        with opener.open(req, timeout=15) as resp:
            print(json.dumps(json.load(resp), ensure_ascii=False, indent=2))
    except HTTPError as exc:
        parser.exit(1, f"HTTP {exc.code}: {exc.read().decode('utf-8')}\n")


if __name__ == "__main__":
    main()
