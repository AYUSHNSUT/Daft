"""CLI entry point; fixture generation and timing are separate commands."""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path

from .fixtures import CASES, generate
from .run import CONFIGS, redacted, run, worker


def main() -> None:
    os.environ["DAFT_ANALYTICS_ENABLED"] = "0"
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    gen = commands.add_parser("generate")
    gen.add_argument("--output", type=Path, required=True)
    size = gen.add_mutually_exclusive_group(required=True)
    size.add_argument("--rows", type=int)
    size.add_argument("--logical-gib", type=float)
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--orders", nargs="+", choices=("sorted", "shuffled"), default=["sorted", "shuffled"])
    gen.add_argument("--layouts", nargs="+", choices=("single", "multi"), default=["single", "multi"])
    gen.add_argument("--files", type=int, default=8)
    gen.add_argument("--batch-rows", type=int, default=65536)
    bench = commands.add_parser("run")
    bench.add_argument("--manifest", type=Path, required=True)
    bench.add_argument("--output", type=Path, required=True)
    bench.add_argument("--cases", nargs="+", choices=CASES, default=list(CASES))
    bench.add_argument("--configs", nargs="+", choices=tuple(CONFIGS), default=list(CONFIGS))
    bench.add_argument("--orders", nargs="+", choices=("sorted", "shuffled"), default=["sorted", "shuffled"])
    bench.add_argument("--layouts", nargs="+", choices=("single", "multi"), default=["single", "multi"])
    bench.add_argument("--rounds", type=int, default=5)
    bench.add_argument("--warmups", type=int, default=1)
    bench.add_argument("--seed", type=int, default=42)
    bench.add_argument("--allow-dev", action="store_true")
    bench.add_argument("--storage-root")
    bench.add_argument("--threads", type=int)
    bench.add_argument("--scan-tasks", type=int)
    bench.add_argument("--batch-size", type=int, default=131072)
    bench.add_argument("--max-concurrent-requests", type=int, default=8)
    bench.add_argument("--max-in-flight-bytes", type=int, default=67108864)
    bench.add_argument("--timeout", type=int, default=600)
    child = commands.add_parser("_worker", help=argparse.SUPPRESS)
    child.add_argument("--spec", type=Path, required=True)
    child.add_argument("--output", type=Path, required=True)
    args = vars(parser.parse_args())
    command = args.pop("command")
    try:
        if command == "generate":
            logical_gib = args.pop("logical_gib")
            if logical_gib is not None:
                if not math.isfinite(logical_gib) or not 0 < logical_gib <= 1_000_000_000 * 136 / 2**30:
                    raise ValueError("logical-gib must be finite and positive, representing at most 1e9 rows")
                # Approximate Arrow logical bytes/row, including nullable strings.
                args["rows"] = max(1, int(logical_gib * 2**30 / 136))
            result = generate(**args)
            print(
                json.dumps(
                    {
                        "rows": result["generator"]["rows"],
                        "datasets": len(result["datasets"]),
                        "logical_bytes": {k: v["logical_bytes"] for k, v in result["datasets"].items()},
                    },
                    sort_keys=True,
                )
            )
        elif command == "_worker":
            result = worker(json.loads(args["spec"].read_text()))
            args["output"].write_text(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n")
        else:
            args["manifest_path"] = args.pop("manifest")
            result = run(**args)
            if result["status"] != "passed":
                raise RuntimeError("Benchmark cases failed; inspect report.json")
    except (ValueError, OSError, RuntimeError, AssertionError) as error:
        parser.exit(1, f"{redacted(str(error))}\n")


if __name__ == "__main__":
    main()
