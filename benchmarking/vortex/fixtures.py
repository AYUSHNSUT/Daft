"""Generate bounded batches of identical logical data in two physical formats."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import random
import time
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

VERSION = 1
SELECTIVITIES = ("0", "0.001", "0.01", "0.5", "1")
CASES = (
    "scan_full",
    "scan_narrow",
    *(f"{op}_{s}" for op in ("range", "eq") for s in SELECTIVITIES),
    "group_by",
    "join",
)
_FIELDS: list[pa.Field[Any]] = [
    pa.field("id", pa.int64()),
    pa.field("value", pa.int64()),
    pa.field("group", pa.int64()),
    pa.field("flag", pa.bool_()),
    pa.field("label", pa.string()),
    pa.field("text", pa.string()),
    pa.field("sel_0001", pa.int64()),
    pa.field("sel_001", pa.int64()),
    pa.field("sel_05", pa.int64()),
    pa.field("constant", pa.int64()),
]
SCHEMA = pa.schema(_FIELDS)


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def payload(row_id: int, seed: int) -> str:
    return hashlib.blake2b(f"{seed}:{row_id}".encode(), digest_size=32).hexdigest()


def cutoff(rows: int, selectivity: str) -> int:
    from decimal import Decimal

    return int(rows * Decimal(selectivity))


def permutation(rows: int, seed: int) -> tuple[int, int]:
    rng = random.Random(seed)
    multiplier = rng.randrange(1, max(rows, 2))
    while math.gcd(multiplier, rows) != 1:
        multiplier += 1
    return multiplier, rng.randrange(rows)


def batches(rows: int, seed: int, order: str, start: int, end: int, batch_rows: int):
    """An affine permutation scatters rows without allocating a full permutation."""
    multiplier, offset = permutation(rows, seed)
    for begin in range(start, end, batch_rows):
        ids = np.arange(begin, min(begin + batch_rows, end), dtype=np.int64)
        if order == "shuffled":
            ids = (ids * multiplier + offset) % rows
        arrays = [
            pa.array(ids),
            pa.array(ids % 10000 - 5000, mask=ids % 17 == 0),
            pa.array(ids % 128),
            pa.array(ids % 2 == 0, mask=ids % 19 == 0),
            pa.array([None if int(i) % 23 == 0 else f"group-{int(i) % 128:03d}" for i in ids], type=pa.string()),
            pa.array([None if int(i) % 29 == 0 else payload(int(i), seed) for i in ids], type=pa.string()),
            *(pa.array((ids < cutoff(rows, s)).astype(np.int64)) for s in ("0.001", "0.01", "0.5")),
            pa.array(np.ones(len(ids), dtype=np.int64)),
        ]
        yield pa.RecordBatch.from_arrays(arrays, schema=SCHEMA)


def oracle(rows: int, seed: int) -> dict[str, list[dict[str, Any]]]:
    """Scalar Python expectations, independent of Arrow and both query readers."""
    total = {"rows": 0, "sum_id": 0, "count_value": 0, "sum_value": 0}
    full = {"sum_group": 0, "count_flag": 0, "sum_flag": 0, "label_length": 0, "text_length": 0}
    groups: dict[int, dict[str, Any]] = {}
    prefixes = {0: dict(total)}
    thresholds = {cutoff(rows, s) for s in SELECTIVITIES}
    joined = 0
    for i in range(rows):
        value = None if i % 17 == 0 else i % 10000 - 5000
        total["rows"] += 1
        total["sum_id"] += i
        group = i % 128
        bucket = groups.setdefault(group, {"group": group, "rows": 0, "sum_value": 0, "count_value": 0})
        bucket["rows"] += 1
        if value is not None:
            total["sum_value"] += value
            total["count_value"] += 1
            bucket["sum_value"] += value
            bucket["count_value"] += 1
            joined += value * (group + 1)
        full["sum_group"] += group
        if i % 19:
            full["count_flag"] += 1
            full["sum_flag"] += int(i % 2 == 0)
        full["label_length"] += 9 if i % 23 else 0
        full["text_length"] += 64 if i % 29 else 0
        if i + 1 in thresholds:
            prefixes[i + 1] = dict(total)

    def nullable_sums(record: dict[str, Any]) -> dict[str, Any]:
        result = dict(record)
        if not result["count_value"]:
            result["sum_value"] = None
        if not result["rows"] and "sum_id" in result:
            result["sum_id"] = None
        return result

    result = {"scan_narrow": [nullable_sums(total)]}
    result["scan_full"] = [
        {
            **nullable_sums(total),
            **full,
            "sel_0001": cutoff(rows, "0.001"),
            "sel_001": cutoff(rows, "0.01"),
            "sel_05": cutoff(rows, "0.5"),
            "constant": rows,
        }
    ]
    if not full["count_flag"]:
        result["scan_full"][0]["sum_flag"] = None
    for name in ("label_length", "text_length"):
        if not full[name]:
            result["scan_full"][0][name] = None
    for op in ("range", "eq"):
        for s in SELECTIVITIES:
            result[f"{op}_{s}"] = [nullable_sums(prefixes[cutoff(rows, s)])]
    result["group_by"] = [nullable_sums(groups[g]) for g in sorted(groups)]
    result["join"] = [{"rows": rows, "weighted_sum": joined if total["count_value"] else None}]
    return result


def generate(
    output: Path,
    *,
    rows: int,
    seed: int = 42,
    orders=("sorted", "shuffled"),
    layouts=("single", "multi"),
    files: int = 8,
    batch_rows: int = 65536,
) -> dict[str, Any]:
    if not 0 < rows <= 1_000_000_000 or files < 1 or batch_rows < 1:
        raise ValueError("rows must be 1..1e9; files and batch_rows must be positive")
    if (
        not orders
        or not set(orders) <= {"sorted", "shuffled"}
        or not layouts
        or not set(layouts) <= {"single", "multi"}
    ):
        raise ValueError("Select supported, nonempty orders and layouts")
    if importlib.metadata.version("vortex-data") != "0.88.0":
        raise ValueError("Fixture writing requires vortex-data==0.88.0")
    import vortex

    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Fixture directory must be absent or empty")
    output.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "manifest_version": VERSION,
        "generator": {
            "rows": rows,
            "seed": seed,
            "batch_rows": batch_rows,
            "files": files,
            "shuffle": "seeded affine bijection; no full permutation allocated",
        },
        "schema": [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in SCHEMA],
        "versions": {"pyarrow": pa.__version__, "vortex-data": "0.88.0", "numpy": np.__version__},
        "writer_options": {
            "parquet": {
                "compression": "zstd",
                "compression_level": 3,
                "use_dictionary": True,
                "write_statistics": True,
                "row_group_size": batch_rows,
            },
            "vortex": {"api": "io.write(RecordBatchReader, path)", "options": "0.88.0 defaults"},
        },
        "datasets": {},
        "files": [],
        "oracle": oracle(rows, seed),
    }
    for order in orders:
        for layout in layouts:
            part_count = 1 if layout == "single" else min(files, rows)
            dataset = {
                "order": order,
                "layout": layout,
                "rows": rows,
                "logical_bytes": 0,
                "paths": {"parquet": [], "vortex": []},
            }
            for part in range(part_count):
                start, end = rows * part // part_count, rows * (part + 1) // part_count
                for fmt in ("parquet", "vortex"):
                    relative = f"{order}/{layout}/part-{part:04d}.{fmt}"
                    path = output / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    logical_bytes = 0
                    semantic = hashlib.sha256()

                    def recorded_batches():
                        nonlocal logical_bytes
                        for batch in batches(rows, seed, order, start, end, batch_rows):
                            logical_bytes += batch.nbytes
                            semantic.update(batch.serialize().to_pybytes())
                            yield batch

                    began = time.perf_counter()
                    if fmt == "parquet":
                        with pq.ParquetWriter(
                            path,
                            SCHEMA,
                            compression="zstd",
                            compression_level=3,
                            use_dictionary=True,
                            write_statistics=True,
                        ) as writer:
                            for batch in recorded_batches():
                                writer.write_batch(batch, row_group_size=batch_rows)
                        dataset["logical_bytes"] += logical_bytes
                    else:
                        with pa.RecordBatchReader.from_batches(SCHEMA, recorded_batches()) as reader:
                            vortex.io.write(reader, str(path))
                    generation_and_write_seconds = time.perf_counter() - began
                    manifest["files"].append(
                        {
                            "path": relative,
                            "format": fmt,
                            "order": order,
                            "layout": layout,
                            "start": start,
                            "end": end,
                            "rows": end - start,
                            "size_bytes": path.stat().st_size,
                            "sha256": file_hash(path),
                            "input_batches_sha256": semantic.hexdigest(),
                            "logical_bytes": logical_bytes,
                            "generation_and_write_seconds": generation_and_write_seconds,
                        }
                    )
                    dataset["paths"][fmt].append(relative)
                left, right = manifest["files"][-2:]
                if left["input_batches_sha256"] != right["input_batches_sha256"]:
                    raise AssertionError("Writers received different data")
            manifest["datasets"][f"{order}/{layout}"] = dataset
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def load_manifest(path: Path) -> dict[str, Any]:
    """Validate schema, inventory, file boundaries and hashes before timed work."""
    root = path.resolve().parent
    manifest: dict[str, Any] = json.loads(path.read_text())
    if manifest.get("manifest_version") != VERSION or manifest.get("schema") != [
        {"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in SCHEMA
    ]:
        raise ValueError("Unsupported manifest version or schema")
    records = {}
    for record in manifest["files"]:
        relative = record["path"]
        resolved = (root / relative).resolve()
        if Path(relative).is_absolute() or not resolved.is_relative_to(root) or relative in records:
            raise ValueError("Unsafe or duplicate fixture path")
        if resolved.stat().st_size != record["size_bytes"] or file_hash(resolved) != record["sha256"]:
            raise ValueError(f"Fixture bytes changed: {relative}")
        records[relative] = record
    referenced = set()
    for dataset in manifest["datasets"].values():
        boundaries = []
        for fmt in ("parquet", "vortex"):
            cursor = 0
            current = []
            for relative in dataset["paths"][fmt]:
                record = records.get(relative)
                if record is None or relative in referenced:
                    raise ValueError("Missing or repeated fixture record")
                if (record["format"], record["order"], record["layout"], record["start"]) != (
                    fmt,
                    dataset["order"],
                    dataset["layout"],
                    cursor,
                ) or record["end"] - cursor != record["rows"]:
                    raise ValueError("Invalid fixture coverage")
                cursor = record["end"]
                current.append((record["start"], record["end"], record["input_batches_sha256"]))
                referenced.add(relative)
            if cursor != manifest["generator"]["rows"]:
                raise ValueError("Incomplete fixture coverage")
            boundaries.append(current)
        if boundaries[0] != boundaries[1]:
            raise ValueError("Format logical inputs differ")
    if referenced != set(records) or not manifest["datasets"]:
        raise ValueError("Unreferenced files or empty fixture inventory")
    if manifest["oracle"] != oracle(manifest["generator"]["rows"], manifest["generator"]["seed"]):
        raise ValueError("Stored oracle differs from independent expectations")
    return manifest
