"""Generate small, reproducible Phase 0 fixtures, not a Daft Vortex connector.

The default mode requires vortex-data==0.88.0. ``--parquet-only`` is an explicit
fallback for capturing a Parquet baseline; it does not validate Vortex support.
All artifacts are written beneath the caller's empty output directory.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import json
import math
import platform
import random
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq

VORTEX_VERSION = "0.88.0"
DEFAULT_SEED = 42
DEFAULT_ROWS = 257
GENERATOR_VERSION = 1
PARQUET_OPTIONS = {
    "version": "2.6",
    "compression": "zstd",
    "compression_level": 3,
    "use_dictionary": True,
    "write_statistics": True,
    "data_page_version": "1.0",
    "row_group_size": 64,
}


def canonical_table(*, seed: int = DEFAULT_SEED, rows: int = DEFAULT_ROWS) -> pa.Table:
    """Build seeded primitive data with a fixed prefix of boundary cases.

    ``value`` is bounded so that its integer sum is a useful, overflow-free oracle
    for these small fixtures. ``signed`` and ``unsigned`` carry integer extremes.
    ``measure`` contains NaN/infinities/signed zero; JSON query oracles do not sum it.
    """
    if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
        raise ValueError("rows must be a non-negative integer")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise TypeError("seed must be an integer")
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("value", pa.int64()),
            ("signed", pa.int64()),
            ("unsigned", pa.uint64()),
            ("flag", pa.bool_()),
            ("measure", pa.float64()),
            ("text", pa.string()),
            ("payload", pa.binary()),
        ]
    )
    prefix = [
        (None, -(2**63), 0, None, None, None, None),
        (-1, 2**63 - 1, 2**64 - 1, False, -0.0, "", b""),
        (0, -1, 2**63, True, 0.0, "ascii", b"\x00\xff"),
        (19, 0, 1, False, math.nan, "汉字", b"\x80"),
        (20, 1, None, True, math.inf, "café", b"hello"),
        (21, None, 2**63 - 1, None, -math.inf, "🙂", b"\x00"),
        (None, 42, 42, True, 1.5, "repeated", b"same"),
        (100, -42, 7, False, -1.5, "repeated", b"same"),
    ]
    rng = random.Random(seed)
    records = []
    for index in range(rows):
        if index < len(prefix):
            values = prefix[index]
        else:
            values = (
                None if index % 11 == 0 else rng.randrange(-100, 101),
                None if index % 13 == 0 else rng.randrange(-(2**31), 2**31),
                None if index % 17 == 0 else rng.getrandbits(64),
                (False, True, None)[rng.randrange(3)],
                None if index % 7 == 0 else rng.randrange(-1000, 1001) / 8.0,
                ("alpha", "beta", "汉字", "", None)[rng.randrange(5)],
                None if index % 19 == 0 else bytes(rng.getrandbits(8) for _ in range(index % 9)),
            )
        records.append(dict(zip(schema.names, (index, *values))))
    return pa.Table.from_pylist(records, schema=schema)


def expected_results(table: pa.Table) -> dict[str, Any]:
    """Compute query expectations in Python, independently of either reader.

    The keys ``rows``, ``count_value_nonnull``, ``sum_value``, and ``filtered_id``
    are the stable interface for the Phase 0 runner. Empty SUM follows SQL semantics.
    """
    ids = table.column("id").to_pylist()
    values = table.column("value").to_pylist()
    present = [value for value in values if value is not None]
    flags = table.column("flag").to_pylist()
    return {
        "rows": len(table),
        "count_value_nonnull": len(present),
        "sum_value": sum(present) if present else None,
        "filtered_id": [row_id for row_id, value in zip(ids, values) if value is not None and value >= 20],
        "null_value_id": [row_id for row_id, value in zip(ids, values) if value is None],
        "flag_counts": {
            "false": sum(value is False for value in flags),
            "true": sum(value is True for value in flags),
            "null": sum(value is None for value in flags),
        },
    }


def semantic_checksum(table: pa.Table) -> str:
    """Hash normalized logical values/schema as Arrow IPC, not JSON floats.

    Chunking, metadata, field nullability, and string/binary offset representation
    are normalized. Null storage and NaN payload bits are rebuilt canonically;
    signed zero, row order, logical types and values remain significant. The hash
    is reproducible for the recorded PyArrow/IPC version, not a cross-version ABI.
    This intentionally materializes small fixture values and is not a scan path.
    """
    fields = []
    arrays = []
    for field, column in zip(table.schema, table.columns):
        dtype = field.type
        if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype):
            dtype = pa.string()
        elif pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_binary_view(dtype):
            dtype = pa.binary()
        values = column.to_pylist()
        if pa.types.is_floating(dtype):
            values = [math.nan if value is not None and math.isnan(value) else value for value in values]
        fields.append(pa.field(field.name, dtype))
        arrays.append(pa.array(values, type=dtype))
    normalized = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    sink = pa.BufferOutputStream()
    options = pa.ipc.IpcWriteOptions(metadata_version=pa.ipc.MetadataVersion.V5)
    with pa.ipc.new_stream(sink, normalized.schema, options=options) as writer:
        writer.write_table(normalized)
    digest = hashlib.sha256()
    digest.update(f"vortex-eval-arrow-ipc-v1:{len(table)}:".encode())
    digest.update(sink.getvalue())
    return digest.hexdigest()


def _file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_vortex() -> Any:
    try:
        version = importlib.metadata.version("vortex-data")
    except importlib.metadata.PackageNotFoundError as error:
        raise RuntimeError(
            f"Vortex fixtures require vortex-data=={VORTEX_VERSION}; install the pinned dependency "
            "or explicitly select --parquet-only for a Parquet-only baseline."
        ) from error
    if version != VORTEX_VERSION:
        raise RuntimeError(
            f"Expected vortex-data=={VORTEX_VERSION}, found {version}; refusing an unpinned fixture writer."
        )
    try:
        return importlib.import_module("vortex")
    except ImportError as error:
        raise RuntimeError(f"vortex-data=={VORTEX_VERSION} is installed but could not be imported: {error}") from error


def generate_fixtures(
    output_dir: str | Path,
    *,
    seed: int = DEFAULT_SEED,
    rows: int = DEFAULT_ROWS,
    mode: Literal["both", "parquet-only"] = "both",
) -> dict[str, Any]:
    """Write equivalent layouts and return their manifest; refuse overwrites.

    Every file is read back and checked before ``manifest.json`` is published.
    A writer/readback failure leaves diagnostic files but no completion manifest.
    """
    if mode not in {"both", "parquet-only"}:
        raise ValueError("mode must be 'both' or 'parquet-only'")
    table = canonical_table(seed=seed, rows=rows)
    vortex = _load_vortex() if mode == "both" else None
    output = Path(output_dir)
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise FileExistsError(f"Fixture output must be an absent or empty directory: {output}")
    output.mkdir(parents=True, exist_ok=True)

    # The empty first part exercises schema discovery; remaining parts cover
    # the same row interval exactly once, including small/zero-row fixtures.
    first = min(rows, 1)
    second = min(rows, 8)
    boundaries = {
        "single": [(0, rows)],
        "multi": [(0, 0), (0, first), (first, second), (second, rows)],
        "empty": [(0, 0)],
    }
    formats = ["parquet", "vortex"] if vortex is not None else ["parquet"]
    manifest: dict[str, Any] = {
        "manifest_version": 1,
        "generator": {"version": GENERATOR_VERSION, "seed": seed, "rows": rows},
        "mode": mode,
        "versions": {
            "python": platform.python_version(),
            "pyarrow": pa.__version__,
            "vortex-data": VORTEX_VERSION if vortex is not None else None,
        },
        "schema": [{"name": field.name, "type": str(field.type), "nullable": field.nullable} for field in table.schema],
        "checksum_algorithm": "sha256:normalized-arrow-ipc-v5:vortex-eval-v1",
        "semantic_checksum": semantic_checksum(table),
        "expected_results": expected_results(table),
        "query_definitions": {
            "filtered_id": "SELECT id FROM input WHERE value >= 20 ORDER BY id",
            "count_value_nonnull": "SELECT COUNT(value) FROM input",
            "sum_value": "SELECT SUM(value) FROM input",
            "rows": "SELECT COUNT(*) FROM input",
        },
        "writer_options": {
            "parquet": dict(PARQUET_OPTIONS),
            "vortex": {"api": "vortex.io.write(pyarrow.Table, path)", "options": "0.88.0 defaults"}
            if vortex is not None
            else None,
        },
        "layouts": {},
        "files": [],
    }
    for layout, ranges in boundaries.items():
        layout_table = table.slice(0, 0) if layout == "empty" else table
        manifest["layouts"][layout] = {
            "rows": len(layout_table),
            "expected_results": expected_results(layout_table),
            "semantic_checksum": semantic_checksum(layout_table),
            "parquet": [],
            "vortex": [],
        }
        (output / layout).mkdir()
        for index, (start, end) in enumerate(ranges):
            part = table.slice(start, end - start)
            checksum = semantic_checksum(part)
            stem = f"part-{index:03d}" if layout == "multi" else "data"
            for file_format in formats:
                relative = f"{layout}/{stem}.{file_format}"
                path = output / relative
                if file_format == "parquet":
                    pq.write_table(part, path, **PARQUET_OPTIONS)
                    actual = pq.ParquetFile(path).read()
                else:
                    vortex.io.write(part, str(path))
                    with vortex.open(str(path)).to_arrow(schema=part.schema) as reader:
                        actual = reader.read_all()
                if semantic_checksum(actual) != checksum:
                    raise RuntimeError(f"Fixture round-trip changed logical data: {path}")
                manifest["layouts"][layout][file_format].append(relative)
                manifest["files"].append(
                    {
                        "path": relative,
                        "format": file_format,
                        "layout": layout,
                        "row_start": start,
                        "row_end": end,
                        "rows": len(part),
                        "bytes": path.stat().st_size,
                        "sha256": _file_checksum(path),
                        "semantic_checksum": checksum,
                    }
                )
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Absent or empty output directory")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--parquet-only", action="store_true", help="Explicitly skip Vortex; does not validate Vortex")
    args = parser.parse_args()
    try:
        manifest = generate_fixtures(
            args.output, seed=args.seed, rows=args.rows, mode="parquet-only" if args.parquet_only else "both"
        )
    except (ValueError, RuntimeError, OSError) as error:
        parser.exit(1, f"Fixture generation failed: {error}\n")
    print(json.dumps({"manifest": str(args.output / "manifest.json"), "mode": manifest["mode"], "rows": args.rows}))


if __name__ == "__main__":
    main()
