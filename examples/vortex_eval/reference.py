"""Optional, local-only Vortex reader for the fork's Phase 0 evaluation.

This is a Python DataSource reference, not a public Daft API or native reader.
It targets ``vortex-data==0.88.0`` and supports flat primitive columns. Vortex
decodes projected columns to Arrow, then each batch crosses into Daft. Daft's
Python-source bridge applies pushed filters before pushed limits; other operators
preserve query ordering where needed. An outer filter or limit operator need not
remain. This reference passes neither predicates nor limits into Vortex.

The shared Python source bridge can buffer without a fixed byte bound. Neither
``batch_size`` nor a downstream limit establishes bounded memory or prompt
cancellation of file reads. Empty projections use footer row counts and must
not be presented as scan throughput measurements.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Generator, Sequence
from dataclasses import dataclass
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING

import pyarrow as pa

from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema

if TYPE_CHECKING:
    from daft import DataFrame
    from daft.io.pushdowns import Pushdowns


def _require_vortex() -> ModuleType:
    try:
        module = import_module("vortex")
    except ModuleNotFoundError as error:
        if error.name != "vortex":
            raise
        raise ImportError(
            "The optional Vortex reference requires vortex-data. Install the evaluated "
            "version with: python -m pip install 'vortex-data==0.88.0'. "
            "It is not a default Daft dependency."
        ) from error
    try:
        installed = version("vortex-data")
    except PackageNotFoundError as error:
        raise ImportError(
            "The imported vortex module has no vortex-data distribution metadata. "
            "Install the evaluated package with: python -m pip install 'vortex-data==0.88.0'."
        ) from error
    if installed != "0.88.0":
        raise ImportError(
            f"The Vortex reference requires vortex-data==0.88.0, but found {installed}. "
            "Install the evaluated version with: python -m pip install 'vortex-data==0.88.0'."
        )
    return module


def _resolve_paths(paths: str | os.PathLike[str] | Sequence[str | os.PathLike[str]]) -> list[str]:
    inputs = [paths] if isinstance(paths, (str, os.PathLike)) else list(paths)
    if not inputs:
        raise ValueError("At least one explicit local Vortex file is required")
    resolved = []
    seen = set()
    for value in inputs:
        path = os.fspath(value)
        if not path:
            raise ValueError("An empty Vortex filepath is not supported")
        if "://" in path:
            raise ValueError(f"Only explicit local file paths are supported, received {path!r}")
        local = Path(path).expanduser().resolve(strict=True)
        if not local.is_file():
            raise ValueError(f"Expected an explicit local file, received {path!r}")
        normalized = str(local)
        if normalized not in seen:
            seen.add(normalized)
            resolved.append(normalized)
    return resolved


def _arrow_schema(file: object) -> pa.Schema:
    # Vortex's default Utf8/Binary Arrow types are views. This Daft checkout
    # maps those to Python columns, so request ordinary Arrow strings/binary.
    schema = file.dtype.to_arrow_schema()
    if len(set(schema.names)) != len(schema.names):
        raise ValueError("Duplicate Vortex field names are not supported")
    fields = []
    for field in schema:
        dtype = field.type
        if pa.types.is_string_view(dtype):
            dtype = pa.string()
        elif pa.types.is_binary_view(dtype):
            dtype = pa.binary()
        if not (
            pa.types.is_null(dtype)
            or pa.types.is_boolean(dtype)
            or pa.types.is_integer(dtype)
            or (pa.types.is_floating(dtype) and dtype.bit_width in (32, 64))
            or pa.types.is_string(dtype)
            or pa.types.is_large_string(dtype)
            or pa.types.is_binary(dtype)
            or pa.types.is_large_binary(dtype)
        ):
            raise NotImplementedError(
                f"Vortex reference supports only flat null, bool, integer, float32/64, string, "
                f"and binary fields; {field.name!r} has unsupported type {dtype}"
            )
        fields.append(pa.field(field.name, dtype, nullable=field.nullable))
    return pa.schema(fields)


def _open_vortex(path: str) -> object:
    try:
        # Per-file caches would confound repeated reference scans and can retain
        # compressed segments in addition to decoded Arrow/Daft batches.
        return _require_vortex().open(path, without_segment_cache=True)
    except (OSError, ValueError, RuntimeError) as error:
        raise OSError(f"Unable to open Vortex file {path!r}: {error}") from error


def _iter_vortex_batches(
    path: str,
    expected_schema: pa.Schema,
    projected_schema: pa.Schema,
    batch_size: int,
) -> Generator[RecordBatch, None, None]:
    file = _open_vortex(path)
    actual_schema = _arrow_schema(file)
    if not actual_schema.equals(expected_schema):
        raise ValueError(
            f"Vortex schema mismatch in {path!r}; all files must match the first file's "
            f"field names, order, types, and nullability. Expected {expected_schema}; received {actual_schema}"
        )

    if len(projected_schema) == 0:
        # Arrow's zero-column conversions can lose cardinality. With no pushed
        # predicate, the exact footer row count supplies the correct result.
        remaining = len(file)
        while remaining:
            num_rows = min(remaining, batch_size)
            yield RecordBatch._from_series([], num_rows=num_rows)
            remaining -= num_rows
        return

    try:
        reader = file.to_arrow(
            projection=projected_schema.names,
            schema=projected_schema,
            batch_size=batch_size,
        )
        try:
            for batch in reader:
                yield RecordBatch.from_arrow_record_batches([batch], batch.schema)
        finally:
            # VortexFile has no close() in 0.88. The Arrow stream owns scan state
            # and is closed even if iteration or Arrow-to-Daft conversion fails.
            reader.close()
    except Exception as error:
        raise OSError(f"Unable to read Vortex file {path!r}: {error}") from error


@dataclass(frozen=True)
class _VortexReferenceTask(DataSourceTask):
    path: str
    full_schema: pa.Schema
    projected_schema: pa.Schema
    batch_size: int

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self.projected_schema)

    async def read(self) -> AsyncIterator[RecordBatch]:
        batches = _iter_vortex_batches(self.path, self.full_schema, self.projected_schema, self.batch_size)
        try:
            while True:
                pending = asyncio.create_task(asyncio.to_thread(next, batches, None))
                try:
                    batch = await asyncio.shield(pending)
                except asyncio.CancelledError:
                    # A running thread cannot be interrupted. Wait until next()
                    # finishes before closing its generator/Arrow reader.
                    await asyncio.gather(pending, return_exceptions=True)
                    raise
                if batch is None:
                    break
                yield batch
        finally:
            await asyncio.to_thread(batches.close)


class _VortexReferenceSource(DataSource):
    def __init__(self, paths: list[str], batch_size: int) -> None:
        self._paths = paths
        self._batch_size = batch_size
        # Opening reads the first footer for planning; no payload is collected.
        self._arrow_schema = _arrow_schema(_open_vortex(paths[0]))
        self._schema = Schema.from_pyarrow_schema(self._arrow_schema)

    @property
    def name(self) -> str:
        return "VortexPythonReference"

    @property
    def schema(self) -> Schema:
        return self._schema

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[_VortexReferenceTask]:
        projected_schema = self._arrow_schema
        if pushdowns.columns is not None:
            # Daft's bridge/operators evaluate predicates on returned batches.
            # Keep columns referenced only by those predicates in scan output.
            required = set(pushdowns.columns) | pushdowns.filter_required_column_names()
            projected_schema = pa.schema([field for field in self._arrow_schema if field.name in required])
        for path in self._paths:
            yield _VortexReferenceTask(path, self._arrow_schema, projected_schema, self._batch_size)


def read_vortex_reference(
    paths: str | os.PathLike[str] | Sequence[str | os.PathLike[str]],
    batch_size: int = 128 * 1024,
) -> DataFrame:
    """Build a lazy Daft scan over explicit local Vortex files.

    Resolved paths are deduplicated in input order. Requires vortex-data 0.88.0.
    Supports flat primitive columns and strict matching schemas. No directories,
    globs, URLs, schema evolution, Vortex predicates, or Vortex limit pushdown
    are implemented. Strings and binary values cross Arrow as ordinary buffers,
    rather than view types unsupported by this checkout's native type mapping.

    The first file's footer is read during construction. Each execution task
    reopens and validates its file, then reads projected Arrow batches. A file
    must remain unchanged from planning through execution. ``batch_size`` limits
    requested rows per Vortex batch, not bytes or Python-bridge buffering.
    """
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError(f"batch_size must be a positive integer, received {batch_size!r}")
    return _VortexReferenceSource(_resolve_paths(paths), batch_size).read()
