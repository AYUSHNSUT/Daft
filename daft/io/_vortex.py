from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from daft import daft as _native
from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.daft import IOConfig, StorageConfig
from daft.filesystem import glob_path_with_stats
from daft.io.source import DataSource, DataSourceTask
from daft.runners import get_or_infer_runner_type
from daft.schema import Field, Schema

if TYPE_CHECKING:
    from daft import DataFrame
    from daft.io.pushdowns import Pushdowns


_FEATURE_ERROR = (
    "Native Vortex support is experimental and disabled in this build. "
    "Rebuild this checkout with Rust features python,vortex, for example: "
    ".venv/bin/maturin develop --uv --features python,vortex. "
    "Installing the Python vortex-data package does not enable this reader."
)


def _require_vortex() -> None:
    if not hasattr(_native, "read_vortex_schema") or not hasattr(_native.PyDataSourceTask, "vortex"):
        raise ImportError(_FEATURE_ERROR)


def _require_native_runner() -> None:
    if get_or_infer_runner_type() != "native":
        raise ValueError("Experimental Vortex reads currently support only the native runner; Ray is not supported")


def _local_path(path: str) -> str:
    parsed = urlsplit(path, allow_fragments=False)
    if not os.path.isabs(path) and parsed.scheme not in {"", "file"}:
        raise ValueError(f"Experimental Vortex reads support only local paths and file:// URLs, received {path!r}")
    if parsed.scheme == "file":
        if parsed.netloc:
            raise ValueError(f"Vortex file:// URLs must not contain a host, received {path!r}")
        if "?" in path or "#" in path:
            raise ValueError(f"Vortex file:// URLs must not contain a query or fragment, received {path!r}")
        # Match native local I/O: percent characters are literal, not URL-decoded.
        path = path.split(":", 1)[1].removeprefix("//")
        if not path:
            raise ValueError("Cannot read DataFrame from an empty Vortex filepath in file:// URL")
        if os.name == "nt" and path.startswith("/") and path[2:3] == ":":
            path = path[1:]
    return os.path.abspath(os.path.expanduser(path))


def _input_paths(path: str | Path | list[str | Path]) -> list[str]:
    values = path if isinstance(path, list) else [path]
    if not values:
        raise ValueError("Cannot read DataFrame from empty list of Vortex filepaths")
    paths = []
    for value in values:
        if not isinstance(value, (str, Path)):
            raise TypeError(f"Vortex paths must be strings or pathlib.Path objects, received {type(value).__name__}")
        value = str(value)
        if not value:
            raise ValueError("Cannot read DataFrame from an empty Vortex filepath")
        paths.append(_local_path(value))
    return paths


def _resolve_vortex_paths(paths: list[str], io_config: IOConfig | None) -> list[str]:
    resolved = []
    seen = set()
    for path in paths:
        infos = glob_path_with_stats(path, file_format=None, io_config=io_config)
        is_glob = any(character in path for character in "*?[{")
        is_file = len(infos.file_paths) == 1 and _local_path(infos.file_paths[0]) == path
        if not is_glob and not is_file:
            infos = glob_path_with_stats(f"{path.rstrip('/')}/**/*.vortex", file_format=None, io_config=io_config)
        if not infos.file_paths:
            raise FileNotFoundError(f"No Vortex files found for {path!r}")
        for discovered in infos.file_paths:
            local = os.path.realpath(_local_path(discovered))
            key = os.path.normcase(local)
            if key not in seen:
                seen.add(key)
                resolved.append(local)
    return resolved


@PublicAPI
def read_vortex(
    path: str | Path | list[str | Path],
    io_config: IOConfig | None = None,
    batch_size: int = 128 * 1024,
) -> DataFrame:
    """Read local Vortex files with the experimental native reader.

    Args:
        path: A local file, directory, glob, or list of paths. Directories are
            searched recursively for ``*.vortex`` files. Explicit files can have
            any extension. Overlapping inputs are deduplicated. Native glob
            syntax applies; escape glob metacharacters in literal filenames.
            Local ``file://`` URLs without a host, query, or fragment are
            accepted. Percent characters are literal, matching native Daft
            path discovery: percent-encoded filenames from ``Path.as_uri()``
            are not decoded. Pass a plain path or ``Path`` for such filenames.
        io_config: Reserved for later Daft I/O integration. Must be ``None`` in
            this phase; explicit I/O controls are unsupported.
        batch_size: Requested maximum rows per native batch, default 131072.
            This does not establish a fixed memory bound.

    Note:
        Requires a build with Rust features ``python,vortex``. The Python
        ``vortex-data`` package is not used at runtime. Only the native runner
        and local files are supported in this experimental phase.

        Schema is inferred from the first file's footer. Every file must have
        matching ordered fields, types, and nullability, including fields that
        a query projects away. Missing or extra fields and schema evolution are
        rejected. Supported fields are null, bool, signed/unsigned 8–64 bit
        integers, float32/64, strings, and binary values. Nested, temporal, and
        extension types are unsupported.

        Each file produces one native task. This phase reads physical columns
        through Vortex, then Daft applies residual filters, projects output,
        and applies limits, preserving query ordering.
        Vortex-native predicate pruning is not implemented. Inputs must remain
        unchanged between planning and execution.

        Vortex uses its local opener rather than Daft's I/O adapter. Daft I/O
        byte/request metrics are unavailable for these reads.

    Examples:
        >>> df = daft.read_vortex("/path/to/data.vortex")  # doctest: +SKIP
        >>> df = daft.read_vortex("/path/to/directory")  # doctest: +SKIP
    """
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError(f"batch_size must be a positive integer, received {batch_size!r}")
    paths = _input_paths(path)
    if io_config is not None:
        raise NotImplementedError(
            "Explicit io_config is unsupported for experimental Vortex reads; "
            "Daft I/O integration is planned for Phase 3. Use io_config=None for local reads."
        )
    _require_native_runner()
    _require_vortex()
    io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
    return VortexSource(paths, io_config, batch_size).read()


class VortexSource(DataSource):
    def __init__(self, paths: list[str], io_config: IOConfig | None, batch_size: int) -> None:
        self._paths = _resolve_vortex_paths(paths, io_config)
        self._batch_size = batch_size
        self._io_config = io_config
        # Preserve native metadata: it carries original field nullability for
        # strict matching, which Daft's logical types alone cannot represent.
        self._scan_schema = Schema._from_pyschema(_native.read_vortex_schema(self._paths[0]))
        public_fields = []
        for field, arrow_field in zip(self._scan_schema, self._scan_schema.to_pyarrow_schema()):
            metadata = {
                key.decode(): value.decode()
                for key, value in (arrow_field.metadata or {}).items()
                if key != b"daft.vortex.nullable"
            }
            public_fields.append(Field.create(field.name, field.dtype, metadata))
        self._schema = Schema._from_fields(public_fields)

    @property
    def name(self) -> str:
        return "Vortex"

    @property
    def schema(self) -> Schema:
        return self._schema

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        from daft.daft import VortexSourceConfig

        _require_native_runner()
        for path in self._paths:
            # Preserve full source validation metadata. This initial reader
            # reads all physical fields, then filters and projects in Daft.
            yield DataSourceTask.vortex(
                path=path,
                schema=self._scan_schema,
                vortex_config=VortexSourceConfig(batch_size=self._batch_size),
                pushdowns=pushdowns,
                storage_config=StorageConfig(multithreaded_io=True, io_config=None),
            )
