from __future__ import annotations

import os
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn
from urllib.parse import urlsplit, urlunsplit

from daft import daft as _native
from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.daft import FileInfos, IOConfig, StorageConfig
from daft.filesystem import glob_path_with_stats
from daft.io.source import DataSource, DataSourceTask
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
_MAX_REQUESTS = sys.maxsize >> 2
_MAX_IN_FLIGHT_BYTES = min(2**32 - 1, _MAX_REQUESTS)
_MAX_METADATA_CONCURRENCY = 64
_MAX_METADATA_SNAPSHOT_BYTES = 64 * 1024 * 1024


def _validate_metadata_concurrency(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= _MAX_METADATA_CONCURRENCY:
        raise ValueError(
            f"metadata_concurrency must be an integer from 1 through {_MAX_METADATA_CONCURRENCY}, received {value!r}"
        )


def _validate_read_controls(enabled: bool, gap: int, maximum: int, discovery: int) -> None:
    if not isinstance(enabled, bool):
        raise TypeError(f"enable_read_coalescing must be a bool, received {enabled!r}")
    for name, value, minimum in (
        ("read_coalesce_gap_bytes", gap, 0),
        ("read_coalesce_max_bytes", maximum, 1),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= sys.maxsize:
            raise ValueError(f"{name} must be an integer from {minimum} through {sys.maxsize}, received {value!r}")
    if isinstance(discovery, bool) or not isinstance(discovery, int) or not 1 <= discovery <= 64:
        raise ValueError(f"discovery_concurrency must be an integer from 1 through 64, received {discovery!r}")


def _require_vortex(*, metadata_reuse: bool = False, row_count_planning: bool = False) -> None:
    if (
        not hasattr(_native, "read_vortex_schema")
        or not hasattr(_native, "read_vortex_metadata")
        or not hasattr(_native, "read_vortex_metadata_bulk")
        or not hasattr(_native, "_io_glob_bulk")
        or not hasattr(_native.PyDataSourceTask, "vortex")
        or not hasattr(getattr(_native, "VortexSourceConfig", None), "metadata_snapshot")
        or not hasattr(getattr(_native, "VortexSourceConfig", None), "enable_large_offsets")
        or (metadata_reuse and not hasattr(_native, "_read_vortex_metadata_bulk_cached"))
        or (row_count_planning and not hasattr(_native, "_read_vortex_row_counts_bulk"))
        or (row_count_planning and metadata_reuse and not hasattr(_native, "_read_vortex_row_counts_bulk_cached"))
    ):
        raise ImportError(_FEATURE_ERROR)


def _display_path(path: str) -> str:
    if os.path.isabs(path):
        return path
    try:
        parsed = urlsplit(path)
    except ValueError:
        return "<invalid URL>"
    if not parsed.scheme:
        return path
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc.rsplit("@", 1)[-1],
            parsed.path,
            "<redacted>" if "?" in path else "",
            "<redacted>" if "#" in path else "",
        )
    )


def _discover(path: str, discovery_path: str, io_config: IOConfig | None) -> FileInfos:
    try:
        return glob_path_with_stats(discovery_path, file_format=None, io_config=io_config)
    except Exception as error:
        _raise_discovery_error(path, discovery_path, error)


def _raise_discovery_error(path: str, discovery_path: str, error: Exception) -> NoReturn:
    if urlsplit(path).scheme not in {"http", "https", "s3"}:
        raise error
    # Sanitize only when consuming this outcome, preserving original error order.
    message = str(error).replace(discovery_path, _display_path(path)).replace(path, _display_path(path))
    parsed = urlsplit(path)
    for secret in (parsed.query, parsed.fragment, parsed.username, parsed.password):
        if secret:
            message = message.replace(secret, "<redacted>")
    error_type = FileNotFoundError if isinstance(error, FileNotFoundError) else OSError
    raise error_type(f"Vortex discovery for {_display_path(path)!r}: {message}") from None


def _discovery_path(path: str) -> str:
    if urlsplit(path).scheme not in {"http", "https"}:
        return path
    authority_end = path.index("://") + 3 + len(urlsplit(path).netloc)
    return path[:authority_end] + "".join(
        f"\\{character}" if character in "\\*?{}[]" else character for character in path[authority_end:]
    )


def _bulk_discover(
    paths: list[str], io_config: IOConfig | None, concurrency: int, *, display_paths: list[str] | None = None
) -> list[dict[str, Any]]:
    from daft.io import _vortex_diagnostics as diagnostics

    discovery_paths = [_discovery_path(path) for path in paths]
    capture_token = diagnostics.capture_token()
    try:
        outcomes, metrics = _native._io_glob_bulk(
            discovery_paths,
            io_config=io_config,
            max_concurrent_inputs=concurrency,
            collect_metrics=capture_token is not None,
        )
    except Exception as error:
        _raise_discovery_error((display_paths or paths)[0], discovery_paths[0], error)
    if metrics is not None:
        diagnostics.record_discovery(metrics, token=capture_token)
    return outcomes


def _discovery_infos(path: str, outcome: dict[str, Any], discovery_path: str | None = None) -> FileInfos:
    if "error" in outcome:
        _raise_discovery_error(path, discovery_path or _discovery_path(path), outcome["error"])
    files = {entry["path"]: entry for entry in outcome["files"]}
    return FileInfos.from_infos(
        file_paths=list(files), file_sizes=[entry["size"] for entry in files.values()], num_rows=[None] * len(files)
    )


def _local_path(path: str) -> str:
    parsed = urlsplit(path, allow_fragments=False)
    if not os.path.isabs(path) and parsed.scheme not in {"", "file"}:
        raise ValueError(
            f"Experimental Vortex reads support only local paths and file:// URLs, received {_display_path(path)!r}"
        )
    if parsed.scheme == "file":
        if parsed.netloc:
            raise ValueError(f"Vortex file:// URLs must not contain a host, received {_display_path(path)!r}")
        if "?" in path or "#" in path:
            raise ValueError(
                f"Vortex file:// URLs must not contain a query or fragment, received {_display_path(path)!r}"
            )
        # Match native local I/O: percent characters are literal, not URL-decoded.
        path = path.split(":", 1)[1].removeprefix("//")
        if not path:
            raise ValueError("Cannot read DataFrame from an empty Vortex filepath in file:// URL")
        if os.name == "nt" and path.startswith("/") and path[2:3] == ":":
            path = path[1:]
    return os.path.abspath(os.path.expanduser(path))


def _normalize_vortex_path(path: str) -> str:
    try:
        parsed = urlsplit(path)
    except ValueError:
        # urlsplit's invalid-authority/NFKC errors can embed raw userinfo.
        # Do not include the parser's message or chain it into diagnostics.
        raise ValueError("Invalid Vortex URL; expected a valid local path, HTTP(S) URL, or S3 URL") from None
    if os.path.isabs(path) or parsed.scheme in {"", "file"}:
        return _local_path(path)
    if parsed.scheme not in {"http", "https", "s3"}:
        raise ValueError(
            f"Experimental Vortex reads support only local, file://, HTTP(S), and s3:// paths, received {_display_path(path)!r}"
        )
    if not parsed.netloc:
        raise ValueError(f"Vortex URLs require a host or bucket, received {_display_path(path)!r}")
    if "#" in path:
        raise ValueError(f"Vortex URLs must not contain a fragment, received {_display_path(path)!r}")
    if parsed.scheme == "s3" and (parsed.query or "?" in path):
        raise ValueError(f"Vortex s3:// paths must not contain a query, received {_display_path(path)!r}")
    if parsed.scheme in {"http", "https"} and (
        not parsed.path or parsed.path.endswith("/") or any(character in parsed.path for character in "*[{?")
    ):
        raise ValueError("Vortex HTTP(S) reads require exact file URLs; directory discovery and globs are unsupported")
    return path


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
        paths.append(_normalize_vortex_path(value))
    return paths


def _resolve_vortex_paths(paths: list[str], io_config: IOConfig | None, discovery_concurrency: int = 8) -> list[str]:
    from daft.io import _vortex_diagnostics as diagnostics

    with diagnostics.stage("discovery"):
        return _resolve_vortex_paths_inner(paths, io_config, discovery_concurrency)


def _resolve_vortex_paths_inner(paths: list[str], io_config: IOConfig | None, discovery_concurrency: int) -> list[str]:
    resolved = []
    seen = set()
    remote_outcomes: Iterator[dict[str, Any]] = iter(())
    remote_remaining = 0
    for index, path in enumerate(paths):
        scheme = urlsplit(path).scheme
        remote = scheme in {"http", "https", "s3"}
        if remote:
            if remote_remaining == 0:
                window = []
                for candidate in paths[index : index + discovery_concurrency]:
                    if urlsplit(candidate).scheme not in {"http", "https", "s3"}:
                        break
                    window.append(candidate)
                remote_outcomes = iter(_bulk_discover(window, io_config, discovery_concurrency))
                remote_remaining = len(window)
            infos = _discovery_infos(path, next(remote_outcomes))
            remote_remaining -= 1
        else:
            infos = _discover(path, path, io_config)
        is_glob = any(character in path for character in "*?[{")
        is_file = len(infos.file_paths) == 1 and _normalize_vortex_path(infos.file_paths[0]) == path
        if not is_glob and not is_file and urlsplit(path).scheme not in {"http", "https"}:
            directory_glob = f"{path.rstrip('/')}/**/*.vortex"
            if remote:
                # Resolve this earlier input's fallback before observing later errors.
                outcome = _bulk_discover([directory_glob], io_config, discovery_concurrency, display_paths=[path])[0]
                infos = _discovery_infos(path, outcome, directory_glob)
            else:
                infos = _discover(path, directory_glob, io_config)
        if not infos.file_paths:
            raise FileNotFoundError(f"No Vortex files found for {_display_path(path)!r}")
        if scheme in {"http", "https"} and not is_file:
            raise ValueError(
                "Vortex HTTP(S) reads require discovery of the exact original file URL; "
                f"HTML directory links are unsupported for {_display_path(path)!r}"
            )
        for discovered in infos.file_paths:
            resolved_path = _normalize_vortex_path(discovered)
            if not remote:
                resolved_path = os.path.realpath(resolved_path)
            key = resolved_path if remote else os.path.normcase(resolved_path)
            if key not in seen:
                seen.add(key)
                resolved.append(resolved_path)
    return resolved


@PublicAPI
def read_vortex(
    path: str | Path | list[str | Path],
    io_config: IOConfig | None = None,
    batch_size: int = 128 * 1024,
    *,
    enable_projection_pushdown: bool = True,
    enable_filter_pushdown: bool = True,
    max_concurrent_requests: int = 8,
    max_in_flight_bytes: int = 64 * 1024 * 1024,
    metadata_concurrency: int = 8,
    enable_read_coalescing: bool = True,
    read_coalesce_gap_bytes: int = 65536,
    read_coalesce_max_bytes: int = 4194304,
    discovery_concurrency: int = 8,
    enable_metadata_reuse: bool = False,
    enable_row_count_planning: bool = False,
    enable_large_offsets: bool = False,
    enable_execution_session_reuse: bool = False,
    enable_file_pruning: bool = False,
    enable_layout_splitting: bool = False,
) -> DataFrame:
    """Read Vortex files with the experimental native reader.

    Args:
        path: A local or S3 file, directory, glob, or list of paths. Directories are
            searched recursively for ``*.vortex`` files. Explicit files can have
            any extension. Overlapping inputs are deduplicated. Native glob
            syntax applies; escape glob metacharacters in literal filenames.
            Local ``file://`` URLs without a host, query, or fragment are
            accepted. Percent characters are literal, matching native Daft
            path discovery: percent-encoded filenames from ``Path.as_uri()``
            are not decoded. Pass a plain path or ``Path`` for such filenames.
            HTTP(S) supports exact file URLs rather than directory discovery
            or globs. IPv6-literal HTTP URLs are outside the validated matrix:
            shared discovery can interpret authority brackets as glob syntax.
            Other remote schemes are unsupported.
        io_config: Daft I/O configuration used for discovery, footer inference,
            and task execution. Defaults to the current planning configuration.
        batch_size: Requested maximum rows per native batch, default 131072.
            This does not establish a fixed memory bound.
        enable_projection_pushdown: Allow Vortex to read only output columns
            and columns needed by the complete residual filter. Default True.
        enable_filter_pushdown: Allow conservative translation of supported
            predicates into Vortex. Default True. The complete Daft residual
            filter is always retained, regardless of this switch.
        max_concurrent_requests: Maximum concurrent range requests per opened
            Vortex reader, including footer reads. Default 8.
        max_in_flight_bytes: Maximum requested range bytes in flight per opened
            reader. Default 67108864. A single larger request fails explicitly.
            This bounds requests rather than decoded data, caches, or query
            memory. Multiple file tasks each have their own request budget.
        metadata_concurrency: Maximum concurrently opened files for planning
            footer metadata, from 1 through 64. Default 8. Each open has its
            own request/byte budget; this is independent of scan-task execution
            concurrency and does not establish a query-wide memory bound.
        enable_read_coalescing: Merge nearby Vortex reads within each opened reader. Default True.
        read_coalesce_gap_bytes: Maximum gap eligible for merging, from 0 through the platform's signed integer maximum.
            Default 65536. Validated even when coalescing is disabled.
        read_coalesce_max_bytes: Maximum merged request size, default 4194304. Must be a positive signed-platform integer.
            The reader clamps this to its request-byte budget. This is not a decoded-memory bound.
        discovery_concurrency: Maximum concurrent remote input discoveries, from 1 through 64. Default 8.
            Results and errors retain input order; local discovery remains sequential.
        enable_metadata_reuse: Experimental S3 opening-range reuse, default False. Planning attaches at most
            64 MiB of encoded opening bytes across the materialization's tasks. Each executing reader validates
            fresh size, ETag and version metadata before reuse, and conditionally fetches uncached ranges.
            Changed objects fail explicitly. HTTP/local inputs and S3 objects without strong ETags use fresh reads.
        enable_row_count_planning: Experimental lightweight footer planning, default False.
            Omits conversion of unused per-file planning schemas and shares one Vortex session within
            each bulk metadata call. Readers and metadata remain fresh per file and materialization.
            Every executed file still receives full schema validation before projection; unsupported
            schemas can therefore fail during execution instead of planning. Footer row counts retain
            join statistics.

        enable_large_offsets: Request large-offset Arrow strings/binary directly. Experimental, default False.
        enable_execution_session_reuse: Share Vortex session initialization within each fresh scan processor.
            Per-file readers, metadata and validators remain independent. Experimental, default False.
        enable_file_pruning: Reject files proven not to match by footer statistics after full schema validation.
            Unknown statistics retain the ordinary scan. Experimental, default False.
        enable_layout_splitting: Align SDK splits to layout boundaries with batch_size as the maximum row count.
            Experimental, default False.

    Note:
        Requires a build with Rust features ``python,vortex``. The Python
        ``vortex-data`` package is not used at runtime. Native and Ray runners
        execute the native reader. Ray workers require this feature-enabled
        build and access to the same paths and configured remote services.

        Schema is inferred from the first file's footer. Every file must have
        matching ordered fields, types, and nullability, including fields that
        a query projects away. Missing or extra fields and schema evolution are
        rejected. Supported fields are null, bool, signed/unsigned 8–64 bit
        integers, float32/64, strings, and binary values. Nested, temporal, and
        extension types are unsupported.

        Each file produces one native task. Selected columns and a small safe
        predicate subset can reach Vortex. Daft then evaluates the complete
        residual filter, projects output, and applies limits, preserving query
        ordering. Unsupported predicates fall back to Daft. Enabling an
        optimization does not guarantee fewer reads or a faster query. Disable
        both switches to compare with the original full-column scan strategy.
        Inputs must remain unchanged between planning and execution.

        Reads use Daft's I/O adapter and task I/O statistics. Request budgets
        are independent of the query result buffer and consumer cancellation.

    Examples:
        >>> df = daft.read_vortex("/path/to/data.vortex")  # doctest: +SKIP
        >>> df = daft.read_vortex("/path/to/directory")  # doctest: +SKIP
    """
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError(f"batch_size must be a positive integer, received {batch_size!r}")
    _validate_metadata_concurrency(metadata_concurrency)
    _validate_read_controls(
        enable_read_coalescing, read_coalesce_gap_bytes, read_coalesce_max_bytes, discovery_concurrency
    )
    for name, value in (
        ("max_concurrent_requests", max_concurrent_requests),
        ("max_in_flight_bytes", max_in_flight_bytes),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer, received {value!r}")
        maximum = _MAX_REQUESTS if name == "max_concurrent_requests" else _MAX_IN_FLIGHT_BYTES
        if value > maximum:
            raise ValueError(f"{name} must be at most {maximum}, received {value!r}")
    for name, value in (
        ("enable_projection_pushdown", enable_projection_pushdown),
        ("enable_filter_pushdown", enable_filter_pushdown),
    ):
        if not isinstance(value, bool):
            raise TypeError(f"{name} must be a bool, received {value!r}")
    if not isinstance(enable_metadata_reuse, bool):
        raise TypeError("enable_metadata_reuse must be a bool")
    if not isinstance(enable_row_count_planning, bool):
        raise TypeError("enable_row_count_planning must be a bool")
    for name, value in (
        ("enable_large_offsets", enable_large_offsets),
        ("enable_execution_session_reuse", enable_execution_session_reuse),
        ("enable_file_pruning", enable_file_pruning),
        ("enable_layout_splitting", enable_layout_splitting),
    ):
        if not isinstance(value, bool):
            raise TypeError(f"{name} must be a bool")
    if any(
        (enable_large_offsets, enable_execution_session_reuse, enable_file_pruning, enable_layout_splitting)
    ) and not hasattr(_native.VortexSourceConfig, "enable_large_offsets"):
        raise ImportError("Rebuild Daft with the experimental Vortex reader controls")
    paths = _input_paths(path)
    _require_vortex(metadata_reuse=enable_metadata_reuse, row_count_planning=enable_row_count_planning)
    io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
    return VortexSource(
        paths,
        io_config,
        batch_size,
        enable_projection_pushdown=enable_projection_pushdown,
        enable_filter_pushdown=enable_filter_pushdown,
        max_concurrent_requests=max_concurrent_requests,
        max_in_flight_bytes=max_in_flight_bytes,
        metadata_concurrency=metadata_concurrency,
        enable_read_coalescing=enable_read_coalescing,
        read_coalesce_gap_bytes=read_coalesce_gap_bytes,
        read_coalesce_max_bytes=read_coalesce_max_bytes,
        discovery_concurrency=discovery_concurrency,
        enable_metadata_reuse=enable_metadata_reuse,
        enable_row_count_planning=enable_row_count_planning,
        enable_large_offsets=enable_large_offsets,
        enable_execution_session_reuse=enable_execution_session_reuse,
        enable_file_pruning=enable_file_pruning,
        enable_layout_splitting=enable_layout_splitting,
    ).read()


class VortexSource(DataSource):
    def __init__(
        self,
        paths: list[str],
        io_config: IOConfig | None,
        batch_size: int,
        *,
        enable_projection_pushdown: bool = True,
        enable_filter_pushdown: bool = True,
        max_concurrent_requests: int = 8,
        max_in_flight_bytes: int = 64 * 1024 * 1024,
        metadata_concurrency: int = 8,
        enable_read_coalescing: bool = True,
        read_coalesce_gap_bytes: int = 65536,
        read_coalesce_max_bytes: int = 4194304,
        discovery_concurrency: int = 8,
        enable_metadata_reuse: bool = False,
        enable_row_count_planning: bool = False,
        enable_large_offsets: bool = False,
        enable_execution_session_reuse: bool = False,
        enable_file_pruning: bool = False,
        enable_layout_splitting: bool = False,
    ) -> None:
        _validate_metadata_concurrency(metadata_concurrency)
        _validate_read_controls(
            enable_read_coalescing, read_coalesce_gap_bytes, read_coalesce_max_bytes, discovery_concurrency
        )
        if not isinstance(enable_metadata_reuse, bool):
            raise TypeError("enable_metadata_reuse must be a bool")
        if not isinstance(enable_row_count_planning, bool):
            raise TypeError("enable_row_count_planning must be a bool")
        if not isinstance(enable_large_offsets, bool):
            raise TypeError("enable_large_offsets must be a bool")
        self._enable_large_offsets = enable_large_offsets
        if not isinstance(enable_execution_session_reuse, bool):
            raise TypeError("enable_execution_session_reuse must be a bool")
        self._enable_execution_session_reuse = enable_execution_session_reuse
        if not isinstance(enable_file_pruning, bool):
            raise TypeError("enable_file_pruning must be a bool")
        self._enable_file_pruning = enable_file_pruning
        if not isinstance(enable_layout_splitting, bool):
            raise TypeError("enable_layout_splitting must be a bool")
        self._enable_layout_splitting = enable_layout_splitting
        self._enable_row_count_planning = enable_row_count_planning
        self._enable_metadata_reuse = enable_metadata_reuse
        self._paths = _resolve_vortex_paths(paths, io_config, discovery_concurrency)
        self._batch_size = batch_size
        self._io_config = io_config
        self._enable_projection_pushdown = enable_projection_pushdown
        self._enable_filter_pushdown = enable_filter_pushdown
        self._max_concurrent_requests = max_concurrent_requests
        self._max_in_flight_bytes = max_in_flight_bytes
        self._metadata_concurrency = metadata_concurrency
        self._enable_read_coalescing = enable_read_coalescing
        self._read_coalesce_gap_bytes = read_coalesce_gap_bytes
        self._read_coalesce_max_bytes = read_coalesce_max_bytes
        self._discovery_concurrency = discovery_concurrency
        # Preserve native metadata: it carries original field nullability for
        # strict matching, which Daft's logical types alone cannot represent.
        from daft.io import _vortex_diagnostics as diagnostics

        with diagnostics.stage("schema_inference"):
            self._scan_schema = Schema._from_pyschema(
                _native.read_vortex_schema(
                    self._paths[0],
                    io_config=io_config,
                    max_concurrent_requests=max_concurrent_requests,
                    max_in_flight_bytes=max_in_flight_bytes,
                    enable_read_coalescing=enable_read_coalescing,
                    read_coalesce_gap_bytes=read_coalesce_gap_bytes,
                    read_coalesce_max_bytes=read_coalesce_max_bytes,
                )
            )
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
        from daft.io import _vortex_diagnostics as diagnostics

        remaining_snapshot_bytes = _MAX_METADATA_SNAPSHOT_BYTES
        for start in range(0, len(self._paths), self._metadata_concurrency):
            paths = self._paths[start : start + self._metadata_concurrency]
            entries: list[tuple[int, bytes | None]]
            options: dict[str, Any] = {
                "io_config": self._io_config,
                "max_concurrent_files": self._metadata_concurrency,
                "max_concurrent_requests": self._max_concurrent_requests,
                "max_in_flight_bytes": self._max_in_flight_bytes,
                "enable_read_coalescing": self._enable_read_coalescing,
                "read_coalesce_gap_bytes": self._read_coalesce_gap_bytes,
                "read_coalesce_max_bytes": self._read_coalesce_max_bytes,
            }
            with diagnostics.stage("planning_metadata"):
                if (
                    self._enable_metadata_reuse
                    and remaining_snapshot_bytes
                    and any(urlsplit(path).scheme == "s3" for path in paths)
                ):
                    if self._enable_row_count_planning:
                        entries, _metrics = _native._read_vortex_row_counts_bulk_cached(
                            paths, max_snapshot_bytes=remaining_snapshot_bytes, **options
                        )
                    else:
                        cached, _metrics = _native._read_vortex_metadata_bulk_cached(
                            paths, max_snapshot_bytes=remaining_snapshot_bytes, **options
                        )
                        entries = [(num_rows, snapshot) for _, num_rows, snapshot in cached]
                        del cached
                elif self._enable_row_count_planning:
                    counts, _metrics = _native._read_vortex_row_counts_bulk(paths, **options)
                    entries = [(num_rows, None) for num_rows in counts]
                else:
                    metadata, _metrics = _native.read_vortex_metadata_bulk(paths, **options)
                    entries = [(num_rows, None) for _, num_rows in metadata]
                    del metadata
            for path, (num_rows, snapshot) in zip(paths, entries, strict=True):
                with diagnostics.stage("task_creation"):
                    if snapshot is not None:
                        if len(snapshot) > remaining_snapshot_bytes:
                            snapshot = None
                        else:
                            remaining_snapshot_bytes -= len(snapshot)
                    # Execution validates every full schema before optimization,
                    # including when planning only requested footer row counts.
                    task = DataSourceTask.vortex(
                        path=path,
                        schema=self._scan_schema,
                        num_rows=num_rows,
                        vortex_config=VortexSourceConfig(
                            batch_size=self._batch_size,
                            enable_projection_pushdown=self._enable_projection_pushdown,
                            enable_filter_pushdown=self._enable_filter_pushdown,
                            max_concurrent_requests=self._max_concurrent_requests,
                            max_in_flight_bytes=self._max_in_flight_bytes,
                            enable_read_coalescing=self._enable_read_coalescing,
                            read_coalesce_gap_bytes=self._read_coalesce_gap_bytes,
                            read_coalesce_max_bytes=self._read_coalesce_max_bytes,
                            enable_large_offsets=self._enable_large_offsets,
                            enable_execution_session_reuse=self._enable_execution_session_reuse,
                            enable_file_pruning=self._enable_file_pruning,
                            enable_layout_splitting=self._enable_layout_splitting,
                            metadata_snapshot=snapshot,
                        ),
                        pushdowns=pushdowns,
                        storage_config=StorageConfig(multithreaded_io=True, io_config=self._io_config),
                    )
                yield task
