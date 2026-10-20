from __future__ import annotations

import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING
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


def _require_vortex() -> None:
    if not hasattr(_native, "read_vortex_schema") or not hasattr(_native.PyDataSourceTask, "vortex"):
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
        if urlsplit(path).scheme not in {"http", "https", "s3"}:
            raise
        # Preserve useful backend status text without exposing a signed query
        # or URL userinfo when discovery fails before native reader creation.
        message = str(error).replace(discovery_path, _display_path(path)).replace(path, _display_path(path))
        parsed = urlsplit(path)
        for secret in (parsed.query, parsed.fragment, parsed.username, parsed.password):
            if secret:
                message = message.replace(secret, "<redacted>")
        error_type = FileNotFoundError if isinstance(error, FileNotFoundError) else OSError
        raise error_type(f"Vortex discovery for {_display_path(path)!r}: {message}") from None


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


def _resolve_vortex_paths(paths: list[str], io_config: IOConfig | None) -> list[str]:
    resolved = []
    seen = set()
    for path in paths:
        scheme = urlsplit(path).scheme
        # Exact HTTP URLs can contain query delimiters that the shared globber
        # would otherwise interpret as wildcard syntax. Its literal-escape
        # path removes these escapes before invoking the HTTP size request.
        discovery_path = path
        if scheme in {"http", "https"}:
            # IPv6 authority brackets are URL syntax, not glob metacharacters.
            authority_end = path.index("://") + 3 + len(urlsplit(path).netloc)
            discovery_path = path[:authority_end] + "".join(
                f"\\{character}" if character in "\\*?{}[]" else character for character in path[authority_end:]
            )
        infos = _discover(path, discovery_path, io_config)
        is_glob = any(character in path for character in "*?[{")
        remote = urlsplit(path).scheme in {"http", "https", "s3"}
        is_file = len(infos.file_paths) == 1 and _normalize_vortex_path(infos.file_paths[0]) == path
        if not is_glob and not is_file and urlsplit(path).scheme not in {"http", "https"}:
            directory_glob = f"{path.rstrip('/')}/**/*.vortex"
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
    paths = _input_paths(path)
    _require_vortex()
    io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
    return VortexSource(
        paths,
        io_config,
        batch_size,
        enable_projection_pushdown=enable_projection_pushdown,
        enable_filter_pushdown=enable_filter_pushdown,
        max_concurrent_requests=max_concurrent_requests,
        max_in_flight_bytes=max_in_flight_bytes,
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
    ) -> None:
        self._paths = _resolve_vortex_paths(paths, io_config)
        self._batch_size = batch_size
        self._io_config = io_config
        self._enable_projection_pushdown = enable_projection_pushdown
        self._enable_filter_pushdown = enable_filter_pushdown
        self._max_concurrent_requests = max_concurrent_requests
        self._max_in_flight_bytes = max_in_flight_bytes
        # Preserve native metadata: it carries original field nullability for
        # strict matching, which Daft's logical types alone cannot represent.
        self._scan_schema = Schema._from_pyschema(
            _native.read_vortex_schema(
                self._paths[0],
                io_config=io_config,
                max_concurrent_requests=max_concurrent_requests,
                max_in_flight_bytes=max_in_flight_bytes,
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

        for path in self._paths:
            # Full schema validation precedes physical optimization. The native
            # reader retains every residual dependency before final projection.
            yield DataSourceTask.vortex(
                path=path,
                schema=self._scan_schema,
                vortex_config=VortexSourceConfig(
                    batch_size=self._batch_size,
                    enable_projection_pushdown=self._enable_projection_pushdown,
                    enable_filter_pushdown=self._enable_filter_pushdown,
                    max_concurrent_requests=self._max_concurrent_requests,
                    max_in_flight_bytes=self._max_in_flight_bytes,
                ),
                pushdowns=pushdowns,
                storage_config=StorageConfig(multithreaded_io=True, io_config=self._io_config),
            )
