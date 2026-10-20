from __future__ import annotations

import builtins
import math
from pathlib import Path

import pyarrow as pa
import pytest

import daft
from daft import daft as _native
from daft.io import _vortex
from daft.pickle import cloudpickle


@pytest.fixture
def vortex_writer():
    if not hasattr(_native, "read_vortex_schema"):
        pytest.skip("requires a Daft build with Rust features python,vortex")
    return pytest.importorskip("vortex", reason="optional vortex-data package is needed only to write test fixtures")


def _write(path: Path, table: pa.Table, writer) -> str:
    writer.io.write(table, str(path))
    return str(path)


@pytest.fixture
def vortex_path(tmp_path: Path, vortex_writer) -> str:
    return _write(
        tmp_path / "data.vortex",
        pa.table({"id": range(9), "name": [None, "", "汉字", "café", "four", "five", "six", "seven", "eight"]}),
        vortex_writer,
    )


def test_read_vortex_exports_without_feature() -> None:
    assert daft.read_vortex is daft.io.read_vortex
    assert "read_vortex" in daft.__all__


def test_read_vortex_disabled_build_is_actionable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delattr(_native, "read_vortex_schema", raising=False)
    with pytest.raises(ImportError, match="python,vortex"):
        daft.read_vortex("unused.vortex")


@pytest.mark.parametrize("batch_size", [0, -1, True, 1.5, "2"])
def test_read_vortex_invalid_batch_size(batch_size) -> None:
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        daft.read_vortex("unused.vortex", batch_size=batch_size)


@pytest.mark.parametrize("path", [[], "", [""], ["file.vortex", ""]])
def test_read_vortex_empty_input_rejected_before_io(path) -> None:
    with pytest.raises(ValueError, match="empty"):
        daft.read_vortex(path)


@pytest.mark.parametrize("path", ["gs://bucket/*.vortex", "az://bucket/file.vortex", "ftp://host/file.vortex"])
def test_read_vortex_remote_input_rejected_before_io(path: str) -> None:
    with pytest.raises(ValueError, match="only local"):
        daft.read_vortex(path)


@pytest.mark.parametrize("suffix", ["?query=1", "#fragment", "?", "#"])
def test_read_vortex_file_uri_query_or_fragment_rejected(suffix: str) -> None:
    with pytest.raises(ValueError, match="query or fragment"):
        daft.read_vortex("file:///unused.vortex" + suffix)


@pytest.mark.parametrize("path", ["file:", "file://", "FILE:", "FILE://"])
def test_read_vortex_empty_file_uri_rejected_before_io(path: str) -> None:
    with pytest.raises(ValueError, match="empty Vortex filepath"):
        daft.read_vortex(path)


def test_read_vortex_explicit_io_config(vortex_path: str) -> None:
    assert daft.read_vortex(vortex_path, io_config=daft.io.IOConfig()).select("id").to_pydict() == {
        "id": list(range(9))
    }


def test_vortex_path_resolution_uses_native_globs_and_deduplicates(tmp_path: Path) -> None:
    first = tmp_path / "first.vortex"
    nested = tmp_path / "nested"
    nested.mkdir()
    second = nested / "second.vortex"
    explicit = tmp_path / "explicit.bin"
    for path in [first, second, explicit, tmp_path / "unrelated.txt"]:
        path.touch()
    inputs = _vortex._input_paths([first, "file://" + str(first), "FILE://" + str(first), tmp_path, explicit])
    assert _vortex._resolve_vortex_paths(inputs, None) == [str(first), str(second), str(explicit)]


def test_read_vortex(vortex_path: str) -> None:
    assert daft.read_vortex(vortex_path, batch_size=2).to_pydict() == {
        "id": list(range(9)),
        "name": [None, "", "汉字", "café", "four", "five", "six", "seven", "eight"],
    }


def test_read_vortex_projection_residual_filter_limit(vortex_path: str) -> None:
    frame = daft.read_vortex(vortex_path, batch_size=2)
    assert frame.select("name", "id").column_names == ["name", "id"]
    assert frame.where(daft.col("id") >= 6).select("name").limit(2).to_pydict() == {"name": ["six", "seven"]}
    assert frame.where(daft.col("id") > 100).select("name").to_pydict() == {"name": []}
    assert frame.limit(2).where(daft.col("id") >= 6).select("name").to_pydict() == {"name": []}
    assert frame.where(daft.col("name").is_null()).select("id").to_pydict() == {"id": [0]}
    assert frame.where(daft.col("id") >= 6).count_rows() == 3
    assert frame.count_rows() == 9
    assert frame.select(daft.lit(1).alias("constant")).to_pydict() == {"constant": [1] * 9}


@pytest.mark.parametrize("limit", [0, 1, 2, 9, 20])
def test_read_vortex_limits(vortex_path: str, limit: int) -> None:
    assert daft.read_vortex(vortex_path, batch_size=2).limit(limit).count_rows() == min(limit, 9)


@pytest.mark.parametrize("kind", ["path", "directory", "trailing_slash", "glob", "list", "overlap"])
def test_read_vortex_paths(tmp_path: Path, vortex_writer, kind: str) -> None:
    first = _write(tmp_path / "first.vortex", pa.table({"id": [1, 2]}), vortex_writer)
    nested = tmp_path / "nested"
    nested.mkdir()
    second = _write(nested / "second.vortex", pa.table({"id": [3, 4]}), vortex_writer)
    (tmp_path / "unrelated.txt").write_text("not a Vortex file")
    paths = {
        "path": tmp_path,
        "directory": str(tmp_path),
        "trailing_slash": str(tmp_path) + "/",
        "glob": str(tmp_path / "**" / "*.vortex"),
        "list": [Path(first), second],
        "overlap": [first, "file://" + first, str(tmp_path / "**" / "*.vortex")],
    }
    assert daft.read_vortex(paths[kind]).sort("id").to_pydict() == {"id": [1, 2, 3, 4]}


@pytest.mark.parametrize("filename", ["data", "data.bin", "file with spaces.vortex", "数据.vortex", "a%20b.vortex"])
def test_read_vortex_exact_file_and_file_uri(tmp_path: Path, vortex_writer, filename: str) -> None:
    path = _write(tmp_path / filename, pa.table({"id": [1, 2]}), vortex_writer)
    assert daft.read_vortex(path).to_pydict() == {"id": [1, 2]}
    assert daft.read_vortex("file://" + path).to_pydict() == {"id": [1, 2]}
    assert daft.read_vortex("FILE://" + path).to_pydict() == {"id": [1, 2]}


def test_read_vortex_file_uri_does_not_decode_missing_path(tmp_path: Path, vortex_writer) -> None:
    path = tmp_path / "space name.vortex"
    _write(path, pa.table({"id": [1]}), vortex_writer)
    # Native Daft file URLs use literal percent characters. Path.as_uri() is
    # encoded and consequently names a different, nonexistent local filename.
    with pytest.raises(FileNotFoundError):
        daft.read_vortex(path.as_uri())


def test_read_vortex_relative_path_fixed_at_planning(vortex_path: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(Path(vortex_path).parent)
    frame = daft.read_vortex("data.vortex")
    monkeypatch.chdir(Path(vortex_path).parent.parent)
    assert frame.select("id").to_pydict() == {"id": list(range(9))}


def test_read_vortex_discovery_is_not_repeated(tmp_path: Path, vortex_writer) -> None:
    _write(tmp_path / "first.vortex", pa.table({"id": [1]}), vortex_writer)
    frame = daft.read_vortex(str(tmp_path / "*.vortex"))
    _write(tmp_path / "later.vortex", pa.table({"id": [2]}), vortex_writer)
    assert frame.to_pydict() == {"id": [1]}


def test_read_vortex_empty_and_empty_first_file(tmp_path: Path, vortex_writer) -> None:
    empty = _write(tmp_path / "empty.vortex", pa.table({"id": pa.array([], pa.int64())}), vortex_writer)
    assert daft.read_vortex(empty).to_pydict() == {"id": []}
    assert daft.read_vortex(empty).count_rows() == 0
    data = _write(tmp_path / "data.vortex", pa.table({"id": [1, 2]}), vortex_writer)
    assert daft.read_vortex([empty, data]).sort("id").to_pydict() == {"id": [1, 2]}


def test_read_vortex_nonempty_zero_field_file(tmp_path: Path, vortex_writer) -> None:
    table = pa.table({"discard": range(5)}).select([])
    assert table.num_rows == 5
    assert table.num_columns == 0
    path = _write(tmp_path / "zero-fields.vortex", table, vortex_writer)
    frame = daft.read_vortex(path, batch_size=2)
    assert frame.column_names == []
    assert frame.count_rows() == 5
    assert frame.limit(3).count_rows() == 3
    assert frame.select(daft.lit(1).alias("constant")).to_pydict() == {"constant": [1] * 5}


@pytest.mark.parametrize("empty", [False, True])
def test_read_vortex_arrow_output_hides_private_schema_metadata(tmp_path: Path, vortex_writer, empty: bool) -> None:
    values = [] if empty else [1, 2]
    path = _write(tmp_path / "metadata.vortex", pa.table({"id": pa.array(values, pa.int64())}), vortex_writer)
    result = daft.read_vortex(path).to_arrow()
    for field in result.schema:
        assert b"daft.vortex.nullable" not in (field.metadata or {})

    logical_schema = daft.read_vortex(path).schema().to_pyarrow_schema()
    assert b"daft.vortex.nullable" not in (logical_schema.field("id").metadata or {})

    for partition in daft.read_vortex(path).iter_partitions():
        for field in partition.schema().to_pyarrow_schema():
            assert b"daft.vortex.nullable" not in (field.metadata or {})
    # PyCapsule export uses the MicroPartition schema rather than to_arrow()'s
    # explicit public DataFrame schema, covering the generic empty fallback.
    capsule_result = pa.table(daft.read_vortex(path))
    for field in capsule_result.schema:
        assert b"daft.vortex.nullable" not in (field.metadata or {})


@pytest.mark.parametrize("bits", [8, 16, 32, 64])
@pytest.mark.parametrize("signed", [True, False])
def test_read_vortex_integer_boundaries(tmp_path: Path, vortex_writer, bits: int, signed: bool) -> None:
    dtype = getattr(pa, f"{'int' if signed else 'uint'}{bits}")()
    values = [-(2 ** (bits - 1)), 0, 2 ** (bits - 1) - 1, None] if signed else [0, 2**bits - 1, None]
    path = _write(tmp_path / "integers.vortex", pa.table({"value": pa.array(values, dtype)}), vortex_writer)
    result = daft.read_vortex(path, batch_size=1)
    assert result.to_pydict() == {"value": values}
    assert result.schema()["value"].dtype == daft.DataType.from_arrow_type(dtype)


@pytest.mark.parametrize("dtype", [pa.float32(), pa.float64()])
def test_read_vortex_float_special_values(tmp_path: Path, vortex_writer, dtype: pa.DataType) -> None:
    path = _write(
        tmp_path / "floats.vortex",
        pa.table({"value": pa.array([0.0, -0.0, math.nan, math.inf, -math.inf, None], dtype)}),
        vortex_writer,
    )
    values = daft.read_vortex(path, batch_size=1).to_pydict()["value"]
    assert values[:2] == [0.0, 0.0]
    assert [math.copysign(1, value) for value in values[:2]] == [1.0, -1.0]
    assert math.isnan(values[2])
    assert values[3:] == [math.inf, -math.inf, None]


def test_read_vortex_bool_binary_and_all_null(tmp_path: Path, vortex_writer) -> None:
    table = pa.table({"flag": [True, False, None], "data": [b"\x00\xff", b"", None], "null": pa.nulls(3)})
    path = _write(tmp_path / "primitives.vortex", table, vortex_writer)
    assert daft.read_vortex(path, batch_size=1).to_pydict() == table.to_pydict()


@pytest.mark.parametrize("mismatch", ["type", "order", "missing", "extra", "nullability"])
def test_read_vortex_strict_full_schema(tmp_path: Path, vortex_writer, mismatch: str) -> None:
    first = pa.table({"id": [1], "unused": pa.array(["a"], pa.string())})
    variants = {
        "type": pa.table({"id": [2], "unused": [42]}),
        "order": pa.table({"unused": ["b"], "id": [2]}),
        "missing": pa.table({"id": [2]}),
        "extra": pa.table({"id": [2], "unused": ["b"], "extra": [True]}),
        "nullability": pa.Table.from_arrays(
            [pa.array([2]), pa.array(["b"])],
            schema=pa.schema([pa.field("id", pa.int64()), pa.field("unused", pa.string(), nullable=False)]),
        ),
    }
    paths = [
        _write(tmp_path / "first.vortex", first, vortex_writer),
        _write(tmp_path / "different.vortex", variants[mismatch], vortex_writer),
    ]
    with pytest.raises(Exception, match="(?i)(schema|nullability)"):
        daft.read_vortex(paths).select("id").collect()


@pytest.mark.parametrize(
    "array",
    [
        pa.array([[1], None], pa.list_(pa.int64())),
        pa.array([{"x": 1}, None], pa.struct([("x", pa.int64())])),
        pa.array([0, None], pa.date32()),
        pa.array([0, None], pa.timestamp("ns")),
    ],
)
def test_read_vortex_unsupported_types(tmp_path: Path, vortex_writer, array: pa.Array) -> None:
    path = _write(tmp_path / "unsupported.vortex", pa.table({"value": array}), vortex_writer)
    with pytest.raises(Exception, match="flat primitive"):
        daft.read_vortex(path)


def test_read_vortex_corrupt_file_context(tmp_path: Path, vortex_writer) -> None:
    path = tmp_path / "corrupt.vortex"
    path.write_bytes(b"not a Vortex file")
    with pytest.raises(Exception, match="corrupt.vortex"):
        daft.read_vortex(path)


def test_read_vortex_missing_path_context(tmp_path: Path, vortex_writer) -> None:
    with pytest.raises(FileNotFoundError, match="missing"):
        daft.read_vortex(tmp_path / "missing.vortex")


def test_read_vortex_does_not_import_python_vortex(vortex_path: str, monkeypatch: pytest.MonkeyPatch) -> None:
    original = builtins.__import__

    def reject_vortex(name, *args, **kwargs):
        if name == "vortex" or name.startswith("vortex."):
            pytest.fail("Native Vortex reading must not import the optional Python writer")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_vortex)
    assert daft.read_vortex(vortex_path).count_rows() == 9


def test_read_vortex_source_serialization(vortex_path: str) -> None:
    source = _vortex.VortexSource([vortex_path], None, 2)
    restored = cloudpickle.loads(cloudpickle.dumps(source))
    assert restored.read().where(daft.col("id") >= 6).select("name").to_pydict() == {"name": ["six", "seven", "eight"]}
