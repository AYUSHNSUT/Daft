"""Public-query and shared-plan checks for the experimental native reader.

Expected rows are computed from Python values independently of either scan
strategy. Diagnostics describe the shared scan plan, not observed disk I/O;
request-range and zone-pruning evidence lives in the Rust reader tests.
"""

from __future__ import annotations

import math
import operator
from collections.abc import AsyncIterator
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft import daft as _native
from daft.io import _vortex
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.pickle import cloudpickle
from daft.schema import Schema


@pytest.fixture(params=[(False, False), (False, True), (True, False), (True, True)])
def flags(request) -> dict[str, bool]:
    projection, filtering = request.param
    return {"enable_projection_pushdown": projection, "enable_filter_pushdown": filtering}


@pytest.fixture
def vortex_writer():
    if not hasattr(_native, "read_vortex_schema"):
        pytest.skip("requires a Daft build with Rust features python,vortex")
    return pytest.importorskip("vortex", reason="optional vortex-data package is needed only to write test fixtures")


def _write(tmp_path: Path, table: pa.Table, writer, name: str = "data.vortex") -> str:
    path = str(tmp_path / name)
    writer.io.write(table, path)
    return path


def _diagnostics(path: str, flags: dict[str, bool], columns: list[str], predicate=None):
    return _native._vortex_scan_diagnostics(
        _native.read_vortex_schema(path),
        columns,
        None if predicate is None else predicate._expr,
        **flags,
    )


@pytest.fixture
def primitive_file(tmp_path: Path, vortex_writer):
    table = pa.table(
        {
            "unused": [f"wide-{index}:" + "x" * 1024 for index in range(8)],
            "id": range(8),
            "value": pa.array([-(2**63), -4, -1, 0, 2, 5, 2**63 - 1, None], pa.int64()),
            "extra": [7, 6, 5, 4, 3, 2, 1, 0],
            "flag": [True, False, None, True, False, None, True, False],
            "all_null": pa.nulls(8),
        }
    )
    return _write(tmp_path, table, vortex_writer), table


@pytest.mark.parametrize("name", ["enable_projection_pushdown", "enable_filter_pushdown"])
@pytest.mark.parametrize("value", [None, 0, 1, "true", [], np.bool_(True)])
def test_vortex_pushdown_flags_require_actual_bools_before_io(name: str, value) -> None:
    with pytest.raises(TypeError, match=f"{name} must be a bool"):
        daft.read_vortex("unused.vortex", **{name: value})


def test_vortex_pushdown_flags_are_keyword_only() -> None:
    with pytest.raises(TypeError, match="positional"):
        daft.read_vortex("unused.vortex", None, 2, False, False)


@pytest.mark.parametrize("op", [operator.eq, operator.ne, operator.lt, operator.le, operator.gt, operator.ge])
@pytest.mark.parametrize("reverse", [False, True])
def test_vortex_exact_int64_comparisons_match_python(primitive_file, flags, op, reverse: bool) -> None:
    path, table = primitive_file
    predicate = op(daft.lit(2), daft.col("value")) if reverse else op(daft.col("value"), daft.lit(2))
    values = table["value"].to_pylist()
    ids = [
        index for index, value in enumerate(values) if value is not None and (op(2, value) if reverse else op(value, 2))
    ]
    result = daft.read_vortex(path, batch_size=2, **flags).where(predicate).select("id").to_pydict()
    assert result == {"id": ids}
    plan = _diagnostics(path, flags, ["id"], predicate)
    assert (plan["pushed_filter"] is not None) == flags["enable_filter_pushdown"]
    assert plan["residual_filter"] is not None
    assert plan["physical_columns"] == (["id", "value"] if flags["enable_projection_pushdown"] else table.column_names)


def test_vortex_exact_uint64_literal_boundaries(tmp_path: Path, vortex_writer, flags) -> None:
    values = [0, 2**63 - 1, 2**63, 2**64 - 1, None]
    path = _write(tmp_path, pa.table({"id": range(5), "value": pa.array(values, pa.uint64())}), vortex_writer)
    predicate = daft.col("value") >= 2**63
    expected = [index for index, value in enumerate(values) if value is not None and value >= 2**63]
    assert daft.read_vortex(path, **flags).where(predicate).select("id").to_pydict() == {"id": expected}
    assert (_diagnostics(path, flags, ["id"], predicate)["pushed_filter"] is not None) == flags[
        "enable_filter_pushdown"
    ]


@pytest.mark.parametrize("op", [operator.eq, operator.ne])
def test_vortex_bool_comparisons(primitive_file, flags, op) -> None:
    path, table = primitive_file
    predicate = op(daft.col("flag"), True)
    expected = [index for index, value in enumerate(table["flag"].to_pylist()) if value is not None and op(value, True)]
    assert daft.read_vortex(path, **flags).where(predicate).select("id").to_pydict() == {"id": expected}
    assert (_diagnostics(path, flags, ["id"], predicate)["pushed_filter"] is not None) == flags[
        "enable_filter_pushdown"
    ]


@pytest.mark.parametrize("column", ["value", "flag", "all_null", "unused"])
@pytest.mark.parametrize("negate", [False, True])
def test_vortex_null_predicates(primitive_file, flags, column: str, negate: bool) -> None:
    path, table = primitive_file
    predicate = daft.col(column).not_null() if negate else daft.col(column).is_null()
    expected = [index for index, value in enumerate(table[column].to_pylist()) if (value is not None) == negate]
    assert daft.read_vortex(path, **flags).where(predicate).select("id").to_pydict() == {"id": expected}
    assert (_diagnostics(path, flags, ["id"], predicate)["pushed_filter"] is not None) == flags[
        "enable_filter_pushdown"
    ]


@pytest.mark.parametrize(
    "case",
    ["mixed_uint64", "mixed_int64", "float_zero", "float_negative_zero", "nan", "cast", "arithmetic", "or", "not"],
)
def test_vortex_unsafe_filter_fallback_matches_independent_oracle(
    tmp_path: Path, vortex_writer, flags, case: str
) -> None:
    if case == "mixed_uint64":
        values = [2**53 - 1, 2**53, 2**53 + 1, 2**53 + 2, 2**64 - 1, None]
        array = pa.array(values, pa.uint64())
        predicate = daft.col("value") == 2**53

        # This Daft version promotes mixed Int64/UInt64 to Float64. The +1
        # value therefore also matches; an exact integer pushdown loses it.
        def keep(value):
            return float(value) == float(2**53)

    elif case == "mixed_int64":
        values = [-(2**63), -1, 2**53, 2**63 - 1024, 2**63 - 1, None]
        array = pa.array(values, pa.int64())
        predicate = daft.col("value") == 2**63

        def keep(value):
            return float(value) == float(2**63)

    elif case in {"float_zero", "float_negative_zero", "nan"}:
        values = [float("nan"), 0.0, -0.0, 2.0, float("inf"), None]
        array = pa.array(values, pa.float64())
        zero = -0.0 if case == "float_negative_zero" else 0.0
        predicate = daft.col("value").is_nan() if case == "nan" else daft.col("value") == zero

        # Daft uses Arrow cmp::eq's totalOrder semantics: +0 and -0 differ.
        # Compute that distinction from Python values, independently of scans.
        def keep(value):
            if case == "nan":
                return math.isnan(value)
            return value == 0.0 and math.copysign(1, value) == math.copysign(1, zero)

    else:
        values = [-4, -1, 0, 2, 5, None]
        array = pa.array(values, pa.int64())
        if case == "cast":
            predicate = daft.col("value").cast(daft.DataType.float64()) > 1.0

            def keep(value):
                return float(value) > 1.0

        elif case == "arithmetic":
            predicate = daft.col("value") + 1 >= 2

            def keep(value):
                return value + 1 >= 2

        elif case == "or":
            predicate = (daft.col("value") < 0) | (daft.col("value") >= 5)

            def keep(value):
                return value < 0 or value >= 5

        else:
            predicate = ~(daft.col("value") >= 0)

            def keep(value):
                return not value >= 0

    expected = [index for index, value in enumerate(values) if value is not None and keep(value)]
    table = pa.table({"id": range(len(values)), "value": array})
    if case in {"float_zero", "float_negative_zero", "nan"}:
        assert daft.from_arrow(table).where(predicate).select("id").to_pydict() == {"id": expected}
    path = _write(tmp_path, table, vortex_writer)
    assert daft.read_vortex(path, batch_size=2, **flags).where(predicate).select("id").to_pydict() == {"id": expected}
    # Inspect the original expression. The public optimizer may equivalently
    # normalize it before planning; either route must preserve the oracle rows.
    plan = _diagnostics(path, flags, ["id"], predicate)
    assert plan["pushed_filter"] is None
    assert plan["residual_filter"] is not None
    assert plan["filter_fallbacks"]


def test_vortex_udf_fallback_keeps_its_input_column(primitive_file, flags) -> None:
    path, table = primitive_file

    @daft.func(return_dtype=daft.DataType.bool())
    def is_even(value: int | None) -> bool:
        return value is not None and value % 2 == 0

    predicate = is_even(daft.col("extra"))
    expected = [index for index, value in enumerate(table["extra"].to_pylist()) if value % 2 == 0]
    assert daft.read_vortex(path, **flags).where(predicate).select("id").to_pydict() == {"id": expected}
    plan = _diagnostics(path, flags, ["id"], predicate)
    assert plan["pushed_filter"] is None
    assert plan["filter_fallbacks"]
    assert "extra" in plan["physical_columns"]


def test_vortex_partial_and_keeps_all_residual_dependencies_and_output_order(primitive_file, flags) -> None:
    path, table = primitive_file
    predicate = (daft.col("value") >= 0) & ((daft.col("extra") + 1 > 2) & daft.col("flag").not_null())
    rows = table.to_pylist()
    expected = [
        row
        for row in rows
        if row["value"] is not None and row["value"] >= 0 and row["extra"] + 1 > 2 and row["flag"] is not None
    ]
    result = daft.read_vortex(path, batch_size=2, **flags).where(predicate).select("flag", "id").to_pydict()
    assert result == {name: [row[name] for row in expected] for name in ["flag", "id"]}
    assert list(result) == ["flag", "id"]
    plan = _diagnostics(path, flags, ["flag", "id"], predicate)
    assert plan["output_columns"] == ["flag", "id"]
    assert plan["physical_columns"] == (
        ["id", "value", "extra", "flag"] if flags["enable_projection_pushdown"] else table.column_names
    )
    assert (plan["pushed_filter"] is not None) == flags["enable_filter_pushdown"]
    assert plan["filter_fallbacks"]


def test_vortex_public_optimizer_delivers_filter_projection_and_limit(
    primitive_file, flags, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, _ = primitive_file
    observed = []
    original = _vortex.VortexSource.get_tasks

    async def observe(self, pushdowns):
        observed.append((pushdowns, _diagnostics(self._paths[0], flags, pushdowns.columns, pushdowns.filters)))
        async for task in original(self, pushdowns):
            yield task

    monkeypatch.setattr(_vortex.VortexSource, "get_tasks", observe)
    result = daft.read_vortex(path, batch_size=2, **flags).where(daft.col("value") >= 2).select("id").limit(2)
    assert result.to_pydict() == {"id": [4, 5]}
    assert len(observed) == 1
    pushdowns, plan = observed[0]
    assert pushdowns.columns == ["id"]
    assert pushdowns.filters is not None
    assert pushdowns.filter_required_column_names() == {"value"}
    assert pushdowns.limit == 2
    assert plan["output_columns"] == ["id"]
    assert (plan["pushed_filter"] is not None) == flags["enable_filter_pushdown"]
    assert plan["enable_projection_pushdown"] == flags["enable_projection_pushdown"]
    assert plan["enable_filter_pushdown"] == flags["enable_filter_pushdown"]


@pytest.mark.parametrize("limit", [0, 1, 2, 3, 20])
def test_vortex_residual_filter_before_limit_and_limit_before_filter(primitive_file, flags, limit: int) -> None:
    path, table = primitive_file
    # Arithmetic remains a residual and late matches cross multiple batches.
    predicate = daft.col("extra") + 1 <= 3
    expected = [index for index, value in enumerate(table["extra"].to_pylist()) if value + 1 <= 3]
    frame = daft.read_vortex(path, batch_size=2, **flags)
    assert frame.where(predicate).select("id").limit(limit).to_pydict() == {"id": expected[:limit]}
    assert frame.limit(limit).where(predicate).select("id").to_pydict() == {
        "id": [index for index in expected if index < limit]
    }


def test_vortex_multiple_files_preserve_global_filter_limit(tmp_path: Path, vortex_writer, flags) -> None:
    paths = [
        _write(
            tmp_path,
            pa.table({"id": range(start, start + 6), "value": range(start, start + 6)}),
            vortex_writer,
            f"{start}.vortex",
        )
        for start in [0, 6, 12]
    ]
    with daft.execution_config_ctx(maintain_order=True):
        frame = daft.read_vortex(paths, batch_size=2, **flags)
        assert frame.where(daft.col("value") >= 5).select("id").limit(8).to_pydict() == {"id": list(range(5, 13))}
        assert frame.limit(8).where(daft.col("value") >= 5).select("id").to_pydict() == {"id": [5, 6, 7]}


def test_vortex_count_literal_empty_and_zero_field_cardinality(tmp_path: Path, vortex_writer, flags) -> None:
    path = _write(tmp_path, pa.table({"id": range(5), "unused": ["x"] * 5}), vortex_writer)
    frame = daft.read_vortex(path, batch_size=2, **flags)
    assert frame.count_rows() == 5
    assert frame.select().count_rows() == 5
    assert frame.select(daft.lit(7).alias("constant")).to_pydict() == {"constant": [7] * 5}
    for field in frame.select("id").to_arrow().schema:
        assert b"daft.vortex.nullable" not in (field.metadata or {})
    empty = frame.where(daft.col("id") > 100).select("id").to_arrow()
    assert empty.num_rows == 0
    assert empty.schema.names == ["id"]
    assert b"daft.vortex.nullable" not in (empty.schema.field("id").metadata or {})
    empty_path = _write(tmp_path, pa.table({"id": pa.array([], pa.int64())}), vortex_writer, "empty.vortex")
    empty_frame = daft.read_vortex(empty_path, **flags)
    assert empty_frame.count_rows() == 0
    assert empty_frame.where(daft.col("id") >= 0).select("id").to_pydict() == {"id": []}
    zero = _write(tmp_path, pa.table({"drop": range(5)}).select([]), vortex_writer, "zero.vortex")
    zero_frame = daft.read_vortex(zero, batch_size=2, **flags)
    assert zero_frame.count_rows() == 5
    assert zero_frame.limit(3).count_rows() == 3
    assert zero_frame.select(daft.lit(1).alias("constant")).to_pydict() == {"constant": [1] * 5}
    plan = _diagnostics(path, flags, [])
    assert plan["output_columns"] == []
    assert plan["physical_columns"] == ([] if flags["enable_projection_pushdown"] else ["id", "unused"])


def test_vortex_public_projection_name_rules_and_native_validation(primitive_file, flags) -> None:
    path, table = primitive_file
    with pytest.raises(Exception, match="missing"):
        daft.read_vortex(path, **flags).select("missing").collect()
    # Public select accepts repeated names; the mapping export has one key.
    # The optimizer deduplicates required physical input columns. A raw native
    # scan request with repeated output names is rejected separately below.
    repeated = daft.read_vortex(path, **flags).select("id", "id")
    assert repeated.column_names == ["id", "id"]
    expected = {"id": table["id"].to_pylist()}
    assert repeated.to_pydict() == expected
    assert daft.from_arrow(table).select("id", "id").to_pydict() == expected
    with pytest.raises(Exception, match="missing"):
        _diagnostics(path, flags, ["missing"])
    with pytest.raises(Exception, match="(?i)(duplicate|already|unique)"):
        _diagnostics(path, flags, ["id", "id"])


class _ConstantResidualSource(DataSource):
    def __init__(self, path: str, predicate, flags: dict[str, bool]) -> None:
        self._path = path
        self._predicate = predicate
        self._flags = flags
        self._scan_schema = Schema._from_pyschema(_native.read_vortex_schema(path))

    @property
    def name(self) -> str:
        return "VortexConstantResidualTest"

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(pa.schema([]))

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        yield DataSourceTask.vortex(
            self._path,
            self._scan_schema,
            vortex_config=_native.VortexSourceConfig(batch_size=2, **self._flags),
            pushdowns=Pushdowns(columns=[], filters=self._predicate),
        )


@pytest.mark.parametrize("value", [True, False, None])
def test_vortex_zero_physical_columns_with_constant_residual(tmp_path: Path, vortex_writer, flags, value) -> None:
    path = _write(tmp_path, pa.table({"unused": range(5)}), vortex_writer)
    predicate = daft.lit(value)
    if value is None:
        predicate = predicate.cast(daft.DataType.bool())
    source = _ConstantResidualSource(path, predicate, flags)
    assert source.read().count_rows() == (5 if value is True else 0)
    plan = _diagnostics(path, flags, [], predicate)
    assert plan["physical_columns"] == ([] if flags["enable_projection_pushdown"] else ["unused"])


@pytest.mark.parametrize("mismatch", ["type", "nullability"])
def test_vortex_projection_does_not_hide_schema_mismatch(tmp_path: Path, vortex_writer, flags, mismatch: str) -> None:
    first = pa.table({"id": [1], "unused": ["a"]})
    other = (
        pa.table({"id": [2], "unused": [42]})
        if mismatch == "type"
        else pa.Table.from_arrays(
            [pa.array([2]), pa.array(["b"])],
            schema=pa.schema([pa.field("id", pa.int64()), pa.field("unused", pa.string(), nullable=False)]),
        )
    )
    paths = [
        _write(tmp_path, first, vortex_writer, "first.vortex"),
        _write(tmp_path, other, vortex_writer, "other.vortex"),
    ]
    with pytest.raises(Exception, match="(?i)(schema|nullability)"):
        daft.read_vortex(paths, **flags).where(daft.col("id") >= 0).select("id").collect()


def test_vortex_join_select_and_groupby_keep_required_columns(tmp_path: Path, vortex_writer, flags) -> None:
    orders = [{"id": index, "key": index % 3, "amount": index * 2} for index in range(9)]
    customers = [
        {"key": 0, "name": "zero", "tier": "a"},
        {"key": 1, "name": "one", "tier": "b"},
        {"key": 2, "name": "two", "tier": "a"},
    ]
    left = daft.read_vortex(_write(tmp_path, pa.Table.from_pylist(orders), vortex_writer, "orders.vortex"), **flags)
    right = daft.read_vortex(
        _write(tmp_path, pa.Table.from_pylist(customers), vortex_writer, "customers.vortex"), **flags
    )
    joined = left.join(right, on="key")
    assert joined.select("name", "id").sort("id").to_pydict() == {
        "name": [customers[row["key"]]["name"] for row in orders],
        "id": list(range(9)),
    }
    totals = {"a": 0, "b": 0}
    counts = {"a": 0, "b": 0}
    for row in orders:
        tier = customers[row["key"]]["tier"]
        totals[tier] += row["amount"]
        counts[tier] += 1
    result = (
        joined.groupby("tier")
        .agg(daft.col("amount").sum().alias("total"), daft.col("id").count().alias("count"))
        .sort("tier")
        .to_pydict()
    )
    assert result == {"tier": ["a", "b"], "total": [totals["a"], totals["b"]], "count": [counts["a"], counts["b"]]}


def test_vortex_source_and_config_pickle_preserve_independent_flags(primitive_file, flags) -> None:
    path, _ = primitive_file
    config = cloudpickle.loads(cloudpickle.dumps(_native.VortexSourceConfig(batch_size=2, **flags)))
    assert config.batch_size == 2
    assert config.enable_projection_pushdown == flags["enable_projection_pushdown"]
    assert config.enable_filter_pushdown == flags["enable_filter_pushdown"]
    source = cloudpickle.loads(cloudpickle.dumps(_vortex.VortexSource([path], None, 2, **flags)))
    assert source._enable_projection_pushdown == flags["enable_projection_pushdown"]
    assert source._enable_filter_pushdown == flags["enable_filter_pushdown"]
    assert source.read().where(daft.col("value") >= 2).select("id").to_pydict() == {"id": [4, 5, 6]}


@pytest.mark.parametrize("name", ["enable_projection_pushdown", "enable_filter_pushdown"])
@pytest.mark.parametrize("value", [None, 0, 1, "true", np.bool_(True)])
def test_vortex_native_config_rejects_non_boolean_switches(name: str, value) -> None:
    # Verify the new ABI first: an old extension's unexpected-keyword error
    # must not count as successful strict-Boolean validation.
    valid = _native.VortexSourceConfig(enable_projection_pushdown=False, enable_filter_pushdown=True)
    assert valid.enable_projection_pushdown is False
    assert valid.enable_filter_pushdown is True
    with pytest.raises(TypeError, match=name):
        _native.VortexSourceConfig(**{name: value})


def test_vortex_native_config_flags_and_pickle_without_reader_feature(flags) -> None:
    config = cloudpickle.loads(cloudpickle.dumps(_native.VortexSourceConfig(batch_size=7, **flags)))
    assert config.batch_size == 7
    assert config.enable_projection_pushdown == flags["enable_projection_pushdown"]
    assert config.enable_filter_pushdown == flags["enable_filter_pushdown"]
    defaults = _native.VortexSourceConfig()
    assert defaults.enable_projection_pushdown is True
    assert defaults.enable_filter_pushdown is True
