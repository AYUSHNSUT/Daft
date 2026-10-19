# corner cases for df.count()
from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft import col


def test_count_star() -> None:
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
    res = df.count("*").to_pydict()
    assert res == {"count": [4]}


def test_count_col_star() -> None:
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
    res = df.count(col("*")).to_pydict()
    assert res == {"a": [4], "b": [2]}


def test_count_mixed_col_star() -> None:
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
    res = df.count(col("*"), col("a").alias("c")).to_pydict()
    assert res == {"a": [4], "b": [2], "c": [4]}


def test_count_mixed_star() -> None:
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
    with pytest.raises(ValueError, match=r"Cannot call count\(\) with both \* and column names"):
        df.count("*", "a")


def test_count_int() -> None:
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
    res = df.count(1).to_pydict()
    assert res == {"count": [4]}


@pytest.mark.parametrize("num_rows", [0, 5])
def test_count_zero_columns(num_rows: int) -> None:
    table = pa.table({"a": pa.array(range(num_rows), type=pa.int64())})
    # The empty projection preserves rows and exercises lazy counting rather than cached cardinality.
    df = daft.from_arrow(table).select()

    assert df.column_names == []
    assert df.count_rows() == num_rows
    assert df.count("*").to_pydict() == {"count": [num_rows]}
