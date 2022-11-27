from dask_deltatable import to_delta_table, read_delta_table
from dask.dataframe.utils import assert_eq
from dask.datasets import timeseries
import dask.dataframe as dd
import pyarrow as pa


def test_write(test_ddf):
    import tempfile
    to_delta_table(test_ddf, "test_delta_table.delta", mode="overwrite")
    ddf = read_delta_table("test_delta_table.delta")
    assert_eq(test_ddf, ddf)