import dask.dataframe as dd
import pyarrow as pa
from dask.dataframe.utils import assert_eq
from dask.datasets import timeseries

from dask_deltatable import read_delta_table, to_delta_table


def test_write(test_ddf):
    import tempfile

    to_delta_table(test_ddf, "test_delta_table.delta", mode="overwrite")
    ddf = read_delta_table("test_delta_table.delta")
    assert_eq(test_ddf, ddf)
