import dask.dataframe as dd
import pytest
from dask.datasets import timeseries


@pytest.fixture(scope="session")
def test_ddf():
    ddf = timeseries(
        dtypes={
            "floats": float,
            "ints": int,
            "strings": str,
            # "categoricals": 'category'
        }
    )
    return ddf.reset_index()
