from dask.datasets import timeseries
import dask_deltatable as ddt
from dask.dataframe.utils import assert_eq
from pathlib import Path
from itertools import chain


def main():
    ddf = timeseries(
        dtypes={
            "floats": float,
            "ints": int,
            "strings": str,
            # "categoricals": 'category'
        }
    )
    # ddf = ddf.reset_index()
    fpath = f"file://{Path.cwd().as_posix()}/test.delta"
    ddt.to_delta_table(ddf, fpath, mode="append", partition_by="strings"
    )

if __name__ == "__main__":
    main()