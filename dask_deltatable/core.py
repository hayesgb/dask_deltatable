from __future__ import annotations

import contextlib
import json
import os
from typing import Dict, List, Optional
from urllib.parse import urlparse
from typing import Union, Literal, Mapping

import dask
from dask.base import tokenize
from dask.blockwise import BlockIndex
import dask.dataframe as dd
from dask.dataframe.core import Scalar
from dask.dataframe.io.utils import _is_local_fs
import pyarrow.parquet as pq
# from boto3 import Session
from dask.base import tokenize
from dask.dataframe.io import from_delayed
from dask.delayed import delayed
from dask.highlevelgraph import HighLevelGraph
from deltalake import DataCatalog, DeltaTable, write_deltalake
from fsspec.core import get_fs_token_paths
from fsspec.utils import stringify_path
from pyarrow import dataset as pa_ds
import pyarrow as pa


__all__ = ("to_delta_table", "read_delta_table")

NONE_LABEL = "__null_dask_index__"

class DeltaTableWrapper(object):
    path: str
    version: int
    columns: List[str]
    datetime: str
    storage_options: Dict[str, any]

    def __init__(
        self,
        path: str,
        version: int,
        columns: List[str],
        datetime: Optional[str] = None,
        storage_options: Dict[str, str] = None,
    ) -> None:
        self.path: str = path
        self.version: int = version
        self.columns = columns
        self.datetime = datetime
        self.storage_options = storage_options
        self.dt = DeltaTable(table_uri=self.path, version=self.version)
        self.fs, self.fs_token, _ = get_fs_token_paths(
            path, storage_options=storage_options
        )
        self.schema = self.dt.pyarrow_schema()

    def read_delta_dataset(self, f: str, **kwargs: Dict[any, any]):
        schema = kwargs.pop("schema", None) or self.schema
        filter = kwargs.pop("filter", None)
        if filter:
            filter_expression = pq._filters_to_expression(filter)
        else:
            filter_expression = None
        return (
            pa_ds.dataset(
                source=f,
                schema=schema,
                filesystem=self.fs,
                format="parquet",
                partitioning="hive",
            )
            .to_table(filter=filter_expression, columns=self.columns)
            .to_pandas()
        )

    def _make_meta_from_schema(self) -> Dict[str, str]:
        meta = dict()

        for field in self.schema:
            if self.columns:
                if field.name in self.columns:
                    meta[field.name] = field.type.to_pandas_dtype()
            else:
                meta[field.name] = field.type.to_pandas_dtype()
        return meta

    def _history_helper(self, log_file_name: str):
        log = self.fs.cat(log_file_name).decode().split("\n")
        for line in log:
            if line:
                meta_data = json.loads(line)
                if "commitInfo" in meta_data:
                    return meta_data["commitInfo"]

    def history(self, limit: Optional[int] = None, **kwargs) -> dd.core.DataFrame:
        delta_log_path = str(self.path).rstrip("/") + "/_delta_log"
        log_files = self.fs.glob(f"{delta_log_path}/*.json")
        if len(log_files) == 0:  # pragma no cover
            raise RuntimeError(f"No History (logs) found at:- {delta_log_path}/")
        log_files = sorted(log_files, reverse=True)
        if limit is None:
            last_n_files = log_files
        else:
            last_n_files = log_files[:limit]
        parts = [
            delayed(
                self._history_helper,
                name="read-delta-history" + tokenize(self.fs_token, f, **kwargs),
            )(f, **kwargs)
            for f in list(last_n_files)
        ]
        return dask.compute(parts)[0]

    def _vacuum_helper(self, filename_to_delete: str) -> None:
        full_path = urlparse(self.path)
        if full_path.scheme and full_path.netloc:  # pragma no cover
            # for different storage backend, delta-rs vacuum gives path to the file
            # it will not provide bucket name and scheme s3 or gcfs etc. so adding
            # manually
            filename_to_delete = (
                f"{full_path.scheme}://{full_path.netloc}/{filename_to_delete}"
            )
        self.fs.rm_file(self.path + "/" + filename_to_delete)

    def vacuum(self, retention_hours: int = 168, dry_run: bool = True) -> None:
        """
        Run the Vacuum command on the Delta Table: list and delete files no
        longer referenced by the Delta table and are older than the
        retention threshold.

        retention_hours: the retention threshold in hours, if none then
        the value from `configuration.deletedFileRetentionDuration` is used
         or default of 1 week otherwise.
        dry_run: when activated, list only the files, delete otherwise

        Returns
        -------
        the list of files no longer referenced by the Delta Table and are
         older than the retention threshold.
        """

        tombstones = self.dt.vacuum(retention_hours=retention_hours)
        if dry_run:
            return tombstones
        else:
            parts = [
                delayed(
                    self._vacuum_helper,
                    name="delta-vacuum"
                    + tokenize(self.fs_token, f, retention_hours, dry_run),
                )(f)
                for f in tombstones
            ]
        dask.compute(parts)[0]

    def get_pq_files(self) -> List[str]:
        """
        get the list of parquet files after loading the
        current datetime version
        """
        __doc__ == self.dt.load_with_datetime.__doc__

        if self.datetime is not None:
            self.dt.load_with_datetime(self.datetime)
        return self.dt.file_uris()

    def read_delta_table(self, **kwargs) -> dd.core.DataFrame:
        """
        Reads the list of parquet files in parallel
        """
        pq_files = self.get_pq_files()
        if len(pq_files) == 0:
            raise RuntimeError("No Parquet files are available")
        parts = [
            delayed(
                self.read_delta_dataset,
                name="read-delta-table-" + tokenize(self.fs_token, f, **kwargs),
            )(f, **kwargs)
            for f in list(pq_files)
        ]
        meta = self._make_meta_from_schema()
        return from_delayed(parts, meta=meta)


def _read_from_catalog(
    database_name: str, table_name: str, **kwargs
) -> dd.core.DataFrame:
    if ("AWS_ACCESS_KEY_ID" not in os.environ) and (
        "AWS_SECRET_ACCESS_KEY" not in os.environ
    ):
        session = Session()
        credentials = session.get_credentials()
        current_credentials = credentials.get_frozen_credentials()
        os.environ["AWS_ACCESS_KEY_ID"] = current_credentials.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = current_credentials.secret_key
    data_catalog = DataCatalog.AWS
    dt = DeltaTable.from_data_catalog(
        data_catalog=data_catalog, database_name=database_name, table_name=table_name
    )

    df = dd.read_parquet(dt.file_uris(), **kwargs)
    return df


def read_delta_table(
    path: Optional[str] = None,
    catalog: Optional[str] = None,
    database_name: str = None,
    table_name: str = None,
    version: int = None,
    columns: List[str] = None,
    storage_options: Dict[str, str] = None,
    datetime: str = None,
    **kwargs,
):
    """
    Read a Delta Table into a Dask DataFrame

    This reads a list of Parquet files in delta table directory into a
    Dask.dataframe.

    Parameters
    ----------
    path: Optional[str]
        path of Delta table directory
    catalog: Optional[str]
        Currently supports only AWS Glue Catalog
        if catalog is provided, user has to provide database and table name, and delta-rs will fetch the
        metadata from glue catalog, this is used by dask to read the parquet tables
    database_name: Optional[str]
        database name present in the catalog
    tablename: Optional[str]
        table name present in the database of the Catalog
    version: int, default None
        DeltaTable Version, used for Time Travelling across the
        different versions of the parquet datasets
    datetime: str, default None
        Time travel Delta table to the latest version that's created at or
        before provided `datetime_string` argument.
        The `datetime_string` argument should be an RFC 3339 and ISO 8601 date
         and time string.

        Examples:
        `2018-01-26T18:30:09Z`
        `2018-12-19T16:39:57-08:00`
        `2018-01-26T18:30:09.453+00:00`
         #(copied from delta-rs docs)
    columns: None or list(str)
        Columns to load. If None, loads all.
    storage_options : dict, default None
        Key/value pairs to be passed on to the file-system backend, if any.
    kwargs: dict,optional
        Some most used parameters can be passed here are:
        1. schema
        2. filter

        schema : pyarrow.Schema
            Used to maintain schema evolution in deltatable.
            delta protocol stores the schema string in the json log files which is
            converted into pyarrow.Schema and used for schema evolution
            i.e Based on particular version, some columns can be
            shown or not shown.

        filter: Union[List[Tuple[str, str, Any]], List[List[Tuple[str, str, Any]]]], default None
            List of filters to apply, like ``[[('col1', '==', 0), ...], ...]``.
            Can act as both partition as well as row based filter, above list of filters
            converted into pyarrow.dataset.Expression built using pyarrow.dataset.Field
            example:
                [("x",">",400)] --> pyarrow.dataset.field("x")>400

    Returns
    -------
    Dask.DataFrame

    Examples
    --------
    >>> df = dd.read_delta_table('s3://bucket/my-delta-table')  # doctest: +SKIP

    """
    if catalog is not None:
        if (database_name is None) or (table_name is None):
            raise ValueError(
                "Since Catalog was provided, please provide Database and table name"
            )
        else:
            resultdf = _read_from_catalog(
                database_name=database_name, table_name=table_name, **kwargs
            )
    else:
        if path is None:
            raise ValueError("Please Provide Delta Table path")
        dtw = DeltaTableWrapper(
            path=path,
            version=version,
            columns=columns,
            storage_options=storage_options,
            datetime=datetime,
        )
        resultdf = dtw.read_delta_table(columns=columns, **kwargs)
    return resultdf


def read_delta_history(
    path: str, limit: Optional[int] = None, storage_options: Dict[str, str] = None
) -> dd.core.DataFrame:
    """
    Run the history command on the DeltaTable.
    The operations are returned in reverse chronological order.

    Parallely reads delta log json files using dask delayed and gathers the
    list of commit_info (history)

    Parameters
    ----------
    path: str
        path of Delta table directory
    limit: int, default None
        the commit info limit to return, defaults to return all history

    Returns
    -------
        list of the commit infos registered in the transaction log
    """

    dtw = DeltaTableWrapper(
        path=path, version=None, columns=None, storage_options=storage_options
    )
    return dtw.history(limit=limit)


def vacuum(
    path: str,
    retention_hours: int = 168,
    dry_run: bool = True,
    storage_options: Dict[str, str] = None,
) -> None:
    """
    Run the Vacuum command on the Delta Table: list and delete
    files no longer referenced by the Delta table and are
    older than the retention threshold.

    retention_hours: int, default 168
    the retention threshold in hours, if none then the value
    from `configuration.deletedFileRetentionDuration` is used
    or default of 1 week otherwise.
    dry_run: bool, default True
        when activated, list only the files, delete otherwise

    Returns
    -------
    None or List of tombstones
    i.e the list of files no longer referenced by the Delta Table
    and are older than the retention threshold.
    """

    dtw = DeltaTableWrapper(
        path=path, version=None, columns=None, storage_options=storage_options
    )
    return dtw.vacuum(retention_hours=retention_hours, dry_run=dry_run)


class ToDeltaTableFunctionWrapper:
    """
    DeltaTable Function-Wrapper Class

    Writes a DataFrame partition into a DeltaTable.
    When called, the function also requires the current block index
    (via ``blockwise.BlockIndex``).
    """

    def __init__(
        self,
        table_or_uri,
        fs,
        schema,
        partition_by,
        mode,
        storage_options,
        file_options,
    ):
        self.table_or_uri=table_or_uri
        self.fs=None
        self.schema=schema
        self.partition_by=partition_by
        self.mode=mode
        self.storage_options=storage_options
        self.file_options=file_options

    def __dask_tokenize__(self):
        return (
            self.table_or_uri,
            self.fs,
            self.schema,
            self.partition_by,
            self.mode,
            self.storage_options
        )

    def __call__(self, df, block_index: tuple[int]):
        # Get partition index from block index tuple
        # part_i = block_index[0]
        # filename = (
        #     f"part.{part_i + self}.parquet"
        #     if self.name_function is None
        # )
        print("call...")
        return write_deltalake(
            data=df,
            table_or_uri=self.table_or_uri,
            schema=self.schema,
            partition_by=self.partition_by,
            filesystem=self.fs,
            mode=self.mode,
            storage_options=self.storage_options,
            file_options=self.file_options,
        )


def to_delta_table(
    df:  dd.DataFrame,
    table_or_uri: Union[str, DeltaTable],
    schema: Optional[pa.Schema] = None,
    partition_by: Optional[List[str]] = None,
    fs: Optional[pa_fs.FileSystem] = None,
    mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    storage_options: Optional[Dict[str, str]] = None,
    compute=True,
    compute_kwargs={},
    file_options=None,
) -> None:

    """
    Store Dask.dataframe to Parquet files
    
    Notes
    -----
    Each partition will be written to a separate file.
    Parameters
    ----------
    df : dask.dataframe.DataFrame
    table_or_uri : string or pathlib.Path
        Destination directory for data.  Prepend with protocol like ``s3://``
        or ``hdfs://`` for remote data.
    schema : pyarrow.Schema, dict, "infer", or None, default "infer"
        Global schema to use for the output dataset. Defaults to "infer", which
        will infer the schema from the dask dataframe metadata. This is usually
        sufficient for common schemas, but notably will fail for ``object``
        dtype columns that contain things other than strings. These columns
        will require an explicit schema be specified. The schema for a subset
        of columns can be overridden by passing in a dict of column names to
        pyarrow types (for example ``schema={"field": pa.string()}``); columns
        not present in this dict will still be automatically inferred.
        Alternatively, a full ``pyarrow.Schema`` may be passed, in which case
        no schema inference will be done. Passing in ``schema=None`` will
        disable the use of a global file schema - each written file may use a
        different schema dependent on the dtypes of the corresponding
        partition. Note that this argument is ignored by the "fastparquet"
        engine.
    partition_by : list, default None
        Construct directory-based partitioning by splitting on these fields'
        values. Each dask partition will result in one or more datafiles,
        there will be no global groupby.
    fs: 
    storage_options : dict, default None
        Key/value pairs to be passed on to the file-system backend, if any.
    **kwargs :
        Extra options to be passed on to the specific backend.
    """

    partition_by = partition_by or []

    if isinstance(partition_by, str):
        partition_by = [partition_by]
    
    # if isinstance(df, dd.DataFrame) and schema is not None:
    #     data = 


    if set(partition_by) - set(df.columns):
        raise ValueError(
            "Partitioning on non-existent column. "
            "partition_on=%s ."
            "columns=%s" % (str(partition_by), str(list(df.columns)))
        )
    
    if hasattr(table_or_uri, "name"):
        table_or_uri = stringify_path(table_or_uri)
    fs, _, _ = get_fs_token_paths(table_or_uri, mode="wb", storage_options=storage_options)
    # Trim any protocol information from the path before forwarding
    path = fs._strip_protocol(table_or_uri)

    annotations = dask.config.get("annotations", {})
    if "retries" not in annotations and not _is_local_fs(fs):
        ctx = dask.annotate(retries=5)
    else:
        ctx = contextlib.nullcontext()
    
    # Create Blockwise layer for delta_lake write
    with ctx:
        write_data = df.map_partitions(
            ToDeltaTableFunctionWrapper(
                table_or_uri=path,
                fs=fs,
                schema=schema,
                partition_by=partition_by,
                mode=mode,
                storage_options=storage_options,
                file_options=file_options,
            ),
        BlockIndex((df.npartitions,)),
        # Pass in the original metadata to avoid
        # metadata emulation in `map_partitions`.
        # This is necessary, because we are not
        # expecting a dataframe-like output.
        meta=df._meta,
        enforce_metadata=False,
        transform_divisions=False,
        align_dataframes=False,
    )

    final_name = f"store-{write_data._name}-{partition_by}-{mode}-{storage_options}"
    dsk = {(final_name,0): (lambda x: None, write_data.__dask_keys__())}

    # Convert write_data + dsk to computable collection
    graph = HighLevelGraph.from_collections(final_name, dsk, dependencies=(write_data,))
    out = Scalar(graph, final_name, "")

    if compute:
        out = out.compute(**compute_kwargs)

    fs.invalidate_cache(table_or_uri)
    return out
