from __future__ import annotations

import contextlib
import json
import os
import uuid
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Any, Dict, List, Literal, Mapping, Optional, Union
from urllib.parse import urlparse

import dask
import dask.dataframe as dd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# from boto3 import Session
from dask.base import tokenize
from dask.blockwise import BlockIndex
from dask.dataframe.core import Scalar
from dask.dataframe.io import from_delayed
from dask.dataframe.io.utils import _is_local_fs
from dask.delayed import delayed
from dask.highlevelgraph import HighLevelGraph
from deltalake import DataCatalog, DeltaTable, write_deltalake
from deltalake._internal import write_new_deltalake
from deltalake.table import (
    MAX_SUPPORTED_WRITER_VERSION,
    DeltaTable,
    DeltaTableProtocolError,
)
from deltalake.writer import (
    AddAction,
    DeltaJSONEncoder,
    get_file_stats_from_metadata,
    # get_partitions_from_path,
    try_get_deltatable,
)
from deltalake.schema import delta_arrow_schema_from_pandas
from fsspec.core import get_fs_token_paths
from fsspec.utils import stringify_path

PYARROW_MAJOR_VERSION = int(pa.__version__.split(".", maxsplit=1)[0])


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
        table_uri,
        fs,
        schema,
        partitioning,
        mode,
        storage_options,
        file_options,
        current_version,
    ):
        self.table_uri = table_uri
        self.fs = None
        self.schema = schema
        self.partitioning = partitioning
        self.mode = mode
        self.storage_options = storage_options
        self.file_options = file_options
        self.current_version = current_version

    def __dask_tokenize__(self):
        return (
            self.table_uri,
            self.fs,
            self.schema,
            self.partitioning,
            self.mode,
            self.storage_options,
        )

    def __call__(self, df, block_index: tuple[int]):

        data = pa.Table.from_pandas(df, schema=self.schema)
        add_actions: List[AddAction] = []

        def visitor(written_file: Any) -> None:
            path, partition_values = get_partitions_from_path(written_file.path)
            stats = get_file_stats_from_metadata(written_file.metadata)

            # PyArrow added support for written_file.size in 9.0.0
            if PYARROW_MAJOR_VERSION >= 9:
                size = written_file.size
            else:
                size = self.fs.get_file_info([path])[0].size  # type: ignore

            add_actions.append(
                AddAction(
                    path,
                    size,
                    partition_values,
                    int(datetime.now().timestamp()),
                    True,
                    json.dumps(stats, cls=DeltaJSONEncoder),
                )
            )

        return (
            ds.write_dataset(
                data=data,
                base_dir=self.table_uri,
                basename_template=f"{self.current_version + 1}-{uuid.uuid4()}-{{i}}.parquet",
                format="parquet",
                partitioning=self.partitioning,
                schema=self.schema,
                file_visitor=visitor,
                existing_data_behavior="overwrite_or_ignore",
                filesystem=self.fs,
            ),
            add_actions,
        )


def get_partitions_from_path(path: str) -> Tuple[str, Dict[str, Optional[str]]]:
    if path[0] == "/":
        path = path[1:]
    parts = path.split("/")
    parts.pop()  # remove filename
    out: Dict[str, Optional[str]] = {}

    for part in parts:
        if part == "" or "=" not in part:
            continue
        key, value = part.split("=", maxsplit=1)
        if value == "__HIVE_DEFAULT_PARTITION__":
            out[key] = None
        else:
            out[key] = value
    return path, out


@delayed
def _write_dataset(
    df,
    table_uri,
    fs,
    schema,
    partitioning,
    mode,
    storage_options,
    file_options,
    current_version,
):
    """ """
    data = pa.Table.from_pandas(df, schema=schema)
    add_actions: List[AddAction] = []

    def visitor(written_file: Any) -> None:
        path, partition_values = get_partitions_from_path(written_file.path)
        stats = get_file_stats_from_metadata(written_file.metadata)

        # PyArrow added support for written_file.size in 9.0.0
        if PYARROW_MAJOR_VERSION >= 9:
            size = written_file.size
        else:
            size = fs.get_file_info([path])[0].size  # type: ignore

        add_actions.append(
            AddAction(
                path,
                size,
                partition_values,
                int(datetime.now().timestamp()),
                True,
                json.dumps(stats, cls=DeltaJSONEncoder),
            )
        )

    ds.write_dataset(
        data=data,
        base_dir=table_uri,
        basename_template=f"{current_version + 1}-{uuid.uuid4()}-{{i}}.parquet",
        format="parquet",
        partitioning=partitioning,
        schema=schema,
        file_visitor=visitor,
        existing_data_behavior="overwrite_or_ignore",
        filesystem=fs,
    )
    return add_actions


def to_delta_table(
    df: dd.DataFrame,
    table_or_uri: Union[str, DeltaTable],
    schema: Optional[pa.Schema] = None,
    partition_by: Optional[List[str]] = None,
    fs: Optional[pa_fs.FileSystem] = None,
    mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    storage_options: Optional[Dict[str, str]] = None,
    compute=True,
    compute_kwargs={},
    file_options=None,
    overwrite_schema: bool = False,
    name: str = "",
    description: str = "",
    configuration: dict = {},
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

    # We use Arrow to write the dataset.
    # See https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.from_pandas
    # for how pyarrow handles the index.
    # if df._meta.index.name is not None:
    # df = df.reset_index()

    if isinstance(partition_by, str):
        partition_by = [partition_by]

    meta, schema = delta_arrow_schema_from_pandas(df.head())

    if fs is not None:
        raise NotImplementedError

    # Get the fs and trim any protocol information from the path before forwarding
    fs, _, paths = get_fs_token_paths(
        table_or_uri, mode="wb", storage_options=storage_options
    )
    table_uri = fs._strip_protocol(table_or_uri)

    # if isinstance(table_or_uri, str):
    #     if "://" in table_or_uri:
    #         table_uri = table_or_uri
    #     else:
    #         # Non-existant local paths are only accepted as fully-qualified URIs
    #         table_uri = "file://" + str(Path(table_or_uri).absolute())
    #         # table_uri = str(Path(table_or_uri).absolute())
    table = try_get_deltatable(table_or_uri, storage_options)
    # else:
    #     table = table_or_uri
    #     table_uri = table._table.table_uri()

    if table:  # already exists
        if schema != table.schema().to_pyarrow() and not (
            mode == "overwrite" and overwrite_schema
        ):
            raise ValueError(
                "Schema of data does not match table schema\n"
                f"Table schema:\n{schema}\nData Schema:\n{table.schema().to_pyarrow()}"
            )

        if mode == "error":
            raise AssertionError("DeltaTable already exists.")
        elif mode == "ignore":
            return

        current_version = table.version()

        if partition_by:
            assert partition_by == table.metadata().partition_columns

        if table.protocol().min_writer_version > MAX_SUPPORTED_WRITER_VERSION:
            raise DeltaTableProtocolError(
                "This table's min_writer_version is "
                f"{table.protocol().min_writer_version}, "
                "but this method only supports version 2."
            )
    else:  # creating a new table
        current_version = -1

    if partition_by:
        partition_schema = pa.schema([schema.field(name) for name in partition_by])
        partitioning = ds.partitioning(partition_schema, flavor="hive")
    else:
        partitioning = None

    annotations = dask.config.get("annotations", {})
    if "retries" not in annotations and not _is_local_fs(fs):
        ctx = dask.annotate(retries=5)
    else:
        ctx = contextlib.nullcontext()

    with ctx:
        dfs = df.to_delayed()
        results = [
            _write_dataset(
                df,
                table_uri,
                fs,
                schema,
                partitioning,
                mode,
                storage_options,
                file_options,
                current_version,
            )
            for df in dfs
        ]

    results = dask.compute(*results, **compute_kwargs)
    add_actions = list(chain.from_iterable(results))
    # for result in results:
    #     for r in result:
    #         for i in j:
    #             add_actions.append(i)

    if table is None:
        write_new_deltalake(
            table_uri,
            schema,
            add_actions,
            mode,
            partition_by or [],
            name,
            description,
            configuration,
            storage_options,
        )
    else:
        table._table.create_write_transaction(
            add_actions,
            mode,
            partition_by or [],
            schema,
        )

    fs.invalidate_cache(table_or_uri)
