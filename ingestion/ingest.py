from enum import Enum, unique
from functools import partial
from typing import List, Optional, Union, Dict, Tuple, Any

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, MetaData, Table, Column, event
from sqlalchemy.engine import Connection
from sqlalchemy.sql.dml import Insert

from ingestion.exceptions import ConnectionNotConfigured, SchemaMisMatch, StrictIngestionError
from ingestion.helpers import BlankParamValue


class DBConnectionFactory:
    """
    generates an RFC 1738 compatible connection url for relational DBs
    RFC 1738: https://www.ietf.org/rfc/rfc1738.txt
    """

    @unique
    class Scheme(Enum):
        POSTGRESQL = 'postgresql'
        MYSQL = 'mysql'
        SQLITE = 'sqlite'
        ORACLE = 'oracle'
        MSSQL = 'mssql+pyodbc'  # Microsoft SQL server
        HIVE = 'hive'
        PRESTO = 'presto'

    @classmethod
    def from_factory(
            cls, conn_type: str, host: str, port: str = '', user: str = '', password: str = '',
            db_name: str = '', **kwargs
    ) -> Optional[str]:
        """

        :param conn_type:
        :param host:
        :param port:
        :param user:
        :param password:
        :param db_name:
        :param kwargs:
        :return:
        """
        try:
            scheme = getattr(cls.Scheme, conn_type.upper()).value
        except AttributeError:
            raise ConnectionNotConfigured("Connection Scheme not provided!")
        if not host:
            raise ConnectionNotConfigured("Connection host not provided!")
        password = f":{password}" if password else ""
        user_pass = f"{user}{password}" if user else ""
        port = f":{port}" if port else ""
        host_str = f"{user_pass}@{host}" if user_pass else f"{host}"
        return f"{scheme}://{host_str}{port}/{db_name}"


class DataToRelations:

    def __init__(
            self, table_name: str, schema: str = None, conn_url: str = None, strict: bool = False,
            auto_add_ts: bool = False, auto_add_ts_col: str = None, batch_size: int = None,
            fields: Union[List, Dict[str, str]] = None, unique_fields: List[Tuple[str, ...]] = None,
            keep_unique: Union[str, List[str]] = 'last',
            non_nullable_fields: List[str] = None, default_vals: Dict[str, Any] = {}, **kwargs
    ) -> None:
        """
        :param table_name: Name of the table to ingest data to
        :param schema: Schema this table belongs to. Note:
        :param conn_url: Connection url, Refer: https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls,
        refer to the default db urls for the vendors, or specify as keyworded arguments scheme, user, password, host,
        port and database as required to generate Connection url.
        Also see: Documentation for DBConnectionFactory.from_factory
        :param strict: Whether to strictly comply to the table relational schema. Note: Useful for loudly failing in
        case of having extra fields/ missing fields in the data passed.
        :param auto_add_ts:
        :param auto_add_ts_col:
        :param batch_size:
        :param fields:
        :param unique_fields: list of fields/ list of tuples to be rendered unique together during ingestion in a batch.
        Note: If uniqueness overall is required, consider placing a unique index on the ingestion table/ adjust the
        batch size accordingly.
        :param keep_unique: option specifying the observation to be kept while resolving unique constraint. Can be an
        option amongst first/ last or a list of values specifying first/last differently for each subset of unique
        fields provided.
        """
        if not conn_url:
            conn_url = DBConnectionFactory.from_factory(**kwargs)

        self.engine = create_engine(conn_url, echo=True)

        self.table: Table = Table(table_name, MetaData(schema=schema), autoload=True, autoload_with=self.engine,
                                  extend_existing=True)

        self.strict = strict
        self.unique_fields = unique_fields
        self.non_nullable_fields = non_nullable_fields
        self.default_vals = default_vals
        self.fields = fields
        mapped_columns = fields if isinstance(fields, List) else (isinstance(fields, Dict) or []) and fields.keys()
        self.match_columns = []  # grab the concerned/ interested fields
        self.data = [{}]  # to avoid any statement error
        if mapped_columns:
            for column in mapped_columns:
                if column in self.table.c:
                    self.match_columns.append(column)

            if self.strict and len(self.match_columns) != len(fields):  # any provided field not in table schema
                raise SchemaMisMatch

            if not len(self.match_columns):
                raise SchemaMisMatch
        else:
            self.match_columns = [c.name for c in self.table.c]

        partial_table = partial(
            Table, self.table.name, self.table.metadata, autoload=True, autoload_with=self.engine,
            extend_existing=True)
        for col in self.match_columns:
            partial_table = partial(
                partial_table, Column(
                    col, self.table.c[col].type, default=self.set_default
                )
            )  # this would ensure a default value when data for these cols are not provided.

            self.table: Table = partial_table()

        event.listen(self.engine, "before_execute", partial(self.keep_unique_vals, keep_unique=keep_unique),
                     retval=True, named=True)
        event.listen(self.engine, "before_execute", self.remove_nas_before_execute, retval=True, named=True)
        event.listen(self.engine, "before_cursor_execute", self.keep_strict_cols_only, retval=True)
        self.conn: Connection = self.engine.connect()

    __init__.__doc__ = ":param conn_url: " + \
                       f"RFC 1738 compatible engine string or pass the connection details as " \
                       f"keyworded arguments:\n" \
                       f"{DBConnectionFactory.from_factory.__doc__}\n" \
                       + __init__.__doc__

    def set_default(self, context, **kwargs):
        """
        Overrides default only for the set which has not been assigned
        """
        col_type = context.current_column.type
        col_name = context.current_column.name
        current_val = context.get_current_parameters()[col_name]
        if isinstance(current_val, BlankParamValue):
            if self.strict:
                raise StrictIngestionError(col_name, context.get_current_parameters())
            try:
                return col_type.python_type(self.default_vals[col_name])
            except KeyError:
                return None  # no default assigned
        return current_val

    def keep_unique_vals(self, conn, clauseelement, multiparams, params, **kwargs):
        if not isinstance(clauseelement, Insert) or not multiparams or not self.unique_fields:
            return clauseelement, multiparams, params

        keep_unique = kwargs['keep_unique']
        if self.unique_fields:
            df: DataFrame = pd.DataFrame.from_records(multiparams[0])
            for idx, subset in enumerate(self.unique_fields):
                if isinstance(keep_unique, list):
                    df.drop_duplicates(subset, keep_unique[idx], inplace=True)
                else:
                    df.drop_duplicates(subset, keep_unique, inplace=True)

        vals = []
        for row in df.itertuples():
            row_val = {}
            for key in multiparams[0][row.Index]:
                row_val[key] = getattr(row, key)  # Preserving the initial imbalance in case if passed in multiple
                # parameters, so as to invoke StatementError.
            vals.append(row_val)
        return clauseelement, tuple(vals), params

    def remove_nas_before_execute(self, conn, clauseelement, multiparams, params, **kwargs):
        self.insert_clause = clauseelement
        if not self.non_nullable_fields or not isinstance(clauseelement, Insert) or not multiparams:
            return clauseelement, multiparams, params

        df: DataFrame = pd.DataFrame.from_records(multiparams)
        df.dropna(subset=self.non_nullable_fields, inplace=True)

        vals = []
        for row in df.itertuples():
            row_val = {}
            for key in multiparams[row.Index]:
                row_val[key] = getattr(row, key)  # Preserving the initial imbalance in case if passed in multiple
                # parameters, so as to invoke StatementError.
            vals.append(row_val)
        return clauseelement, tuple(vals), params

    def keep_strict_cols_only(self, conn, cursor, statement, parameters, context, executemany):
        if not context.isinsert:
            return statement, parameters

        statement = str(self.insert_clause.compile(dialect=self.engine.dialect, column_keys=self.match_columns))
        if executemany:
            parameters = parameters[1:]
        return statement, parameters

    def _append_dict_line(self, data_dict: Dict[str, Any]):
        item = {}
        for col in self.match_columns:
            item[col] = data_dict.get(self.fields[col], BlankParamValue())
        self.data.append(item)

    def ingest_json_or_dict(self, data: Union[str, List[Dict[str, Any]], Dict[str, Any]], **kwargs):
        """
        Ingests from json
        :return:
        """
        if isinstance(data, Dict):
            self._append_dict_line(data)

        if isinstance(data, str):
            data = pd.read_json(data, typ=None)

        elif hasattr(data, '__iter__'):
            for data_dict in data:
                self._append_dict_line(data_dict)

        self.conn.execute(self.table.insert(), self.data)

    def ingest_csv(self):
        pass

    def ingest_pd_dataframe(self):
        pass

    def ingest_orc(self):
        pass

    def ingest_parquet(self):
        pass
