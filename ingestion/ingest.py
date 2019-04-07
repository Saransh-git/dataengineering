from enum import Enum, unique
from functools import partial

import pandas as pd
import numpy as np
from pandas import DataFrame
from sqlalchemy import create_engine, MetaData, Table, UniqueConstraint, Column, Integer, Unicode, String, event
from sqlalchemy.sql.dml import Insert
from sqlalchemy.engine import reflection
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import List, Optional, Union, Dict, Tuple
from sqlalchemy.types import BigInteger, ARRAY
from sqlalchemy import cast, type_coerce
from ingestion.exceptions import ConnectionNotConfigured, SchemaMisMatch


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
            non_nullable_fields: List[str] = None, **kwargs
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

        self.table = Table(
            'testtable', MetaData(schema=schema),
            Column('name', String(50), nullable=False), autoload=True, autoload_with=self.engine
        )
        self.strict = strict
        self.unique_fields = unique_fields
        self.non_nullable_fields = non_nullable_fields
        event.listen(self.engine, "before_execute", partial(self.before_execute, keep_unique=keep_unique),
                     retval=True, named=True)
        event.listen(self.engine, "before_execute", self.remove_nas_before_execute, retval=True, named=True)
        self.conn = self.engine.connect()
        mapped_columns = fields if isinstance(fields, List) else (isinstance(fields, Dict) or []) and fields.values()
        self.match_columns = []

        if mapped_columns:
            for column in mapped_columns:
                if column not in self.table.c:
                    self.match_columns.append(column)

            if self.strict and len(self.match_columns) != len(fields):  # any provided field not in table schema
                raise SchemaMisMatch

            if not len(self.match_columns):
                raise SchemaMisMatch
        else:
            self.match_columns = [c.name for c in self.table.c]

    __init__.__doc__ = ":param conn_url: " + \
                       f"RFC 1738 compatible engine string or pass the connection details as " \
                       f"keyworded arguments:\n" \
                       f"{DBConnectionFactory.from_factory.__doc__}\n" \
                       + __init__.__doc__

    def before_execute(self, conn, clauseelement, multiparams, params, **kwargs):
        if not isinstance(clauseelement, Insert) or not multiparams or not self.unique_fields:
            return clauseelement, multiparams, params

        keep_unique = kwargs['keep_unique']
        if self.unique_fields:
            df: DataFrame = pd.DataFrame.from_records(multiparams)
            for idx, subset in enumerate(self.unique_fields):
                if isinstance(keep_unique, list):
                    df.drop_duplicates(subset, keep_unique[idx], inplace=True)
                else:
                    df.drop_duplicates(subset, keep_unique, inplace=True)

        vals = []
        for row in df.itertuples():
            row_val = {}
            for key in multiparams[row.Index]:
                row_val[key] = getattr(row, key)  # Preserving the initial imbalance in case if passed in multiple
                # parameters, so as to invoke StatementError.
            vals.append(row_val)
        return clauseelement, tuple(vals), params

    def remove_nas_before_execute(self, conn, clauseelement, multiparams, params, **kwargs):
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

    def ingest_json_or_dict(self, **kwargs):
        """
        Ingests from json
        :return:
        """
        pd.read_json()

    def ingest_csv(self):
        pd.read_csv

    def ingest_pd_dataframe(self):
        pass

    def ingest_orc(self):
        pass

    def ingest_parquet(self):
        pass
