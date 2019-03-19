from enum import Enum, unique
from functools import wraps

from sqlalchemy import create_engine
from typing import List, Optional

from ingestion.exceptions import SchemaNotConfigured


class DBConnectionFactory:
    """
    generates an RFC 1738 compatible connection url for relational DBs
    RFC 1738: https://www.ietf.org/rfc/rfc1738.txt
    """
    @unique
    class Schema(Enum):
        POSTGRESQL = 'postgresql'
        MYSQL = 'mysql'
        SQLITE = 'sqlite'
        ORACLE = 'oracle'
        MSSQL = 'mssql+pyodbc'  # Microsoft SQL server

    @classmethod
    def from_factory(
            cls, conn_type: str, host: str, port: str = '', user: str = '', password: str = '',
            db_name: str='', **kwargs
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
            scheme = getattr(cls.Schema, conn_type).value
        except AttributeError:
            raise SchemaNotConfigured
        password = f":{password}" if password else ""
        user_pass = f"{user}{password}" if user else ""
        port = f":{port}" if port else ""
        host_str = f"@{host}" if user_pass else f"{host}"
        return f"{scheme}://{host_str}{port}/{db_name}"


class DataToRelations:

    def __init__(
            self, conn_url: str = None, strict: bool=False, auto_add_ts: bool=False, auto_add_ts_col: str=None,
            batch_size: int = None, fields: List = None, **kwargs
    ):
        """
        :param strict:
        :param auto_add_ts:
        :param auto_add_ts_col:
        :param batch_size:
        :param fields:
        :param kwargs:

        """
        if not conn_url:
            self.conn_url = DBConnectionFactory.from_factory(**kwargs)

    __init__.__doc__ = ":param conn_url: " + \
                       f"RFC 1738 compatible engine string or pass the connection details as " \
                       f"keyworded arguments:\n" \
                       f"conn_type: Refer {DBConnectionFactory.from_factory.__doc__} + " \
                       """
                       host:
                       port: (Optional when specifying localhost as host)
                       user: (Optional)
                       password: (Optional)
                       """ + __init__.__doc__

    def ingest_json(self):
        """
        Ingests from json
        :return:
        """
        pass

    def ingest_csv(self):
        pass

    def ingest_pd_dataframe(self):
        pass

    def ingest_orc(self):
        pass

    def ingest_parquet(self):
        pass
