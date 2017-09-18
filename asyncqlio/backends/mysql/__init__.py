from pkgutil import extend_path
import itertools
import operator

from asyncqlio.exc import DatabaseException
from asyncqlio.sentinels import NO_DEFAULT
from asyncqlio.backends.base import BaseDialect
from asyncqlio.orm.schema import column as md_column
from asyncqlio.orm.schema import index as md_index
from asyncqlio.orm.schema import types as md_types

__path__ = extend_path(__path__, __name__)

DEFAULT_CONNECTOR = "aiomysql"


class MysqlDialect(BaseDialect):
    """
    The dialect for MySQL.
    """

    @property
    def has_checkpoints(self):
        return True

    @property
    def has_serial(self):
        return True

    @property
    def lastval_method(self):
        return "LAST_INSERT_ID()"

    @property
    def has_returns(self):
        return False

    @property
    def has_default(self):
        return True

    @property
    def has_ilike(self):
        # sigh
        return False

    @property
    def has_truncate(self):
        return True

    def get_primary_key_index_name(self, table):
        return "PRIMARY"

    def get_unique_column_index_name(self, table_name, column_name):
        return column_name

    def get_column_sql(self, table_name=None, *, emitter):
        sql = ("SELECT * FROM information_schema.columns WHERE "
               "table_schema IN (SELECT database() FROM dual)")
        if table_name:
            sql += "AND table_name={}".format(emitter("table_name"))
        return sql

    def get_index_sql(self, table_name=None, *, emitter):
        sql = ("SELECT * FROM information_schema.statistics WHERE "
               "table_schema IN (SELECT database() FROM dual)")
        if table_name:
            sql += "AND table_name={}".format(emitter("table_name"))
        return sql

    def transform_rows_to_columns(self, *rows, table_name=None):
        for row in rows:
            table_name = row['TABLE_NAME']
            column_name = row['COLUMN_NAME']
            key_type = row['COLUMN_KEY']
            primary_key = key_type == "PRI"
            nullable = row["IS_NULLABLE"]
            default = row["COLUMN_DEFAULT"] or NO_DEFAULT
            mysql_type = row["DATA_TYPE"]

            if mysql_type.startswith("int"):
                real_type = md_types.Integer
            elif mysql_type == "text":
                real_type = md_types.Text
            elif mysql_type == "varchar":
                real_type = md_types.String
            elif mysql_type == "smallint":
                real_type = md_types.SmallInt
            elif mysql_type == "bigint":
                real_type = md_types.BigInt
            elif mysql_type == "tinyint":
                real_type = md_types.Boolean
            elif mysql_type == "float":
                real_type = md_types.Real
            elif mysql_type == "timestamp":
                real_type = md_types.Timestamp
            else:
                raise DatabaseException("Cannot parse type {}".format(mysql_type))

            yield md_column.Column.with_name(
                name=column_name,
                type_=real_type(),
                table_name=table_name,
                nullable=nullable,
                default=default,
                primary_key=primary_key,
            )

    def transform_rows_to_indexes(self, *rows, table_name=None):
        for name, rows in itertools.groupby(rows, operator.itemgetter('INDEX_NAME')):
            columns = []
            for row in rows:
                columns.append(row["COLUMN_NAME"])
            unique = not row["NON_UNIQUE"]
            table = row["TABLE_NAME"]
            index = md_index.Index.with_name(name, *columns, table_name=table, unique=unique)
            index.table_name = table
            index.table = None
            yield index
