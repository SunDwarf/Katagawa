import io
import logging
import typing

from asyncqlio.orm.schema import column as md_column

logger = logging.getLogger(__name__)


class Index(object):
    """
    Represents an index in a table in a database.

    .. code-block:: python3

        class MyTable(Table):
            id = Column(Integer, primary_key=True)
            name = Column(Text)
            name_index = Index(name)

    """
    def __init__(self, *columns: 'typing.Union[md_column.Column, str]',
                 unique: bool = False):
        self.columns = columns
        self.unique = unique

    def __repr__(self):
        return "<Index table={} columns={} name={}>".format(self.table_name, self.columns, self.name)

    def __hash__(self):
        return super().__hash__()

    def __set_name__(self, owner, name):
        """
        Called to update the table and name of this Index.

        :param owner: The :class:`.Table` this Column is on.
        :param name: The str name of this table.
        """
        self.table = owner
        self.table_name = owner.__tablename__
        if name.startswith(self.table_name):
            self.name = name
        else:
            self.name = "{}_{}".format(self.table_name, name)
        logger.debug("Index created with name {} on {}".format(name, owner))

    def get_column_names(self):
        for column in self.columns:
            if isinstance(column, str):
                yield column
            else:
                yield column.name

    @classmethod
    def with_name(cls, name: str, *args, table_name: str = None, **kwargs) -> 'Index':
        """
        Creates this column with a name and, optionally, table name alrady set.
        """
        idx = cls(*args, **kwargs)
        idx.name = name
        idx.table_name = table_name
        idx.table = None
        return idx

    def get_ddl_sql(self) -> str:
        """
        Gets the DDL SQL for this index.
        """
        base = io.StringIO()
        base.write("CREATE ")
        if self.unique:
            base.write("UNIQUE ")
        base.write("INDEX ")
        base.write(self.name)
        base.write(" ON ")
        base.write(self.table_name)
        base.write(" (")
        base.write(', '.join(self.get_column_names()))
        base.write(")")

        return base.getvalue()

    def _get_column_refs(self) -> 'typing.Generator[str, None, None]':
        for column in self.columns:
            if isinstance(column, str):
                yield '"{}"'.format(column)
            else:
                yield column.name

    def generate_schema(self, fp) -> str:
        """
        Generates the library schema for this index.
        """
        schema = fp or io.StringIO()

        schema.write(self.name)
        schema.write(" = ")
        schema.write(type(self).__name__)
        schema.write("(")
        schema.write(", ".join(self._get_column_refs()))
        if self.unique:
            schema.write(", unique=True")
        schema.write(")")

        return schema.getvalue() if fp is None else ""
