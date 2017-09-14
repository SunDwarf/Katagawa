import logging
import io

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
        return "<Index table={} column={} name={}>".format(self.table, self.column, self.name)

    def __hash__(self):
        return super().__hash__()

    def __set_name__(self, owner, name):
        """
        Called to update the table and name of this Index.

        :param owner: The :class:`.Table` this Column is on.
        :param name: The str name of this table.
        """
        logger.debug("Index created with name {} on {}".format(name, owner))
        self.name = name
        self.table = owner

    @classmethod
    def with_name(cls, name: str, *args, **kwargs):
        idx = cls(*args, **kwargs)
        idx.name = name
        return idx

    def get_ddl_sql(self) -> str:
        """
        Gets the DDL SQL for this index.
        """
        base = io.StringIO()
        col_names = ", ".join(column.name for column in self.columns)
        base.write("CREATE ")
        if self.unique:
            base.write("UNIQUE ")
        base.write("INDEX ")
        base.write(self.name)
        base.write(" ON ")
        base.write(self.table.__tablename__)
        base.write(" (")
        base.write(col_names)
        base.write(")")

        return base.getvalue()
