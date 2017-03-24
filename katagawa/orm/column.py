from katagawa.sql.dialects.common import Eq, Gt, Lt, Ne, IsNotNull, IsNull, Column as sql_Column, \
    Operator
from katagawa.sql.types import BaseType
from katagawa.orm import table

d = {
    0: Eq,
    1: Gt,
    2: Lt,
    3: Ne,
    4: IsNotNull,
    5: IsNull
}


class _Operator(object):
    """
    Represents something returned from an equality comparison on some columns.
    """
    def __init__(self, operator: int, column: 'Column', other: str):
        """
        :param operator: The ID of the operator to use for this equality comparison. 
        :param column: The name of the column being compared.
        :param other: The value comparing against. Could be an escape.
        """
        self.operator = operator
        self.column = column
        self.other = other

        # prevent slow global lookup
        self._d = d

    def get_token(self) -> Operator:
        """
        Gets the :class:`~.tokens.Operator` that represents this operator. 
        """
        op = self._d[self.operator]
        ident = '"{}"."{}"'.format(self.column.table.name, self.column.name)
        col = sql_Column(identifier=ident)

        # create the operator
        return op(col, self.other)

    def __call__(self, *args, **kwargs):
        # Return the appropriate SQL operator token.
        return self.get_token()


class Column(object):
    """
    A column is a class that represents a column in a table in a database.

    A table is comprised of multiple columns.
    """

    def __init__(self,
                 name: str,
                 type_: BaseType,
                 *,
                 primary_key: bool=False,
                 autoincrement: bool=False):
        """
        :param name:
            The name of this column. Is used to create the column in the database.

        :param type_:
            The type of items this column accepts.
            This should be an instance of a class that subclasses BaseType.

        :param primary_key:
            Is this column a primary key?

        :param autoincrement:
            Should this column autoincrement?
        """
        self.name = name

        self.type_ = type_
        if not isinstance(self.type_, BaseType):
            # Try and instantiate it.
            if not issubclass(self.type_, BaseType):
                raise TypeError("type_ should be an instance or subclass of BaseType")
            else:
                self.type_ = type_()

        self.primary_key = primary_key
        self.autoincrement = autoincrement

        # The table this is registered to.
        self.table = None  # type: table.Table

    def register_table(self, tbl: table.Table):
        self.table = tbl

    # properties
    @property
    def sql_field(self) -> sql_Column:
        """
        :return: This column as a :class:`~.Field`. 
        """
        return sql_Column(identifier=self.name)

    # operator methods
    def __eq__(self, other):
        return _Operator(0, self, other)
