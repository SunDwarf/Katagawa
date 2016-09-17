"""
A transaction is a way to isolate a set of queries in one transaction. It can be rolled back or commited at the end
of the transaction.
"""
import abc

from katagawa.engine import BaseEngine


class Transaction(abc.ABC):
    """
    The base class for a transaction.

    This requires implementation of some methods which are DBAPI specific, but also provides some common methods
    across different ones that can be used.

    For example, ``__aenter__`` or ``acquire`` should implicitly emit a BEGIN TRANSACTION operator.
    This is an async context manager.

    If any errors happen in the transaction when used as a context manager, it will automatically ROLLBACK.
    """
    def __init__(self, engine: BaseEngine, *, read_only: bool=False):
        """
        Creates a new Transaction instance.

        :param engine: The engine to bind to.
            This is responsible for handling the SQL itself.

        :param read_only: Is this transaction a read-only transaction?
            This is useful for SELECT transactions.
        """
        self.engine = engine
        self.read_only = read_only

        # Define the started status.
        self.started = False

        # Define the completed status.
        # This is used to signal the transaction should be discarded, and should not perform any more actions.
        self.completed = False

    @abc.abstractmethod
    async def _acquire(self):
        """
        The actual acquisition logic.
        """

    async def acquire(self):
        """
        Acquires the transaction.

        This cannot be done on a completed transaction.
        :return: Ourselves.
        """
        if self.completed:
            raise RuntimeError("Cannot acquire an already completed transaction")

        self.started = True

        return await self._acquire()

    @abc.abstractmethod
    async def _release(self):
        """
        The actual release logic.
        """

    async def release(self):
        """
        Releases the transaction.

        This cannot be done on a non-started transaction.
        :return: Ourselves.
        """
        if not self.started:
            raise RuntimeError("Cannot release a non-started transaction")

        if self.completed:
            raise RuntimeError("Cannot release an already completed transaction")

        self.completed = True

        return await self._release()

    @abc.abstractmethod
    async def execute(self, sql: str, params: dict):
        """
        Executes a SQL command inside a transaction.

        :param sql: The SQL query to use.
        :param params: Parameters to pass into the query.

        :return: The results of the query.
        """
