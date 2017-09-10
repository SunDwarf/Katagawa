"""
py.test configuration
"""
import asyncio
import os

import pytest

from asyncqlio import DatabaseInterface


@pytest.fixture(scope="module")
async def db() -> DatabaseInterface:
    iface = DatabaseInterface()
    await iface.connect(dsn=os.environ["ASQL_DSN"])
    yield iface
    await iface.close()


# override for a module scope
@pytest.fixture(scope="module")
def event_loop():
    return asyncio.get_event_loop()
