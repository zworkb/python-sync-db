import os

import dbsync
import pytest
from dbsync.client.wsclient import SyncClient
from sqlalchemy import create_engine

from .models_websockets import Base, PORT, SERVER_URL, server_db, client_db, A, B


@pytest.fixture(scope="function")
def sync_client(sync_server):
    try:
        os.remove(client_db)
    except FileNotFoundError:
        print(f"ignore non existing file {client_db}")

    engine_client = create_engine(f"sqlite:///{client_db}")
    Base.metadata.create_all(engine_client)
    dbsync.set_engine(engine_client)
    dbsync.create_all()

    client = SyncClient(port=PORT, path="sync", engine=engine_client)
    # client.connect()
    return client
