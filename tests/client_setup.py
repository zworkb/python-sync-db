import os

import dbsync
import pytest
import sqlalchemy
from dbsync import client
from dbsync.client.wsclient import SyncClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models_websockets import Base, PORT, SERVER_URL, server_db, client_db, A, B


def register_client_tracking():
    client.track(A)
    client.track(B)


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

    register_client_tracking()

    clientws = SyncClient(port=PORT, path="sync", engine=engine_client)
    # client.connect()
    return clientws


@pytest.fixture(scope="function")
def client_session() -> sqlalchemy.orm.session.Session:
    """
    provides a session object to the server database for sync checking
    """
    engine_client = create_engine(f"sqlite:///{client_db}")
    Session = sessionmaker(engine_client)
    res = Session()

    return res