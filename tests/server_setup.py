import os
from datetime import time
from time import sleep as tsleep
from multiprocessing.pool import ApplyResult

import dbsync
import pytest
import multiprocessing as mp

import sqlalchemy
from dbsync import server
from dbsync.server.wsserver import SyncServer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models_websockets import Base, PORT, SERVER_URL, server_db, A, B


def register_server_tracking():
    server.track(A)
    server.track(B)


def start_ws_server(**kw):
    engine_server = create_engine(f"sqlite:///{server_db}")
    Base.metadata.create_all(engine_server)
    dbsync.set_engine(engine_server)
    dbsync.create_all()
    server = SyncServer(port=PORT, engine=engine_server, **kw)
    print("starting server...")
    # server.start(run_forever=False, start_new_loop=True)
    server.start_in_thread()
    server.started_thead_event.wait()
    print("server ready in thread")

    return "OK"


@pytest.fixture(scope="function")
def sync_server():
    try:
        os.remove(server_db)
    except FileNotFoundError:
        print(f"ignore non existing file {server_db}")

    with mp.Pool() as pool:
        task: ApplyResult = pool.apply_async(start_ws_server)
        task.wait()
        yield task



@pytest.fixture(scope="function")
def server_session() -> sqlalchemy.orm.session.Session:
    """
    provides a session object to the server database for sync checking
    """
    engine_server = create_engine(f"sqlite:///{server_db}")
    Session = sessionmaker(engine_server)
    res = Session()

    return res


