import asyncio
import os
from datetime import time
from time import sleep as tsleep
from multiprocessing.pool import ApplyResult

from sqlalchemy.engine import Engine

import dbsync
import pytest
import multiprocessing as mp

import sqlalchemy
from dbsync import server
from dbsync.server.wsserver import SyncServer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models_websockets import Base, PORT, SERVER_URL, server_db, A, B, server_uri_postgres


def register_server_tracking():
    server.track(A)
    server.track(B)


def start_ws_server(**kw):
    """
    postgres must be installed with permissions to create a db
    """
    from dbsync import core
    core.mode = "server"
    # engine_server = create_engine(f"sqlite:///{server_db}")
    try:
        os.system("dropdb synctest")
        os.system("createdb synctest")
        engine_server: Engine = create_engine(server_uri_postgres,
                                              isolation_level='SERIALIZABLE'
                                              )
    except Exception as ex:
        print("**** PLEASE INSTALL POSTGRES ***")
        raise
    Base.metadata.create_all(engine_server)
    dbsync.set_engine(engine_server)
    dbsync.create_all()

    try:
        asyncio.get_event_loop()
    except RuntimeError as e:
        asyncio.set_event_loop(asyncio.new_event_loop())

    server = SyncServer(port=PORT, engine=engine_server, **kw)
    print("starting server...")
    # server.start(run_forever=False, start_new_loop=True)
    server.start_in_thread()
    server.started_thead_event.wait()
    print("server ready in thread")

    return PORT


@pytest.fixture(scope="function")
def sync_server():
    try:
        os.remove(server_db)
    except FileNotFoundError:
        print(f"ignore non existing file {server_db}")

    with mp.Pool() as pool:
        res: ApplyResult = pool.apply_async(start_ws_server)
        res.wait()
        yield res.get()  # that runs well, but makes prolems with pydevd
    print("tear down")
    return res.get()


@pytest.fixture(scope="function")
def server_session() -> sqlalchemy.orm.session.Session:
    """
    provides a session object to the server database for sync checking
    """
    # engine_server = create_engine(f"sqlite:///{server_db}")
    engine_server = server_uri_postgres
    Session = sessionmaker(engine_server)
    res = Session()

    return res


