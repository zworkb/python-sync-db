import os

import dbsync
import pytest
import multiprocessing as mp
from dbsync.server.wsserver import SyncServer
from sqlalchemy import create_engine
from .models_websockets import Base, PORT, SERVER_URL, server_db


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
    pool = mp.Pool()
    task = pool.apply_async(start_ws_server)
    # task.wait()
    print("taskready:", task.get())
    yield task

