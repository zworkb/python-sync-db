import asyncio
import datetime
import logging
import json
import multiprocessing
import os
import time

import pytest
import sqlalchemy
import websockets
from dbsync import models, core, wscommon
from dbsync.client import PushRejected, PullSuggested
from dbsync.client.wsclient import SyncClient
from dbsync.messages.push import PushMessage
from dbsync.models import Node, Operation, Version
from dbsync.server.wsserver import SyncServer
from dbsync.socketclient import GenericWSClient
from dbsync.socketserver import Connection


from tests.models_websockets import SERVER_URL, addstuff, changestuff, A, B, datapath
from .models_websockets import PORT
from .server_setup import sync_server, server_session, server_db
from .client_setup import sync_client, client_session, sync_client_registered, client_db, create_sync_client_registered


@SyncServer.handler("/counter")
async def counter(conn: Connection):
    n = int(await conn.socket.recv())

    for i in range(n):
        await conn.socket.send(str(i))


@SyncServer.handler("/fuckup")
async def fuckup(conn: Connection):
    """
    provoke an exception
    """

    raise PushRejected("fucked up on purpose")
    # return 1/0
    # raise Exception("this was on purpose")


@pytest.mark.asyncio
async def test_fuckup(sync_server):
    async def action(client: SyncClient):
        print("ho!")

    client = GenericWSClient(port=sync_server, path="fuckup")

    with pytest.raises(PushRejected):
        res = await client.connect_async(action=action)


# @SyncServer.handler("/longrun")
# async def longrun(conn: Connection):
#     print("LONGRUN")
#     n = int(await conn.socket.recv())
#
#     for i in range(n):
#         print(f"sleeping for a second")
#         await asyncio.sleep(1)
#         # await conn.socket.ping()
#         # await conn.socket.send(str(i))
#         print(f"running for {i} seconds")
#
#     await conn.socket.send(f"waited for {n} ticks")
#
# @pytest.mark.asyncio
# async def test_longrun(sync_server):
#     async def action(client: GenericWSClient):
#         await client.websocket.send("60")
#
#         # wenn ich nicht eine antwort erwarte, bricht die verbindung vorzeitig ab
#         async for msg in client.websocket:
#             print(f"msg is:{msg}")
#
#     client = GenericWSClient(port=sync_server, path="longrun")
#     await client.connect_async(action=action)


@pytest.mark.asyncio
async def test_server_only(sync_server):
    """
    this one tests just the server, client side is done using websockets, but not SyncClient
    """
    async with websockets.connect(f"ws://localhost:{PORT}/counter") as ws:
        await ws.send("5")
        async for resp in ws:
            print("COUNT:", resp)


def test_server_start(sync_server):
    print("server is:", sync_server)

    async def action(client: SyncClient):
        await client.websocket.send("Hi")
        resp = await client.websocket.recv()
        print("answer recv:", resp)

    GenericWSClient(port=sync_server).connect(action=action, do_wait=False)
    # sync_server.wait()


@pytest.mark.asyncio
async def test_register(sync_server, sync_client, server_session: sqlalchemy.orm.session.Session, client_session):
    res = await sync_client.register()

    sync_nodes = sync_client.Session().query(Node).all()
    assert len(sync_nodes) == 1
    print(f"REGISTERED:{res}")

    server_nodes = server_session.query(Node).all()

    # after registration there should be one node in the server database
    assert len(server_nodes) == 1


@pytest.mark.asyncio
async def test_registered(sync_server, sync_client_registered,
                          server_session: sqlalchemy.orm.session.Session, client_session):

    sync_nodes = sync_client_registered.Session().query(Node).all()
    assert len(sync_nodes) == 1

    server_nodes = server_session.query(Node).all()

    # after registration there should be one node in the server database
    assert len(server_nodes) == 1


@pytest.mark.asyncio
async def test_tracking_add(sync_server, sync_client):
    sess = sync_client.Session()
    addstuff(sync_client.Session)

    ops = sess.query(Operation).all()

    assert len(ops) == 6


@pytest.mark.asyncio
async def test_tracking_add_change(sync_server, sync_client, server_session, client_session):
    addstuff(sync_client.Session)
    changestuff(sync_client.Session)

    ops = client_session.query(Operation).all()

    assert len(ops) == 9


# @pytest.mark.asyncio
@pytest.mark.parametrize("compress_info", [(False, 9), (True, 5)])
def test_push_message(sync_client, client_session, compress_info):
    do_compress, N = compress_info
    addstuff(sync_client.Session)
    changestuff(sync_client.Session)

    msg: PushMessage = sync_client.create_push_message(do_compress=do_compress)

    print(f"push_message:{msg}")

    for op in msg.operations:
        print(f"op: {op}")

    assert len(msg.operations) == N


@pytest.mark.asyncio
async def test_push(sync_server, sync_client_registered, server_session, client_session):
    addstuff(sync_client_registered.Session)
    # addstuff(core.Session)
    # sync_client.register()
    # sync_client.connect()
    nodes = client_session.query(Node).all()
    print(f"NODES:{nodes}")
    assert len(nodes) == 1

    server_nodes = server_session.query(Node).all()

    # after registration there should be one node in the server database
    assert len(server_nodes) == 1

    await sync_client_registered.synchronize()

    # check if stuff got trasmitted
    As = server_session.query(A).all()
    assert len(As) > 0

    Bs = server_session.query(B).all()
    assert len(Bs) > 0

    versions = client_session.query(Version).all()
    assert len(versions) == 1

    # and now change something
    changestuff(sync_client_registered.Session)


    await sync_client_registered.synchronize()

    b2 = server_session.query(B).filter(B.name == "second b updated").one()
    # check if data have been transferred
    fpath = datapath(b2.data, "server")
    with open(fpath) as fh:
        data = fh.read()
        assert data == "b2 changed"

    versions = client_session.query(Version).all()
    assert len(versions) == 2


def push_in_process(nr: int):
    """
    simply invokes synchronize() but
    have to start different clients in separate processes
    because dbsync lives with global variables and thus
    different clients would interfere
    """
    print(">>>>>>>>>>>>>>>>>test in process", nr)
    sync_client: SyncClient = create_sync_client_registered(nr)
    addstuff(sync_client.Session, nr)
    asyncio.run(sync_client.synchronize())
    print("<<<<<<<<<<<<<<<<<test in process", nr)

    return 42


@pytest.mark.asyncio
async def test_push_with_multiple_clients_parallel(sync_server, sync_client_registered, server_session, client_session):
    addstuff(sync_client_registered.Session)
    ids = [1, 2, 3]
    with multiprocessing.Pool() as pool:
        res = pool.map(push_in_process, ids)
        print(f"res={res}")


@pytest.mark.asyncio
async def test_push_with_multiple_clients_sequential(sync_server, sync_client_registered, server_session, client_session):
    addstuff(sync_client_registered.Session)
    ids = [1, 2, 3]
    with multiprocessing.Pool() as pool:
        for id in ids:
            res = pool.apply(push_in_process, [id])
            print(f"res={res}")


def push_and_change_in_process(nr: int):
    """
    syncs data, modifies local data and syncs again
    """
    print(">>>>>>>>>>>>>>>>>test in process", nr)
    sync_client: SyncClient = create_sync_client_registered(nr)
    addstuff(sync_client.Session, nr)
    asyncio.run(sync_client.synchronize())
    changestuff(sync_client.Session, nr)

    try:
        asyncio.run(sync_client.synchronize())
        print("<<<<<<<<<<<<<< success test in process", nr)
    except PullSuggested as ex:
        breakpoint()
        print(f"PULL SUGGESTED: {nr}")
        print("<<<<<<<<<<<<<<test in process", nr)
        raise
    except Exception as ex:
        print(f"FAIL: {ex}")
        print("<<<<<<<<<<<<<<test in process", nr)

    return 42


@pytest.mark.asyncio
async def test_push_and_change_with_multiple_clients_parallel(sync_server, sync_client_registered, server_session, client_session):
    """
    calls push_and_change_in_process in parallel
    """
    addstuff(sync_client_registered.Session)
    try:
        with multiprocessing.Pool() as pool:
            pool.map(push_and_change_in_process, [2, 3, 4, 5])
    except PullSuggested as ex:
        raise

    # a seperate run for the third client should now fetch all other's a's
    with multiprocessing.Pool() as pool:
        res = pool.apply(push_and_change_in_process, [3])

@pytest.mark.asyncio
async def test_push_and_change_with_multiple_clients_sequential(sync_server, sync_client_registered, server_session, client_session):
    """
    calls push_and_change_in_process in sequential order
    """
    ids = [1, 2, 3]
    addstuff(sync_client_registered.Session)
    try:
        with multiprocessing.Pool() as pool:
            for id in ids:
                res = pool.apply(push_and_change_in_process, [id])
                print(f"res={res}")
    except PullSuggested as ex:
        raise

def test_subquery(sync_client, client_session):
    addstuff(sync_client.Session)

    As = client_session.query(A).filter(A.id.notin_(
        client_session.query(B.a_id)
    ))

    print(f"As: {As.all()}")
    # sync_client.connect()