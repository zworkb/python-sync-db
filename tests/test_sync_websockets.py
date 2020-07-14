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
from sqlalchemy import and_

from dbsync import models, core, wscommon
from dbsync.client import PushRejected, PullSuggested
from dbsync.client.wsclient import SyncClient
from dbsync.messages.push import PushMessage
from dbsync.models import Node, Operation, Version
from dbsync.server.wsserver import SyncServer
from dbsync.socketclient import GenericWSClient
from dbsync.socketserver import Connection


from tests.models_websockets import SERVER_URL, addstuff, changestuff, A, B, datapath, deletestuff
from .models_websockets import PORT
from .server_setup import sync_server, server_session, server_db
from .client_setup import sync_client, client_session, sync_client_registered, client_db, create_sync_client_registered, \
    create_client_session


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

    assert len(ops) == 8


@pytest.mark.asyncio
async def test_tracking_add_change(sync_server, sync_client, server_session, client_session):
    addstuff(sync_client.Session)
    changestuff(sync_client.Session)

    ops = client_session.query(Operation).all()

    assert len(ops) == 13


# @pytest.mark.asyncio
@pytest.mark.parametrize("compress_info", [(False, 13), (True, 7)])
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

    # breakpoint()
    server_nodes = server_session.query(Node).all()

    # after registration there should be one node in the server database
    assert len(server_nodes) == 1

    await sync_client_registered.synchronize()

    # check if stuff got trasmitted
    As = server_session.query(A).all()
    assert len(As) > 0

    Bs = server_session.query(B).all()
    assert len(Bs) > 0

    for b in Bs:
        assert bool(b.comment)
        assert b.comment == f"processed_before_i: {str(b.id).replace('-','')}"
        assert b.comment_after == f"processed_after_i: {str(b.id).replace('-','')}"

    # check if before_operation_fn has been called
    # test is setup so that before_operation_fn sets the ``comment`` field

    versions = client_session.query(Version).all()
    assert len(versions) == 1

    # and now change something
    changestuff(sync_client_registered.Session)
    await sync_client_registered.synchronize()

    b2 = server_session.query(B).filter(B.name == "second b  updated").one()
    # check if data have been transferred
    fpath = datapath(b2.data, "server")
    with open(fpath) as fh:
        data = fh.read()
        assert data == "b2  changed"

    versions = client_session.query(Version).all()
    assert len(versions) == 2

    Bs = server_session.query(B).all()
    for b in Bs:
        server_session.refresh(b)
        assert bool(b.comment)
        assert b.comment == f"processed_before_u: {str(b.id).replace('-','')}"
        assert b.comment_after == f"processed_after_u: {str(b.id).replace('-','')}"

    As_client = client_session.query(A).all()
    assert len(As_client) == 5
    a5_client: A = client_session.query(A).filter(A.key == "a5").one()
    a5_client.comment = "lets update now"
    client_session.commit()
    await sync_client_registered.synchronize()

    # because one A should not be synced len is one less
    As_server = server_session.query(A).all()
    assert len(As_server) == 4

    a5_server: A = server_session.query(A).filter(A.key == "a5").one()
    ### assert a5_server.comment_after_update == f"after update for A: {str(a5_server.id).replace('-', '')}"
    # comment_after_update must be None because for a5 update will be skipped
    assert a5_server.comment_after_update is None

    # now check the delete handlers, we delete a3 and a5, but for a3 we will raise a SkipOperation
    # so it wont be deleted
    a3_client = client_session.query(A).filter(A.key == "a3").one()
    a5_client = client_session.query(A).filter(A.key == "a5").one()

    # client_session.delete(a3_client)
    # client_session.delete(a5_client)
    # client_session.commit()

    deletestuff(sync_client_registered.Session)

    As_client = client_session.query(A).all()
    assert len(As_client) == 3

    await sync_client_registered.synchronize()

    # because we skipped deletion of a3 we should have 3 As on server
    As_server = server_session.query(A).all()
    assert len(As_server) == 3

    # add new a7, and as long name = "donttrack" it wont be synced
    client_session.add(A(name="donttrack", key="a7"))
    client_session.commit()
    # there should be no operation be triggered
    a7_client = client_session.query(A).filter(A.key == "a7").one_or_none()
    op7 = client_session.query(Operation).filter(Operation.row_id == a7_client.id).one_or_none()
    assert op7 is None
    await sync_client_registered.synchronize()

    a7_server = server_session.query(A).filter(A.key == "a7").one_or_none()
    assert a7_server is None

    # now we change name from "donttrack" to something else and thus it should now be transmitted
    a7_client = client_session.query(A).filter(A.key == "a7").one()
    a7_client.name = "a7, lets sync now"
    client_session.commit()
    # now we should have a tracked op
    op7 = client_session.query(Operation).filter(Operation.row_id == a7_client.id).one()

    await sync_client_registered.synchronize()
    a7_server = server_session.query(A).filter(A.key == "a7").one_or_none()
    assert a7_server is not None


def push_only(nr: int):
    print(">>>>>>>>>>>>>>>>> push only:", nr)
    sync_client: SyncClient = create_sync_client_registered(nr, reset_db=False)
    sess = sync_client.Session()
    a1, a2, a3 = sess.query(A).filter(A.pid == str(nr))[:3]
    a2.name = f"!!!second a {nr} fixed"
    sess.commit()
    # addstuff(sync_client.Session, nr)
    # a=A(name=f"last a {nr}", pid=nr)
    # sess.add(a)
    # sess.commit()
    # breakpoint()
    asyncio.run(sync_client.synchronize(id=nr))
    print("<<<<<<<<<<<<<<<<<push only:", nr)


def synchronize_in_process(nr: int):
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
    """
    create 5 As and 2 Bs and sync them, nothing more
    """
    addstuff(sync_client_registered.Session)
    ids = [1, 2, 3]
    with multiprocessing.Pool() as pool:
        res = pool.map(synchronize_in_process, ids)
        print(f"res={res}")


@pytest.mark.asyncio
async def test_push_with_multiple_clients_sequential(sync_server, sync_client_registered, server_session, client_session):
    """
    create 5 As and 2 Bs and sync them, nothing more
    """
    addstuff(sync_client_registered.Session)
    ids = [1, 2, 3]
    # ids = [1]
    with multiprocessing.Pool() as pool:
        for id in ids:
            res = pool.apply(synchronize_in_process, [id])
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
async def test_push_and_change_with_multiple_clients_parallel(sync_server, server_session, client_session):
    """
    calls push_and_change_in_process in parallel, a4 is not pushed
    """

    ids1 = [2, 3, 4, 5, 6, 7]
    ids2 = [3, 5]
    try:
        with multiprocessing.Pool() as pool:
            pool.map(push_and_change_in_process, ids1)
    except PullSuggested as ex:
        raise

    # a seperate run for the third client should now fetch all other's a's
    with multiprocessing.Pool() as pool:
        pool.map(push_only, ids2)
        # for id in [3, 5]:
        #     res = pool.apply(push_only, [id])

    # check tracking on server
    for b in server_session.query(B):
        assert b.comment_after_tracking_server == 'ok'

    for a in server_session.query(A):
        assert a.comment_after_tracking_server == 'ok'

    # check if the post fetched records are here
    # check for downloaded A's
    for id in ids2:
        client_session = create_client_session(id)
        keys = ["a1", "a2", "a3"]
        for pid in ids1:
            for k in keys:
                a = client_session.query(A).filter(and_(A.key == k, A.pid == str(pid))).one()


@pytest.mark.asyncio
async def test_push_and_change_with_multiple_clients_sequential(sync_server, server_session, client_session):
    """
    calls push_and_change_in_process in sequential order, deterministic
    """
    ids = [1, 2, 3, 4]
    ids2 = [4]  # in the seuential case the last client gets all other clients' items

    try:
        with multiprocessing.Pool() as pool:
            for id in ids:
                res = pool.apply(push_and_change_in_process, [id])
                print(f"res={res}")
    except PullSuggested as ex:
        raise

    # check tracking on server
    for b in server_session.query(B):
        assert b.comment_after_tracking_server == 'ok'

    for a in server_session.query(A):
        assert a.comment_after_tracking_server == 'ok'


    # check for downloaded A's
    # a4 should not be uploaded because its blocked in the before_tracking_fn function
    for id in ids2:
        client_session = create_client_session(id)
        keys = ["a1", "a2", "a3", "a5"]
        for pid in ids:
            for k in keys:
                a = client_session.query(A).filter(and_(A.key == k, A.pid == str(pid))).one()




def test_subquery(sync_client, client_session):
    addstuff(sync_client.Session)

    As = client_session.query(A).filter(A.id.notin_(
        client_session.query(B.a_id)
    ))

    print(f"As: {As.all()}")
    # sync_client.connect()