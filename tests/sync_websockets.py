import asyncio
import datetime
import logging
import json

import pytest
import sqlalchemy
import websockets
from dbsync import models, core
from dbsync.client.wsclient import SyncClient
from dbsync.messages.push import PushMessage
from dbsync.models import Node, Operation
from dbsync.server.wsserver import SyncServer
from dbsync.socketserver import Connection


from tests.models_websockets import SERVER_URL, addstuff, changestuff
from .models_websockets import PORT
from .server_setup import sync_server, server_session
from .client_setup import sync_client


@SyncServer.handler("/counter")
async def counter(conn: Connection):
    n = int(await conn.socket.recv())

    for i in range(n):
        await conn.socket.send(str(i))


@pytest.mark.asyncio
async def test_server_only(sync_server):
    async with websockets.connect(f"ws://localhost:{PORT}/counter") as ws:
        await ws.send("5")
        async for resp in ws:
            print("COUNT:", resp)


def test_server_start(sync_server, sync_client):
    print("server is:", sync_server)

    async def action(client: SyncClient):
        await client.websocket.send("Hi")
        resp = await client.websocket.recv()
        print("answer recv:", resp)

    sync_client.connect(action=action, do_wait=True)
    # sync_server.wait()


@pytest.mark.asyncio
async def test_register(sync_server, sync_client, server_session: sqlalchemy.orm.session.Session):
    res = await sync_client.register()

    sync_nodes = sync_client.Session().query(Node).all()
    assert len(sync_nodes) == 1
    print(f"REGISTERED:{res}")

    server_nodes = server_session.query(Node).all()

    # after registration there should be one node in the server database
    assert len(server_nodes) == 1


@pytest.mark.asyncio
async def test_tracking_add(sync_server, sync_client):
    sess = sync_client.Session()
    addstuff(sync_client.Session)

    ops = sess.query(Operation).all()

    assert len(ops) == 5


@pytest.mark.asyncio
async def test_tracking_add_change(sync_server, sync_client, server_session):
    sess = sync_client.Session()
    print("SESS:", sess.__class__)
    addstuff(sync_client.Session)
    changestuff(sync_client.Session)

    ops = sess.query(Operation).all()

    assert len(ops) == 8


