import asyncio
import datetime
import logging
import json

import pytest
import websockets
from dbsync import models, core
from dbsync.client.wsclient import SyncClient
from dbsync.messages.push import PushMessage
from dbsync.server.wsserver import SyncServer
from dbsync.socketserver import Connection


from tests.models import A, B, Session
from tests.models_websockets import sync_server, sync_client, SERVER_URL
from .models_websockets import PORT

def addstuff():
    a1 = A(name="first a")
    a2 = A(name="second a")
    b1 = B(name="first b", a=a1)
    b2 = B(name="second b", a=a1)
    b3 = B(name="third b", a=a2)
    session = Session()
    session.add_all([a1, a2, b1, b2, b3])
    session.commit()

def changestuff():
    session = Session()
    a1, a2 = session.query(A).all()
    b1, b2, b3 = session.query(B).all()
    a1.name = "first a modified"
    b2.a = a2
    session.delete(b3)
    session.commit()

def setup():
    pass

@core.with_listening(False)
def teardown():
    session = Session()
    for a in session.query(A).all():
        session.delete(a)
    for b in session.query(B).all():
        session.delete(b)
    for op in session.query(models.Operation).all():
        session.delete(op)
    session.commit()


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
    sync_server.wait()