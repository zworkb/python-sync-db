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


from tests.models_websockets import  SERVER_URL
from .models_websockets import PORT
from .server_setup import sync_server
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
async def test_register(sync_server, sync_client):
    res = await sync_client.register()

    print(f"REGISTERED:{res}")