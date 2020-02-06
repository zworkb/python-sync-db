import asyncio
import json
from dataclasses import dataclass
from typing import Optional, Dict

import sqlalchemy
import websockets
from dbsync import core
from dbsync.client.compression import compress
from dbsync.client.net import post_request
from dbsync.client.register import RegisterRejected
from dbsync.messages.codecs import encode_dict, SyncdbJSONEncoder
from dbsync.messages.push import PushMessage
from dbsync.messages.register import RegisterMessage
from dbsync.models import Node
from dbsync.socketclient import GenericWSClient
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker


# @core.with_transaction()
def register(registry_url, extra_data: Optional[Dict] = None,
             encode=None, decode=None, headers=None, timeout=None,
             session=None):
    """
    Request a node registry from the server.

    If there is already a node registered in the local database, it
    won't be used for the following operations. Additional data can be
    passed to the request by giving *extra_data*, a dictionary of
    values.

    By default, the *encode* function is ``json.dumps``, the *decode*
    function is ``json.loads``, and the *headers* are appropriate HTTP
    headers for JSON.
    """
    assert isinstance(registry_url, str), "registry url must be a string"
    assert bool(registry_url), "registry url can't be empty"
    if extra_data is not None:
        assert isinstance(extra_data, dict), "extra data must be a dictionary"

    code, reason, response = post_request(
        registry_url, extra_data or {}, encode, decode, headers, timeout)

    if (code // 100 != 2) or response is None:
        raise RegisterRejected(code, reason, response)

    message = RegisterMessage(response)
    session.add(message.node)
    return response


@dataclass
class SyncClient(GenericWSClient):
    engine: Optional[Engine] = None
    Session: Optional[sessionmaker] = None

    def __post_init__(self):
        if not self.Session:
            self.Session = sessionmaker(bind=self.engine)

    @property
    def register_url(self):
        return f"{self.base_uri}/register"

    @core.with_transaction()
    async def register(self, extra_data: Optional[Dict] = None,
                       encode=None, decode=None, headers=None, timeout=None,
                       session=None):
        async with websockets.connect(self.register_url) as ws:
            #  TODO:conv to strings, parse the params at server side
            params = dict(
                extra=extra_data,
                encode=encode,
                decode=decode,
                headers=headers
            )
            msg = json.dumps(params)
            await ws.send(msg)
            resp = json.loads(await ws.recv())

            message = RegisterMessage(resp)
            session.add(message.node)

            session.commit()


            assert len(session.query(Node).all()) > 0
            return resp



    def create_push_message(self, session: Optional[sqlalchemy.orm.session.Session] = None,
                            extensions=True, do_compress=True) -> PushMessage:

        # TODO: mit do_compress=True muss noch getestet werden, welche Szenarien die referentielle Integritaet
        # verletzen koennen. Denn wenn die Tabellen in richtiger Reihenfolge synchronisiert werden
        # koennte man auf das Aussetzen der RI verzichten

        if not session:
            session = self.Session()  # TODO: p

        message = PushMessage()
        message.latest_version_id = core.get_latest_version_id(session=session)
        if do_compress:
            compress(session=session)
        message.add_unversioned_operations(
            session=session, include_extensions=extensions)

        return message

    async def push(self, session: Optional[sqlalchemy.orm.session.Session] = None):
        message = self.create_push_message()

        if not session:
            session = self.Session()

        node = session.query(Node).order_by(Node.node_id.desc()).first()
        message.set_node(node)  # TODO to should be migrated to GUID and ordered by creation date

        message_json = message.to_json(include_operations=False)
        # message_encoded = encode_dict(PushMessage)(message_json)
        message_encoded = json.dumps(message_json, cls=SyncdbJSONEncoder)
        await self.websocket.send(message_encoded)
        res = await self.websocket.recv()
        print("RES:", res)


        if not message.operations:
            return {}

    def request_push(self):
        ...

    async def run(self):
        return await self.push()