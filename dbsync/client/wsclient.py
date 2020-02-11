import asyncio
import importlib
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any

import sqlalchemy
import websockets
from dbsync import core, wscommon
from dbsync.client import PushRejected, PullSuggested
from dbsync.client.compression import compress
from dbsync.client.net import post_request
from dbsync.client.register import RegisterRejected
from dbsync.createlogger import create_logger
from dbsync.messages.codecs import encode_dict, SyncdbJSONEncoder
from dbsync.messages.push import PushMessage
from dbsync.messages.register import RegisterMessage
from dbsync.models import Node, model_extensions, get_model_extension_for_obj
from dbsync.socketclient import GenericWSClient
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

wscommon.register_exception(PushRejected)
wscommon.register_exception(PullSuggested)

logger = create_logger("wsclient")

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

    async def send_field_payload(self, session: sqlalchemy.orm.Session, msg: Dict[str, Any]):
        ...
        print(f"!!send_field_payload:{msg}")
        # breakpoint()
        module = importlib.import_module(msg['package_name'])
        klass = getattr(module, msg['class_name'])
        pkname = msg['id_field']
        obj = session.query(klass).filter(getattr(klass, pkname) == msg[pkname]).one()

        extension = get_model_extension_for_obj(obj)

        print("EXT:", extension)
        fieldname = msg['field_name']
        extension_field = extension[fieldname]
        await extension_field.send_payload_fn(obj, fieldname, self.websocket)



    async def push(self, session: Optional[sqlalchemy.orm.session.Session] = None):
        message = self.create_push_message()
        if not session:
            session = self.Session()

        node = session.query(Node).order_by(Node.node_id.desc()).first()
        message.set_node(node)  # TODO to should be migrated to GUID and ordered by creation date
        logger.warn(f"message key={message.key}")
        logger.warn(f"message secret={message._secret}")
        message_json = message.to_json(include_operations=True)
        # message_encoded = encode_dict(PushMessage)(message_json)
        message_encoded = json.dumps(message_json, cls=SyncdbJSONEncoder)
        await self.websocket.send(message_encoded)

        async for msg_ in self.websocket:
            msg = json.loads(msg_)
            print("MSG:", msg)
            if msg['type'] == "request_field_payload":
                logger.warn(f"obj from server:{msg}")
                await self.send_field_payload(session, msg)
            else:
                logger.info(f"response from server:{msg}")

        if not message.operations:
            return {}

    def request_push(self):
        ...

    async def run(self):
        return await self.push()