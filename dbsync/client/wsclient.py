import asyncio
import importlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any, Callable

import sqlalchemy
from sqlalchemy import and_
import websockets
from sqlalchemy.exc import OperationalError

from dbsync import core, wscommon
from dbsync.client import PushRejected, PullSuggested, UniqueConstraintError
from dbsync.client.compression import compress
from dbsync.client.net import post_request
from dbsync.client.pull import BadResponseError, merge
from dbsync.client.register import RegisterRejected
from dbsync.createlogger import create_logger
from dbsync.messages.codecs import encode_dict, SyncdbJSONEncoder
from dbsync.messages.pull import PullRequestMessage, PullMessage
from dbsync.messages.push import PushMessage
from dbsync.messages.register import RegisterMessage
from dbsync.models import Node, model_extensions, get_model_extension_for_obj, Version, Operation
from dbsync.socketclient import GenericWSClient
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from dbsync.wscommon import SerializationError

wscommon.register_exception(PushRejected)
wscommon.register_exception(PullSuggested)

logger = create_logger("wsclient")

@dataclass
class SyncClient(GenericWSClient):
    engine: Optional[Engine] = None
    Session: Optional[sessionmaker] = None
    id: int = -1

    def __post_init__(self):
        if not self.Session:
            self.engine = core.get_engine()
            self.Session = lambda: core.SessionClass(
                bind=self.engine)  # to behave like core.Session() but dont set the internal flag
            # self.Session = sessionmaker(bind=self.engine)

    @property
    def register_url(self):
        return f"{self.base_uri}/register"

    @core.with_transaction()
    async def register(self, extra_data: Optional[Dict] = None,
                       encode=None, decode=None, headers=None, timeout=None,
                       session=None):
        async with websockets.connect(self.register_url) as ws:
            #  TODO:conv to strings, parse the params at server side
            logger.debug("register begin")
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
            logger.debug("register finished")

            assert len(session.query(Node).all()) > 0
            return resp

    def create_push_message(self, session: Optional[sqlalchemy.orm.session.Session] = None,
                            extensions=True, do_compress=True) -> PushMessage:

        # TODO: mit do_compress=True muss noch getestet werden, welche Szenarien die referentielle Integritaet
        # verletzen koennen. Denn wenn die Tabellen in richtiger Reihenfolge synchronisiert werden
        # koennte man auf das Aussetzen der RI verzichten

        if not session:
            session = self.Session()  # TODO: p
        # breakpoint()
        message = PushMessage()
        message.latest_version_id = core.get_latest_version_id(session=session)
        if do_compress:
            compress(session=session)
        message.add_unversioned_operations(
            session=session, include_extensions=extensions)

        return message

    async def send_field_payload(self, session: sqlalchemy.orm.Session, msg: Dict[str, Any]):
        ...
        logger.debug(f"send_field_payload:{msg}")
        # breakpoint()
        module = importlib.import_module(msg['package_name'])
        klass = getattr(module, msg['class_name'])
        pkname = msg['id_field']
        obj = session.query(klass).filter(getattr(klass, pkname) == msg[pkname]).one()

        extension = get_model_extension_for_obj(obj)

        logger.debug(f"model extension: {extension}")
        fieldname = msg['field_name']
        extension_field = extension[fieldname]
        await extension_field.send_payload_fn(obj, fieldname, self.websocket, session)

    async def run_push(self, session: Optional[sqlalchemy.orm.session.Session] = None):
        # breakpoint()
        new_version_id: Optional[int]
        message = self.create_push_message()
        if not session:
            session = self.Session()

        logger.info(f"number of operations: {len(message.operations)}")
        if not message.operations:
            return {}

        node = session.query(Node).order_by(Node.node_id.desc()).first()
        message.set_node(node)  # TODO to should be migrated to GUID and ordered by creation date
        logger.info(f"message key={message.key}")
        logger.info(f"message secret={message._secret}")
        message_json = message.to_json(include_operations=True)
        # message_encoded = encode_dict(PushMessage)(message_json)
        message_encoded = json.dumps(message_json, cls=SyncdbJSONEncoder, indent=4)

        # here it happens
        logger.info("sending message to server")
        await self.websocket.send(message_encoded)
        logger.info("message sent to server")
        session.commit()
        logger.debug(f"message: {message_encoded}")
        new_version_id = None
        # accept incoming requests for payload data (optional)
        async for msg_ in self.websocket:
            logger.info(f"client:{self.id} msg: {msg_}")
            msg = json.loads(msg_)
            # logger.debug(f"msg: {msg}")
            if msg['type'] == "request_field_payload":
                logger.info(f"obj from server:{msg}")
                await self.send_field_payload(session, msg)
            elif msg['type'] == 'result':
                new_version_id = msg['new_version_id']
            else:
                logger.info(f"response from server:{msg}")

        # else:
        #     print("ENDE:")
        # EEEEK TODO this is to prevent sqlite blocking due to other sessions
        session.close_all()

        if new_version_id is None:
            return
            # breakpoint()
            # raise ValueError("did not get a versionid from server")
        session = self.Session()

        # because of the above reason (all sessions closed) we have to reselect the operations for updating
        for (i, op) in enumerate(message.operations):
            ops1 = session.query(Operation).filter(
                Operation.row_id == op.row_id,
            ).all()
            for op1 in ops1:
                op1.version_id = new_version_id

        session.add(
            Version(version_id=new_version_id, created=datetime.now()))

        session.commit()

    async def run_pull(self, session: Optional[sqlalchemy.orm.session.Session] = None,
                       extra_data: Dict[str, Any] = None, monitor: Optional[Callable[[Dict[str, Any]], None]] = None):
        include_extensions = False
        if extra_data is None:
            extra_data = {}

        logger.info(f"run_pull begin")
        # new_version_id: Optional[int]
        # message = self.create_push_message()
        if not session:
            session = self.Session()

        if extra_data is not None:
            assert isinstance(extra_data, dict), "extra data must be a dictionary"
        request_message = PullRequestMessage()
        for op in compress():
            request_message.add_operation(op)
        data = request_message.to_json()
        data.update({'extra_data': extra_data or {}})
        msg = json.dumps(data,  cls=SyncdbJSONEncoder)
        await self.websocket.send(msg)

        response_str = await self.websocket.recv()
        response = json.loads(response_str)
        message = None
        try:
            message = PullMessage(response)
        except KeyError:
            if monitor:
                monitor({
                    'status': "error",
                    'reason': "invalid message format"})
            raise BadResponseError(
                "response object isn't a valid PullMessage", response)

        logger.info(f"pull message contains {len(message.operations)} operations")

        if monitor:
            monitor({
                'status': "merging",
                'operations': len(message.operations)})


        merge(message, include_extensions=include_extensions)  #TODO: request_payload etc.
        if monitor:
            monitor({'status': "done"})
        # return the response for the programmer to do what she wants
        # afterwards
        return response

    async def synchronize(self, id=None):
        """

        XXX: implement a more sophisticated retry strategy (random delay, longer delay schedule)
        """
        tries = 5
        for _round in range(tries):
            try:
                logger.info(f"-- round {_round} for {id}: try push")
                await self.connect_async(method=self.run_push, path="push")

            except (SerializationError, PullSuggested) as ex:
                try:
                    # raise
                    logger.info(f"-- round {_round} for {id}: pull suggested: try pull")
                    await self.connect_async(method=self.run_pull, path="pull")
                    logger.info(f"-- round {_round}: pull successful")
                except UniqueConstraintError as e:
                    # breakpoint()
                    raise
                    for model, pk, columns in e.entries:
                        pass  # TODO: handle exception
                except Exception as ex:
                    # breakpoint()
                    raise
            except Exception as ex:
                # breakpoint()
                raise