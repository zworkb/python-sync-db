import asyncio
import datetime
import importlib
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

import sqlalchemy

from dbsync import server, core
from dbsync.client import PushRejected, PullSuggested
from dbsync.core import with_transaction, with_transaction_async
from dbsync.messages.codecs import SyncdbJSONEncoder
from dbsync.messages.pull import PullRequestMessage, PullMessage
from dbsync.messages.push import PushMessage
from dbsync.models import OperationError, Version, Operation, attr, SQLClass, call_after_tracking_fn
from dbsync.server import before_push, after_push
from dbsync.server.conflicts import find_unique_conflicts
from dbsync.server.handlers import PullRejected
from dbsync.socketserver import GenericWSServer, Connection
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from dbsync.createlogger import create_logger
from sqlalchemy.orm import sessionmaker, make_transient

from dbsync.utils import get_pk, properties_dict

import logging
logger = create_logger("dbsync-server")


class SyncServerConnection(Connection):
    ...

async def send_field_payload(connection: Connection, msg: Dict[str, Any]):
    from ..models import get_model_extensions_for_obj
    session = connection.server.Session()
    logger.debug(f"send_field_payload:{msg}")
    # breakpoint()
    module = importlib.import_module(msg['package_name'])
    klass = getattr(module, msg['class_name'])
    pkname = msg['id_field']
    obj = session.query(klass).filter(getattr(klass, pkname) == msg[pkname]).one()

    extensions = get_model_extensions_for_obj(obj)

    logger.debug(f"model extension: {extensions}")
    # fieldname = msg['field_name']

    for extension in extensions:
        if extension.send_payload_fn:
            await extension.send_payload_fn(obj, connection.socket, session)

    session.close()


@dataclass
class SyncServer(GenericWSServer):
    engine: Optional[Engine] = None
    Session: Optional[sessionmaker] = None

    def __post_init__(self):
        if not self.Session:
            self.Session = sessionmaker(bind=self.engine)


@SyncServer.handler("/push")
@with_transaction_async()
async def handle_push(connection: Connection, session: sqlalchemy.orm.Session) -> Optional[int]:
    msgs_got = 0
    version: Optional[Version] = None
    async for msg in connection.socket:
        msgs_got += 1
        msg_json = json.loads(msg)
        pushmsg = PushMessage(msg_json)
        # print(f"pushmsg: {msg}")
        if not pushmsg.operations:
            logger.warn("empty operations list in client PushMessage")
        for op in pushmsg.operations:
            logger.info(f"operation: {op}")
        # await connection.socket.send(f"answer is:{msg}")
        logger.info(f"message key={pushmsg.key}")

        latest_version_id = core.get_latest_version_id(session=session)
        logger.info(f"** version on server:{latest_version_id}, version in pushmsg:{pushmsg.latest_version_id}")
        if latest_version_id != pushmsg.latest_version_id:
            exc = f"version identifier isn't the latest one; " \
                  f"incoming: {pushmsg.latest_version_id}, on server:{latest_version_id}"

            if latest_version_id is None:
                logger.warn(exc)
                raise PushRejected(exc)
            if pushmsg.latest_version_id is None:
                logger.warn(exc)
                raise PullSuggested(exc)
            if pushmsg.latest_version_id < latest_version_id:
                logger.warn(exc)
                raise PullSuggested(exc)
            raise PushRejected(exc)
        if not pushmsg.islegit(session):
            raise PushRejected("message isn't properly signed")

        for listener in before_push:
            listener(session, pushmsg)


        # I) detect unique constraint conflicts and resolve them if possible
        unique_conflicts = find_unique_conflicts(pushmsg, session)
        conflicting_objects = set()
        for uc in unique_conflicts:
            obj = uc['object']
            conflicting_objects.add(obj)
            for key, value in zip(uc['columns'], uc['new_values']):
                setattr(obj, key, value)
        for obj in conflicting_objects:
            make_transient(obj)  # remove from session
        for model in set(type(obj) for obj in conflicting_objects):
            pk_name = get_pk(model)
            pks = [getattr(obj, pk_name)
                   for obj in conflicting_objects
                   if type(obj) is model]
            session.query(model).filter(getattr(model, pk_name).in_(pks)). \
                delete(synchronize_session=False)  # remove from the database
        session.add_all(conflicting_objects)  # reinsert
        session.flush()

        # II) perform the operations
        operations = [o for o in pushmsg.operations if o.tracked_model is not None]
        post_operations: List[Tuple[Operation, SQLClass, Optional[SQLClass]]] = []
        try:
            op: Operation
            for op in operations:
                (obj, old_obj) = await op.perform_async(pushmsg, session, pushmsg.node_id, connection.socket)

                if obj:
                    # if the op has been skipped, it wont be appended for post_operation handling
                    post_operations.append((op, obj, old_obj))

                    resp = dict(
                        type="info",
                        op=dict(
                            row_id=op.row_id,
                            version=op.version,
                            command=op.command,
                            content_type_id=op.content_type_id,
                        )
                    )
                    call_after_tracking_fn(session, op, obj)
                    await connection.socket.send(json.dumps(resp))

        except OperationError as e:
            logger.exception("Couldn't perform operation in push from node %s.",
                             pushmsg.node_id)
            raise PushRejected("at least one operation couldn't be performed",
                               *e.args)

        # III) insert a new version
        if post_operations: # only if operations have been done -> create the new version
            version = Version(created=datetime.datetime.now(), node_id=pushmsg.node_id)
            session.add(version)

        # IV) insert the operations, discarding the 'order' column
        accomplished_operations = [op for (op, obj, old_obj) in post_operations]
        for op in sorted(accomplished_operations, key=attr('order')):
            new_op = Operation()
            for k in [k for k in properties_dict(op) if k != 'order']:
                setattr(new_op, k, getattr(op, k))
            session.add(new_op)
            new_op.version = version
            session.flush()

        for op, obj, old_obj in post_operations:
            op.call_after_operation_fn(session, obj)
            # from woodmaster.model.sql.model import WoodPile, Measurement
            # orphans = session.query(Measurement).filter(Measurement.woodpile_id == None).all()
            # print(f"orphans:{orphans}")

        for listener in after_push:
            listener(session, pushmsg)

        # return the new version id back to the client
        logger.info(f"version is: {version}")
        if version:
            await connection.socket.send(json.dumps(
                dict(
                    type="result",
                    new_version_id=version.version_id
                )
            ))
            return {'new_version_id': version.version_id}
        else:
            await connection.socket.send(json.dumps(
                dict(
                    type="result",
                    new_version_id=None
                )
            ))
            logger.info("sent nothing message")
            await connection.socket.close()

    logger.info("push ready")
    # session.commit()


@SyncServer.handler("/pull")
# @with_transaction_async()
async def handle_pull(connection: Connection):
    """
    Handle the pull request and return a dictionary object to be sent
    back to the node.

    *data* must be a dictionary-like object, usually one obtained from
    decoding a JSON dictionary in the POST body.
    """
    swell = False,
    include_extensions = True
    data_str = await connection.socket.recv()
    data = json.loads(data_str)
    try:
        request_message = PullRequestMessage(data)
    except KeyError:
        raise PullRejected("request object isn't a valid PullRequestMessage", data)

    message = PullMessage()
    message.fill_for(
        request_message,
        swell=swell,
        include_extensions=include_extensions,
        connection=connection
    )

    # sends the whole bunch to the client,
    # there it is received by client's run_pull and handled by pull.py/merge
    await connection.socket.send(json.dumps(message.to_json(), indent=4,  cls=SyncdbJSONEncoder))

    # fetch messages from client
    logger.debug(f"server listening for messages after sending object")
    async for msg_ in connection.socket:
        logger.debug(f"server getting for msg: {msg_}")
        msg = json.loads(msg_)
        # logger.debug(f"msg: {msg}")
        if msg['type'] == "request_field_payload":
            # sends payload data to client here
            logger.info(f"obj from client:{msg}")
            await send_field_payload(connection, msg)
        elif msg['type'] == 'result':
            new_version_id = msg['new_version_id']
            if new_version_id is None:
                break
        else:
            logger.debug(f"response from server:{msg}")


@SyncServer.handler("/status")
async def status(connection: Connection):
    logger.info("STATUS")
    res = dict(
        id=connection.server.id,
        connections=[
            dict(
                xx=str(conn.__class__),
                path=conn.path,
                ip=conn.socket.remote_address[0],
                port=conn.server.port
            )
            for conn in connection.server.connections
            if conn is not connection
        ]
    )
    await connection.socket.send(
        json.dumps(res)
    )


@SyncServer.handler("/register", SyncServerConnection)
async def register(conn: SyncServerConnection):
    params = json.loads(await conn.socket.recv())
    res = server.handle_register()
    await conn.socket.send(json.dumps(res))

    # return (json.dumps(server.handle_register()),
    #         200,
    #         {"Content-Type": "application/json"})
