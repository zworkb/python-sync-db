import asyncio
import datetime
import json
from dataclasses import dataclass
from typing import Optional

import sqlalchemy

from dbsync import server, core
from dbsync.client import PushRejected, PullSuggested
from dbsync.core import with_transaction, with_transaction_async
from dbsync.messages.push import PushMessage
from dbsync.models import OperationError, Version, Operation, attr
from dbsync.server import before_push, after_push
from dbsync.server.conflicts import find_unique_conflicts
from dbsync.socketserver import GenericWSServer, Connection
import sqlalchemy as sa
from sqlalchemy.engine import Engine


from dbsync.createlogger import create_logger
from sqlalchemy.orm import sessionmaker, make_transient

from dbsync.utils import get_pk, properties_dict

logger = create_logger("dbsync-server")

class SyncServerConnection(Connection):
    ...

@dataclass
class SyncServer(GenericWSServer):
    engine: Optional[Engine] = None
    Session: Optional[sessionmaker] = None

    def __post_init__(self):
        if not self.Session:
            self.Session = sessionmaker(bind=self.engine)


@SyncServer.handler("/sync")
@with_transaction_async()
async def sync(connection: Connection, session: sqlalchemy.orm.Session):
    # asyncio.ensure_future(keepalive(connection.socket))

    async for msg in connection.socket:
        msg_json = json.loads(msg)
        pushmsg = PushMessage(msg_json)
        print(f"PUSHMSG:{pushmsg}")
        # await connection.socket.send(f"answer is:{msg}")
        logger.warn(f"message key={pushmsg.key}")
        # logger.warn(f"message secret={pushmsg._secret}")

        latest_version_id = core.get_latest_version_id(session=session)
        if latest_version_id != pushmsg.latest_version_id:
            exc = "version identifier isn't the latest one; "\
                "given: %s" % pushmsg.latest_version_id
            if latest_version_id is None:
                raise PushRejected(exc)
            if pushmsg.latest_version_id is None:
                raise PullSuggested(exc)
            if pushmsg.latest_version_id < latest_version_id:
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
            make_transient(obj) # remove from session
        for model in set(type(obj) for obj in conflicting_objects):
            pk_name = get_pk(model)
            pks = [getattr(obj, pk_name)
                   for obj in conflicting_objects
                   if type(obj) is model]
            session.query(model).filter(getattr(model, pk_name).in_(pks)).\
                delete(synchronize_session=False) # remove from the database
        session.add_all(conflicting_objects) # reinsert
        session.flush()

        # II) perform the operations
        operations = [o for o in pushmsg.operations if o.tracked_model is not None]
        try:
            for op in operations:
                op.perform(pushmsg, session, pushmsg.node_id)
        except OperationError as e:
            logger.exception("Couldn't perform operation in push from node %s.",
                             pushmsg.node_id)
            raise PushRejected("at least one operation couldn't be performed",
                               *e.args)

        # III) insert a new version
        version = Version(created=datetime.datetime.now(), node_id=pushmsg.node_id)
        session.add(version)

        # IV) insert the operations, discarding the 'order' column
        for op in sorted(operations, key=attr('order')):
            new_op = Operation()
            for k in [k for k in properties_dict(op) if k != 'order']:
                setattr(new_op, k, getattr(op, k))
            session.add(new_op)
            new_op.version = version
            session.flush()

        for listener in after_push:
            listener(session, pushmsg)

        # return the new version id back to the node
        await connection.socket.send(str(version.version_id))
        return {'new_version_id': version.version_id}

@SyncServer.handler("/status")
async def status(connection: Connection):
    print("STATUS")
    logger.warn("STATUS")
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
    print(f"conn:{conn}")
    params = json.loads(await conn.socket.recv())
    print(f"register: {params}")

    res = server.handle_register()
    await conn.socket.send(json.dumps(res))


    # return (json.dumps(server.handle_register()),
    #         200,
    #         {"Content-Type": "application/json"})
