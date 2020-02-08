import json
from dataclasses import dataclass
from typing import Optional

import sqlalchemy

from dbsync import server, core
from dbsync.client import PushRejected, PullSuggested
from dbsync.core import with_transaction
from dbsync.messages.push import PushMessage
from dbsync.socketserver import GenericWSServer, Connection
import sqlalchemy as sa
from sqlalchemy.engine import Engine


from dbsync.createlogger import create_logger
from sqlalchemy.orm import sessionmaker

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
@with_transaction()
async def sync(connection: Connection, session: sqlalchemy.orm.Session):
    async for msg in connection.socket:
        msg_json = json.loads(msg)
        pushmsg = PushMessage(msg_json)
        print(f"PUSHMSG:{pushmsg}")
        await connection.socket.send(f"answer is:{msg}")
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
                ip=conn.socket.host,
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
