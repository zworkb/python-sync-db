import json
from dataclasses import dataclass
from typing import Optional

from dbsync.socketserver import GenericWSServer, Connection
import sqlalchemy as sa
from sqlalchemy.engine import Engine


from dbsync.createlogger import create_logger
from sqlalchemy.orm import sessionmaker

logger = create_logger("dbsync-server")


@dataclass
class SyncServer(GenericWSServer):
    engine: Optional[Engine] = None
    Session: Optional[sessionmaker] = None

    def __post_init__(self):
        if not self.Session:
            self.Session = sessionmaker(bind=self.engine)



@SyncServer.handler("/sync")
async def sync(connection: Connection):
    async for msg in connection.socket:
        await connection.socket.send(f"answer is:{msg}")


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
