import json
from dataclasses import dataclass
from typing import Optional

from dbsync.socketserver import GenericWSServer, Connection
import sqlalchemy as sa
from sqlalchemy.engine import Engine


from dbsync.createlogger import create_logger

logger = create_logger("dbsync-server")


@dataclass
class SyncServer(GenericWSServer):
    engine: Optional[Engine] = None


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
