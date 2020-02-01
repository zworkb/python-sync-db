from dataclasses import dataclass
from typing import Optional

from dbsync.socketclient import GenericWSClient
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker


@dataclass
class SyncClient(GenericWSClient):
    engine: Optional[Engine] = None
    Session: Optional[sessionmaker] = None

    def __post_init__(self):
        if not self.Session:
            self.Session = sessionmaker(bind=self.engine)

    def request_push(self):
        ...

    async def push(self):
        ...