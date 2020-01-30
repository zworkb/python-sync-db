
import multiprocessing as mp
import datetime
import time
import uuid

import pytest
from dbsync.server.wsserver import SyncServer
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from dbsync.dialects import GUID
from dbsync.utils import generate_secret
import dbsync
from dbsync import client, models


engine_server = create_engine("sqlite:///./test_server.db")
engine_client = create_engine("sqlite:///./test_client.db")
# engine = create_engine("sqlite://")

PORT = 7081

Base = declarative_base()


@client.track
class A(Base):
    __tablename__ = "test_a"

    id = Column(GUID, primary_key=True, default=lambda: uuid.uuid4())
    name = Column(String)

    def __repr__(self):
        return u"<A id:{0} name:{1}>".format(self.id, self.name)


@client.track
class B(Base):
    __tablename__ = "test_b"

    id = Column(GUID, primary_key=True, default=lambda: uuid.uuid4())
    name = Column(String)
    a_id = Column(GUID, ForeignKey("test_a.id"))

    a = relationship(A, backref="bs")

    def __repr__(self):
        return u"<B id:{0} name:{1} a_id:{2}>".format(
            self.id, self.name, self.a_id)


def start_ws_server(**kw):
    engine_server = create_engine("sqlite:///./test_server.db")
    Base.metadata.create_all(engine_server)
    dbsync.set_engine(engine_server)
    dbsync.create_all()
    server = SyncServer(port=PORT, engine=engine_server, **kw)
    print("starting server...")
    server.start(run_forever=False, start_new_loop=True)
    print("server finished")
    return server

@pytest.fixture(scope="function")
def sync_server():
    pool = mp.Pool()
    task = pool.apply_async(start_ws_server)
    # task.wait()

    return task


@pytest.fixture(scope="function")
def sync_client():
    ...

# Base.metadata.create_all(engine)
# dbsync.set_engine(engine)
# dbsync.create_all()
# dbsync.generate_content_types()
# _session = Session()
# _session.add(
#     models.Node(registered=datetime.datetime.now(), secret=generate_secret(128)))
# _session.commit()
# _session.close()
