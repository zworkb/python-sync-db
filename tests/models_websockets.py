
import multiprocessing as mp
import datetime
import os
import time
import uuid

import pytest
from dbsync.client.wsclient import SyncClient
from dbsync.core import extend
from dbsync.server.wsserver import SyncServer
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from dbsync.dialects import GUID
from dbsync.utils import generate_secret
import dbsync
from dbsync import client, models, core

server_db = "./test_server.db"
client_db = "./test_client.db"


PORT = 7081
SERVER_URL=f"ws://localhost:{PORT}/"

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


def addstuff(Session: sessionmaker):
    a1 = A(name="first a")
    a2 = A(name="second a")
    a3 = A(name="third a")
    b1 = B(name="first b", a=a1)
    b2 = B(name="second b", a=a1)
    b3 = B(name="third b", a=a2)
    session = Session()
    session.add_all([a1, a2, a3,  b1, b2, b3])
    session.commit()

def changestuff(Session: sessionmaker):
    session = Session()
    a1, a2, a3 = session.query(A).all()
    b1, b2, b3 = session.query(B).all()
    a1.name = "first a modified"
    b2.a = a2
    session.delete(b3)
    session.commit()


@core.with_listening(False)
def teardown(Session: sessionmaker):
    session = Session()
    for a in session.query(A).all():
        session.delete(a)
    for b in session.query(B).all():
        session.delete(b)
    for op in session.query(models.Operation).all():
        session.delete(op)
    session.commit()


# demoes the the extension, the two handler functions do nothing

def load_data(o: B) -> str:
    print(f"LOAD: {o}")
    return "loaded"


def save_data(o:B, val: str) -> None:
    print(f"SAVE:{o} ({val})")


extend(
    B,
    "data",
    String,
    load_data,
    save_data
)
