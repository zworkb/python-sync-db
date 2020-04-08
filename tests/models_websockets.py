import json
import multiprocessing as mp
import datetime
import os
import time
import uuid
from typing import Union

import pytest
from websockets import WebSocketCommonProtocol

from dbsync.client.wsclient import SyncClient
from dbsync.models import extend, Operation, SQLClass
from dbsync.server.wsserver import SyncServer
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship, sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base

from dbsync.dialects import GUID
from dbsync.utils import generate_secret
import dbsync
from dbsync import client, models, core

server_db = "./test_server.db"


def client_db(pid=None):
    if pid is None:
        pid = os.getpid()
    return f"./test_client_{pid}.db"


PORT = 7081
SERVER_URL = f"ws://localhost:{PORT}/"

Base = declarative_base()


def datapath(fname="", mode=None, pid=""):
    if mode is None:
        mode = core.mode

    if pid is None:
        pid = os.getpid()

    path0 = "server" if mode == "server" else f"client/{pid}"

    path = f"./testblobs/{path0}"
    if not os.path.exists(path):
        os.makedirs(path, 0o777, True)
    res = os.path.join(path, fname)

    return res


@client.track
class A(Base):
    __tablename__ = "test_a"

    id = Column(GUID, primary_key=True, default=lambda: uuid.uuid4())
    name = Column(String)
    pid = Column(String)

    def __repr__(self):
        return u"<A id:{0} name:{1}>".format(self.id, self.name)


@client.track
class B(Base):
    __tablename__ = "test_b"

    id = Column(GUID, primary_key=True, default=lambda: uuid.uuid4())
    name = Column(String)
    a_id = Column(GUID, ForeignKey("test_a.id"))
    data = Column(String)
    pid = Column(String)

    a = relationship(A, backref="bs")

    def __repr__(self):
        return u"<B id:{0} name:{1} a_id:{2}>".format(
            self.id, self.name, self.a_id)


def addstuff(Session: sessionmaker, par: Union[str, int] = ""):
    print("ADDSTUFF", par)
    a1 = A(name=f"first a {par}", pid=par)
    a2 = A(name=f"second a {par}", pid=par)
    a3 = A(name=f"third a {par}", pid=par)
    b1 = B(name=f"first b {par}", a=a1, data=f"b1{par}.txt", pid=par)
    b2 = B(name=f"second b {par}", a=a1, data=f"b2{par}.txt", pid=par)
    b3 = B(name=f"third b {par}", a=a2, pid=par)

    with open(datapath(b1.data, pid=par), "w") as fh:
        fh.write("b1" * 10_000)

    with open(datapath(b2.data, pid=par), "w") as fh:
        fh.write("b2" * 10_000)

    session = Session()
    session.add_all([a1, a2, a3,  b1, b2, b3])
    session.commit()


def changestuff(Session: sessionmaker, par=""):
    session = Session()
    a1, a2, a3 = session.query(A).all()
    b1, b2, b3 = session.query(B).all()
    a1.name = "first a modified"
    b2.a = a2
    # lets change b1
    b1.name = f"first b {par} updated"
    # lets change the files of b2
    b2.name = f"second b {par} updated"
    with open(datapath(b2.data, pid=par), "w") as fh:
        fh.write(f"b2 {par} changed")

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


# demos the the extension, the two handler functions do nothing


async def send_payload_data(obj: B, fieldname: str, websocket: WebSocketCommonProtocol, session: Session):
    """
    This is an example function for sending payload data.
    in this example we first send a flag that signifies wether we have a payload to send
    and then send the payload
    """
    assert core.mode == 'client'
    print(f"SEND PAYLOAD DATA: obj{obj}")
    if obj.data:
        await websocket.send(json.dumps(True))
        with open(datapath(obj.data, pid=obj.pid), "r") as fh:
            await websocket.send(fh.read())
    else:
        await websocket.send(json.dumps(False))


async def receive_payload_data(op: Operation, o: B, fieldname: str, websocket: WebSocketCommonProtocol, session: Session):
    """
    this example function shows the receive end for the payload data
    """
    assert core.mode == 'server'
    flag_ = await websocket.recv()
    flag = json.loads(flag_)
    print(f"!!RECEIVE_PAYLOAD: field:{fieldname}, flag: {flag}, obj:{o}, op:{op}")
    if flag:
        payload = await websocket.recv()
        with open(datapath(o.data), "w") as fh:
            fh.write(payload)


extend(
    B,
    "data",
    String,
    receive_payload_fn=receive_payload_data,
    send_payload_fn=send_payload_data

)

# extend(
#     B,
#     "otherdata",
#     String,
#     # load_data,
#     # save_data,
#     receive_payload_fn=receive_payload_data,
#     send_payload_fn=send_payload_data
#
# )