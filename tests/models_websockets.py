import json
import multiprocessing as mp
import datetime
import os
import time
import uuid
from typing import Union, Optional

import pytest
from websockets import WebSocketCommonProtocol

from dbsync.client.wsclient import SyncClient
from dbsync.createlogger import create_logger
from dbsync.models import Operation, SQLClass, add_field_extension, ExtensionField, extend_model, SkipOperation
from dbsync.server.wsserver import SyncServer
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship, sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base

from dbsync.dialects import GUID
from dbsync.utils import generate_secret
import dbsync
from dbsync import client, models, core

server_db = "./test_server.db"
server_uri_postgres = 'postgresql:///synctest'

logger = create_logger("models_websockets")


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
    key = Column(String)
    name = Column(String)
    pid = Column(String)
    comment = Column(String)
    comment_after = Column(String)
    comment_after_update = Column(String)

    def __repr__(self):
        return u"<A id:{0} name:{1}>".format(self.id, self.name)

    __str__ = __repr__


@client.track("push")
class B(Base):
    __tablename__ = "test_b"

    id = Column(GUID, primary_key=True, default=lambda: uuid.uuid4())
    key = Column(String)
    name = Column(String)
    a_id = Column(GUID, ForeignKey("test_a.id"))
    data = Column(String)
    pid = Column(String)
    comment = Column(String)
    comment_after = Column(String)

    a = relationship(A, backref="bs")

    def __repr__(self):
        return u"<B id:{0} name:{1} a_id:{2}>".format(
            self.id, self.name, self.a_id)


def addstuff(Session: sessionmaker, par: Union[str, int] = ""):
    logger.debug("ADDSTUFF", par)
    a1 = A(key="a1", name=f"first a {par}", pid=par)
    a2 = A(key="a2", name=f"second a {par}", pid=par)
    a3 = A(key="a3", name=f"third a {par}", pid=par)
    a4 = A(key="a4", name=f"dontsync_insert {par}", pid=par)
    a5 = A(key="a5", name=f"dontsync_update {par}", pid=par)

    b1 = B(key="b1", name=f"first b {par}", a=a1, data=f"b1{par}.txt", pid=par)
    b2 = B(key="b2", name=f"second b {par}", a=a1, data=f"b2{par}.txt", pid=par)
    b3 = B(key="b3", name=f"third b {par}", a=a2, pid=par)


    with open(datapath(b1.data, pid=par), "w") as fh:
        fh.write("b1" * 10_000)

    with open(datapath(b2.data, pid=par), "w") as fh:
        fh.write("b2" * 10_000)

    session = Session()
    session.add_all([a1, a2, a3, a4, a5, b1, b2, b3])
    session.commit()


def changestuff(Session: sessionmaker, par=""):
    session = Session()
    a1, a2, a3, a4, a5 = session.query(A).all()[:5]
    b1, b2, b3 = session.query(B).all()[:3]
    a1.name = f"first a {par} modified"
    a5.name = f"a5 modified"
    b2.a = a2
    # lets change b1
    b1.name = f"first b {par} updated"
    # lets change the files of b2
    b2.name = f"second b {par} updated"
    with open(datapath(b2.data, pid=par), "w") as fh:
        fh.write(f"b2 {par} changed")


    session.delete(b3)
    session.commit()


def deletestuff(Session: sessionmaker, par=""):
    session = Session()
    a1, a2, a3, a4, a5 = session.query(A).all()[:5]
    session.delete(a3)
    session.delete(a5)
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


async def receive_payload_data(op: Operation, o: B, fieldname: str, websocket: WebSocketCommonProtocol,
                               session: Session):
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


def before_b(session: Session, operation: Operation, model: SQLClass, old_model: Optional[SQLClass]):
    print("***************before_name:", operation.command, model, session)
    model.comment = f"processed_before_{operation.command}: {model.id}"
    print(model.comment)


def after_b(session: Session, operation: Operation, model: SQLClass):
    """
    after update it is important to call 'merge' if model is changed in this function
    since model is not part of the session
    """
    print("***************before_name:", operation.command, model, session)
    print(model.comment)
    if operation.command != 'd':
        model.comment_after = f"processed_after_{operation.command}: {model.id}"

    if operation.command == 'u':
        session.merge(model)
    print(model.comment)


def before_a_insert(session: Session, obj:A):
    if obj.key == "a4":
        raise SkipOperation
    obj.comment_after = f"before insert A:{obj.id}"


def after_a_insert(session: Session, obj:A):
    obj.comment_after = f"after insert A:{obj.id}"


def before_a_update(session: Session, obj: A, old_object: A):
    if obj.key == "a5":
        raise SkipOperation
    obj.comment = f"update for A: {obj.id}"


def after_a_update(session: Session, obj: A):
    obj.comment_after_update = f"after update for A: {obj.id}"
    session.merge(obj)
    print("after update:", obj.comment_after_update)


def before_a_delete(session: Session, obj:A):
    if obj.key == "a3":
        print(f"skipping delete for {obj}")
        raise SkipOperation


def before_a_tracking(session: Session, command: str, obj: SQLClass):
    print(f"------      before tracking {command} on {obj.key}/{obj.name}")
    if "donttrack" in obj.name:
        raise SkipOperation

    if obj.name == "a5 modified":
        print(f"obj: syncing {obj}")


extend_model(
    A,
    before_insert_fn=before_a_insert,
    after_insert_fn=after_a_insert,
    before_update_fn=before_a_update,
    after_update_fn=after_a_update,
    before_delete_fn=before_a_delete,
    before_tracking_fn=before_a_tracking
)

extend_model(
    B,
    before_operation_fn=before_b,
    after_operation_fn=after_b
)

add_field_extension(
    B,
    "data",
    ExtensionField(
        String,
        receive_payload_fn=receive_payload_data,
        send_payload_fn=send_payload_data
    )
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
