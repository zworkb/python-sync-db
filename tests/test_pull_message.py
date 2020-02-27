from nose.tools import *
import datetime
import logging
import json

from dbsync.lang import *
from dbsync import models, core
from dbsync.messages.codecs import SyncdbJSONEncoder
from dbsync.messages.pull import PullMessage

from tests.models import A, B, Session


def addstuff():
    a1 = A(name="first a")
    a2 = A(name="second a")
    b1 = B(name="first b", a=a1)
    b2 = B(name="second b", a=a1)
    b3 = B(name="third b", a=a2)
    session = Session()
    session.add_all([a1, a2, b1, b2, b3])
    version = models.Version()
    version.created = datetime.datetime.now()
    session.add(version)
    session.flush()
    version_id = version.version_id
    session.commit()
    session = Session()
    for op in session.query(models.Operation):
        op.version_id = version_id
    session.commit()

def setup(): pass

@core.with_listening(False)
def teardown():
    session = Session()
    # map(session.delete, session.query(A))

    for a in session.query(A).all():
        session.delete(a)
    for b in session.query(B).all():
        session.delete(b)
    for op in session.query(models.Operation).all():
        session.delete(op)
    for ver in session.query(models.Version).all():
        session.delete(ver)

    session.commit()


@with_setup(setup, teardown)
def test_create_message():
    addstuff()
    session = Session()
    message = PullMessage()
    version = session.query(models.Version).first()
    message.add_version(version)
    message_json = message.to_json()
    pullmessage_json = PullMessage(message_json).to_json()

    assert message_json == pullmessage_json


@with_setup(setup, teardown)
def test_encode_message():
    addstuff()
    session = Session()
    message = PullMessage()
    version = session.query(models.Version).first()
    message.add_version(version)
    msg_json = json.dumps(message.to_json(), cls=SyncdbJSONEncoder)
    assert message.to_json() == json.loads(msg_json)


@with_setup(setup, teardown)
def test_message_query():
    addstuff()
    session = Session()
    message = PullMessage()
    version = session.query(models.Version).first()
    message.add_version(version)
    # test equal representation, because the test models are well printed
    for b in session.query(B):
        assert repr(b) == repr(message.query(B).filter(
                attr('id') == b.id).all()[0])
    for op in session.query(models.Operation):
        assert repr(op) == repr(message.query(models.Operation).filter(
                attr('order') == op.order).all()[0])
    try:
        message.query(1)
        raise Exception("Message query did not fail")
    except TypeError:
        pass


@with_setup(setup, teardown)
def test_message_does_not_contaminate_database():
    addstuff()
    session = Session()
    message = PullMessage()
    version = session.query(models.Version).first()
    message.add_version(version)
    # test that the are no unversioned operations
    assert not session.query(models.Operation).\
        filter(models.Operation.version_id == None).all()
