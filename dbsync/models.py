"""
Internal model used to keep track of versions and operations.
"""
from typing import Union, Optional, Tuple

from sqlalchemy.sql import Join

try:
    from typing import Protocol
except ImportError:
    from typing import _Protocol as Protocol


from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, BigInteger, Table
from sqlalchemy.orm import relationship, backref, validates, Session, Mapper
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from websockets import WebSocketServerProtocol, WebSocketCommonProtocol

from dbsync.dialects import GUID
from dbsync.lang import *
from dbsync.utils import get_pk, query_model, properties_dict
from dbsync.logs import get_logger


logger = get_logger(__name__)


class SQLClass(Protocol):

    """duck typing for sqlalchemy content class"""
    __table__: Table
    __tablename__: str
    __name__: str
    __mapper__: Mapper

    mapped_table: Union[Table, Join]
    primary_key: Tuple[Column, ...]


#: Database tables prefix.
tablename_prefix = "sync_"


class PrefixTables(DeclarativeMeta):
    def __init__(cls, classname, bases, dict_):
        if '__tablename__' in dict_:
            tn = dict_['__tablename__']
            cls.__tablename__ = dict_['__tablename__'] = tablename_prefix + tn
        return super(PrefixTables, cls).__init__(classname, bases, dict_)

Base = declarative_base(metaclass=PrefixTables)


class ContentType(Base):
    """A weak abstraction over a database table."""

    __tablename__ = "content_types"

    content_type_id = Column(BigInteger, primary_key=True)
    table_name = Column(String(500))
    model_name = Column(String(500))

    def __repr__(self):
        return "<ContentType id: {0}, table_name: {1}, model_name: {2}>".\
            format(self.content_type_id, self.table_name, self.model_name)


class Node(Base):
    """
    A node registry.

    A node is a client application installed somewhere else.
    """

    __tablename__ = "nodes"

    node_id = Column(Integer, primary_key=True)
    registered = Column(DateTime)
    registry_user_id = Column(Integer)
    secret = Column(String(128))

    def __init__(self, *args, **kwargs):
        super(Node, self).__init__(*args, **kwargs)

    def __repr__(self):
        return "<Node node_id: {0}, registered: {1}, "\
            "registry_user_id: {2}, secret: {3}>".\
            format(self.node_id,
                   self.registered,
                   self.registry_user_id,
                   self.secret)


class Version(Base):
    """
    A database version.

    These are added for each 'push' accepted and executed without
    problems.
    """

    __tablename__ = "versions"

    version_id = Column(Integer, primary_key=True)
    node_id = Column(Integer, ForeignKey(Node.__tablename__ + ".node_id"))
    created = Column(DateTime)

    node = relationship(Node)

    def __repr__(self):
        return "<Version version_id: {0}, created: {1}>".\
            format(self.version_id, self.created)


class OperationError(Exception): pass


class Operation(Base):
    """
    A database operation (insert, delete or update).

    The operations are grouped in versions and ordered as they are
    executed.
    """

    __tablename__ = "operations"

    # row_id = Column(Integer)
    row_id = Column(GUID)
    version_id = Column(
        Integer,
        ForeignKey(Version.__tablename__ + ".version_id"),
        nullable=True)
    content_type_id = Column(BigInteger)
    tracked_model = None # to be injected
    command = Column(String(1))
    command_options = ('i', 'u', 'd')
    order = Column(Integer, primary_key=True)

    version = relationship(Version, backref=backref("operations", lazy="joined"))

    @validates('command')
    def validate_command(self, key, command):
        assert command in self.command_options
        return command

    def __repr__(self):
        return "<Operation row_id: {0}, model: {1}, command: {2}>".\
            format(self.row_id, self.tracked_model, self.command)

    def references(self, obj):
        "Whether this operation references the given object or not."
        if self.row_id != getattr(obj, get_pk(obj), None):
            return False
        model = self.tracked_model
        if model is None:
            return False # operation doesn't even refer to a tracked model
        return model is type(obj)

    def perform(operation, container, session, node_id=None):
        """
        Performs *operation*, looking for required data in
        *container*, and using *session* to perform it.

        *container* is an instance of
        dbsync.messages.base.BaseMessage.

        *node_id* is the node responsible for the operation, if known
        (else ``None``).

        If at any moment this operation fails for predictable causes,
        it will raise an *OperationError*.
        """
        model = operation.tracked_model
        if model is None:
            raise OperationError("no content type for this operation", operation)

        if operation.command == 'i':
            obj = query_model(session, model).\
                filter(getattr(model, get_pk(model)) == operation.row_id).first()
            pull_obj = container.query(model).\
                filter(attr('__pk__') == operation.row_id).first()
            if pull_obj is None:
                raise OperationError(
                    "no object backing the operation in container", operation)
            if obj is None:
                session.add(pull_obj)
            else:
                # Don't raise an exception if the incoming object is
                # exactly the same as the local one.
                if properties_dict(obj) == properties_dict(pull_obj):
                    logger.warning("insert attempted when an identical object "
                                   "already existed in local database: "
                                   "model {0} pk {1}".format(model.__name__,
                                                              operation.row_id))
                else:
                    raise OperationError(
                        "insert attempted when the object already existed: "
                        "model {0} pk {1}".format(model.__name__,
                                                   operation.row_id))

        elif operation.command == 'u':
            obj = query_model(session, model).\
                filter(getattr(model, get_pk(model)) == operation.row_id).first()
            if obj is None:
                # For now, the record will be created again, but is an
                # error because nothing should be deleted without
                # using dbsync
                # raise OperationError(
                #     "the referenced object doesn't exist in database", operation)
                logger.warning(
                    "The referenced object doesn't exist in database. "
                    "Node %s. Operation %s",
                    node_id,
                    operation)

            pull_obj = container.query(model).\
                filter(attr('__pk__') == operation.row_id).first()
            if pull_obj is None:
                raise OperationError(
                    "no object backing the operation in container", operation)
            session.merge(pull_obj)

        elif operation.command == 'd':
            obj = query_model(session, model, only_pk=True).\
                filter(getattr(model, get_pk(model)) == operation.row_id).first()
            if obj is None:
                # The object is already deleted in the server
                # The final state in node and server are the same. But
                # it's an error because nothing should be deleted
                # without using dbsync
                logger.warning(
                    "The referenced object doesn't exist in database. "
                    "Node %s. Operation %s",
                    node_id,
                    operation)
            else:
                session.delete(obj)

        else:
            raise OperationError(
                "the operation doesn't specify a valid command ('i', 'u', 'd')",
                operation)

    async def request_payloads_for_extensions(obj: SQLClass, websocket: WebSocketCommonProtocol):
        ext: ExtensionField
        extensions = model_extensions.get(type(obj).__name__, {})

        for field, ext in list(extensions.items()):
            reqfn = ext.request_payload_fn
            try:
                await reqfn(obj, getattr(obj, field, None))
            except:
                logger.exception(
                    "Couldn't save extension %s for object %s", field, obj)

    async def perform_async(operation: "Operation", container: "BaseMessage", session: Session, node_id=None,
                      websocket: Optional[WebSocketCommonProtocol] = None
                      ):
        """
        Performs *operation*, looking for required data in
        *container*, and using *session* to perform it.

        *container* is an instance of
        dbsync.messages.base.BaseMessage.

        *node_id* is the node responsible for the operation, if known
        (else ``None``).

        If at any moment this operation fails for predictable causes,
        it will raise an *OperationError*.
        """
        model = operation.tracked_model
        if model is None:
            raise OperationError("no content type for this operation", operation)

        if operation.command == 'i':
            # check if the given object is already in the database
            obj = query_model(session, model).\
                filter(getattr(model, get_pk(model)) == operation.row_id).first()

            # retrieve the object from the PullMessage
            pull_obj = container.query(model).\
                filter(attr('__pk__') == operation.row_id).first()
            if pull_obj is None:
                raise OperationError(
                    "no object backing the operation in container", operation)
            if obj is None:
                # await request_payloads_for_extensions(pull_obj, websocket)
                session.add(pull_obj)
            else:
                # Don't raise an exception if the incoming object is
                # exactly the same as the local one.
                if properties_dict(obj) == properties_dict(pull_obj):
                    logger.warning("insert attempted when an identical object "
                                   "already existed in local database: "
                                   "model {0} pk {1}".format(model.__name__,
                                                              operation.row_id))
                else:
                    raise OperationError(
                        "insert attempted when the object already existed: "
                        "model {0} pk {1}".format(model.__name__,
                                                   operation.row_id))

        elif operation.command == 'u':
            obj = query_model(session, model).\
                filter(getattr(model, get_pk(model)) == operation.row_id).first()
            if obj is None:
                # For now, the record will be created again, but is an
                # error because nothing should be deleted without
                # using dbsync
                # raise OperationError(
                #     "the referenced object doesn't exist in database", operation)
                logger.warning(
                    "The referenced object doesn't exist in database. "
                    "Node %s. Operation %s",
                    node_id,
                    operation)

            # get new object from the PushMessage
            pull_obj = container.query(model).\
                filter(attr('__pk__') == operation.row_id).first()
            if pull_obj is None:
                raise OperationError(
                    "no object backing the operation in container", operation)
            session.merge(pull_obj)

        elif operation.command == 'd':
            obj = query_model(session, model, only_pk=True).\
                filter(getattr(model, get_pk(model)) == operation.row_id).first()
            if obj is None:
                # The object is already deleted in the server
                # The final state in node and server are the same. But
                # it's an error because nothing should be deleted
                # without using dbsync
                logger.warning(
                    "The referenced object doesn't exist in database. "
                    "Node %s. Operation %s",
                    node_id,
                    operation)
            else:
                session.delete(obj)

        else:
            raise OperationError(
                "the operation doesn't specify a valid command ('i', 'u', 'd')",
                operation)

