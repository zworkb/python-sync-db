"""
Internal model used to keep track of versions and operations.
"""
import inspect
import json
from dataclasses import dataclass
from typing import Union, Optional, Tuple, Callable, Any, Coroutine, Dict, Type

from sqlalchemy.sql import Join
from sqlalchemy.sql.type_api import TypeEngine

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

# Extensions Stuff


@dataclass
class ExtensionField:
    klass: TypeEngine
    loadfn: Optional[Callable[[SQLClass], Any]] = None
    savefn: Optional[Callable[[SQLClass, Any], None]] = None
    deletefn: Optional[Callable[[SQLClass, SQLClass], None]] = None

    receive_payload_fn: Optional[Callable[["Operation", SQLClass, str, WebSocketServerProtocol], Coroutine[Any, Any, None]]] = None
    """is called on server side to request payload from the client side"""
    send_payload_fn: Optional[Callable[[SQLClass, str, WebSocketCommonProtocol], Coroutine[Any, Any, None]]] = None
    """is called on client side as to accept the request from server side and send over the payload"""


Extension = Dict[
    str,
    ExtensionField
]

Extensions = Dict[
    str,
    Extension
]

#: Extensions to tracked models.
model_extensions: Extensions = {}


def extend(
        model: SQLClass,
        fieldname: str,
        fieldtype: Union[Type[TypeEngine], TypeEngine],
        loadfn: Callable[[SQLClass], Any] = None,
        savefn: Callable[[SQLClass, Any], None] = None,
        deletefn: Optional[Callable[[SQLClass, SQLClass], None]] = None,
        receive_payload_fn: Optional[Callable[["Operation", SQLClass, str, WebSocketServerProtocol], Coroutine[Any, Any, None]]] = None,
        send_payload_fn: Optional[Callable[[SQLClass, str, WebSocketCommonProtocol], Coroutine[Any, Any, None]]] = None
):
    """
    Extends *model* with a field of name *fieldname* and type
    *fieldtype*.

    *fieldtype* should be an instance of a SQLAlchemy type class, or
    the class itself.

    *loadfn* is a function called to populate the extension. It should
    accept an instance of the model and should return the value of the
    field.

    *savefn* is a function called to persist the field. It should
    accept the instance of the model and the field's value. It's
    return value is ignored.

    *deletefn* is a function called to revert the side effects of
    *savefn* for old values. It gets called after an update on the
    object with the old object's values, or after a delete. *deletefn*
    is optional, and if given it should be a function of two
    arguments: the first is the object in the previous state, the
    second is the object in the current state.

    Original proposal: https://gist.github.com/kklingenberg/7336576
    """
    assert inspect.isclass(model), "model must be a mapped class"
    assert isinstance(fieldname, str) and bool(fieldname),\
        "field name must be a non-empty string"
    assert not hasattr(model, fieldname),\
        "the model {0} already has the attribute {1}".\
        format(model.__name__, fieldname)
    assert loadfn is None or inspect.isroutine(loadfn), "load function must be a callable"
    assert loadfn is None or inspect.isroutine(savefn), "save function must be a callable"
    assert deletefn is None or inspect.isroutine(deletefn),\
        "delete function must be a callable"
    extension: Extension = model_extensions.get(model.__name__, {})
    type_: TypeEngine = fieldtype if not inspect.isclass(fieldtype) else fieldtype()
    extension[fieldname] = ExtensionField(type_, loadfn, savefn, deletefn, receive_payload_fn, send_payload_fn)
    model_extensions[model.__name__] = extension


def get_model_extension_for_obj(obj: SQLClass) -> Extension:
    ext = model_extensions.get(type(obj).__name__, {})

    return ext


def _has_extensions(obj):
    return bool(model_extensions.get(type(obj).__name__, {}))

def _has_delete_functions(obj):
    ext: ExtensionField
    return any(
        ext.deletefn is not None
        for ext in list(model_extensions.get(
            type(obj).__name__, {}).values()))


def save_extensions(obj):
    """
    Executes the save procedures for the extensions of the given
    object.
    """
    ext: ExtensionField
    extensions = model_extensions.get(type(obj).__name__, {})
    for field, ext in list(extensions.items()):
        savefn = ext.savefn
        try:
            savefn(obj, getattr(obj, field, None))
        except:
            logger.exception(
                "Couldn't save extension %s for object %s", field, obj)


def create_field_request_message(obj: SQLClass, field: str):
    id_field = get_pk(obj)
    res = dict(
        type="request_field_payload",
        field_name=field,
        table=obj.__tablename__,
        id_field=id_field,
        id=getattr(obj, id_field),
        class_name=obj.__class__.__name__,
        package_name=obj.__class__.__module__
    )

    from dbsync.messages.codecs import SyncdbJSONEncoder
    return json.dumps(res, cls=SyncdbJSONEncoder)


async def request_payloads_for_extension(operation: "Operation", obj: SQLClass, websocket: WebSocketCommonProtocol):
    """
    requests payload data for a given object via a given websocket
    """
    ext: ExtensionField
    extensions = model_extensions.get(type(obj).__name__, {})

    for field, ext in list(extensions.items()):
        if ext.receive_payload_fn:
            try:
                await websocket.send(create_field_request_message(obj, field))
                await ext.receive_payload_fn(operation, obj, field, websocket)
            except Exception as e:
                logger.exception(
                    f"Couldn't request extension {field} for object {obj}")
                raise


def delete_extensions(old_obj: SQLClass, new_obj: SQLClass):
    """
    Executes the delete procedures for the extensions of the given
    object. *old_obj* is the object in the previous state, and
    *new_obj* is the object in the current state (or ``None`` if the
    object was deleted).
    """
    ext: ExtensionField
    extensions = model_extensions.get(type(old_obj).__name__, {})
    for field, ext in list(extensions.items()):
        deletefn = ext.deletefn
        if deletefn is not None:
            try:
                deletefn(old_obj, new_obj)
            except:
                logger.exception(
                    f"Couldn't delete extension {field} for object {new_obj}")


# Database model
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
                await request_payloads_for_extension(operation, pull_obj, websocket)
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

