"""
Internal model used to keep track of versions and operations.
"""
import inspect
import json
from dataclasses import dataclass, field
from typing import Union, Optional, Tuple, Callable, Any, Coroutine, Dict, Type, List, _SpecialForm
from copy import deepcopy

import sqlalchemy
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import Join
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.exc import NoSuchColumnError

# from dbsync.socketserver import Connection

try:
    from typing import Protocol
except ImportError:
    from typing import _Protocol as Protocol

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, BigInteger, Table
from sqlalchemy.orm import relationship, backref, validates, Session, Mapper, Query
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from websockets import WebSocketServerProtocol, WebSocketCommonProtocol

from dbsync.dialects import GUID
from dbsync.lang import *
from dbsync.utils import get_pk, query_model, properties_dict, copy
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


# ExtensionRegistry Stuff


@dataclass
class ExtensionField:
    """
    Extends *model* with a field of name *fieldname* and type
    *fieldtype*.
    Original proposal: https://gist.github.com/kklingenberg/7336576
    """

    klass: TypeEngine = None
    """
    *klass* should be an instance of a SQLAlchemy type class, or
    the class itself.
    """
    loadfn: Optional[Callable[[SQLClass], Any]] = None
    """
    *loadfn* is a function called to populate the extension. It should
    accept an instance of the model and should return the value of the
    field.
    """
    savefn: Optional[Callable[[SQLClass, Any], None]] = None
    """
    *savefn* is a function called to persist the field. It should
    accept the instance of the model and the field's value. It's
    return value is ignored."""
    deletefn: Optional[Callable[[SQLClass, SQLClass], None]] = None
    """
    *deletefn* is a function called to revert the side effects of
    *savefn* for old values. It gets called after an update on the
    object with the old object's values, or after a delete. *deletefn*
    is optional, and if given it should be a function of two
    arguments: the first is the object in the previous state, the
    second is the object in the current state."""

    def __post_init__(self):
        assert self.loadfn is None or inspect.isroutine(self.loadfn), "load function must be a callable"
        assert self.savefn is None or inspect.isroutine(self.savefn), "save function must be a callable"
        assert self.deletefn is None or inspect.isroutine(self.deletefn), \
            "delete function must be a callable"


@dataclass
class Extension:
    """
    all before_* functions can skip the operation by raising SkipOperation
    """
    before_operation_fn: Optional[Callable[[Session, "Operation", SQLClass, Optional[SQLClass]], None]] = None
    """is called before an object is inserted/updated/deleted"""
    before_insert_fn: Optional[Callable[[Session, SQLClass], None]] = None
    """is called before an object is inserted"""
    before_update_fn: Optional[Callable[[Session, SQLClass, SQLClass], None]] = None
    """is called before an object is updated"""
    before_delete_fn: Optional[Callable[[Session, SQLClass], None]] = None
    """is called before an object is deleted"""
    before_tracking_fn: Optional[Callable[[Session, str, SQLClass], None]] = None
    """is called before an object is tracked"""
    after_tracking_fn: Optional[Callable[[Session, "Operation", SQLClass], None]] = None
    """is called after an object is tracked"""
    before_insert_tracking_fn: Optional[Callable[[Session, SQLClass], None]] = None
    """is called before an object is tracked for insert"""
    before_update_tracking_fn: Optional[Callable[[Session, SQLClass], None]] = None
    """is called before an object is tracked for update"""
    before_delete_tracking_fn: Optional[Callable[[Session, SQLClass], None]] = None
    """is called before an object is tracked for delete"""

    after_operation_fn: Optional[Callable[[Session, "Operation", SQLClass], None]] = None
    """is called after an object is inserted/updated/deleted"""
    after_insert_fn: Optional[Callable[[Session, SQLClass], None]] = None
    """is called after an object is inserted"""
    after_update_fn: Optional[Callable[[Session, SQLClass], None]] = None
    """is called after an object is updated"""
    after_delete_fn: Optional[Callable[[Session, SQLClass], None]] = None
    """is called after an object is deleted"""

    receive_payload_fn: Optional[
        Callable[["Operation", SQLClass, WebSocketCommonProtocol, Session], Coroutine[Any, Any, None]]] = None
    """is called on server side to request payload from the client side"""
    send_payload_fn: Optional[
        Callable[[SQLClass, WebSocketCommonProtocol, Session], Coroutine[Any, Any, None]]] = None
    """is called on client side as to accept the request from server side and send over the payload"""

    filter_operations_fn: Optional[
        Callable[["Connection", Session, Query], Query]
    ] = None
    """is called after operations have been selected on server side
    here you can refine the query on your needs
    """
    before_server_add_operation_fn: Optional[Callable[["Connection", Session, "Operation", SQLClass], None]] = None
    """is called before an operation is added to the pull message on the server side"""
    before_client_add_object_fn: Optional[Callable[[Session, "Operation", SQLClass], None]] = None
    """is called before the object is pushed on client side"""


    fields: Dict[str, ExtensionField] = field(default_factory=dict)


class ExtensionRegistry(dict):
    def add_extension(self, model: Union[DeclarativeMeta, _SpecialForm],  extension: Extension):
        name = "Any" if model is Any else model.__name__
        if not name in self:
            self[name] = []

        self[name].append(extension)


def get_model_extensions_for_obj(obj: SQLClass, include_any=True) -> List[Extension]:
    return get_model_extensions_for_class(type(obj), include_any=include_any)


def get_model_extensions_for_class(klass: DeclarativeMeta, include_any=True) -> List[Extension]:
    """
    when include_any is true -> include all extensions mapped to Any before
    """
    extensions = model_extension_registry.get(klass.__name__, []) if klass is not Any else []
    return model_extension_registry.get("Any", []) + extensions if include_any else extensions


#: ExtensionRegistry to tracked models.
model_extension_registry: ExtensionRegistry = ExtensionRegistry()


def call_filter_operations(connection: "Connection", session: Session, ops: Query):
    extensions: List[Extension] = get_model_extensions_for_class(Any)
    for extension in extensions:
        if extension.filter_operations_fn:
            ops = extension.filter_operations_fn(connection, session, ops)

    return ops

def call_before_server_add_operation_fn(connection: "Connection", session: Session, op:"Operation", obj:SQLClass):
    """
    there we cann check permissions before an operation is added to the pull_message on server side
    """
    extensions: List[Extension] = get_model_extensions_for_obj(obj)
    for extension in extensions:
        if extension.before_server_add_operation_fn:
            extension.before_server_add_operation_fn(connection, session, op, obj)


def call_before_tracking_fn(session: Session, command: str, obj: SQLClass):
    extensions: List[Extension] = get_model_extensions_for_obj(obj)
    for extension in extensions:
        if extension.before_tracking_fn:
            extension.before_tracking_fn(session, command, obj)


def call_after_tracking_fn(session: Session, op: "Operation", obj: SQLClass):
    extensions: List[Extension] = get_model_extensions_for_obj(obj)
    for extension in extensions:
        if extension.after_tracking_fn:
            extension.after_tracking_fn(session, op, obj)


def add_field_extension(model: DeclarativeMeta,
                        fieldname: str, extension_field: ExtensionField):
    assert inspect.isclass(model), "model must be a mapped class"
    assert isinstance(fieldname, str) and bool(fieldname), \
        "field name must be a non-empty string"

    extension = Extension()

    # type_: TypeEngine = fieldtype if not inspect.isclass(fieldtype) else fieldtype()
    extension.fields[fieldname] = extension_field
    model_extension_registry.add_extension(model, extension)


def extend_model(klass: Union[DeclarativeMeta, _SpecialForm] = Any, **kw) -> None:
    model_extension_registry.add_extension(klass, Extension(**kw))


def _has_extensions(obj: SQLClass) -> bool:
    return bool(get_model_extensions_for_obj(obj))


def _has_delete_functions(obj):
    ext: ExtensionField

    extensions: List[Extension] = get_model_extensions_for_obj(obj)

    res = any([
        any(
            extfield.deletefn is not None
            for extfield in list(extension.fields.values()))
        for extension
        in extensions
    ])

    return res

def save_extensions(obj):
    """
    Executes the save procedures for the extensions of the given
    object.
    """
    extfield: ExtensionField
    extensions: List[Extension] = get_model_extensions_for_obj(obj)
    for extension in extensions:
        for fieldname, extfield in list(extension.fields.items()):
            savefn = extfield.savefn
            try:
                if savefn:
                    savefn(obj, getattr(obj, fieldname, None))
            except:
                logger.exception(
                    "Couldn't save extension %s for object %s", fieldname, obj)


def create_payload_request_message(obj: SQLClass, name: str = "default"):
    id_field = get_pk(obj)
    res = dict(
        type="request_field_payload",
        name=name,
        table=obj.__tablename__,
        id_field=id_field,
        id=getattr(obj, id_field),
        class_name=obj.__class__.__name__,
        package_name=obj.__class__.__module__
    )

    from dbsync.messages.codecs import SyncdbJSONEncoder
    return json.dumps(res, cls=SyncdbJSONEncoder)


async def request_payloads_for_extension(operation: "Operation", obj: SQLClass,
                                         websocket: WebSocketCommonProtocol, session: Session):
    """
    requests payload data for a given object via a given websocket, invoked by perform_async()
    """
    extensions: List[Extension] = get_model_extensions_for_obj(obj)

    for extension in extensions:
        if extension.receive_payload_fn:
            try:
                await websocket.send(create_payload_request_message(obj))
                await extension.receive_payload_fn(operation, obj, websocket, session)
            except Exception as e:
                logger.exception(
                    f"Couldn't request extension data for object {obj}")
                raise


def delete_extensions(old_obj: SQLClass, new_obj):
    """
    Executes the delete procedures for the extensions of the given
    object. *old_obj* is the object in the previous state, and
    *new_obj* is the object in the current state (or ``None`` if the
    object was deleted).
    """
    extfield: ExtensionField
    extensions = get_model_extensions_for_obj(new_obj)
    for extension in extensions:
        for fieldname, extfield in list(extension.fields.items()):
            deletefn = extfield.deletefn
            if deletefn is not None:
                try:
                    deletefn(old_obj, new_obj)
                except:
                    logger.exception(
                        f"Couldn't delete extension {fieldname} for object {new_obj}")


# Database model
#: Database tables prefix.
tablename_prefix = "sync_"


class PrefixTables(DeclarativeMeta):
    def __init__(cls, classname, bases, dict_):
        if '__tablename__' in dict_:
            tn = dict_['__tablename__']
            cls.__tablename__ = dict_['__tablename__'] = tablename_prefix + tn
        return super(PrefixTables, cls).__init__(classname, bases, dict_)


Base: DeclarativeMeta = declarative_base(metaclass=PrefixTables)


class ContentType(Base):
    """A weak abstraction over a database table."""

    __tablename__ = "content_types"

    content_type_id = Column(BigInteger, primary_key=True)
    table_name = Column(String(500))
    model_name = Column(String(500))

    def __repr__(self):
        return "<ContentType id: {0}, table_name: {1}, model_name: {2}>". \
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
        return "<Node node_id: {0}, registered: {1}, " \
               "registry_user_id: {2}, secret: {3}>". \
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
        return "<Version version_id: {0}, created: {1}>". \
            format(self.version_id, self.created)


class OperationError(Exception): pass


class SkipOperation(Exception):
    """
    This exception is not an error, it is intended to be raised in the
    before_insert_fn, before_update_fn, before_delete_fn to avoid the operation to be conducted
    """


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
    tracked_model: DeclarativeMeta = None  # to be injected
    command = Column(String(1))
    order = Column(Integer, primary_key=True)
    version = relationship(Version, backref=backref("operations", lazy="joined"))
    whitelist = Column(JSONB)
    """
    is a binary JSON (Fallback to normal JSON for SQLite) field that holds
    an array of user ids to be allowed to pull that object
    dbsync does not fill or test this field, this has to be accomplished
    by extensions
    """

    command_options = ('i', 'u', 'd')
    _target: SQLClass
    """temp reference to the operation target for after_tracking_fn"""

    @validates('command')
    def validate_command(self, key, command):
        assert command in self.command_options
        return command

    def __repr__(self):
        return f"<Operation row_id: {self.row_id}, model: {self.tracked_model}, command: {self.command}, version:{self.version}>"

    def references(self, obj):
        "Whether this operation references the given object or not."
        if self.row_id != getattr(obj, get_pk(obj), None):
            return False
        model = self.tracked_model
        if model is None:
            return False  # operation doesn't even refer to a tracked model
        return model is type(obj)

    def call_before_operation_fn(self, session: Session, obj: SQLClass, old_obj: Optional[SQLClass] = None):
        extensions: List[Extension] = get_model_extensions_for_obj(obj)
        for extension in extensions:
            if extension:
                if extension.before_operation_fn:
                    extension.before_operation_fn(session, self, obj, old_obj)
                if self.command == 'i' and extension.before_insert_fn:
                    extension.before_insert_fn(session, obj)
                elif self.command == 'u' and extension.before_update_fn:
                    extension.before_update_fn(session, obj, old_obj)
                elif self.command == 'd' and extension.before_delete_fn:
                    extension.before_delete_fn(session, obj)

    def call_after_operation_fn(self, session: Session, obj: SQLClass):
        extensions: List[Extension] = get_model_extensions_for_obj(obj)
        for extension in extensions:
            if extension:
                if extension.after_operation_fn:
                    extension.after_operation_fn(session, self, obj)
                if self.command == 'i' and extension.after_insert_fn:
                    extension.after_insert_fn(session, obj)
                elif self.command == 'u' and extension.after_update_fn:
                    extension.after_update_fn(session, obj)
                elif self.command == 'd' and extension.after_delete_fn:
                    extension.after_delete_fn(session, obj)

    def call_before_client_add_object_fn(self, session: Session, op: "Operation", obj: SQLClass):
        extensions: List[Extension] = get_model_extensions_for_obj(obj)
        for extension in extensions:
            if extension:
                if extension.before_client_add_object_fn:
                    extension.before_client_add_object_fn(session, op, obj)

    async def perform_async(operation: "Operation", container: "BaseMessage", session: Session, node_id=None,
                            websocket: Optional[WebSocketCommonProtocol] = None
                            ) -> (Optional[SQLClass], Optional[SQLClass]):
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
        from dbsync.core import mode
        model: DeclarativeMeta = operation.tracked_model
        res: Tuple[Optional[SQLClass], Optional[SQLClass]] = (None, None)
        if model is None:
            raise OperationError("no content type for this operation", operation)

        if operation.command == 'i':
            # check if the given object is already in the database
            obj = query_model(session, model). \
                filter(getattr(model, get_pk(model)) == operation.row_id).first()

            # retrieve the object from the PullMessage
            qu = container.query(model). \
                filter(attr('__pk__') == operation.row_id)
            # breakpoint()
            pull_obj = qu.first()
            # pull_obj._session = session
            if pull_obj is None:
                raise OperationError(
                    f"no object backing the operation in container on {mode}", operation)
            if obj is None:
                logger.info(f"insert: calling request_payloads_for_extension for: {pull_obj.id}")
                try:
                    operation.call_before_operation_fn(session, pull_obj)
                    await request_payloads_for_extension(operation, pull_obj, websocket, session)
                    session.add(pull_obj)
                    res = pull_obj, None
                except SkipOperation as e:
                    logger.info(f"operation {operation} skipped")
                # operation.call_after_operation_fn(pull_obj, session)
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
            obj = query_model(session, model). \
                filter(getattr(model, get_pk(model)) == operation.row_id).one_or_none()
            if obj is not None:
                logger.info(f"update: calling request_payloads_for_extension for: {obj.id}")
                # breakpoint()
            else:
                # For now, the record will be created again, but is an
                # error because nothing should be deleted without
                # using dbsync
                # raise OperationError(
                #     "the referenced object doesn't exist in database", operation)
                # addendum:
                # this can happen when tracking of an object has been suppressed and
                # later been activated during a 'u' operation,
                # so we keep this logic
                logger.warning(
                    "The referenced object doesn't exist in database. "
                    "Node %s. Operation %s",
                    node_id,
                    operation)

            # get new object from the PushMessage
            pull_obj = container.query(model). \
                filter(attr('__pk__') == operation.row_id).first()
            if pull_obj is None:
                raise OperationError(
                    "no object backing the operation in container", operation)

            try:
                operation.call_before_operation_fn(session, pull_obj, obj)
                await request_payloads_for_extension(operation, pull_obj, websocket, session)
                old_obj = deepcopy(obj)
                session.merge(pull_obj)
                res = pull_obj, old_obj
            except SkipOperation as e:
                logger.info(f"operation {operation} skipped")

            # operation.call_after_operation_fn(pull_obj, session)

        elif operation.command == 'd':
            try:
                obj = query_model(session, model, only_pk=True). \
                    filter(getattr(model, get_pk(model)) == operation.row_id).first()
            except NoSuchColumnError as ex:
                # for joins only_pk doesnt seem to work
                obj = query_model(session, model, only_pk=False). \
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
                try:
                    # breakpoint()
                    operation.call_before_operation_fn(session, obj)
                    session.delete(obj)
                    res = obj, None
                except SkipOperation as e:
                    logger.info(f"operation {operation} skipped")

        else:
            raise OperationError(
                "the operation doesn't specify a valid command ('i', 'u', 'd')",
                operation)

        return res
