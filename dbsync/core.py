"""
Common functionality for model synchronization and version tracking.
"""

import zlib
import inspect
import contextlib
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Tuple, Union, Type, Any, Callable, List

from sqlalchemy.sql import Join
from sqlalchemy.sql.type_api import TypeEngine
from typing_extensions import Protocol

from sqlalchemy import Table, Column

logging.getLogger('dbsync').addHandler(logging.NullHandler())

from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine

from dbsync.lang import *
from dbsync.utils import get_pk, query_model, copy, class_mapper
from dbsync.models import ContentType, Operation, Version
from dbsync import dialects
from dbsync.logs import get_logger


logger = get_logger(__name__)


#: Approximate maximum number of variables allowed in a query
MAX_SQL_VARIABLES = 900


INTERNAL_SESSION_ATTR = '_dbsync_internal'


SessionClass = sessionmaker(autoflush=False, expire_on_commit=False)
def Session():
    s = SessionClass(bind=get_engine())
    s._model_changes = dict() # for flask-sqlalchemy
    setattr(s, INTERNAL_SESSION_ATTR, True) # used to disable listeners
    return s


def session_closing(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        closeit = kwargs.get('session', None) is None
        session = Session() if closeit else kwargs['session']
        kwargs['session'] = session
        try:
            return fn(*args, **kwargs)
        finally:
            if closeit:
                session.close()
    return wrapped


def session_committing(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        closeit = kwargs.get('session', None) is None
        session = Session() if closeit else kwargs['session']
        kwargs['session'] = session
        try:
            result = fn(*args, **kwargs)
            if closeit:
                session.commit()
            else:
                session.flush()
            return result
        except:
            if closeit:
                session.rollback()
            raise
        finally:
            if closeit:
                session.close()
    return wrapped


@contextlib.contextmanager
def committing_context():
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


#: The internal use mode, used to prevent client-server module
#  collision. Possible values are 'modeless', 'client', 'server'.
mode = 'modeless'


#: The engine used for database connections.
_engine = None


def set_engine(engine):
    """
    Sets the SA engine to be used by the library.

    It should point to the same database as the application's.
    """
    assert isinstance(engine, Engine), "expected sqlalchemy.engine.Engine object"
    global _engine
    _engine = engine


class ConfigurationError(Exception): pass

def get_engine():
    "Returns a defined (not None) engine."
    if _engine is None:
        raise ConfigurationError("database engine hasn't been set yet")
    return _engine


class SQLClass(Protocol):
    def __init__(self):
        self.primary_key = None

    """duck typing for sqlalchemy content class"""
    __table__: Table
    __name__: str
    mapped_table: Union[Table, Join]
    primary_key: Tuple[Column, ...]


@dataclass(frozen=True)
class tracked_record:
    model: Optional[SQLClass] = None
    id: Optional[int] = None

null_model = tracked_record()


@dataclass
class SyncedModels:
    tables: Dict[str, tracked_record] = field(default_factory=dict)
    models: Dict[SQLClass, tracked_record] = field(default_factory=dict)
    ids: Dict[int, tracked_record] = field(default_factory=dict)
    model_names: Dict[str, tracked_record] = field(default_factory=dict)

    def install(self, model: SQLClass) -> None:
        """
        Installs the model in synched_models, indexing by class, class
        name, table name and content_type_id.
        """
        ct_id = make_content_type_id(model)
        tname = model.__table__.name
        record = tracked_record(model=model, id=ct_id)
        self.model_names[model.__name__] = record
        self.models[model] = record
        self.tables[tname] = record
        self.ids[ct_id] = record


synched_models = SyncedModels()


def table_id(tablename: str) -> Optional[int]:
    """
    returns the id of a given table
    this is needed for referencing the table from sync_oberations
    """
    return synched_models.tables[tablename].id


def tracked_model(operation: Operation) -> Optional[SQLClass]:
    "Get's the tracked model (SA mapped class) for this operation."
    return synched_models.ids.get(operation.content_type_id, null_model).model


# Injects synched models lookup into the Operation class.
Operation.tracked_model = property(tracked_model)


#: Set of classes in *synched_models* that are subject to pull handling.
pulled_models: Set[SQLClass] = set()


#: Set of classes in *synched_models* that are subject to push handling.
pushed_models: Set[SQLClass] = set()

Extension = Dict[
    str,
    Tuple[
        TypeEngine,
        Callable[[SQLClass], Any],
        Callable[[SQLClass, Any], None],
        Optional[Callable[[SQLClass, SQLClass], None]]
    ]]

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
        loadfn: Callable[[SQLClass], Any],
        savefn: Callable[[SQLClass, Any], None],
        deletefn: Optional[Callable[[SQLClass, SQLClass], None]] = None):
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
    assert inspect.isroutine(loadfn), "load function must be a callable"
    assert inspect.isroutine(savefn), "save function must be a callable"
    assert deletefn is None or inspect.isroutine(deletefn),\
        "delete function must be a callable"
    extension: Extension = model_extensions.get(model.__name__, {})
    type_: TypeEngine = fieldtype if not inspect.isclass(fieldtype) else fieldtype()
    extension[fieldname] = (type_, loadfn, savefn, deletefn)
    model_extensions[model.__name__] = extension


def _has_extensions(obj):
    return bool(model_extensions.get(type(obj).__name__, {}))

def _has_delete_functions(obj):
    return any(
        delfn is not None
        for t, loadfn, savefn, delfn in list(model_extensions.get(
            type(obj).__name__, {}).values()))


def save_extensions(obj):
    """
    Executes the save procedures for the extensions of the given
    object.
    """
    extensions = model_extensions.get(type(obj).__name__, {})
    for field, ext in list(extensions.items()):
        _, _, savefn, _ = ext
        try: savefn(obj, getattr(obj, field, None))
        except:
            logger.exception(
                "Couldn't save extension %s for object %s", field, obj)


def delete_extensions(old_obj, new_obj):
    """
    Executes the delete procedures for the extensions of the given
    object. *old_obj* is the object in the previous state, and
    *new_obj* is the object in the current state (or ``None`` if the
    object was deleted).
    """
    extensions = model_extensions.get(type(old_obj).__name__, {})
    for field, ext in list(extensions.items()):
        _, _, _, deletefn = ext
        if deletefn is not None:
            try: deletefn(old_obj, new_obj)
            except:
                logger.exception(
                    "Couldn't delete extension %s for object %s", field, new_obj)


#: Toggled variable used to disable listening to operations momentarily.
listening = True


def toggle_listening(enabled=None):
    """
    Change the listening state.

    If set to ``False``, no operations will be registered. This can be
    used to disable dbsync temporarily, in scripts or blocks that
    execute in a single-threaded environment.
    """
    global listening
    listening = enabled if enabled is not None else not listening


def with_listening(enabled):
    """
    Decorator for procedures to be executed with the specified
    listening status.
    """
    def wrapper(proc):
        @wraps(proc)
        def wrapped(*args, **kwargs):
            prev = bool(listening)
            toggle_listening(enabled)
            try:
                return proc(*args, **kwargs)
            finally:
                toggle_listening(prev)
        return wrapped
    return wrapper


# Helper functions used to queue extension operations in a transaction.

def _track_added(fn, added):
    def tracked(o, **kws):
        if _has_extensions(o): added.append(o)
        return fn(o, **kws)
    return tracked

def _track_deleted(fn, deleted, session, always=False):
    def tracked(o, **kws):
        if _has_delete_functions(o):
            if always: deleted.append((copy(o), None))
            else:
                prev = query_model(session, type(o)).filter_by(
                    **{get_pk(o): getattr(o, get_pk(o), None)}).\
                    first()
                if prev is not None:
                    deleted.append((copy(prev), o))
        return fn(o, **kws)
    return tracked


def with_transaction(include_extensions=True):
    """
    Decorator for a procedure that uses a session and acts as an
    atomic transaction. It feeds a new session to the procedure, and
    commits it, rolls it back, and / or closes it when it's
    appropriate. If *include_extensions* is ``False``, the transaction
    will ignore model extensions.
    """
    def wrapper(proc):
        @wraps(proc)
        def wrapped(*args, **kwargs):
            extensions = kwargs.pop('include_extensions', include_extensions)
            session = Session()
            previous_state = dialects.begin_transaction(session)
            added = []
            deleted = []
            if extensions:
                session.add = _track_deleted(
                    _track_added(session.add, added),
                    deleted,
                    session)
                session.merge = _track_deleted(
                    _track_added(session.merge, added),
                    deleted,
                    session)
                session.delete = _track_deleted(
                    session.delete,
                    deleted,
                    session,
                    always=True)
            result = None
            try:
                kwargs.update({'session': session})
                result = proc(*args, **kwargs)
                session.commit()
            except Exception as e:
                session.rollback()
                raise
            finally:
                dialects.end_transaction(previous_state, session)
                session.close()
            for old_obj, new_obj in deleted: delete_extensions(old_obj, new_obj)
            for obj in added: save_extensions(obj)
            return result
        return wrapped
    return wrapper


def make_content_type_id(model):
    "Returns a content type id for the given model."
    mname = model.__name__
    tname = model.__table__.name
    text = "{0}/{1}".format(mname, tname)
    return zlib.crc32(text.encode("latin-1"), 0) & 0xffffffff


@session_committing
def generate_content_types(session=None):
    """
    Fills the content type table.

    Inserts content types into the internal table used to describe
    operations.
    """
    for tname, record in list(synched_models.tables.items()):
        content_type_id = record.id
        mname = record.model.__name__
        if session.query(ContentType).\
                filter(ContentType.table_name == tname).count() == 0:
            session.add(ContentType(table_name=tname,
                                    model_name=mname,
                                    content_type_id=content_type_id))


@session_closing
def is_synched(obj, session=None):
    """
    Returns whether the given tracked object is synched.

    Raises a TypeError if the given object is not being tracked
    (i.e. the content type doesn't exist).
    """
    if type(obj) not in synched_models.models:
        raise TypeError("the given object of class {0} isn't being tracked".\
                            format(obj.__class__.__name__))
    session = Session()
    last_op = session.query(Operation).\
        filter(Operation.content_type_id == synched_models.models[type(obj)].id,
               Operation.row_id == getattr(obj, get_pk(obj))).\
               order_by(Operation.order.desc()).first()
    return last_op is None or last_op.version_id is not None


@session_closing
def get_latest_version_id(session=None):
    """
    Returns the latest version identifier or ``None`` if no version is
    found.
    """
    # assuming version identifiers grow monotonically
    # might need to order by 'created' datetime field
    version = session.query(Version).order_by(Version.version_id.desc()).first()
    return maybe(version, attr('version_id'), None)
