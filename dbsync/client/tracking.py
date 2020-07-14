"""
Listeners to SQLAlchemy events to keep track of CUD operations.
"""
import functools
import logging
import inspect
import warnings
from collections import deque
from typing import Optional, Callable, Deque, Union, List

from sqlalchemy.ext.declarative import DeclarativeMeta

from dbsync.core import SQLClass
from sqlalchemy import event
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Mapper
from sqlalchemy.orm.session import Session as GlobalSession, Session

from dbsync import core
from dbsync.models import Operation, Extension, get_model_extensions_for_obj, SkipOperation, call_before_tracking_fn
from dbsync.logs import get_logger
from sqlalchemy.sql import Join

logger = get_logger(__name__)


if core.mode == 'server':
    warnings.warn("don't import both client and server")
core.mode = 'client'


#: Operations to be flushed to the database after a commit.
_operations_queue: Deque[Operation] = deque()


def flush_operations(committed_session):
    """Flush operations after a commit has been issued."""
    if not _operations_queue or \
            getattr(committed_session, core.INTERNAL_SESSION_ATTR, False):
        return
    if not core.listening:
        logger.warning("dbsync is disabled; aborting flush_operations")
        return
    with core.committing_context() as session:
        while _operations_queue:
            op = _operations_queue.popleft()
            session.add(op)
            # TODO: call call_after_tracking_fn here
            session.flush()


def empty_queue(*args):
    """Empty the operations queue."""
    session = None if not args else args[0]
    if getattr(session, core.INTERNAL_SESSION_ATTR, False):
        return
    if not core.listening:
        logger.warning("dbsync is disabled; aborting empty_queue")
        return
    while _operations_queue:
        _operations_queue.pop()


def make_listener(command: str) -> Callable[[Mapper, Connection, SQLClass], Optional[Operation]]:
    """Builds a listener for the given command (i, u, d)."""

    def listener(mapper: Mapper, connection: Connection, target: SQLClass) -> Optional[Operation]:
        # if command == 'd':
        #     breakpoint()
        return _add_operation(command, mapper, target)

    return listener


def add_operation(command: str, target: SQLClass, session: Optional[Session], force = False) -> Optional[Operation]:
    return _add_operation(command, target.__mapper__, target, session, force=force)


def _add_operation(command: str, mapper: Mapper, target: SQLClass, session: Optional[Session] = None, force = False) -> Optional[Operation]:
    if session is None:
        session = core.SessionClass.object_session(target)
    if getattr(session,
               core.INTERNAL_SESSION_ATTR,
               False):
        return None

    if not core.listening:
        logger.warning("dbsync is disabled; "
                       "aborting listener to '{0}' command".format(command))
        return None

    if command == 'u' and not force and not session.\
            is_modified(target, include_collections=False):
        return None

    mt = mapper.mapped_table
    if isinstance(mt, Join):
        tname = mapper.mapped_table.right.name
    else:
        tname = mapper.mapped_table.name

    if tname not in core.synched_models.tables:
        logging.error("you must track a mapped class to table {0} "\
                          "to log operations".format(tname))
        return None

    try:
        call_before_tracking_fn(session, command, target)
        pk = getattr(target, mapper.primary_key[0].name)
        op = Operation(
            row_id=pk,
            version_id=None,  # operation not yet versioned
            content_type_id=core.synched_models.tables[tname].id,
            command=command)
        _operations_queue.append(op)
        return op
    except SkipOperation:
        logger.info(f"operation {command} skipped for {target}")
        return None


def start_tracking(model: DeclarativeMeta, directions=("push", "pull")) -> DeclarativeMeta:

    if 'pull' in directions:
        core.pulled_models.add(model)
    if 'push' in directions:
        core.pushed_models.add(model)
    if model in core.synched_models.models:
        return model
    core.synched_models.install(model)
    if 'push' not in directions:
        return model # don't track operations for pull-only models
    event.listen(model, 'after_insert', make_listener('i'))
    event.listen(model, 'after_update', make_listener('u'))
    event.listen(model, 'after_delete', make_listener('d'))
    return model


def track(*directions: Union[List[str], SQLClass]):
    """
    Adds an ORM class to the list of synchronized classes.

    It can be used as a class decorator. This will also install
    listeners to keep track of CUD operations for the given model.

    *directions* are optional arguments of values in ('push', 'pull')
    that can restrict the way dbsync handles the class during those
    procedures. If not given, both values are assumed. If only one of
    them is given, the other procedure will ignore the tracked class.
    """
    valid = ('push', 'pull')
    if not directions:
        return lambda model: start_tracking(model, valid)
    if len(directions) == 1 and inspect.isclass(directions[0]):
        return start_tracking(directions[0], valid)
    assert all(d in valid for d in directions), \
        "track only accepts the arguments: {0}".format(', '.join(valid))
    return lambda model: start_tracking(model, directions)


event.listen(GlobalSession, 'after_commit', flush_operations)
event.listen(GlobalSession, 'after_soft_rollback', empty_queue)
