"""
Listeners to SQLAlchemy events to keep track of CUD operations.

On the server side, each operation will also trigger a new version, so
as to allow direct use of the database while maintaining occassionally
connected nodes capable of synchronizing their data.
"""

import logging
import inspect
import datetime
import warnings
from typing import List, Union

from sqlalchemy import event
from sqlalchemy.sql import Join

from dbsync import core
from dbsync.models import Operation, Version, SQLClass, call_after_tracking_fn, call_before_tracking_fn, SkipOperation
from dbsync.logs import get_logger

from dbsync.createlogger import create_logger
from logging import DEBUG

logger = create_logger("dbsync.server.tracking")
logger.level = DEBUG


if core.mode == 'client':
    warnings.warn("don't import both server and client")
core.mode = 'server'


def make_listener(command: str):
    """Builds a listener for the given command (i, u, d)."""
    @core.session_committing
    def listener(mapper, connection, target, session=None) -> None:
        logger.info(f"tracking {target}")
        if getattr(core.SessionClass.object_session(target),
                   core.INTERNAL_SESSION_ATTR,
                   False):
            logger.debug(f"internal session object not tracked: {target}")
            return
        if not core.listening:
            logger.warning("dbsync is disabled; "
                           "aborting listener to '{0}' command".format(command))
            return
        if command == 'u' and not core.SessionClass.object_session(target).\
                is_modified(target, include_collections=False):
            logger.debug(f"updated and not modified -> no tracking: {target}")
            return

        mt = mapper.mapped_table
        if isinstance(mt, Join):
            tname = mt.right.name
        else:
            tname = mt.name

        if tname not in core.synched_models.tables:
            logging.error("you must track a mapped class to table {0} "\
                              "to log operations".format(tname))
            return
        # one version for each operation
        # TODO: can be minimized by collecting ops in one flush queue
        try:
            call_before_tracking_fn(session, command, target)
        except SkipOperation:
            logger.info(f"skip operation for {target}")
            return
        # TODO:
        # we should try to make only one version per transaction
        # so adding a new version should happen in the flush
        # is not so easy but we should do that
        # perhaps by creating a version for each new session.begin,
        # holding that version until the flush/commit
        # other idea:
        # for the first time an operation is added to this session,
        # we create a version object and pin it to the session(session.current_version=version)
        # point the operations to this version
        # and finally during flush() the operations have only one session
        # problem is that the @core.session_committing decorator creates a new
        # session for each call to this function => have to dig deeper

        version = Version(created=datetime.datetime.now())
        # if not hasattr(session, '__current_version__'):
        #     session.__current_version__ = Version(created=datetime.datetime.now())
        #     session.add(session.__current_version__)
        # version = session.__current_version__
        #
        logger.info(f"new version: {version.version_id}")
        pk = getattr(target, mapper.primary_key[0].name)
        op = Operation(
            row_id=pk,
            content_type_id=core.synched_models.tables[tname].id,
            command=command)
        session.add(version)
        call_after_tracking_fn(session, op, target)
        session.add(op)
        op.version = version
    return listener


def start_tracking(model, directions=("push", "pull")):
    if 'pull' in directions:
        core.pulled_models.add(model)
    if 'push' in directions:
        core.pushed_models.add(model)
    if model in core.synched_models.models:
        return model
    li = make_listener('i')
    lu = make_listener('u')
    ld = make_listener('d')
    core.synched_models.install(model)
    core.synched_models.register_handlers(model, (li, lu, ld))
    return model


def track(*directions: Union[List[str], SQLClass]):
    """
    Adds an ORM class to the list of synchronized classes.

    It can be used as a class decorator. This will also install
    listeners to keep track of CUD operations for the given model.

    *directions* are optional arguments of values in ('push', 'pull')
    that can restrict the way dbsync handles the class during those
    handlers. If not given, both values are assumed. If only one of
    them is given, the other handler will ignore the tracked class.
    """
    valid = ('push', 'pull')
    if not directions:
        return lambda model: start_tracking(model, valid)
    if len(directions) == 1 and inspect.isclass(directions[0]):
        return start_tracking(directions[0], valid)
    assert all(d in valid for d in directions), \
        "track only accepts the arguments: {0}".format(', '.join(valid))
    return lambda model: start_tracking(model, directions)
