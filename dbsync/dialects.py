"""
.. module:: dbsync.dialects
   :synopsis: DBMS-dependent statements.
"""
import datetime
import json
import uuid


import rfc3339 as rfc3339
from sqlalchemy import func, TypeDecorator, CHAR
from sqlalchemy.dialects.postgresql import UUID

from dbsync.utils import class_mapper, get_pk


class GUID(TypeDecorator):
    """Platform-independent GUID type.
    Uses Postgresql's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.
    http://docs.sqlalchemy.org/en/rel_0_8/core/types.html#backend-agnostic-guid-type
    """

    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % int(uuid.UUID(value))
            else:
                # hexstring
                return "%.32x" % int(value)

    def process_result_value(self, value, dialect):

        if value is None:
            return value
        else:
            return uuid.UUID(value)


########################################
# JSONB support, faking it in SQLite
########################################
# evil hack: force sqlite to swallow JSONB as JSON
# We want to have it because we can query JSONB fields with GIN indices

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
SQLiteTypeCompiler.visit_JSONB = SQLiteTypeCompiler.visit_JSON


def begin_transaction(session):
    """
    Returns information of the state the database was on before the
    transaction began.
    """
    engine = session.bind
    dialect = engine.name
    if dialect == 'sqlite':
        cursor = engine.execute("PRAGMA foreign_keys;")
        state = cursor.fetchone()[0]
        cursor.close()
        engine.execute("PRAGMA foreign_keys = OFF;")
        engine.execute("BEGIN EXCLUSIVE TRANSACTION;")
        return state
    if dialect == 'mysql':
        # temporal by default
        # see http://dev.mysql.com/doc/refman/5.7/en/using-system-variables.html
        engine.execute("SET foreign_key_checks = 0;")
        return None
    if dialect == 'postgresql':
        # defer constraints
        engine.execute("SET CONSTRAINTS ALL DEFERRED;")
        return None
    return None


def end_transaction(state, session):
    """
    *state* is whatever was returned by :func:`begin_transaction`
    """
    engine = session.bind
    dialect = engine.name
    if dialect == 'sqlite':
        if state not in (0, 1): state = 1
        engine.execute("PRAGMA foreign_keys = {0}".format(int(state)))


def max_local(sa_class, session):
    """
    Returns the maximum primary key used for the given table.
    """
    engine = session.bind
    dialect = engine.name
    table_name = class_mapper(sa_class).mapped_table.name
    # default, strictly incorrect query
    found = session.query(func.max(getattr(sa_class, get_pk(sa_class)))).scalar()
    if dialect == 'sqlite':
        cursor = engine.execute("SELECT seq FROM sqlite_sequence WHERE name = ?",
                                table_name)
        result = cursor.fetchone()[0]
        cursor.close()
        return max(result, found)
    return found
