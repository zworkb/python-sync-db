"""
.. module:: dbsync.utils
   :synopsis: Utility functions.
"""

import random
import inspect
from typing import List, Optional, Tuple, Dict, Any

from sqlalchemy import Table
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import (
    object_mapper,
    class_mapper,
    ColumnProperty,
    noload,
    defer,
    instrumentation,
    state, Session, Query)
from sqlalchemy.sql import Join


def generate_secret(length=128):
    chars = "0123456789" \
            "abcdefghijklmnopqrstuvwxyz" \
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ" \
            ".,_-+*@:;[](){}~!?|<>=/\&$#"
    return "".join(random.choice(chars) for _ in range(length))


def properties_dict(sa_object):
    """
    Returns a dictionary of column-properties for the given SQLAlchemy
    mapped object.
    """
    mapper = object_mapper(sa_object)
    return dict((prop.key, getattr(sa_object, prop.key))
                for prop in mapper.iterate_properties
                if isinstance(prop, ColumnProperty))


def column_properties(sa_variant: "SQLClass"):
    "Returns a list of column-properties."
    mapper = class_mapper(sa_variant) if inspect.isclass(sa_variant) \
        else object_mapper(sa_variant)
    return [prop.key for prop in mapper.iterate_properties
            if isinstance(prop, ColumnProperty)]


def types_dict(sa_class: DeclarativeMeta) -> "SQLClass":
    """
    Returns a dictionary of column-properties mapped to their
    SQLAlchemy types for the given mapped class.
    """
    mapper = class_mapper(sa_class)
    return dict((prop.key, prop.columns[0].type)
                for prop in mapper.iterate_properties
                if isinstance(prop, ColumnProperty))


def construct_bare(class_: DeclarativeMeta) -> "SQLClass":
    """
    Returns an object of type *class_*, without invoking the class'
    constructor.
    """
    obj = class_.__new__(class_)
    manager = getattr(class_, instrumentation.ClassManager.MANAGER_ATTR)
    setattr(obj, manager.STATE_ATTR, state.InstanceState(obj, manager))
    return obj


def object_from_dict(class_: DeclarativeMeta, dict_: Dict[str, Any]) -> "SQLClass":
    "Returns an object from a dictionary of attributes."
    obj = construct_bare(class_)
    for k, v in list(dict_.items()):
        setattr(obj, k, v)
    return obj


def copy(obj: "SQLClass") -> "SQLClass":
    "Returns a copy of the given object, not linked to a session."
    return object_from_dict(type(obj), properties_dict(obj))


def get_pk(sa_variant: "SQLClass") -> str:
    "Returns the primary key name for the given mapped class or object."
    mapper = class_mapper(sa_variant) if inspect.isclass(sa_variant) \
        else object_mapper(sa_variant)
    return mapper.primary_key[0].key


def parent_references(sa_object: "SQLClass", models: List[DeclarativeMeta]) -> List[Tuple[DeclarativeMeta, str]]:
    """
    Returns a list of pairs (*sa_class*, *pk*) that reference all the
    parent objects of *sa_object*.
    """
    mapper = object_mapper(sa_object)
    references = [(getattr(sa_object, k.parent.name), k.column.table)
                  for k in mapper.mapped_table.foreign_keys]

    def get_model(table: Table) -> Optional[DeclarativeMeta]:
        for m in models:
            if class_mapper(m).mapped_table == table:
                return m
        return None

    return [(m, pk)
            for m, pk in ((get_model(table), v) for v, table in references)
            if m is not None]


def parent_objects(sa_object: "SQLClass", models: List[DeclarativeMeta], session: Session,
                   only_pk=False) -> "List[SQLClass]":
    """
    Returns all the parent objects the given *sa_object* points to
    (through foreign keys in *sa_object*).

    *models* is a list of mapped classes.

    *session* must be a valid SA session instance.
    """
    return [obj for obj in (query_model(session, m, only_pk=only_pk). \
                                filter_by(**{get_pk(m): val}).first()
                            for m, val in parent_references(sa_object, models)) if obj is not None]


def query_model(session: Session, sa_class: DeclarativeMeta, only_pk=False) -> Query:
    """
    Returns a query for *sa_class* that doesn't load any relationship
    attribute.
    """
    opts = (noload('*'),)
    if only_pk:
        pk = get_pk(sa_class)
        opts += tuple(
            defer(prop.key)
            for prop in class_mapper(sa_class).iterate_properties
            if isinstance(prop, ColumnProperty)
            if prop.key != pk)
    return session.query(sa_class).options(*opts)


class EventRegister(object):

    def __init__(self):
        self._listeners = []

    def __iter__(self):
        for listener in self._listeners:
            yield listener

    def listen(self, listener):
        "Register a listener. May be used as a decorator."
        assert inspect.isroutine(listener), "invalid listener"
        if listener not in self._listeners:
            self._listeners.append(listener)
        return listener


def entity_name(mapped_table):
    if isinstance(mt, Join):
        tname = mapped_table.right.name
    else:
        tname = mapped_table.name

    return tname