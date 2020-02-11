"""
Base functionality for synchronization messages.
"""

import inspect
from typing import Dict, Any, Union, Set

from dbsync.lang import *
from dbsync.utils import get_pk, properties_dict, construct_bare
from dbsync.models import model_extensions, SQLClass, ExtensionField
from dbsync.core import null_model, synched_models

from dbsync import models
from dbsync.messages.codecs import decode_dict, encode_dict


class ObjectType(object):
    """Wrapper for tracked objects."""

    def __init__(self, mname, pk, **kwargs):
        self.__model_name__ = mname
        self.__pk__ = pk
        self.__keys__ = []
        for k, v in list(kwargs.items()):
            if k != '__model_name__' and k != '__pk__' and k != '__keys__':
                setattr(self, k, v)
                self.__keys__.append(k)

    def __repr__(self):
        return "<ObjectType {0} pk: {1}>".format(
            self.__model_name__, self.__pk__)

    def __eq__(self, other):
        if not isinstance(other, ObjectType):
            raise TypeError("not an instance of ObjectType")
        return self.__model_name__ == other.__model_name__ and \
               self.__pk__ == other.__pk__

    def __hash__(self):
        return hash(self.__pk__)

    def to_dict(self):
        return dict((k, getattr(self, k)) for k in self.__keys__)

    def to_mapped_object(self):
        model = synched_models.model_names. \
            get(self.__model_name__, null_model).model
        if model is None:
            raise TypeError(
                "model {0} isn't being tracked".format(self.__model_name__))
        obj = construct_bare(model)
        for k in self.__keys__:
            setattr(obj, k, getattr(self, k))
        return obj


class MessageQuery(object):
    """Query over internal structure of a message."""
    target: str
    payload: Dict[str, Any]

    def __init__(self, target: Union[SQLClass, str], payload):
        if target == models.Operation or \
                target == models.Version or \
                target == models.Node:
            self.target = 'models.' + target.__name__
        elif inspect.isclass(target):
            self.target = target.__name__
        elif isinstance(target, str):
            self.target = target
        else:
            raise TypeError(
                "query expected a class or string, got %s" % type(target))
        self.payload = payload

    def query(self, model):
        """
        Returns a new query with a different target, without
        filtering.
        """
        return MessageQuery(model, self.payload)

    def filter(self, predicate):
        """
        Returns a new query with the collection filtered according to
        the predicate applied to the target objects.
        """
        to_filter = self.payload.get(self.target, None)
        if to_filter is None:
            return self
        return MessageQuery(
            self.target,
            dict(self.payload, **{self.target: list(filter(predicate, to_filter))}))

    def __iter__(self):
        """Yields objects mapped to their original type (*target*)."""
        m = identity if self.target.startswith('models.') \
            else method('to_mapped_object')
        lst = self.payload.get(self.target, None)
        if lst is not None:
            for e in map(m, lst):
                yield e

    def all(self):
        """Returns a list of all queried objects."""
        return list(self)

    def first(self):
        """
        Returns the first of the queried objects, or ``None`` if no
        objects matched.
        """
        try:
            return next(iter(self))
        except StopIteration:
            return None


class BaseMessage(object):
    "The base type for messages with a payload."

    #: dictionary of (model name, set of wrapped objects)
    payload: Dict[str, Set]

    def __init__(self, raw_data: Dict[str, Any] = None):
        self.payload = {}
        if raw_data is not None:
            self._from_raw(raw_data)

    def _from_raw(self, data):

        def getm(k):
            return synched_models.model_names.get(k, null_model).model

        for k, v, m in [k_v_m for k_v_m in
                        [(k_v[0], k_v[1], getm(k_v[0])) for k_v in iter(list(data['payload'].items()))] if
                        k_v_m[2] is not None]:
            self.payload[k] = set(
                [ObjectType(k, dict_[get_pk(m)], **dict_) for dict_ in map(decode_dict(m), v)])

    def query(self, model):
        """Returns a query object for this message."""
        return MessageQuery(model, self.payload)

    def to_json(self) -> Dict[str, Any]:
        """Returns a JSON-friendly python dictionary."""
        encoded: Dict[str, Any] = {'payload': {}}
        for k, objects in list(self.payload.items()):
            model = synched_models.model_names.get(k, null_model).model
            if model is not None:
                encoded['payload'][k] = list(map(encode_dict(model),
                                                 list(map(method('to_dict'), objects))))
        return encoded

    def add_object(self, obj, include_extensions=True):
        """Adds an object to the message, if it's not already in."""
        class_ = type(obj)
        classname = class_.__name__
        obj_set = self.payload.get(classname, set())
        objt = ObjectType(classname, getattr(obj, get_pk(class_)))
        if objt in obj_set:
            return self
        properties = properties_dict(obj)
        if include_extensions:
            ext: ExtensionField
            for field, ext in list(model_extensions.get(classname, {}).items()):
                loadfn = ext.loadfn
                if loadfn:
                    properties[field] = loadfn(obj)
        obj_set.add(ObjectType(
            classname, getattr(obj, get_pk(class_)), **properties))
        self.payload[classname] = obj_set
        return self
