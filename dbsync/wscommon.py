import json
import traceback
from json import JSONDecodeError
from typing import Union, Dict, Any, Type, Optional


def exception_as_dict_recursive(ex):
    """
    converts a python exception recursively to a dictionary
    :param ex: an exception
    :return: dictionary ready for json.dumps
    """
    # import pdb;pdb.set_trace()
    return dict(type=ex.__class__.__name__,
                args=[
                    exception_as_dict_recursive(arg) if isinstance(arg, Exception) else arg
                    for arg in ex.args
                ],
                traceback=traceback.format_tb(ex.__traceback__))

def exception_as_dict(ex):
    """
    converts an exception to a dict and try to keep the string length under 123 because
    socket.close reasons may not exceed 123 chars

    """
    res = dict(type=ex.__class__.__name__,
                args=[
                    str(arg)
                    for arg in ex.args
                ])

    if len(json.dumps(res)) < 124:
        return res
    else:
        # strip it futrher down
        argstr = ",".join(ex.args)[:100]
        return dict(
            type=ex.__class__.__name__,
            args=[argstr]
        )


def register_exception(klass: Type, name: Optional[str]=None):
    """
    registers an exception so that it can be instanciated by exception_from_dict
    """
    if name is None:
        name = klass.__name__

    globals()[name] = klass

def exception_from_dict(ex: Union[str, Dict[str, Any]]) -> Exception:
    """
    converts an error dict produced by server side to an exception
    muss noch besser werden und ohne eval auskommen.

    Moegliche Loesung:
    exception per key am server merken und mit einem seperaten request abholen
    """

    try:
        exdict = json.loads(ex) if isinstance(ex, str) else ex
        klassname = exdict['type'].replace("(", "").replace(")", "")
        args = exdict["args"]
    except JSONDecodeError as e:
        args = [ex]
    try:
        klass = eval(klassname)
        res = klass(*exdict["args"])
        return res
    except Exception as e:
        return Exception(*args)