"""
Request for node registry.

This is vulnerable to many things if used by itself. It should at
least be used over HTTPS and with some sort of user authentication
layer on the server.
"""

from dbsync import core
from dbsync.models import Node
from dbsync.messages.register import RegisterMessage


class RegisterRejected(Exception): pass


@core.with_listening(False)
@core.with_transaction
def register(registry_url, extra_data=None):
    """Request a node registry from the server.

    If there is already a node registered in the local database, it
    won't be used for the following operations. Additional data can be
    passed to the request by giving *extra_data*, a dictionary of
    values."""
    assert isinstance(registry_url, basestring), "registry url must be a string"
    assert bool(registry_url), "registry url can't be empty"
    if extra_data is not None:
        assert isinstance(extra_data, dict), "extra data must be a dictionary"

    code, reason, response = post_request(registry_url, extra_data)

    if (code // 100 != 2) or response is None:
        raise RegisterRejected(code, reason, response)

    message = RegisterMessage(response)
    session.add(message.node)
    return response


def isregistered():
    """Checks whether this client application has at least one node
    registry."""
    session = core.Session()
    return session.query(Node).first() is not None