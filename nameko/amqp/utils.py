from __future__ import absolute_import

from contextlib import contextmanager

import amqp
import six
from amqp.exceptions import NotAllowed
from kombu import Connection
from kombu.messaging import Queue
from kombu.pools import connections
from kombu.transport.pyamqp import Transport


BAD_CREDENTIALS = (
    'Error connecting to broker, probably caused by invalid credentials'
)
BAD_VHOST = (
    'Error connecting to broker, probably caused by using an invalid '
    'or unauthorized vhost'
)


class ConnectionTester(amqp.Connection):
    """Kombu doesn't have any good facilities for diagnosing rabbit
    connection errors, e.g. bad credentials, or unknown vhost. This hack
    attempts some heuristic diagnosis"""

    def __init__(self, *args, **kwargs):
        try:
            super(ConnectionTester, self).__init__(*args, **kwargs)
        except IOError as exc:
            if not hasattr(self, '_wait_tune_ok'):
                raise
            elif self._wait_tune_ok:
                six.raise_from(IOError(BAD_CREDENTIALS), exc)
            else:  # pragma: no cover (rabbitmq >= 3.6.0)
                six.raise_from(IOError(BAD_VHOST), exc)
        except NotAllowed as exc:  # pragma: no cover (rabbitmq < 3.6.0)
            six.raise_from(IOError(BAD_VHOST), exc)


class TestTransport(Transport):
    Connection = ConnectionTester


def verify_amqp_uri(amqp_uri):
    connection = Connection(amqp_uri)
    if connection.transport_cls not in ('amqp', 'pyamqp'):
        # Can't use these heuristics. Fall back to the existing error behaviour
        return

    transport = TestTransport(connection.transport.client)
    with transport.establish_connection():
        pass


@contextmanager
def get_connection(amqp_uri):
    conn = Connection(amqp_uri)
    with connections[conn].acquire(block=True) as connection:
        yield connection


def get_queue_info(amqp_uri, queue_name):
    with get_connection(amqp_uri) as conn:
        queue = Queue(name=queue_name)
        queue = queue.bind(conn)
        return queue.queue_declare(passive=True)
