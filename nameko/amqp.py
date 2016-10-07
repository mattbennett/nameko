from __future__ import absolute_import

import random

import amqp
import six
from amqp.exceptions import NotAllowed
from kombu import Connection
from kombu.common import maybe_declare
from kombu.messaging import Exchange, Queue
from kombu.pools import connections, producers
from kombu.transport.pyamqp import Transport

from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.extensions import SharedExtension

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
    if connection.transport_cls != 'amqp':
        # Can't use these heuristics. Fall back to the existing error behaviour
        return

    transport = TestTransport(connection.transport.client)
    with transport.establish_connection():
        pass


class BackoffMeta(type):
    @property
    def max_delay(cls):
        return sum(
            cls.get_next_schedule_item(index) for index in range(cls.limit)
        )


@six.add_metaclass(BackoffMeta)
class Backoff(Exception):

    schedule = (1000, 2000, 3000, 5000, 8000, 13000, 21000, 34000, 55000)
    randomness = 100  # standard deviation as milliseconds
    limit = 20

    class Expired(Exception):
        pass

    @classmethod
    def get_next_schedule_item(cls, index):
        if index >= len(cls.schedule):
            item = cls.schedule[-1]
        else:
            item = cls.schedule[index]
        return item

    @classmethod
    def get_next_expiration(cls, message, backoff_exchange_name):

        total_attempts = 0
        for deadlettered in message.headers.get('x-death', ()):
            if deadlettered['exchange'] == backoff_exchange_name:
                total_attempts = int(deadlettered['count'])
                break

        if total_attempts >= cls.limit:
            raise cls.Expired(
                "Backoff aborted after '{}' retries (~{:.0f} seconds)".format(
                    cls.limit, cls.max_delay / 1000  # pylint: disable=E1101
                )
            )

        expiration = cls.get_next_schedule_item(total_attempts)

        if cls.randomness:
            expiration = int(random.gauss(expiration, cls.randomness))
        return expiration


class BackoffPublisher(SharedExtension):

    @property
    def exchange(self):
        backoff_exchange = Exchange(
            type="topic",
            name="backoff"
        )
        return backoff_exchange

    @property
    def queue(self):
        backoff_queue = Queue(
            name="backoff",
            exchange=self.exchange,
            routing_key="#",
            queue_arguments={
                'x-dead-letter-exchange': "",   # default exchange
            }
        )
        return backoff_queue

    def republish(self, backoff_cls, message, target_queue):

        expiration = backoff_cls.get_next_expiration(
            message, self.exchange.name
        )

        # republish to backoff queue
        conn = Connection(self.container.config[AMQP_URI_CONFIG_KEY])
        with connections[conn].acquire(block=True) as connection:

            maybe_declare(self.exchange, connection)
            maybe_declare(self.queue, connection)

            with producers[conn].acquire(block=True) as producer:

                properties = message.properties.copy()
                headers = properties.pop('application_headers')

                producer.publish(
                    message.body,
                    headers=headers,
                    exchange=self.exchange,
                    routing_key=target_queue,
                    expiration=expiration / 1000,
                    **properties
                )
