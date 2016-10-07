import json
from collections import defaultdict

import pytest
from kombu import Connection
from kombu.messaging import Exchange, Queue
from kombu.pools import connections, producers
from kombu.serialization import register, unregister
from mock import ANY, Mock, call, patch

from nameko.amqp import Backoff
from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.events import event_handler
from nameko.exceptions import RemoteError
from nameko.extensions import DependencyProvider
from nameko.messaging import consume
from nameko.rpc import rpc
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ClusterRpcProxy
from nameko.testing.services import entrypoint_waiter

BACKOFF_COUNT = 3


@pytest.yield_fixture(autouse=True)
def fast_backoff():
    with patch.object(Backoff, 'schedule', new=[50]):
        yield


@pytest.yield_fixture(autouse=True)
def no_randomness():
    with patch.object(Backoff, 'randomness', new=0):
        yield


@pytest.yield_fixture()
def limited_backoff():
    limit = 1
    with patch.object(Backoff, 'limit', new=limit):
        yield limit


@pytest.fixture
def exchange():
    return Exchange("messages")


@pytest.fixture
def queue(exchange):
    return Queue("messages", exchange=exchange, routing_key="message")


@pytest.fixture
def publish_message(rabbit_config):

    def publish(
        exchange, payload, routing_key=None, serializer="json", **kwargs
    ):
        conn = Connection(rabbit_config[AMQP_URI_CONFIG_KEY])

        with connections[conn].acquire(block=True) as connection:
            exchange.maybe_bind(connection)
            with producers[conn].acquire(block=True) as producer:
                producer.publish(
                    payload,
                    exchange=exchange,
                    routing_key=routing_key,
                    serializer=serializer,
                    **kwargs
                )

    return publish


@pytest.fixture
def dispatch_event(rabbit_config):

    def dispatch(service_name, event_type, event_data, **kwargs):
        dispatcher = event_dispatcher(rabbit_config, **kwargs)
        dispatcher(service_name, event_type, event_data)

    return dispatch


@pytest.yield_fixture
def rpc_proxy(rabbit_config):
    with ClusterRpcProxy(rabbit_config) as proxy:
        yield proxy


@pytest.fixture
def counter():

    class Counter(object):
        """ Counter with support for nested counts for hashable objects:

        Usage::

            counter = Counter()
            counter.increment()  # 1
            counter.increment()  # 2
            counter == 2  # True

            counter['foo'].increment()  # 1
            counter['bar'].increment()  # 1
            counter['foo'] == counter['bar'] == 1  # True

        """
        class Item(object):
            def __init__(self):
                self._value = 0

            def increment(self):
                self._value += 1
                return self._value

            def __eq__(self, other):
                return self._value == other

        sentinel = object()

        def __init__(self):
            self.items = defaultdict(Counter.Item)

        def __getitem__(self, key):
            return self.items[key]

        def increment(self):
            return self[Counter.sentinel].increment()

        def __eq__(self, other):
            return self[Counter.sentinel] == other

    return Counter()


@pytest.fixture
def service_cls(queue, counter):

    class Service(object):
        name = "service"

        @rpc
        @consume(queue)
        @event_handler("src_service", "event_type")
        def method(self, payload):
            if counter.increment() <= BACKOFF_COUNT:
                raise Backoff()
            return "result"

    return Service


@pytest.fixture
def container(container_factory, rabbit_config, service_cls):

    container = container_factory(service_cls, rabbit_config)
    container.start()
    return container


@pytest.fixture
def entrypoint_tracker():

    class CallTracker(object):

        def __init__(self):
            self.calls = []

        def __len__(self):
            return len(self.calls)

        def track(self, **call):
            self.calls.append(call)

        def get_results(self):
            return [call['result'] for call in self.calls]

        def get_exceptions(self):
            return [call['exc_info'] for call in self.calls]

    return CallTracker()


@pytest.fixture
def wait_for_result(entrypoint_tracker):
    """ Callback for the `entrypoint_waiter` that waits until the entrypoint
    returns a non-exception.

    Captures the entrypoint executions using `entrypoint_tracker`.
    """
    def cb(worker_ctx, res, exc_info):
        entrypoint_tracker.track(result=res, exc_info=exc_info)
        if exc_info is None or exc_info[0] is Backoff.Expired:
            return True
    return cb


@pytest.fixture
def wait_for_backoff_expired(entrypoint_tracker):
    """ Callback for the `entrypoint_waiter` that waits until the entrypoint
    raises a `BackoffExpired` exception.

    Captures the entrypoint executions using `entrypoint_tracker`.
    """
    def cb(worker_ctx, res, exc_info):
        entrypoint_tracker.track(result=res, exc_info=exc_info)
        if exc_info and exc_info[0] is Backoff.Expired:
            return True
    return cb


class TestRpc(object):

    def test_rpc(
        self, container, entrypoint_tracker, rpc_proxy, wait_for_result
    ):
        """ RPC entrypoint supports backoff
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            res = rpc_proxy.service.method("arg")

        assert res == result.get() == "result"

        # entrypoint fired BACKOFF_COUNT + 1 times
        assert entrypoint_tracker.get_results() == (
            [None] * BACKOFF_COUNT + ["result"]
        )
        # entrypoint raised `Backoff` for all but the last execution
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * BACKOFF_COUNT + [None]
        )

    def test_expiry(
        self, container, entrypoint_tracker, rpc_proxy, limited_backoff,
        wait_for_backoff_expired
    ):
        """ RPC entrypoint supports backoff expiry
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_backoff_expired
        ) as result:
            with pytest.raises(RemoteError) as raised:
                rpc_proxy.service.method("arg")
            assert raised.value.exc_type == "Expired"

        with pytest.raises(Backoff.Expired) as raised:
            result.get()
        assert (
            "Backoff aborted after '{}' retries".format(limited_backoff)
        ) in str(raised.value)

        # entrypoint fired `limited_backoff` + 1 times
        assert entrypoint_tracker.get_results() == (
            [None] * limited_backoff + [None]
        )
        # entrypoint raised `Backoff` for all but the last execution,
        # and then raised `Backoff.Expired`
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * limited_backoff +
            [(Backoff.Expired, ANY, ANY)]
        )

    def test_multiple_services(
        self, rpc_proxy, wait_for_result, counter,
        container_factory, rabbit_config, entrypoint_tracker
    ):
        """ RPC backoff works correctly when multiple services use it
        """
        class ServiceOne(object):
            name = "service_one"

            @rpc
            def method(self, payload):
                if counter["one"].increment() <= BACKOFF_COUNT:
                    raise Backoff()
                return "one"

        class ServiceTwo(object):
            name = "service_two"

            @rpc
            def method(self, payload):
                if counter["two"].increment() <= BACKOFF_COUNT:
                    raise Backoff()
                return "two"

        container_one = container_factory(ServiceOne, rabbit_config)
        container_one.start()
        container_two = container_factory(ServiceTwo, rabbit_config)
        container_two.start()

        with entrypoint_waiter(
            container_one, 'method', callback=wait_for_result
        ) as result:
            res = rpc_proxy.service_one.method("arg")
        assert result.get() == res == "one"
        assert entrypoint_tracker.get_results() == (
            [None] * BACKOFF_COUNT + ["one"]
        )
        assert counter['one'] == BACKOFF_COUNT + 1

        with entrypoint_waiter(
            container_two, 'method', callback=wait_for_result
        ) as result:
            res = rpc_proxy.service_two.method("arg")
        assert result.get() == res == "two"
        assert entrypoint_tracker.get_results() == (
            [None] * BACKOFF_COUNT + ["one"] +
            [None] * BACKOFF_COUNT + ["two"]
        )
        assert counter['two'] == BACKOFF_COUNT + 1

    def test_multiple_methods(
        self, container_factory, rabbit_config, wait_for_result, rpc_proxy,
        entrypoint_tracker, counter
    ):
        """ RPC backoff works correctly when multiple entrypoints in the same
        service use it
        """
        class Service(object):
            name = "service"

            @rpc
            def a(self):
                if counter["a"].increment() <= BACKOFF_COUNT:
                    raise Backoff()
                return "a"

            @rpc
            def b(self):
                if counter["b"].increment() <= BACKOFF_COUNT:
                    raise Backoff()
                return "b"

        container = container_factory(Service, rabbit_config)
        container.start()

        with entrypoint_waiter(container, 'a', callback=wait_for_result):
            rpc_proxy.service.a()
        assert entrypoint_tracker.get_results() == (
            [None] * BACKOFF_COUNT + ["a"]
        )
        assert counter['a'] == BACKOFF_COUNT + 1

        with entrypoint_waiter(container, 'b', callback=wait_for_result):
            rpc_proxy.service.b()
        assert entrypoint_tracker.get_results() == (
            [None] * BACKOFF_COUNT + ["a"] +
            [None] * BACKOFF_COUNT + ["b"]
        )
        assert counter['b'] == BACKOFF_COUNT + 1


class TestEvents(object):

    def test_events(
        self, container, entrypoint_tracker, dispatch_event, wait_for_result
    ):
        """ Event handler supports backoff
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            dispatch_event("src_service", "event_type", {})

        assert result.get() == "result"

        assert entrypoint_tracker.get_results() == (
            [None] * BACKOFF_COUNT + ["result"]
        )
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * BACKOFF_COUNT + [None]
        )

    def test_expiry(
        self, container, entrypoint_tracker, dispatch_event, limited_backoff,
        wait_for_backoff_expired
    ):
        """ Event handler supports backoff expiry
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_backoff_expired
        ) as result:
            dispatch_event("src_service", "event_type", {})

        with pytest.raises(Backoff.Expired) as raised:
            result.get()
        assert (
            "Backoff aborted after '{}' retries".format(limited_backoff)
        ) in str(raised.value)

        assert entrypoint_tracker.get_results() == (
            [None] * limited_backoff + [None]
        )
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * limited_backoff +
            [(Backoff.Expired, ANY, ANY)]
        )

    def test_multiple_services(
        self, dispatch_event, wait_for_result, container_factory,
        rabbit_config, counter, entrypoint_tracker, rabbit_manager
    ):
        """ Event handler backoff works when multiple services use it
        """
        class ServiceOne(object):
            name = "service_one"

            @event_handler("src_service", "event_type")
            def method(self, payload):
                if counter["one"].increment() <= BACKOFF_COUNT:
                    raise Backoff()
                return "one"

        class ServiceTwo(object):
            name = "service_two"

            @event_handler("src_service", "event_type")
            def method(self, payload):
                if counter["two"].increment() <= BACKOFF_COUNT:
                    raise Backoff()
                return "two"

        container_one = container_factory(ServiceOne, rabbit_config)
        container_one.start()
        container_two = container_factory(ServiceTwo, rabbit_config)
        container_two.start()

        with entrypoint_waiter(
            container_one, 'method', callback=wait_for_result
        ) as result_one:

            with entrypoint_waiter(
                container_two, 'method', callback=wait_for_result
            ) as result_two:

                dispatch_event("src_service", "event_type", {})

        assert result_one.get() == "one"
        assert result_two.get() == "two"

        assert counter['one'] == BACKOFF_COUNT + 1
        assert counter['two'] == BACKOFF_COUNT + 1

        results = entrypoint_tracker.get_results()
        # order not guaranteed
        assert results.count(None) == BACKOFF_COUNT * 2
        assert results.count("one") == results.count("two") == 1

    def test_multiple_handlers(
        self, container_factory, rabbit_config, wait_for_result,
        entrypoint_tracker, dispatch_event, counter
    ):
        """ Event handler backoff works when multiple entrypoints in the same
        service use it, including events with identical types originating from
        different services.
        """
        class Service(object):
            name = "service"

            @event_handler("s1", "e1")
            def a(self, payload):
                if counter["a"].increment() <= BACKOFF_COUNT:
                    raise Backoff()
                return "a"

            @event_handler("s1", "e2")
            def b(self, payload):
                if counter["b"].increment() <= BACKOFF_COUNT:
                    raise Backoff()
                return "b"

            @event_handler("s2", "e1")
            def c(self, payload):
                if counter["c"].increment() <= BACKOFF_COUNT:
                    raise Backoff()
                return "c"

        container = container_factory(Service, rabbit_config)
        container.start()

        with entrypoint_waiter(container, 'a', callback=wait_for_result):
            dispatch_event('s1', 'e1', {})
        assert entrypoint_tracker.get_results() == (
            [None] * BACKOFF_COUNT + ["a"]
        )
        assert counter['a'] == BACKOFF_COUNT + 1

        with entrypoint_waiter(container, 'b', callback=wait_for_result):
            dispatch_event('s1', 'e2', {})
        assert entrypoint_tracker.get_results() == (
            [None] * BACKOFF_COUNT + ["a"] +
            [None] * BACKOFF_COUNT + ["b"]
        )
        assert counter['b'] == BACKOFF_COUNT + 1

        with entrypoint_waiter(container, 'c', callback=wait_for_result):
            dispatch_event('s2', 'e1', {})
        assert entrypoint_tracker.get_results() == (
            [None] * BACKOFF_COUNT + ["a"] +
            [None] * BACKOFF_COUNT + ["b"] +
            [None] * BACKOFF_COUNT + ["c"]
        )
        assert counter['c'] == BACKOFF_COUNT + 1


class TestMessaging(object):

    @pytest.mark.usefixtures('container')
    def test_messaging(
        self, container, entrypoint_tracker, publish_message, exchange, queue,
        wait_for_result
    ):
        """ Message consumption supports backoff
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            publish_message(exchange, "msg", routing_key=queue.routing_key)

        assert result.get() == "result"

        assert entrypoint_tracker.get_results() == (
            [None] * BACKOFF_COUNT + ["result"]
        )
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * BACKOFF_COUNT + [None]
        )

    def test_expiry(
        self, container, entrypoint_tracker, publish_message, exchange, queue,
        limited_backoff, wait_for_backoff_expired
    ):
        """ Message consumption supports backoff expiry
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_backoff_expired
        ) as result:
            publish_message(exchange, "msg", routing_key=queue.routing_key)

        with pytest.raises(Backoff.Expired) as raised:
            result.get()
        assert (
            "Backoff aborted after '{}' retries".format(limited_backoff)
        ) in str(raised.value)

        assert entrypoint_tracker.get_results() == (
            [None] * limited_backoff + [None]
        )
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * limited_backoff +
            [(Backoff.Expired, ANY, ANY)]
        )

    def test_multiple_queues_with_same_exchange_and_routing_key(
        self, container_factory, wait_for_result, rabbit_manager, exchange,
        entrypoint_tracker, publish_message, counter, rabbit_config
    ):
        """ Message consumption backoff works when there are muliple queues
        receiving the published message
        """
        queue_one = Queue("one", exchange=exchange, routing_key="message")
        queue_two = Queue("two", exchange=exchange, routing_key="message")

        class ServiceOne(object):
            name = "service_one"

            @consume(queue_one)
            def method(self, payload):
                if counter["one"].increment() <= BACKOFF_COUNT:
                    raise Backoff()
                return "one"

        class ServiceTwo(object):
            name = "service_two"

            @consume(queue_two)
            def method(self, payload):
                counter["two"].increment()
                return "two"

        container_one = container_factory(ServiceOne, rabbit_config)
        container_one.start()
        container_two = container_factory(ServiceTwo, rabbit_config)
        container_two.start()

        with entrypoint_waiter(
            container_one, 'method', callback=wait_for_result
        ) as result_one:

            with entrypoint_waiter(
                container_two, 'method', callback=wait_for_result
            ) as result_two:

                publish_message(exchange, "msg", routing_key="message")

        # ensure all messages are processed
        vhost = rabbit_config['vhost']
        backoff_queue = rabbit_manager.get_queue(vhost, 'backoff')
        service_queue_one = rabbit_manager.get_queue(vhost, queue_one.name)
        service_queue_two = rabbit_manager.get_queue(vhost, queue_two.name)
        assert backoff_queue['messages'] == 0
        assert service_queue_one['messages'] == 0
        assert service_queue_two['messages'] == 0

        assert result_one.get() == "one"
        assert result_two.get() == "two"

        # backoff from service_one not seen by service_two
        assert counter['one'] == BACKOFF_COUNT + 1
        assert counter['two'] == 1


class TestCallStack(object):

    @pytest.fixture
    def container(self, container_factory, rabbit_config, service_cls):

        class CallStack(DependencyProvider):
            """ Exposes the call stack directly to the service
            """

            def get_dependency(self, worker_ctx):
                return worker_ctx.context_data['call_id_stack']

        class Service(service_cls):
            call_stack = CallStack()

        container = container_factory(Service, rabbit_config)
        container.start()
        return container

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_rpc_call_stack(self, container, rpc_proxy):
        """ RPC backoff extends call stack
        """
        call_stacks = []

        def callback(worker_ctx, result, exc_info):
            call_stacks.append(worker_ctx.call_id_stack)
            if exc_info is None:
                return True

        with entrypoint_waiter(container, 'method', callback=callback):
            rpc_proxy.service.method("msg")

        assert call_stacks == [
            [
                'standalone_rpc_proxy.call.0',
                'service.method.1'
            ],
            [
                'standalone_rpc_proxy.call.0',
                'service.method.1.backoff',
                'service.method.2'
            ],
            [
                'standalone_rpc_proxy.call.0',
                'service.method.1.backoff',
                'service.method.2.backoff',
                'service.method.3',
            ],
            [
                'standalone_rpc_proxy.call.0',
                'service.method.1.backoff',
                'service.method.2.backoff',
                'service.method.3.backoff',
                'service.method.4'
            ],
        ]

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_events_call_stack(self, container, dispatch_event):
        """ Event handler backoff extends call stack
        """
        call_stacks = []

        def callback(worker_ctx, result, exc_info):
            call_stacks.append(worker_ctx.call_id_stack)
            if exc_info is None:
                return True

        with entrypoint_waiter(container, 'method', callback=callback):
            dispatch_event(
                "src_service",
                "event_type",
                {},
                headers={
                    'nameko.call_id_stack': ['event.dispatch']
                }
            )

        assert call_stacks == [
            [
                'event.dispatch',
                'service.method.0'
            ],
            [
                'event.dispatch',
                'service.method.0.backoff',
                'service.method.1'
            ],
            [
                'event.dispatch',
                'service.method.0.backoff',
                'service.method.1.backoff',
                'service.method.2',
            ],
            [
                'event.dispatch',
                'service.method.0.backoff',
                'service.method.1.backoff',
                'service.method.2.backoff',
                'service.method.3'
            ],
        ]

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_messaging_call_stack(
        self, container, publish_message, exchange, queue
    ):
        """ Message consumption backoff extends call stack
        """
        call_stacks = []

        def callback(worker_ctx, result, exc_info):
            call_stacks.append(worker_ctx.call_id_stack)
            if exc_info is None:
                return True

        with entrypoint_waiter(container, 'method', callback=callback):
            publish_message(
                exchange,
                "msg",
                routing_key=queue.routing_key,
                headers={
                    'nameko.call_id_stack': ['message.publish']
                }
            )

        assert call_stacks == [
            [
                'message.publish',
                'service.method.0'
            ],
            [
                'message.publish',
                'service.method.0.backoff',
                'service.method.1'
            ],
            [
                'message.publish',
                'service.method.0.backoff',
                'service.method.1.backoff',
                'service.method.2',
            ],
            [
                'message.publish',
                'service.method.0.backoff',
                'service.method.1.backoff',
                'service.method.2.backoff',
                'service.method.3'
            ],
        ]


class TestSerialization(object):

    @pytest.yield_fixture(autouse=True)
    def custom_serializer(self, rabbit_config):

        def encode(value):
            value = json.dumps(value)
            return value.upper()

        def decode(value):
            value = value.lower()
            return json.loads(value)

        # register new serializer
        register(
            "upperjson", encode, decode, "application/x-upper-json", "utf-8"
        )
        # update config so consumers expect it
        rabbit_config['serializer'] = "upperjson"
        yield
        unregister("upperjson")

    def test_custom_serialization(
        self, container, publish_message, exchange, queue, wait_for_result
    ):
        """ Backoff can be used with a custom AMQP message serializer
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            publish_message(
                exchange,
                "msg",
                serializer="upperjson",
                routing_key=queue.routing_key
            )

        assert result.get() == "result"


class TestDeadLetteredMessages(object):

    @pytest.yield_fixture(autouse=True)
    def limited_backoff(self):
        # allow exactly `BACKOFF_COUNT` backoffs
        limit = BACKOFF_COUNT
        with patch.object(Backoff, 'limit', new=limit):
            yield limit

    @pytest.fixture
    def deadlettering_exchange(self, rabbit_config, exchange, queue):
        conn = Connection(rabbit_config[AMQP_URI_CONFIG_KEY])

        with connections[conn].acquire(block=True) as connection:

            deadletter_exchange = Exchange(name="deadletter", type="topic")
            deadletter_exchange.maybe_bind(connection)
            deadletter_exchange.declare()

            deadletter_queue = Queue(
                name="deadletter",
                exchange=deadletter_exchange,
                routing_key="#",
                queue_arguments={
                    'x-dead-letter-exchange': exchange.name
                }
            )
            deadletter_queue.maybe_bind(connection)
            deadletter_queue.declare()

        return deadletter_exchange

    def test_backoff_works_on_previously_deadlettered_message(
        self, container, publish_message, deadlettering_exchange,
        queue, exchange, wait_for_result, entrypoint_tracker, limited_backoff
    ):
        """ Backoff can be used even if the original message has previously
        been deadlettered
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            # dispatch a message to the deadlettering exchange.
            # it will be deadlettered into the normal `exchange`
            # and should afterwards be processed as "normal" message
            publish_message(
                deadlettering_exchange,
                "msg",
                routing_key=queue.routing_key,
                expiration=1.0
            )

        # the initial deadlettering should not count towards the backoff limit,
        # so we shouldn't see Backoff.Expired here
        assert result.get() == "result"

        assert entrypoint_tracker.get_results() == (
            [None] * limited_backoff + ["result"]
        )
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * limited_backoff + [None]
        )


class TestGetNextExpiration(object):

    @pytest.fixture
    def backoff(self):
        class CustomBackoff(Backoff):
            schedule = [1000, 2000, 3000]
            randomness = 0
            limit = 10

        return CustomBackoff()

    @pytest.fixture
    def backoff_with_randomness(self):
        class CustomBackoff(Backoff):
            schedule = [1000, 2000, 3000]
            randomness = 100
            limit = 10

        return CustomBackoff()

    def test_first_backoff(self, backoff):
        message = Mock()
        message.headers = {}
        assert backoff.get_next_expiration(message, "backoff") == 1000

    def test_next_backoff(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'count': 1
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 2000

    def test_last_backoff(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'count': 3
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 3000

    def test_count_greater_than_schedule_length(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'count': 5
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 3000

    def test_count_greater_than_limit(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'count': 99
            }]
        }
        with pytest.raises(Backoff.Expired) as exc_info:
            backoff.get_next_expiration(message, "backoff")
        # 27 = 1 + 2 + 3 * 8
        assert str(exc_info.value) == (
            "Backoff aborted after '10' retries (~27 seconds)"
        )

    def test_count_equal_to_limit(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'count': 10
            }]
        }
        with pytest.raises(Backoff.Expired) as exc_info:
            backoff.get_next_expiration(message, "backoff")
        # 27 = 1 + 2 + 3 * 8
        assert str(exc_info.value) == (
            "Backoff aborted after '10' retries (~27 seconds)"
        )

    def test_previously_deadlettered_first_backoff(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                # previously deadlettered elsewhere
                'exchange': 'not-backoff',
                'count': 99
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 1000

    def test_previously_deadlettered_next_backoff(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'count': 1
            }, {
                # previously deadlettered elsewhere
                'exchange': 'not-backoff',
                'count': 99
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 2000

    @patch('nameko.amqp.random')
    def test_backoff_randomness(self, random_patch, backoff_with_randomness):

        random_patch.gauss.return_value = 2200.0

        backoff = backoff_with_randomness

        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'count': 1
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 2200
        assert random_patch.gauss.call_args_list == [
            call(2000, backoff.randomness)
        ]
