import json
from collections import defaultdict

import pytest
from kombu import Connection
from kombu.messaging import Exchange, Queue
from kombu.pools import connections, producers
from kombu.serialization import register, unregister
from mock import ANY, patch

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
    with patch.object(Backoff, 'schedule', new=[10.0]):
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

    def publish(exchange, payload, routing_key=None, **kwargs):
        conn = Connection(rabbit_config[AMQP_URI_CONFIG_KEY])

        with connections[conn].acquire(block=True) as connection:
            exchange.maybe_bind(connection)
            with producers[conn].acquire(block=True) as producer:
                producer.publish(
                    payload,
                    exchange=exchange,
                    routing_key=routing_key,
                    **kwargs
                )

    return publish


@pytest.fixture
def dispatch_event(rabbit_config):
    return event_dispatcher(rabbit_config)


@pytest.yield_fixture
def rpc_proxy(rabbit_config):
    with ClusterRpcProxy(rabbit_config) as proxy:
        yield proxy


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
def counter():

    class Counter(object):
        value = 0

        def count(self):
            self.value += 1
            return self.value

    return Counter()


@pytest.fixture
def keyed_counter():

    class Counter(object):

        def __init__(self):
            self.values = defaultdict(int)

        def __getitem__(self, key):
            return self.values[key]

        def count(self, key):
            self.values[key] += 1
            return self.values[key]

    return Counter()


@pytest.fixture
def service_cls(queue, counter):

    class Service(object):
        name = "service"

        @rpc
        @consume(queue)
        @event_handler("src_service", "event_type")
        def method(self, payload):
            if counter.count() <= BACKOFF_COUNT:
                raise Backoff()
            return "result"

    return Service


@pytest.fixture
def container(container_factory, rabbit_config, service_cls):

    container = container_factory(service_cls, rabbit_config)
    container.start()
    return container


@pytest.fixture
def wait_for_result(entrypoint_tracker):
    def cb(worker_ctx, res, exc_info):
        entrypoint_tracker.track(result=res, exc_info=exc_info)
        if exc_info is None or exc_info[0] is Backoff.Expired:
            return True
    return cb


@pytest.fixture
def wait_for_backoff_expired(entrypoint_tracker):
    def cb(worker_ctx, res, exc_info):
        entrypoint_tracker.track(result=res, exc_info=exc_info)
        if exc_info and exc_info[0] is Backoff.Expired:
            return True
    return cb


class TestRpc(object):

    def test_rpc(
        self, container, entrypoint_tracker, rpc_proxy, wait_for_result
    ):
        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            res = rpc_proxy.service.method("arg")

        assert res == result.get() == "result"

        # 'normal' backoff is 3
        assert entrypoint_tracker.get_results() == [
            None, None, None, "result"
        ]
        assert entrypoint_tracker.get_exceptions() == [
            (Backoff, ANY, ANY), (Backoff, ANY, ANY), (Backoff, ANY, ANY), None
        ]

    def test_expiry(
        self, container, entrypoint_tracker, rpc_proxy, limited_backoff,
        wait_for_backoff_expired
    ):
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

        # backoff limited to 1
        assert entrypoint_tracker.get_results() == [
            None, None
        ]
        assert entrypoint_tracker.get_exceptions() == [
            (Backoff, ANY, ANY), (Backoff.Expired, ANY, ANY)
        ]

    def test_multiple_services(
        self, rpc_proxy, wait_for_result, keyed_counter,
        container_factory, rabbit_config, entrypoint_tracker
    ):

        class ServiceOne(object):
            name = "service_one"

            @rpc
            def method(self, payload):
                if keyed_counter.count("one") <= BACKOFF_COUNT:
                    raise Backoff()
                return "one"

        class ServiceTwo(object):
            name = "service_two"

            @rpc
            def method(self, payload):
                if keyed_counter.count("two") <= BACKOFF_COUNT:
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
        assert entrypoint_tracker.get_results() == [
            None, None, None, "one"
        ]
        assert keyed_counter['one'] == BACKOFF_COUNT + 1

        with entrypoint_waiter(
            container_two, 'method', callback=wait_for_result
        ) as result:
            res = rpc_proxy.service_two.method("arg")
        assert result.get() == res == "two"
        assert entrypoint_tracker.get_results() == [
            None, None, None, "one",
            None, None, None, "two",
        ]
        assert keyed_counter['two'] == BACKOFF_COUNT + 1

    def test_multiple_methods(
        self, container_factory, rabbit_config, wait_for_result, rpc_proxy,
        entrypoint_tracker, keyed_counter
    ):

        class Service(object):
            name = "service"

            @rpc
            def a(self):
                if keyed_counter.count("a") <= BACKOFF_COUNT:
                    raise Backoff()
                return "a"

            @rpc
            def b(self):
                if keyed_counter.count("b") <= BACKOFF_COUNT:
                    raise Backoff()
                return "b"

        container = container_factory(Service, rabbit_config)
        container.start()

        with entrypoint_waiter(container, 'a', callback=wait_for_result):
            rpc_proxy.service.a()
        assert entrypoint_tracker.get_results() == [
            None, None, None, "a"
        ]
        assert keyed_counter['a'] == BACKOFF_COUNT + 1

        with entrypoint_waiter(container, 'b', callback=wait_for_result):
            rpc_proxy.service.b()
        assert entrypoint_tracker.get_results() == [
            None, None, None, "a",
            None, None, None, "b",
        ]
        assert keyed_counter['b'] == BACKOFF_COUNT + 1

    def test_queues_and_exchanges(self):
        pass


class TestEvents(object):

    def test_events(
        self, container, entrypoint_tracker, dispatch_event, wait_for_result
    ):
        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            dispatch_event("src_service", "event_type", {})

        assert result.get() == "result"

        # 'normal' backoff is 3
        assert entrypoint_tracker.get_results() == [
            None, None, None, "result"
        ]
        assert entrypoint_tracker.get_exceptions() == [
            (Backoff, ANY, ANY), (Backoff, ANY, ANY), (Backoff, ANY, ANY), None
        ]

    def test_expiry(
        self, container, entrypoint_tracker, dispatch_event, limited_backoff,
        wait_for_backoff_expired
    ):
        with entrypoint_waiter(
            container, 'method', callback=wait_for_backoff_expired
        ) as result:
            dispatch_event("src_service", "event_type", {})

        with pytest.raises(Backoff.Expired) as raised:
            result.get()
        assert (
            "Backoff aborted after '{}' retries".format(limited_backoff)
        ) in str(raised.value)

        # backoff limited to 1
        assert entrypoint_tracker.get_results() == [
            None, None
        ]
        assert entrypoint_tracker.get_exceptions() == [
            (Backoff, ANY, ANY), (Backoff.Expired, ANY, ANY)
        ]

    def test_multiple_services(
        self, dispatch_event, wait_for_result, container_factory,
        rabbit_config, keyed_counter, entrypoint_tracker
    ):
        class ServiceOne(object):
            name = "service_one"

            @event_handler("src_service", "event_type")
            def method(self, payload):
                if keyed_counter.count("one") <= BACKOFF_COUNT:
                    raise Backoff()
                return "one"

        class ServiceTwo(object):
            name = "service_two"

            @event_handler("src_service", "event_type")
            def method(self, payload):
                if keyed_counter.count("two") <= BACKOFF_COUNT:
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

        assert keyed_counter['one'] == BACKOFF_COUNT + 1
        assert keyed_counter['two'] == BACKOFF_COUNT + 1

        results = entrypoint_tracker.get_results()
        assert results[:6] == 6 * [None]
        assert set(results[-2:]) == {"one", "two"}  # order not guaranteed

    def test_multiple_handlers(
        self, container_factory, rabbit_config, wait_for_result,
        entrypoint_tracker, dispatch_event, keyed_counter
    ):

        class Service(object):
            name = "service"

            @event_handler("s1", "e1")
            def a(self, payload):
                if keyed_counter.count("a") <= BACKOFF_COUNT:
                    raise Backoff()
                return "a"

            @event_handler("s1", "e2")
            def b(self, payload):
                if keyed_counter.count("b") <= BACKOFF_COUNT:
                    raise Backoff()
                return "b"

            @event_handler("s2", "e1")
            def c(self, payload):
                if keyed_counter.count("c") <= BACKOFF_COUNT:
                    raise Backoff()
                return "c"

        container = container_factory(Service, rabbit_config)
        container.start()

        with entrypoint_waiter(container, 'a', callback=wait_for_result):
            dispatch_event('s1', 'e1', {})
        assert entrypoint_tracker.get_results() == [
            None, None, None, "a"
        ]
        assert keyed_counter['a'] == BACKOFF_COUNT + 1

        with entrypoint_waiter(container, 'b', callback=wait_for_result):
            dispatch_event('s1', 'e2', {})
        assert entrypoint_tracker.get_results() == [
            None, None, None, "a",
            None, None, None, "b",
        ]
        assert keyed_counter['b'] == BACKOFF_COUNT + 1

        with entrypoint_waiter(container, 'c', callback=wait_for_result):
            dispatch_event('s2', 'e1', {})
        assert entrypoint_tracker.get_results() == [
            None, None, None, "a",
            None, None, None, "b",
            None, None, None, "c",
        ]
        assert keyed_counter['c'] == BACKOFF_COUNT + 1

    def test_queues_and_exchanges(self):
        pass


class TestMessaging(object):

    @pytest.mark.usefixtures('container')
    def test_messaging(
        self, container, entrypoint_tracker, publish_message, exchange, queue,
        wait_for_result
    ):
        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            publish_message(exchange, "msg", routing_key=queue.routing_key)

        assert result.get() == "result"

        # 'normal' backoff is 3
        assert entrypoint_tracker.get_results() == [
            None, None, None, "result"
        ]
        assert entrypoint_tracker.get_exceptions() == [
            (Backoff, ANY, ANY), (Backoff, ANY, ANY), (Backoff, ANY, ANY), None
        ]

    def test_expiry(
        self, container, entrypoint_tracker, publish_message, exchange, queue,
        limited_backoff, wait_for_backoff_expired
    ):
        with entrypoint_waiter(
            container, 'method', callback=wait_for_backoff_expired
        ) as result:
            publish_message(exchange, "msg", routing_key=queue.routing_key)

        with pytest.raises(Backoff.Expired) as raised:
            result.get()
        assert (
            "Backoff aborted after '{}' retries".format(limited_backoff)
        ) in str(raised.value)

        # backoff limited to 1
        assert entrypoint_tracker.get_results() == [
            None, None
        ]
        assert entrypoint_tracker.get_exceptions() == [
            (Backoff, ANY, ANY), (Backoff.Expired, ANY, ANY)
        ]

    def test_multiple_exchanges(self):
        pass

    def test_multiple_queues(self):
        pass

    def test_multiple_routing_keys(self):
        pass

    def test_queues_and_exchanges(self):
        pass

# class TestMultipleEntrypoints(object):

#     @pytest.fixture
#     def counter(self):
#         class Counter(object):

#             def __init__(self):
#                 self.reset()

#             def __getitem__(self, key):
#                 return self.values[key]

#             def count(self, key):
#                 self.values[key] += 1
#                 return self.values[key]

#             def reset(self):
#                 self.values = defaultdict(int)

#         return Counter()

#     @pytest.fixture
#     def container(self, container_factory, rabbit_config, counter):

#         class Service(object):
#             name = "service"

#             def process(self, handler):
#                 if counter.count(handler) <= BACKOFF_COUNT:
#                     raise Backoff()
#                 return "result"

#             @event_handler("s1", "e1")
#             def a(self, payload):
#                 return self.process("a")

#             @event_handler("s1", "e2")
#             def b(self, payload):
#                 return self.process("b")

#             @event_handler("s2", "e1")
#             def c(self, payload):
#                 return self.process("c")

#         container = container_factory(Service, rabbit_config)
#         container.start()
#         return container

#     def test_multiple_entrypoints(
#         self, container, dispatch_event, rabbit_manager, rabbit_config,
#         wait_for_result, counter
#     ):
#         # create backoff queues and exchanges
#         with entrypoint_waiter(container, 'a', callback=wait_for_result):
#             dispatch_event('s1', 'e1', {})
#         with entrypoint_waiter(container, 'b', callback=wait_for_result):
#             dispatch_event('s1', 'e2', {})
#         with entrypoint_waiter(container, 'c', callback=wait_for_result):
#             dispatch_event('s2', 'e1', {})

#         # verify queues and exchanges created
#         vhost = rabbit_config['vhost']
#         queues = rabbit_manager.get_queues(vhost)
#         exchanges = rabbit_manager.get_exchanges(vhost)

#         queue_names = [queue['name'] for queue in queues]
#         exchange_names = [exchange['name'] for exchange in exchanges]

#         assert "backoff--s1.events" in queue_names
#         assert "backoff--s2.events" in queue_names
#         assert "backoff--s1.events" in exchange_names
#         assert "backoff--s2.events" in exchange_names

#         # verify correct entrypoint called
#         counter.reset()
#         with entrypoint_waiter(container, 'a', callback=wait_for_result):
#             dispatch_event('s1', 'e1', {})

#         assert counter['a'] == 4
#         assert counter['b'] == 0
#         assert counter['c'] == 0


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
                # 'service.method.1',
                'service.method.2'
            ],
            [
                'standalone_rpc_proxy.call.0',
                # 'service.method.1',
                # 'service.method.2',
                'service.method.3',
            ],
            [
                'standalone_rpc_proxy.call.0',
                # 'service.method.1',
                # 'service.method.2',
                # 'service.method.3',
                'service.method.4'
            ],
        ]

    @pytest.mark.usefixtures('predictable_call_ids')
    @pytest.mark.xfail  # standalone event dispatcher does not include call_id
    def test_events_call_stack(self, container, dispatch_event):

        call_stacks = []

        def callback(worker_ctx, result, exc_info):
            call_stacks.append(worker_ctx.call_id_stack)
            if exc_info is None:
                return True

        with entrypoint_waiter(container, 'method', callback=callback):
            dispatch_event("src_service", "event_type", {})

        assert call_stacks == [
            [
                'standalone_event.call.0',
                'service.method.1'
            ],
            [
                'standalone_event.call.0',
                # 'service.method.1',
                'service.method.2'
            ],
            [
                'standalone_event.call.0',
                # 'service.method.1',
                # 'service.method.2',
                'service.method.3',
            ],
            [
                'standalone_event.call.0',
                # 'service.method.1',
                # 'service.method.2',
                # 'service.method.3',
                'service.method.4'
            ],
        ]

    @pytest.mark.usefixtures('predictable_call_ids')
    @pytest.mark.xfail  # predictable_call_ids not in play (unlike in RPC case)
    def test_messaging_call_stack(
        self, container, publish_message, exchange, queue
    ):

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
                    'call_id_stack': ['message.send.0']
                }
            )

        assert call_stacks == [
            [
                'message.send.0',
                'service.method.1'
            ],
            [
                'message.send.0',
                # 'service.method.1',
                'service.method.2'
            ],
            [
                'message.send.0',
                # 'service.method.1',
                # 'service.method.2',
                'service.method.3',
            ],
            [
                'message.send.0',
                # 'service.method.1',
                # 'service.method.2',
                # 'service.method.3',
                'service.method.4'
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
            yield

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
        self, container, publish_message, deadlettering_exchange, queue, exchange,
        wait_for_result, entrypoint_tracker
    ):

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

        # 'normal' backoff is 3
        assert entrypoint_tracker.get_results() == [
            None, None, None, "result"
        ]
        assert entrypoint_tracker.get_exceptions() == [
            (Backoff, ANY, ANY), (Backoff, ANY, ANY), (Backoff, ANY, ANY), None
        ]


class TestGetNextExpiration(object):

    @pytest.fixture
    def backoff(self):
        Backoff.schedule = [1000, 2000, 3000]
        Backoff.randomness = 0
        Backoff.limit = 10
        return Backoff()

    def test_no_headers(self, backoff):
        assert backoff.get_next_expiration(None) == 1000

    def test_first_backoff(self, backoff):
        headers = [{
            'count': 1
        }]
        assert backoff.get_next_expiration(headers) == 1000

    def test_next_backoff(self, backoff):
        headers = [{
            'count': 2
        }]
        assert backoff.get_next_expiration(headers) == 2000

    def test_last_backoff(self, backoff):
        headers = [{
            'count': 4
        }]
        assert backoff.get_next_expiration(headers) == 3000

    def test_count_greater_than_schedule_length(self, backoff):
        headers = [{
            'count': 5
        }]
        assert backoff.get_next_expiration(headers) == 3000

    def test_count_greater_than_limit(self, backoff):
        headers = [{
            'count': 99
        }]
        with pytest.raises(Backoff.Expired) as exc_info:
            backoff.get_next_expiration(headers)
        # 27 = 1 + 2 + 3 * 8
        assert str(exc_info.value) == (
            "Backoff aborted after '10' retries (~27 seconds)"
        )

    def test_count_equal_to_limit(self, backoff):
        headers = [{
            'count': 10
        }]
        with pytest.raises(Backoff.Expired) as exc_info:
            backoff.get_next_expiration(headers)
        # 27 = 1 + 2 + 3 * 8
        assert str(exc_info.value) == (
            "Backoff aborted after '10' retries (~27 seconds)"
        )
