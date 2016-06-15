"""
Utilities for testing nameko services.
"""

import inspect
from collections import OrderedDict
from contextlib import contextmanager

import eventlet
import six
from eventlet import event
from mock import MagicMock

from nameko.exceptions import ExtensionNotFound
from nameko.extensions import DependencyProvider, Entrypoint
from nameko.testing.utils import get_extension


@contextmanager
def entrypoint_hook(container, method_name, context_data=None):
    """ Yield a function providing an entrypoint into a hosted service.

    The yielded function may be called as if it were the bare method defined
    in the service class. Intended to be used as an integration testing
    utility.

    :Parameters:
        container : ServiceContainer
            The container hosting the service owning the entrypoint
        method_name : str
            The name of the entrypoint decorated method on the service class
        context_data : dict
            Context data to provide for the call, e.g. a language, auth
            token or session.

    **Usage**

    To verify that `ServiceX` and `ServiceY` are compatible, make an
    integration test that checks their interaction:

    .. literalinclude:: ../examples/testing/integration_x_y_test.py

    """
    entrypoint = get_extension(container, Entrypoint, method_name=method_name)
    if entrypoint is None:
        raise ExtensionNotFound(
            "No entrypoint for '{}' found on container {}.".format(
                method_name, container))

    def hook(*args, **kwargs):
        hook_result = event.Event()

        def wait_for_entrypoint():
            with entrypoint_waiter(container, method_name) as waiter_result:
                container.spawn_worker(
                    entrypoint, args, kwargs,
                    context_data=context_data
                )
            try:
                hook_result.send(waiter_result.get())
            except Exception as exc:
                hook_result.send_exception(exc)

        def wait_for_container():
            try:
                container.wait()
            except Exception as exc:
                if not hook_result.ready():
                    hook_result.send_exception(exc)

        # If the container errors (e.g. due to a bad entrypoint), the
        # entrypoint_waiter never completes. To mitigate, we exit the hook
        # on the first greenthread to complete.
        eventlet.spawn_n(wait_for_entrypoint)
        eventlet.spawn_n(wait_for_container)

        return hook_result.wait()

    yield hook


class EntrypointWaiterTimeout(Exception):
    pass


class EntrypointWaiter(object):

    class Result(object):
        res = None
        exc_info = None

        def __init__(self):
            self.event = event.Event()

        def send(self, res, exc_info):
            if not self.event.ready():
                self.res = res
                self.exc_info = exc_info
                self.event.send(True)

        def get(self):
            if self.exc_info is not None:
                six.reraise(*self.exc_info)
            return self.res

        def wait(self):
            self.event.wait()

    def __init__(self, callback):
        self.callback = callback
        self.result = EntrypointWaiter.Result()
        self.teardown = EntrypointWaiter.Result()

    @contextmanager
    def wait(self):
        yield
        self.result.wait()
        self.teardown.wait()

    def worker_result(self, worker_ctx, result, exc_info):
        complete = True
        if callable(self.callback):
            complete = self.callback(worker_ctx, result, exc_info)

        if complete:
            self.result.send(result, exc_info)
        return complete

    def worker_teardown(self, worker_ctx):
        self.teardown.send(True, None)
        return True


@contextmanager
def entrypoint_waiter(container, method_name, timeout=30, callback=None):
    """ Context manager that waits until an entrypoint has fired (and
    completed).

    It yields a :class:`nameko.testing.services.EntrypointWaiter.Result` object
    that can be used to get the result returned (exception raised) by the
    entrypoint after the waiter has exited.

    :Parameters:
        container : ServiceContainer
            The container hosting the service owning the entrypoint
        method_name : str
            The name of the entrypoint decorated method on the service class
        timeout : int
            Maximum seconds to wait
        callback : callable
            Function to conditionally control whether the entrypoint_waiter
            should exit for a particular invocation

    Optionally allows a `callback` to be provided which is invoked whenever
    the entrypoint fires. If provided, the callback must return `True`
    for the `entrypoint_waiter` to exit. The signature for the callback
    function is::

        def callback(worker_ctx, result, exc_info):
            pass

    Where there parameters are as follows:

        worker_ctx (WorkerContext): WorkerContext of the entrypoint call.

        result (object): The result, if any, that the entrypoint returned.

        exc_info (tuple): Tuple as returned by `sys.exc_info` if the
            entrypoint raised an exception, otherwise `None`.


    The `timeout` argument specifies the maximum number of seconds the
    `entrypoint_waiter` should wait before exiting. It can be disabled by
    passing `None`.

    **Usage**

    ::
        class Service(object):
            name = "service"

            @event_handler('srcservice', 'eventtype')
            def handle_event(self, msg):
                return msg

        container = ServiceContainer(Service, config)
        container.start()

        # basic
        with entrypoint_waiter(container, 'handle_event'):
            ...  # action that dispatches event

        # giving access to the result
        with entrypoint_waiter(container, 'handle_event') as result:
            ...  # action that dispatches event
        res = result.get()

        # with custom timeout
        with entrypoint_waiter(container, 'handle_event', timeout=5):
            ...  # action that dispatches event

        # with callback that waits until entrypoint stops raising
        def callback(worker_ctx, result, exc_info):
            if exc_info is None:
                return True

        with entrypoint_waiter(container, 'handle_event', callback=callback):
            ...  # action that dispatches event

    """
    if not get_extension(container, Entrypoint, method_name=method_name):
        raise RuntimeError("{} has no entrypoint `{}`".format(
            container.service_name, method_name))

    exc = EntrypointWaiterTimeout(
        "EntrypointWaiterTimeout on {}.{} after {} seconds".format(
            container.service_name, method_name, timeout)
    )

    waiter = EntrypointWaiter(callback)

    with eventlet.Timeout(timeout, exception=exc):
        with container.register_entrypoint_waiter(method_name, waiter):
            with waiter.wait():
                yield waiter.result


def worker_factory(service_cls, **dependencies):
    """ Return an instance of ``service_cls`` with its injected dependencies
    replaced with :class:`~mock.MagicMock` objects, or as given in
    ``dependencies``.

    **Usage**

    The following example service proxies calls to a "maths" service via
    an ``RpcProxy`` dependency::

        from nameko.rpc import RpcProxy, rpc

        class ConversionService(object):
            name = "conversions"

            maths_rpc = RpcProxy("maths")

            @rpc
            def inches_to_cm(self, inches):
                return self.maths_rpc.multiply(inches, 2.54)

            @rpc
            def cm_to_inches(self, cms):
                return self.maths_rpc.divide(cms, 2.54)

    Use the ``worker_factory`` to create an instance of
    ``ConversionService`` with its dependencies replaced by MagicMock objects::

        service = worker_factory(ConversionService)

    Nameko's entrypoints do not modify the service methods, so instance methods
    can be called directly with the same signature. The replaced dependencies
    can be used as any other MagicMock object, so a complete unit test for
    the conversion service may look like this::

        # create worker instance
        service = worker_factory(ConversionService)

        # replace "maths" service
        service.maths_rpc.multiply.side_effect = lambda x, y: x * y
        service.maths_rpc.divide.side_effect = lambda x, y: x / y

        # test inches_to_cm business logic
        assert service.inches_to_cm(300) == 762
        service.maths_rpc.multiply.assert_called_once_with(300, 2.54)

        # test cms_to_inches business logic
        assert service.cms_to_inches(762) == 300
        service.maths_rpc.divide.assert_called_once_with(762, 2.54)

    *Providing Dependencies*

    The ``**dependencies`` kwargs to ``worker_factory`` can be used to provide
    a replacement dependency instead of a mock. For example, to unit test a
    service against a real database:

    .. literalinclude::
        ../examples/testing/alternative_dependency_unit_test.py

    If a named dependency provider does not exist on ``service_cls``, a
    ``ExtensionNotFound`` exception is raised.

    """
    service = service_cls()
    for name, attr in inspect.getmembers(service_cls):
        if isinstance(attr, DependencyProvider):
            try:
                dependency = dependencies.pop(name)
            except KeyError:
                dependency = MagicMock()
            setattr(service, name, dependency)

    if dependencies:
        raise ExtensionNotFound(
            "DependencyProvider(s) '{}' not found on {}.".format(
                dependencies.keys(), service_cls))

    return service


class MockDependencyProvider(DependencyProvider):
    def __init__(self, attr_name):
        self.attr_name = attr_name
        self.dependency = MagicMock()

    def get_dependency(self, worker_ctx):
        return self.dependency


def replace_dependencies(container, *dependencies):
    """ Replace the dependency providers on ``container`` with
    :class:`MockDependencyProvider` objects if they are named in
    ``dependencies``.

    Return the :attr:`MockDependencyProvider.dependency` of the replacements,
    so that calls to the replaced dependencies can be inspected. Return a
    single object if only one dependency was replaced, and a generator
    yielding the replacements in the same order as ``names`` otherwise.

    Replacements are made on the container instance and have no effect on the
    service class. New container instances are therefore unaffected by
    replacements on previous instances.

    **Usage**

    ::

        from nameko.rpc import RpcProxy, rpc
        from nameko.standalone.rpc import ServiceRpcProxy

        class ConversionService(object):
            name = "conversions"

            maths_rpc = RpcProxy("maths")

            @rpc
            def inches_to_cm(self, inches):
                return self.maths_rpc.multiply(inches, 2.54)

            @rpc
            def cm_to_inches(self, cms):
                return self.maths_rpc.divide(cms, 2.54)

        container = ServiceContainer(ConversionService, config)
        maths_rpc = replace_dependencies(container, "maths_rpc")

        container.start()

        with ServiceRpcProxy('conversionservice', config) as proxy:
            proxy.cm_to_inches(100)

        # assert that the dependency was called as expected
        maths_rpc.divide.assert_called_once_with(100, 2.54)

    """
    if container.started:
        raise RuntimeError('You must replace dependencies before the '
                           'container is started.')

    dependency_names = {dep.attr_name for dep in container.dependencies}

    missing = set(dependencies) - dependency_names
    if missing:
        raise ExtensionNotFound("Dependency(s) '{}' not found on {}.".format(
            missing, container))

    replacements = OrderedDict()

    named_dependencies = {dep.attr_name: dep for dep in container.dependencies
                          if dep.attr_name in dependencies}
    for name in dependencies:
        dependency = named_dependencies[name]
        replacement = MockDependencyProvider(name)
        replacements[dependency] = replacement
        container.dependencies.remove(dependency)
        container.dependencies.add(replacement)

    # if only one name was provided, return any replacement directly
    # otherwise return a generator
    res = (replacement.dependency for replacement in replacements.values())
    if len(dependencies) == 1:
        return next(res)
    return res


def restrict_entrypoints(container, *entrypoints):
    """ Restrict the entrypoints on ``container`` to those named in
    ``entrypoints``.

    This method must be called before the container is started.

    **Usage**

    The following service definition has two entrypoints:

    .. code-block:: python

        class Service(object):
            name = "service"

            @timer(interval=1)
            def foo(self, arg):
                pass

            @rpc
            def bar(self, arg)
                pass

            @rpc
            def baz(self, arg):
                pass

        container = ServiceContainer(Service, config)

    To disable the timer entrypoint on ``foo``, leaving just the RPC
    entrypoints:

    .. code-block:: python

        restrict_entrypoints(container, "bar", "baz")

    Note that it is not possible to identify multiple entrypoints on the same
    method individually.

    """
    if container.started:
        raise RuntimeError('You must restrict entrypoints before the '
                           'container is started.')

    entrypoint_deps = list(container.entrypoints)
    entrypoint_names = {ext.method_name for ext in entrypoint_deps}

    missing = set(entrypoints) - entrypoint_names
    if missing:
        raise ExtensionNotFound("Entrypoint(s) '{}' not found on {}.".format(
            missing, container))

    for entrypoint in entrypoint_deps:
        if entrypoint.method_name not in entrypoints:
            container.entrypoints.remove(entrypoint)


class Once(Entrypoint):
    """ Entrypoint that spawns a worker exactly once, as soon as
    the service container started.
    """
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def start(self):
        self.container.spawn_worker(self, self.args, self.kwargs)

once = Once.decorator

# dummy entrypoint
dummy = Entrypoint.decorator
