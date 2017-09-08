import pytest
from flaky import flaky


def port_already_in_use(err, *args):  # pragma: no cover
    return issubclass(err[0], OSError) and "already in use" in str(err[0])


@pytest.fixture
def retry_if_port_in_use(request):
    decorator = flaky(rerun_filter=port_already_in_use)
    decorator(request.function)
