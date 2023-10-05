from functools import wraps
from os import write
from typing import Callable, ParamSpec
from prefect import task
from prefect.tasks import exponential_backoff
from requests import Response


P = ParamSpec("P")


def json_task(func: Callable[P, Response]):
    """
    Decorator to wrap a function in a Prefect task with retry logic,
    which makes a request and returns the JSON response.

    This decorator will create a new Prefect task that wraps the
    provided function. The wrapped function will be retried up to
    3 times with an exponential backoff in case of failure, and
    will raise a HTTPError if the response status is an error.

    The type hints of the original function are preserved.

    Parameters:
    -----------
    func : Callable[P, Response]
        The function to be wrapped. It is expected to return a
        `requests.Response` object.

    Returns:
    --------
    Callable
        A new function that wraps the original function in a Prefect
        task with retry logic, and returns the JSON content of the
        response.

    Usage:
    ------
    class BaseUrlSession(Session):
        def __init__(self, base_url):
            super().__init__()
            self.base_url = base_url

        def request(self, method, url, **kwargs):
            url = f"{self.base_url}/{url}"
            return super().request(method, url, **kwargs)

    API_URL = 'https://api.example.com'
    _session = BaseUrlSession(API_URL)
    _auth = ('username', 'password')
    _session.auth = _auth
    get = json_task(_session.get)
    """

    @task(retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=10))
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs):
        response = func(*args, **kwargs)
        response.raise_for_status()
        return response.json()

    return wrapper
