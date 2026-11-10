import httpx
import pytest
import respx

from pipeline.http import PoliteClient, RetryExhausted


class FakeClock:
    """Deterministic clock: sleep() advances time instead of waiting."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def make_client(clock: FakeClock, **kwargs) -> PoliteClient:
    return PoliteClient(
        user_agent="OpenHague/test (+https://example.org)",
        min_interval=1.0,
        clock=clock.monotonic,
        sleep=clock.sleep,
        **kwargs,
    )


@respx.mock
def test_sends_user_agent():
    route = respx.get("https://api.test/x").mock(return_value=httpx.Response(200))
    clock = FakeClock()
    with make_client(clock) as client:
        client.get("https://api.test/x")
    assert route.calls.last.request.headers["User-Agent"] == "OpenHague/test (+https://example.org)"


@respx.mock
def test_rate_limits_consecutive_requests():
    respx.get("https://api.test/x").mock(return_value=httpx.Response(200))
    clock = FakeClock()
    with make_client(clock) as client:
        client.get("https://api.test/x")
        clock.now += 0.25  # caller spent 0.25s processing
        client.get("https://api.test/x")
    assert clock.sleeps == [pytest.approx(0.75)]


@respx.mock
def test_retries_on_503_then_succeeds():
    route = respx.get("https://api.test/x").mock(
        side_effect=[httpx.Response(503), httpx.Response(200, text="ok")]
    )
    clock = FakeClock()
    with make_client(clock, max_retries=3, backoff_base=2.0) as client:
        response = client.get("https://api.test/x")
    assert response.text == "ok"
    assert route.call_count == 2
    assert 2.0 in clock.sleeps


@respx.mock
def test_respects_retry_after_header():
    respx.get("https://api.test/x").mock(
        side_effect=[httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200)]
    )
    clock = FakeClock()
    with make_client(clock) as client:
        client.get("https://api.test/x")
    assert 7.0 in clock.sleeps


@respx.mock
def test_retries_transport_errors():
    respx.get("https://api.test/x").mock(
        side_effect=[httpx.ConnectError("boom"), httpx.Response(200)]
    )
    clock = FakeClock()
    with make_client(clock) as client:
        assert client.get("https://api.test/x").status_code == 200


@respx.mock
def test_gives_up_after_max_retries():
    respx.get("https://api.test/x").mock(return_value=httpx.Response(502))
    clock = FakeClock()
    with make_client(clock, max_retries=2) as client, pytest.raises(RetryExhausted):
        client.get("https://api.test/x")


@respx.mock
def test_does_not_retry_client_errors():
    route = respx.get("https://api.test/x").mock(return_value=httpx.Response(404))
    clock = FakeClock()
    with make_client(clock) as client, pytest.raises(httpx.HTTPStatusError):
        client.get("https://api.test/x")
    assert route.call_count == 1


@respx.mock
def test_backoff_is_capped():
    respx.get("https://api.test/x").mock(
        side_effect=[httpx.Response(500)] * 3 + [httpx.Response(200)]
    )
    clock = FakeClock()
    with make_client(clock, max_retries=5, backoff_base=50.0, backoff_cap=60.0) as client:
        client.get("https://api.test/x")
    assert max(clock.sleeps) <= 60.0
