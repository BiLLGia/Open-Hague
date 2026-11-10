from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def obv_fixture():
    def load(name: str) -> bytes:
        return (FIXTURES / "openbesluitvorming" / name).read_bytes()

    return load


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def fast_client():
    """A PoliteClient that never really sleeps."""
    from pipeline.http import PoliteClient

    clock = FakeClock()
    client = PoliteClient(
        user_agent="OpenHague/test", min_interval=1.0, clock=clock.monotonic, sleep=clock.sleep
    )
    yield client
    client.close()
