import pytest

from hdp.config import Settings
from hdp.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark(Settings(), "hdp-tests")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()


@pytest.fixture()
def settings(tmp_path):
    return Settings(base_path=str(tmp_path / "lake"))
