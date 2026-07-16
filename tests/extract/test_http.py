from requests.adapters import HTTPAdapter

from energy_pipeline.extract.http import build_retry_session


def test_build_retry_session_configures_retries_for_transient_errors():
    session = build_retry_session()

    adapter = session.get_adapter("https://")

    assert isinstance(adapter, HTTPAdapter)
    assert adapter.max_retries.total == 3
    assert adapter.max_retries.backoff_factor == 1
    assert adapter.max_retries.status_forcelist == [429, 500, 502, 503, 504]
    assert adapter.max_retries.allowed_methods == ["GET"]


def test_build_retry_session_configures_http_and_https_adapters():
    session = build_retry_session()

    assert isinstance(session.get_adapter("https://"), HTTPAdapter)
    assert isinstance(session.get_adapter("http://"), HTTPAdapter)
