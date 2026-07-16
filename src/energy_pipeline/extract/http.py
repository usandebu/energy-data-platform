from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_retry_session() -> Session:
    session = Session()
    adapter = HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session
