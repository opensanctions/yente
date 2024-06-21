import pytest
import json
import re
from .conftest import FIXTURES_PATH
from yente.settings import ENTITY_INDEX
from typing import List, Any
import httpx
from yente.search.indexer import (
    delta_update_catalog,
)


@pytest.fixture
def non_mocked_hosts() -> List[str]:
    return ["localhost"]


@pytest.mark.asyncio
async def test_end_to_end(httpx_mock: Any, sanctions_catalog: Any) -> None:
    """
    Test getting the delta versions and updating the index, using the data
    mocks in the fixtures directory.
    """
    # No alias or index exists, so the first run should build the index from the beginning
    available_versions = json.loads(
        (FIXTURES_PATH / "dataset/t2/delta.json").read_text()
    )
    # Point the entities to our local fixture of 7 entities
    url_pat = re.compile(
        "https:\/\/(mirror|data)\.opensanctions\.net\/datasets\/[\w-]+\/sanctions\/entities\.ftm\.json"
    )
    httpx_mock.add_response(
        200,
        url=url_pat,
        content=(FIXTURES_PATH / "dataset/t1/entities.ftm.json").read_bytes(),
    )
    # Point the delta index to our local fixture containing only one version
    url_pat = re.compile(
        "https:\/\/(data|mirror)\.opensanctions\.org\/artifacts\/sanctions\/[\w-]+\/delta\.json"
    )
    httpx_mock.add_response(
        200,
        url=url_pat,
        content=(FIXTURES_PATH / "dataset/t1/delta.json").read_bytes(),
    )
    # The catalog index gets a copy of a real index, as seen at test writing time
    httpx_mock.add_response(
        200,
        url="https://data.opensanctions.org/datasets/latest/index.json",
        content=(FIXTURES_PATH / "dataset/t1/index.json").read_bytes(),
    )
    await delta_update_catalog()
    # Pretend that new versions have been created and mock each call to them to a fixture
    for version, url in available_versions["versions"].items():
        httpx_mock.add_response(
            200,
            url=url,
            content=(
                FIXTURES_PATH / f"dataset/t2/{version}/entities.delta.json"
            ).read_bytes(),
        )
    # Point the index to our fixture containing the new versions
    httpx_mock.add_response(
        200,
        url="https://data.opensanctions.org/artifacts/sanctions/20240527134702-zbn/delta.json",
        content=(FIXTURES_PATH / "dataset/t2/delta.json").read_bytes(),
    )
    await delta_update_catalog()
    httpx.post(f"http://localhost:9200/{ENTITY_INDEX}/_refresh")
    resp = httpx.get(f"http://localhost:9200/{ENTITY_INDEX}/_count")
    assert resp.status_code == 200
    assert resp.json().get("count") == 10
