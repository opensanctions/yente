import pytest
import json
from .conftest import FIXTURES_PATH

from yente.search.indexer import (
    delta_update_index,
)

# TODO: Mock httpx instead
DS_WITH_DELTAS = "https://data.opensanctions.org/artifacts/sanctions/versions.json"


@pytest.mark.asyncio
async def test_end_to_end(httpx_mock):
    """
    Test getting the delta versions and updating the index, using the data
    mocks in the fixtures directory.
    """
    # No alias or index exists, so the first run should build the index from the beginning
    available_versions = json.loads((FIXTURES_PATH / "versions.json").read_text())
    httpx_mock.add_response(
        200,
        url="https://data.opensanctions.org/datasets/sanctions/entities.ftm.json",
        content=(FIXTURES_PATH / "dataset/has_deltas/entities.ftm.json").read_bytes(),
    )
    httpx_mock.add_response(
        200,
        url="https://data.opensanctions.org/artifacts/sanctions/versions.json",
        content=(FIXTURES_PATH / "start_version.json").read_bytes(),
    )
    await delta_update_index()
    for version, url in available_versions["versions"].items():
        httpx_mock.add_response(
            200,
            url=url,
            content=(
                FIXTURES_PATH / f"dataset/has_deltas/{version}/entities.delta.json"
            ).read_bytes(),
        )
    httpx_mock.add_response(
        200,
        url="https://data.opensanctions.org/artifacts/sanctions/versions.json",
        content=(FIXTURES_PATH / "versions.json").read_bytes(),
    )
    await delta_update_index()
    pass
