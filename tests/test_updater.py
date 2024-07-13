import re
import json
import pytest
from .conftest import FIXTURES_PATH
from typing import List, Any

from yente.data import get_catalog, refresh_catalog
from yente.data.updater import DatasetUpdater


@pytest.fixture
def non_mocked_hosts() -> List[str]:
    return ["localhost"]


@pytest.mark.asyncio
async def test_updater(httpx_mock: Any, sanctions_catalog: None) -> None:
    """
    Test getting the delta versions and updating the index, using the data
    mocks in the fixtures directory.
    """
    # Point the entities to our local fixture of 7 entities
    url_pat = re.compile(
        "https:\/\/data\.opensanctions\.org\/datasets\/[\w-]+\/sanctions\/entities\.ftm\.json"
    )
    httpx_mock.add_response(
        200,
        url=url_pat,
        content=(FIXTURES_PATH / "dataset/t1/entities.ftm.json").read_bytes(),
    )
    # The catalog index gets a copy of a real index, as seen at test writing time
    httpx_mock.add_response(
        200,
        url="https://data.opensanctions.org/datasets/latest/index.json",
        content=(FIXTURES_PATH / "dataset/t1/index.json").read_bytes(),
    )
    await refresh_catalog()
    catalog = await get_catalog()
    dataset = catalog.get("sanctions")
    updater = await DatasetUpdater.build(dataset, dataset.version)
    assert not updater.needs_update()

    updater = await DatasetUpdater.build(dataset, None)
    assert not updater.is_incremental
    assert updater.needs_update()
    assert updater.delta_urls is None

    operations = [x async for x in updater.load()]
    assert len(operations) == 7, operations
    for op in operations:
        assert op["op"] == "ADD"

    base_version = dataset.version
    dataset.version = "20240528134729-3iv"
    url = f"https://data.opensanctions.org/artifacts/sanctions/{dataset.version}/delta.json"
    dataset.delta_url = url
    delta_index_path = FIXTURES_PATH / "dataset/t2/delta.json"

    with open(delta_index_path, "r") as f:
        index = json.load(f)
        for version, delta_url in index["versions"].items():
            if version == base_version:
                continue
            httpx_mock.add_response(
                200,
                url=delta_url,
                content=(
                    FIXTURES_PATH / f"dataset/t2/{version}/entities.delta.json"
                ).read_bytes(),
            )

    # Point the index to our fixture containing the new versions
    httpx_mock.add_response(
        200,
        url=url,
        content=delta_index_path.read_bytes(),
    )
    updater = await DatasetUpdater.build(dataset, base_version)
    assert updater.needs_update()
    assert updater.is_incremental
    assert len(updater.delta_urls) == 4

    operations = [x async for x in updater.load()]
    assert len(operations) == 6, operations
    ops = {"ADD": 0, "DEL": 0, "MOD": 0}
    for op in operations:
        ops[op["op"]] += 1

    assert ops["ADD"] == 4
    assert ops["DEL"] == 1
    assert ops["MOD"] == 1
