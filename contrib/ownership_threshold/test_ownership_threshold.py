import io
import json
from pathlib import Path
from typing import Any

from ownership_threshold import run


def entity(eid: str, schema: str, **props: Any) -> dict[str, Any]:
    data: dict[str, Any] = {"id": eid, "schema": schema, "properties": {}}
    if "datasets" in props:
        data["datasets"] = props.pop("datasets")
    data["properties"] = {
        k: v if isinstance(v, list) else [v] for k, v in props.items()
    }
    return data


def owns(eid: str, owner: str, asset: str, **props: Any) -> dict[str, Any]:
    return entity(eid, "Ownership", owner=owner, asset=asset, **props)


ENTITIES: list[dict[str, Any]] = [
    entity("sdn", "Company", name="Listed Holding", datasets=["us_ofac_sdn"]),
    entity("sdn-s", "Sanction", entity="sdn", program="RUSSIA-EO14024"),
    entity("uk", "Company", name="UK Listed", datasets=["gb_fcdo_sanctions"]),
    entity("uk-s", "Sanction", entity="uk", program="Russia"),
    # 60% held by an SDN: blocked under the OFAC rule.
    entity(
        "a", "Company", name="Majority Sub", country="cy", topics="sanction.control"
    ),
    owns("o-a", "sdn", "a", percentage="60"),
    # 30% held by an SDN: tagged sanction.control, but under the threshold.
    entity(
        "b", "Company", name="Minority Sub", country="cy", topics="sanction.control"
    ),
    owns("o-b", "sdn", "b", percentage="30"),
    # 100% held by a party that is not on the SDN List.
    entity("c", "Company", name="UK Sub", country="cy", topics="sanction.control"),
    owns("o-c", "uk", "c", percentage="100"),
    # OFAC's "Owned or Controlled By" carries no percentage.
    entity("d", "Company", name="OFAC Sub", country="cy", topics="sanction.control"),
    owns("o-d", "sdn", "d", role="Owned or Controlled By"),
    # A holding that ended the day it started (dissolved on registration).
    entity("e", "Company", name="Dissolved", country="ru"),
    owns(
        "o-e",
        "sdn",
        "e",
        percentage="100",
        startDate="2018-12-07",
        endDate="2018-12-07",
    ),
    entity("unrelated", "Person", name="Nobody"),
]


def test_threshold_on_sanction_control(tmp_path: Path) -> None:
    path = tmp_path / "entities.ftm.json"
    path.write_text("".join(json.dumps(e) + "\n" for e in ENTITIES))
    out = io.StringIO()
    log = io.StringIO()
    summary = run([str(path)], ["ofac-50"], out, as_of="2026-11-18", log=log)

    rows = {r["entity"]: r for r in map(json.loads, out.getvalue().splitlines())}
    assert sorted(rows) == ["a", "d"]
    assert rows["a"]["verdict"] == "definitely"
    assert rows["a"]["share"] == [60, 60]
    assert rows["a"]["caption"] == "Majority Sub"
    assert rows["a"]["topics"] == ["sanction.control"]
    assert "sdn (listed) owns 60% of a" in rows["a"]["explain"]
    assert rows["d"]["verdict"] == "possibly"
    assert summary["ofac-50"] == {"definitely": 1, "possibly": 1}
    assert "4 tagged sanction.control" in log.getvalue()
    assert "'not': 2" in log.getvalue()
