from followthemoney import model

from yente.scoring import compare_ofac
from yente.data.entity import Entity


def _make_named(*names):
    data = {"id": "test", "schema": "Person", "properties": {"name": names}}
    return Entity.from_dict(model, data)


def test_ofac_scoring():
    a = _make_named("Vladimir Putin")
    b = _make_named("Vladimir Putin")
    assert compare_ofac(a, b)["score"] == 1.0
    b = _make_named("Vladimir Pudin")
    assert compare_ofac(a, b)["score"] == 0.95
