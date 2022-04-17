from banal import as_bool
from datetime import datetime
from typing import Any, Dict, List
from elastic_transport import ObjectApiResponse
from pydantic import BaseModel, Field

from yente.data.common import ResultsResponse
from yente.data.util import iso_datetime


class StatementModel(BaseModel):
    id: str = Field(..., example="0000ad52d4d91a8...")
    entity_id: str = Field(..., example="ofac-1234")
    canonical_id: str = Field(..., example="NK-1234")
    prop: str = Field(..., example="alias")
    prop_type: str = Field(..., example="name")
    schema_: str = Field(..., example="LegalEntity", alias="schema")
    value: str = Field(..., example="John Doe")
    dataset: str = Field(..., example="default")
    target: bool = Field(..., example=True)
    first_seen: datetime
    last_seen: datetime

    def to_doc(self, index: str) -> Dict[str, Any]:
        data = self.dict(exclude={"id", "schema_"})
        data["schema"] = self.schema_
        return {"_index": index, "_id": self.id, "_source": data}

    @classmethod
    def from_row(cls, row: Dict[str, str]):
        return cls(
            id=row["id"],
            entity_id=row["entity_id"],
            canonical_id=row["canonical_id"],
            prop=row["prop"],
            prop_type=row["prop_type"],
            schema=row["schema"],
            value=row["value"],
            dataset=row["dataset"],
            target=as_bool(row["target"]),
            first_seen=iso_datetime(row["first_seen"]),
            last_seen=iso_datetime(row["last_seen"]),
        )

    @classmethod
    def from_search(cls, response: ObjectApiResponse) -> List["StatementModel"]:
        results: List[StatementModel] = []
        hits = response.get("hits", {})
        for hit in hits.get("hits", []):
            source = hit["_source"]
            stmt = cls(
                id=hit["_id"],
                entity_id=source["entity_id"],
                canonical_id=source["canonical_id"],
                prop=source["prop"],
                prop_type=source["prop_type"],
                schema=source["schema"],
                value=source["value"],
                dataset=source["dataset"],
                target=source["target"],
                first_seen=source["first_seen"],
                last_seen=source["last_seen"],
            )
            results.append(stmt)
        return results


class StatementResponse(ResultsResponse):
    results: List[StatementModel]
