"""
FeatureStore module - Core functionality for managing and serving ML features
"""

from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel

# from sqlalchemy import and_
from sqlalchemy.orm import Session

from featurium.core.models import (  # Feature,; FeatureValue,; JoinKeyValue,; JoinKey,
    Attribute,
    Entity,
    Project,
)


class EntityRequest(BaseModel):
    """
    EntityRequest is a dictionary with a join_key and a value.
    """

    join_key: str
    value: Any


class FeatureRequest(BaseModel):
    """
    FeatureRequest is a dictionary with either a name or an id, but not both.
    At least one must be provided.
    """

    name: Optional[str] = None
    id: Optional[int] = None

    def __init__(self, **data):
        super().__init__(**data)
        if (self.name is None and self.id is None) or (
            self.name is not None and self.id is not None
        ):
            raise ValueError("Exactly one of 'name' or 'id' must be provided")


class FeatureViewRequest(BaseModel):
    """
    FeatureViewRequest is a dictionary with a list of entities, features and timestamps.
    """

    # entities: [
    #     {"driver_id": 1, "trip_id": 1},
    #     {"driver_id": 2, "trip_id": 2},
    #     {"driver_id": 3, "trip_id": 3},
    # ]

    # features:[

    #     {"name": "driver_avg_rating", }
    #     {"id":2}
    # ]

    # timestamps: [
    #     datetime(2021, 4, 12, 10, 59, 42),
    #     datetime(2021, 4, 12, 8, 12, 10),
    #     datetime(2021, 4, 12, 16, 40, 26),
    # ]

    entities: List[EntityRequest]
    features: List[FeatureRequest]
    timestamps: List[datetime]


class FeatureStore:
    """
    A lightweight feature store for managing and serving machine learning features.
    """

    def __init__(self, db: Session):
        """Initialize the feature store with a database session."""
        self.db = db

    def list_projects(self) -> List[str]:
        """List all projects in the feature store."""
        return [p[0] for p in self.db.query(Project.name).distinct().all()]

    def list_features(self, project_name: Optional[str] = None) -> List[str]:
        """List all features in the feature store."""
        query = self.db.query(Attribute.name)
        if project_name:
            query = query.join(Project).filter(Project.name == project_name)
        return [f[0] for f in query.distinct().all()]

    def list_entities(self, project_name: Optional[str] = None) -> List[str]:
        """List all entities in the feature store."""
        query = self.db.query(Entity.name)
        if project_name:
            query = query.join(Project).filter(Project.name == project_name)
        return [e[0] for e in query.distinct().all()]

    def get_historical_features(
        self,
        # project_name: Optional[str] = None,
        # entity_name: Optional[str] = None,
        # feature_names: Optional[list[str]] = None,
        # join_key_values: Optional[list[dict[str, any]]] = None,
        timestamp: Optional[datetime] = None,
    ) -> list[dict[str, any]]:
        """Get historical features for a given entity and join key values."""

        # project = self.db.query(Project).filter(Project.name == project_name).one()
        # entity = (
        #     self.db.query(Entity)
        #     .filter(Entity.name == entity_name)
        #     .filter(Entity.project_id == project.id)
        #     .one()
        # )

        # join_keys = entity.join_keys
        # join_key_map = {jk.key: jk.id for jk in join_keys}

        # # buscar las features por nombre

        # features = (
        #     self.db.query(Feature)
        #     .filter(Feature.name.in_(feature_names))
        #     .filter(Feature.entities.contains(entity))
        #     .all()
        # )
        # feature_ids = {f.id: f.name for f in features}

        results = []
        # for key_value in join_key_values:
        #     row = {}
        #     filters = []
        #     for key, value in key_value.items():
        #         jk_id = join_key_map.get(key)
        #         if not jk_id:
        #             raise ValueError(
        #                 f"Join key '{key}' not defined for entity '{entity_name}'"
        #             )
        #         filters.append(
        #             and_(
        #                 JoinKeyValue.join_key_id == jk_id,
        #                 JoinKeyValue.value["integer"].as_integer() == value,
        #             )
        #         )

        #     subquery = self.db.query(JoinKeyValue.id).filter(and_(*filters)).subquery() # noqa: E501

        #     query = (
        #         self.db.query(FeatureValue, FeatureJoinKeyValue, JoinKeyValue)
        #         .join(
        #             FeatureJoinKeyValue,
        #             FeatureJoinKeyValue.feature_value_id == FeatureValue.id,
        #         )
        #         .join(
        #             JoinKeyValue,
        #             JoinKeyValue.id == FeatureJoinKeyValue.join_key_value_id,
        #         )
        #         .filter(JoinKeyValue.id.in_(subquery))
        #         .filter(FeatureValue.feature_id.in_(feature_ids.keys()))
        #     )

        #     if timestamp:
        #         query = query.filter(FeatureValue.timestamp <= timestamp)

        #     query = query.order_by(FeatureValue.timestamp.desc())

        #     for fv, link, jkv in query:
        #         row[feature_ids[fv.feature_id]] = fv.value.get(
        #             fv.data_type.name.lower()
        #         )
        #         row[jkv.join_key.key] = jkv.value.get(jkv.data_type.name.lower())
        #         row["timestamp"] = fv.timestamp

        #     if row:
        #         results.append(row)

        return results


# if __name__ == "__main__":
#     request = FeatureViewRequest(
#         entities=[EntityRequest(join_key="driver_id", value=1)],
#         features=[FeatureRequest(name="driver_avg_rating", id=2)],
#         timestamps=[datetime(2021, 4, 12, 10, 59, 42)],
#     )
