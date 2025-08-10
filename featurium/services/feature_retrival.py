import itertools
import json
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Protocol

import pandas as pd
from sqlalchemy import Select, and_, select
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.functions import func

from featurium.core.models import (
    Entity,
    Feature,
    FeatureValue,
    JoinKey,
    JoinKeyValue,
    Project,
)


class FeatureRetrievalProtocol(Protocol):
    """
    Protocol for the FeatureRetrieval Service.
    """

    def get_feature_values(  # noqa: E704
        self,
        project_name: str,
        entity_name: str,
        join_keys: List[Any] | None = None,
        feature_names: List[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        include_timestamp: bool = False,
    ) -> pd.DataFrame:
        """
        Get the feature values.

        Args:
            project_name: The name of the project.
            entity_name: The name of the entity.
            join_keys: The join keys.
            feature_names: The feature names.
            start_time: The start time.
            end_time: The end time.
            include_timestamp: Whether to include the timestamp in the dataframe.

        Returns:
            A pandas dataframe with the feature values.
                The columns are the feature names,
                the rows are the entity values.
                The values are the feature values.
        """
        ...


class FeatureRetrieval:
    def __init__(self, db: Session):
        """
        Initialize the FeatureRetrieval Service.
        """
        self.db = db

    def get_feature_values(
        self,
        project_name: str,
        entity_name: str,
        join_keys: List[Any] | None = None,
        feature_names: List[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        include_timestamp: bool = False,
    ):
        """
        Get the feature values.

        Args:
            project_name: The name of the project.
            entity_name: The name of the entity.
            join_keys: The join keys.
            feature_names: The feature names.
            start_time: The start time.
            end_time: The end time.
            include_timestamp: Whether to include the timestamp in the dataframe.
        """
        project, entity, features, join_key_values = self._validate_args(
            project_name,
            entity_name,
            join_keys,
            feature_names,
            start_time,
            end_time,
        )

        query = self._build_features_query(
            project,
            entity,
            features,
            # I don't need to filter if the user doesn't require it
            join_key_values if join_keys else None,
            start_time,
            end_time,
        )

        result = self.db.execute(query).mappings().all()

        df = self._build_features_df(
            result,
            features,
            join_key_values,
            strict=len(join_keys) > 0,
            include_timestamp=include_timestamp,
        )

        return df

    def _get_project(self, project_name: str) -> Project:
        """
        Get a project by name.

        Args:
            project_name: The name of the project.

        """
        project = self.db.scalar(select(Project).where(Project.name == project_name))
        if not project:
            raise ValueError(f"Project '{project_name}' not found")
        return project

    def _get_entity(self, project: Project, entity_name: str) -> Entity:
        """
        Get an entity from the database.

        Args:
            project: The project.
            entity_name: The name of the entity.
        """
        entity = self.db.scalar(
            select(Entity).where(
                Entity.name == entity_name, Entity.project_id == project.id
            )
        )
        if not entity:
            raise ValueError(
                f"Entity '{entity_name}' not found in project '{project.name}'"
            )
        return entity

    def _get_features(
        self,
        project: Project,
        entity: Entity,
        feature_names: List[str] | None = None,
    ) -> List[Feature]:
        """
        Get features from the project filtered by entity.

        - If feature_names are provided, only the features
            that match the names are returned.
        - If no feature_names are provided, all features from the project are returned.

        Args:
            project: The project.
            entity: The entity.
            feature_names: The feature names.
        """
        if feature_names:
            if len(feature_names) != len(set(feature_names)):
                duplicates = [
                    item for item, count in Counter(feature_names).items() if count > 1
                ]
                raise ValueError(
                    f"feature_names must be a list of unique feature names. "
                    f"Found duplicates: {duplicates}"
                )
            features = self.db.scalars(
                select(Feature).where(
                    Feature.project_id == project.id,
                    Feature.name.in_(set(feature_names)),
                    # this is required to avoid returning features within
                    # the same project but from other entities
                    Feature.entities.contains(entity),
                )
            ).all()
            if len(features) != len(set(feature_names)):
                raise ValueError(
                    f"Some features not found in project {project.name}. "
                    f"Missing features: {set(feature_names) - set(f.name for f in features)}"  # noqa:E501
                )
            # Sort features by the order of feature_names
            feature_dict = {f.name: f for f in features}
            features = [feature_dict[name] for name in feature_names]
        else:
            # if no feature_names are provided,
            # all features from the project are returned
            features = self.db.scalars(
                select(Feature).where(
                    Feature.project_id == project.id,
                    Feature.entities.contains(entity),
                )
            ).all()

        if not features:
            raise ValueError("No features found")

        return features

    def _extract_single_value(self, d: Any) -> Any:
        """
        Extract the simple value from the `feature_value` dictionary
        """
        if isinstance(d, dict) and len(d) == 1:
            return next(iter(d.values()))
        return d  # optional: pass if not a dict

    def _get_join_key_values(
        self, entity: Entity, join_keys: List[Any]
    ) -> List[JoinKeyValue]:
        """
        Get the join key values for the entity.

        Args:
            entity: The entity.
            join_keys: The join keys.
        """
        join_key = self.db.scalar(select(JoinKey).where(JoinKey.entity_id == entity.id))
        if not join_key:
            raise ValueError(f"No join key defined for entity '{entity.name}'")

        query = select(JoinKeyValue).filter(JoinKeyValue.join_key_id == join_key.id)

        if join_keys:
            if len(join_keys) != len(set(join_keys)):
                duplicates = [
                    item for item, count in Counter(join_keys).items() if count > 1
                ]
                raise ValueError(
                    f"join_keys must be a list of unique join key values. "
                    f"Found duplicates: {duplicates}"
                )

            join_key_values = self.db.scalars(
                query.filter(JoinKeyValue.value.in_(set(map(json.dumps, join_keys))))
            ).all()
            if len(join_key_values) != len(set(join_keys)):
                raise ValueError(
                    f"Some join keys not found for entity '{entity.name}'. "
                    f"Missing join keys: {set(join_keys) - set(j.value for j in join_key_values)}"  # noqa
                )
            return join_key_values

        return self.db.scalars(query).all()

    def _validate_args(
        self,
        project_name: str,
        entity_name: str,
        join_keys: List[Any] | None = None,
        feature_names: List[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ):
        """
        Validate the arguments.
        Args:
            project_name: The name of the project.
            entity_name: The name of the entity.
            join_keys: The join keys.
            feature_names: The feature names.
            start_time: The start time.
            end_time: The end time.
        """
        if start_time and end_time and start_time > end_time:
            raise ValueError("start_time must be before end_time")

        # 1. Get the project
        project = self._get_project(project_name)

        # 2. Get the entity
        entity = self._get_entity(project, entity_name)

        # 3. Get the join key values
        join_key_values = self._get_join_key_values(entity, join_keys)

        # 4. Get filtered features
        features = self._get_features(project, entity, feature_names)

        return project, entity, features, join_key_values

    def _build_features_query(
        self,
        project: Project,
        entity: Entity,
        features: List[Feature],
        join_key_values: List[JoinKeyValue],
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> Select:
        """
        Build the query to retrieve feature values.
        Get the most recent historical values by combination.
        Args:
            project: The project.
            entity: The entity.
            features: The features.
            join_key_values: The join key values.
            start_time: The start time.
            end_time: The end time.
        """
        # Use alias to avoid column ambiguity problems
        fv = aliased(FeatureValue)
        f = aliased(Feature)
        e = aliased(Entity)
        jk = aliased(JoinKey)
        jkv = aliased(JoinKeyValue)

        selectable = (
            select(
                e.name.label("entity_name"),
                jk.name.label("join_key"),
                jkv.value.label("join_key_value"),
                f.name.label("feature_name"),
                f.data_type.label("feature_type"),
                f.id.label("feature_id"),
                fv.value.label("feature_value"),
                fv.timestamp.label("timestamp"),
                func.row_number()
                .over(
                    partition_by=[jkv.id, f.id],
                    order_by=[fv.timestamp.desc(), fv.id.desc()],
                )
                .label("rownum"),
            )
            .join(f, f.id == fv.feature_id)
            .join(jkv, jkv.id == fv.join_key_value_id)
            .join(jk, jk.id == jkv.join_key_id)
            .join(e, and_(jk.entity_id == e.id, e.project_id == project.id))
            # This is not necessary, but I leave it mostly for clarity
            # there are constrains in the database
            # that ensure that the entity_id and feature_id
            # are valid
            .filter(e.id == entity.id, f.id.in_([f.id for f in features]))
            .filter()
        )

        # 6. Apply the optional filters
        filters = [
            fv.timestamp >= start_time if start_time else None,
            fv.timestamp <= end_time if end_time else None,
            f.name.in_([f.name for f in features]) if features else None,
            (jkv.id.in_([j.id for j in join_key_values]) if join_key_values else None),
        ]

        if filters := [f for f in filters if f is not None]:
            selectable = selectable.filter(*filters)

        # 7. Create CTE, retrieve the most recent value
        # grouped by combination of join_key_value and feature_id
        # That is, if there are 2 values for the same combination,
        # they will be taken as historical by "timestamp"
        # and the most recent one is retrieved.
        cte = selectable.order_by(fv.timestamp.asc()).cte("ranked_feature_values")
        stmt = (
            select(
                cte.c.entity_name,
                cte.c.join_key,
                cte.c.join_key_value,
                cte.c.feature_name,
                cte.c.feature_type,
                cte.c.feature_value,
                cte.c.timestamp,
            )
            .where(cte.c.rownum == 1)
            .order_by(cte.c.timestamp.asc())
        )

        return stmt

    def _build_features_df(
        self,
        result: List[Dict[str, Any]],
        features: List[Feature],
        join_key_values: List[JoinKeyValue] | None = None,
        strict: bool = False,
        include_timestamp: bool = False,
    ) -> pd.DataFrame:
        """
        Build the features dataframe.

        Args:
            result: The result of the query.
            features: The features.
            join_key_values: The join key values.
            strict: Whether to raise an error if the join key values are not found.
            include_timestamp: Whether to include the timestamp in the dataframe.
        """
        feature_id_map = {f.id: f.name for f in features}
        feature_name_set = set(feature_id_map.values())

        df = pd.DataFrame(
            result,
            columns=[
                "entity_name",
                "join_key",
                "join_key_value",
                "feature_name",
                "feature_type",
                "feature_value",
                "timestamp",
            ],
        )

        if not strict and df.empty:
            return pd.DataFrame(columns=["join_key_value"] + list(feature_name_set))

        df["feature_value"] = df["feature_value"].apply(self._extract_single_value)
        df["join_key_value"] = df["join_key_value"].apply(self._extract_single_value)

        # 1. Get list of tuples (entity_name, join_key, join_key_value)
        key_tuples = [
            (
                jkv.join_key.entity.name,
                jkv.join_key.name,
                self._extract_single_value(jkv.value),
            )
            for jkv in join_key_values
        ]

        # 2. Create cartesian product with feature names
        all_combinations = pd.DataFrame(
            itertools.product(key_tuples, list(feature_name_set)),
            columns=["key_tuple", "feature_name"],
        )

        # 3. Expand key_tuple into separate columns
        all_combinations[["entity_name", "join_key", "join_key_value"]] = pd.DataFrame(
            all_combinations["key_tuple"].tolist(), index=all_combinations.index
        )

        # 4. Drop auxiliary column
        all_combinations = all_combinations.drop(columns=["key_tuple"])

        df = all_combinations.merge(
            df,
            how="left",
            on=["entity_name", "join_key", "join_key_value", "feature_name"],
        )

        if df["join_key"].isna().any():
            raise ValueError(
                f"Some join keys not found for entities '{df['entity_name'][df['join_key'].isna()].unique()}'"  # noqa
            )

        # Create a pivot table,
        # columns are the feature names,
        # rows are the join key values
        pivot = df.pivot(
            index=["join_key_value"],
            columns="feature_name",
            values=(
                ["feature_value", "timestamp"] if include_timestamp else "feature_value"
            ),
        )

        return pivot
