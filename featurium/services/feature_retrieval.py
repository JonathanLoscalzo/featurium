import itertools
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Literal, Protocol

import duckdb
import pandas as pd
from sqlalchemy import Engine, Select, and_, make_url, select
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.functions import func

from featurium.core.models import (
    Attribute,
    AttributeType,
    AttributeValue,
    Entity,
    JoinKey,
    JoinKeyValue,
    Project,
)
from featurium.services.retrieval import RetrievalService


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


class FeatureRetrieval(RetrievalService):
    def __init__(self, db: Session):
        """
        Initialize the FeatureRetrieval Service.
        """
        super().__init__(db)

    def get_target_values(
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
        Get the feature and target values.
        """
        return self._get_feature_values(
            project_name,
            entity_name,
            join_keys,
            feature_names,
            start_time,
            end_time,
            include_timestamp,
            AttributeType.TARGET,
        )

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
        """
        return self._get_feature_values(
            project_name,
            entity_name,
            join_keys,
            feature_names,
            start_time,
            end_time,
            include_timestamp,
            AttributeType.FEATURE,
        )

    def get_values(
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
        Get the values.
        """
        return self._get_feature_values(
            project_name,
            entity_name,
            join_keys,
            feature_names,
            start_time,
            end_time,
            include_timestamp,
            "ALL",
        )

    def _get_feature_values(
        self,
        project_name: str,
        entity_name: str,
        join_keys: List[Any] | None = None,
        feature_names: List[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        include_timestamp: bool = False,
        attr_type: AttributeType | Literal["ALL"] = AttributeType.FEATURE,
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
            attr_type,
        )

        query = self._build_features_query(
            project,
            entity,
            features,
            # I don't need to filter if the user doesn't require it
            join_key_values if join_keys else None,
            start_time,
            end_time,
            attr_type,
        )

        result = self.db.execute(query).mappings().all()

        df = self._build_features_df(
            result,
            features,
            join_key_values,
            strict=len(join_keys or []) > 0,
            include_timestamp=include_timestamp,
        )

        return df

    def _get_features(
        self,
        project: Project,
        entity: Entity,
        feature_names: List[str] | None = None,
        attr_type: AttributeType | Literal["ALL"] = AttributeType.FEATURE,
    ) -> List[Attribute]:
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

            query = select(Attribute).where(
                Attribute.project_id == project.id,
                Attribute.name.in_(set(feature_names)),
                # this is required to avoid returning features within
                # the same project but from other entities
                Attribute.entities.contains(entity),
                Attribute.type == attr_type if attr_type != "ALL" else None,
            )

            if attr_type != "ALL":
                query = query.filter(Attribute.type == attr_type)

            features = self.db.scalars(query).all()
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
            query = select(Attribute).where(
                Attribute.project_id == project.id,
                Attribute.entities.contains(entity),
            )

            if attr_type != "ALL":
                query = query.filter(Attribute.type == attr_type)

            features = self.db.scalars(query).all()

        if not features:
            raise ValueError("No features found")

        return features

    def _validate_args(
        self,
        project_name: str,
        entity_name: str,
        join_keys: List[Any] | None = None,
        feature_names: List[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        attr_type: AttributeType | Literal["ALL"] = AttributeType.FEATURE,
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
        features = self._get_features(project, entity, feature_names, attr_type)

        return project, entity, features, join_key_values

    def _build_features_query(
        self,
        project: Project,
        entity: Entity,
        features: List[Attribute],
        join_key_values: List[JoinKeyValue],
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        attr_type: AttributeType | Literal["ALL"] = AttributeType.FEATURE,
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
        fv = aliased(AttributeValue)
        f = aliased(Attribute)
        e = aliased(Entity)
        jk = aliased(JoinKey)
        jkv = aliased(JoinKeyValue)

        selectable = (
            select(
                e.name.label("entity_name"),
                jk.name.label("join_key"),
                jkv.value.label("join_key_value"),
                f.id.label("attribute_id"),
                f.type.label("attribute"),
                f.name.label("name"),
                f.data_type.label("type"),
                fv.value.label("value"),
                fv.timestamp.label("timestamp"),
                func.row_number()
                .over(
                    partition_by=[jkv.id, f.id],
                    order_by=[fv.timestamp.desc(), fv.id.desc()],
                )
                .label("rownum"),
            )
            .join(f, f.id == fv.attribute_id)
            .join(jkv, jkv.id == fv.join_key_value_id)
            .join(jk, jk.id == jkv.join_key_id)
            .join(e, and_(jk.entity_id == e.id, e.project_id == project.id))
            # This is not necessary, but I leave it mostly for clarity
            # there are constrains in the database
            # that ensure that the entity_id and attribute_id
            # are valid
            .filter(e.id == entity.id)
            .filter()
        )

        # 6. Apply the optional filters
        filters = [
            f.id.in_([f.id for f in features]) if features else None,
            fv.timestamp >= start_time if start_time else None,
            fv.timestamp <= end_time if end_time else None,
            f.name.in_([f.name for f in features]) if features else None,
            (jkv.id.in_([j.id for j in join_key_values]) if join_key_values else None),
            (f.type == attr_type if attr_type != "ALL" else None),
        ]

        if filters := [f for f in filters if f is not None]:
            selectable = selectable.filter(*filters)

        # 7. Create CTE, retrieve the most recent value
        # grouped by combination of join_key_value and attribute_id
        # That is, if there are 2 values for the same combination,
        # they will be taken as historical by "timestamp"
        # and the most recent one is retrieved.
        cte = selectable.order_by(fv.timestamp.asc()).cte("ranked_feature_values")
        stmt = (
            select(
                cte.c.entity_name,
                cte.c.join_key,
                cte.c.join_key_value,
                cte.c.attribute_id,
                cte.c.attribute,
                cte.c.name,
                cte.c.type,
                cte.c.value,
                cte.c.timestamp,
            )
            .where(cte.c.rownum == 1)
            .order_by(cte.c.timestamp.asc())
        )

        return stmt

    def _build_features_df(
        self,
        result: List[Dict[str, Any]],
        features: List[Attribute],
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
                "attribute_id",
                "attribute",
                "name",
                "type",
                "value",
                "timestamp",
            ],
        )

        if not strict and df.empty:
            return pd.DataFrame(columns=["join_key_value"] + list(feature_name_set))

        df["value"] = df["value"].apply(self._extract_single_value)
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
            columns=["key_tuple", "name"],
        )

        # 3. Expand key_tuple into separate columns
        all_combinations[["entity_name", "join_key", "join_key_value"]] = pd.DataFrame(
            all_combinations["key_tuple"].tolist(), index=all_combinations.index
        )

        # 4. Drop auxiliary column
        all_combinations = all_combinations.drop(columns=["key_tuple"])
        all_combinations["join_key_value"] = all_combinations["join_key_value"].astype(
            str
        )

        df = all_combinations.merge(
            df,
            how="left",
            on=["entity_name", "join_key", "join_key_value", "name"],
        )

        if df["join_key"].isna().any():
            raise ValueError(
                f"Some join keys not found for entities '{df['entity_name'][df['join_key'].isna()].unique()}'"  # noqa
            )

        # Use pandas pivot (original behavior)
        pivot = df.pivot(
            index=["join_key_value"],
            columns="name",
            values=(["value", "type", "timestamp"] if include_timestamp else "value"),
        )

        return pivot


class FeatureRetrievalDuckDB(RetrievalService):
    """
    FeatureRetrieval Service that uses DuckDB as a backend.
    """

    RETRIVAL_QUERY = """
        WITH data AS (
            WITH ranked_feature_values AS (
                SELECT
                    projects_1.name AS project_name,
                    entities_1.name AS entity_name,
                    join_keys_1.name AS join_key,
                    join_key_values_1.value AS join_key_value,
                    attributes_1.id AS attribute_id,
                    attributes_1.type AS attribute,
                    attributes_1.name AS name,
                    attributes_1.data_type AS type,
                    attribute_values_1.value AS value,
                    attribute_values_1.timestamp AS timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            entities_1.id,
                            join_keys_1.id,
                            join_key_values_1.id,
                            attributes_1.id
                        ORDER BY
                            attribute_values_1.timestamp DESC
                    ) AS rownum
                FROM
                    attribute_values AS attribute_values_1
                JOIN attributes AS attributes_1
                    ON attributes_1.id = attribute_values_1.attribute_id
                JOIN join_key_values AS join_key_values_1
                    ON join_key_values_1.id = attribute_values_1.join_key_value_id
                JOIN join_keys AS join_keys_1
                    ON join_keys_1.id = join_key_values_1.join_key_id
                JOIN entities AS entities_1
                    ON join_keys_1.entity_id = entities_1.id
                JOIN projects AS projects_1
                    ON projects_1.id = entities_1.project_id
                {where_clause}
                ORDER BY
                    attribute_values_1.timestamp ASC
            )
            SELECT
                ranked_feature_values.entity_name,
                ranked_feature_values.join_key,
                ranked_feature_values.join_key_value,
                ranked_feature_values.name,
                ranked_feature_values.value,
                ranked_feature_values.timestamp,
                ranked_feature_values.rownum
            FROM
                ranked_feature_values
        ), pivot_alias as (
            PIVOT data
            ON name
            USING first(value order by timestamp desc)
        )
        select *
        from pivot_alias
        where rownum = 1
        ORDER BY join_key_value;
        """

    def __init__(self, db: Session, sqlalchemy_url: str | None = None):
        super().__init__(db)

        if sqlalchemy_url:
            self.conn = self._connect_duckdb_via_sqlalchemy_url(sqlalchemy_url)
        else:
            self.conn = self._connect_duckdb_via_sqlalchemy_url(db.get_bind().engine)

    def get_target_values(
        self,
        project_name: str,
        entity_name: str,
        join_keys: List[Any] | None = None,
        feature_names: List[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ):
        """
        Get the target values.
        """
        return self._get_feature_values(
            project_name,
            entity_name,
            join_keys,
            feature_names,
            start_time,
            end_time,
            AttributeType.TARGET,
        )

    def get_feature_values(
        self,
        project_name: str,
        entity_name: str,
        join_keys: List[Any] | None = None,
        feature_names: List[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
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

        """
        Get the values.
        """
        return self._get_feature_values(
            project_name,
            entity_name,
            join_keys,
            feature_names,
            start_time,
            end_time,
            AttributeType.FEATURE,
        )

    def _get_feature_values(
        self,
        project_name: str,
        entity_name: str,
        join_keys: List[Any] | None = None,
        feature_names: List[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        attr_type: AttributeType | Literal["ALL"] = AttributeType.FEATURE,
    ):

        where_clause = self._build_where_clause(
            project_name,
            entity_name,
            join_keys,
            feature_names,
            start_time,
            end_time,
            attr_type,
        )

        # TODO: use prepared statement!
        result = self.conn.execute(
            self.RETRIVAL_QUERY.format(where_clause=where_clause)
        ).df()
        return result

    def _connect_duckdb_via_sqlalchemy_url(
        self, sqlalchemy_url: str | Engine, alias: str = "extdb"
    ) -> duckdb.DuckDBPyConnection:
        """
        Connect DuckDB to another DB using a SQLAlchemy connection string.
        Supports MySQL, Postgres and SQLite.
        """
        driver_map = {
            "mysql": "mysql",
            "mysql+pymysql": "mysql",
            "postgresql": "postgres",
            "postgresql+psycopg2": "postgres",
            "sqlite": "sqlite",
        }

        # Normalizar a string
        if isinstance(sqlalchemy_url, Engine):
            sqlalchemy_url = str(sqlalchemy_url.url)
            if "***" in sqlalchemy_url and self.db.get_bind().engine.url.password:
                sqlalchemy_url = sqlalchemy_url.replace(
                    "***", self.db.get_bind().engine.url.password
                )
        url = make_url(sqlalchemy_url)

        if url.drivername not in driver_map:
            raise ValueError(f"Driver no soportado: {url.drivername}")

        db_type = driver_map[url.drivername]

        # Conexión DuckDB en memoria
        conn = duckdb.connect(":memory:")
        conn.install_extension(db_type)
        conn.load_extension(db_type)

        if db_type == "sqlite":
            # SQLite: path en database
            duckdb_conn_string = url.database
            conn.execute(
                f"ATTACH '{duckdb_conn_string}' AS {alias} (TYPE SQLITE, READ_ONLY)"
            )
        elif db_type == "postgres":
            # Postgres: formato key=value
            duckdb_conn_string = (
                f"dbname={url.database} "
                f"user={url.username} "
                f"password={url.password or ''} "
                f"host={url.host} "
                f"port={url.port or 5432}"
            )
            conn.execute(
                f"ATTACH '{duckdb_conn_string}' AS {alias} (TYPE POSTGRES, READ_ONLY)"
            )
        elif db_type == "mysql":
            duckdb_conn_string = (
                f"host={url.host} "
                f"user={url.username} "
                f"password={url.password or ''} "
                f"database={url.database} "
                f"port={url.port or 3306}"
            )
            conn.execute(
                f"ATTACH '{duckdb_conn_string}' AS {alias} (TYPE MYSQL, READ_ONLY)"
            )

        conn.execute(f"USE {alias}")
        return conn

    def _build_where_clause(
        self,
        project_name: str,
        entity_name: str,
        join_keys: List[Any] | None = None,
        feature_names: List[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        attr_type: AttributeType | Literal["ALL"] = AttributeType.FEATURE,
    ):
        where_clauses = []

        if project_name:
            where_clauses.append(f"projects_1.name = '{project_name}'")

        if entity_name:
            where_clauses.append(f"entities_1.name = '{entity_name}'")

        if join_keys:
            join_keys_str = ",".join(map(lambda jk: f"'{jk}'", join_keys))
            where_clauses.append(
                f"json_extract_string(join_key_values_1.value, '') IN ({join_keys_str})"  # noqa: E501
            )

        if feature_names:
            feature_names_str = ",".join(map(lambda fn: f"'{fn}'", feature_names))
            where_clauses.append(
                f"attributes_1.name IN ({feature_names_str})"  # noqa: E501
            )

        if start_time:
            where_clauses.append(
                f"attribute_values_1.timestamp >= '{start_time.isoformat()}'"
            )

        if end_time:
            where_clauses.append(
                f"attribute_values_1.timestamp <= '{end_time.isoformat()}'"
            )

        if attr_type == AttributeType.TARGET:
            where_clauses.append("attributes_1.type = 'TARGET'")
        elif attr_type == AttributeType.FEATURE:
            where_clauses.append("attributes_1.type = 'FEATURE'")

        return "WHERE " + " AND ".join(where_clauses)
