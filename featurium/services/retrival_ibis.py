from __future__ import annotations

"""
Ibis-based Feature Retrieval Service.

This module provides a retrieval path similar to FeatureRetrieval/FeatureRetrievalDuckDB
but expressed with Ibis to ease portability across SQL engines and push-down
of analytical logic (joins, window functions) to the backend.

Notes
- Keeps SQLAlchemy ORM for schema/constraints via the existing Session, but uses
  Ibis to express the retrieval query.
- Does not modify the DB schema.
- Import of ibis is optional at runtime; a helpful error is raised if unavailable.
"""

from datetime import datetime
from functools import reduce
from typing import Any, List, Literal, Optional

import pandas as pd
from sqlalchemy import Engine, make_url
from sqlalchemy.orm import Session

from featurium.core.models import AttributeType
from featurium.services.base_retrieval import RetrievalService

try:
    import ibis
except Exception as e:  # pragma: no cover - optional dependency handling
    ibis = None  # type: ignore
    _ibis_import_error = e
else:  # pragma: no cover
    _ibis_import_error = None


class FeatureRetrievalIbis(RetrievalService):
    """
    Feature retrieval implemented with Ibis expressions.

    Provides API-compatible methods to fetch the latest feature (or target) values
    per (join_key_value, attribute) combination within optional time bounds,
    returning a wide DataFrame (one column per attribute name).
    """

    def __init__(self, db: Session, sqlalchemy_url: Optional[str] = None):
        super().__init__(db)
        if ibis is None:  # pragma: no cover
            raise RuntimeError(
                "ibis is not installed. Please `pip install ibis-framework` "
                "to use FeatureRetrievalIbis"
            ) from _ibis_import_error

        # Determine connection target
        if sqlalchemy_url is None:
            bind = db.get_bind()
            if isinstance(bind, Engine):
                sqlalchemy_url = str(bind.url)
                # In some contexts passwords are masked as '***';
                # if so and available, unmask
                if "***" in sqlalchemy_url and getattr(bind.url, "password", None):
                    sqlalchemy_url = sqlalchemy_url.replace(
                        "***", bind.url.password or ""
                    )
            else:
                raise ValueError(
                    "Database session is not bound to an Engine; "
                    "provide sqlalchemy_url explicitly."
                )

        self.con = self._connect_ibis_via_sqlalchemy_url(sqlalchemy_url)

    def get_feature_values(
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

    def get_target_values(
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
        Get the target values.
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

    def _get_feature_values(  # noqa: C901
        self,
        project_name: str,
        entity_name: str,
        join_keys: List[Any] | None = None,
        feature_names: List[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        include_timestamp: bool = False,
        attr_type: AttributeType | Literal["ALL"] = AttributeType.FEATURE,
    ) -> pd.DataFrame:
        """
        Get the feature values.
        """
        if start_time and end_time and start_time > end_time:
            raise ValueError("start_time must be before end_time")

        # Base tables
        av = self.con.table("attribute_values")
        attr = self.con.table("attributes")
        jkv = self.con.table("join_key_values")
        jk = self.con.table("join_keys")
        e = self.con.table("entities")
        p = self.con.table("projects")

        # Select only required columns and rename to avoid collisions on join
        av_s = av[
            ["attribute_id", "join_key_value_id", "value", "timestamp", "id"]
        ].rename(av_id="id")
        attr_s = attr[["id", "type", "name", "data_type"]].rename(attr_id="id")
        jkv_s = jkv[["id", "join_key_id", "value"]].rename(
            jkv_id="id", join_key_value="value"
        )
        jk_s = jk[["id", "entity_id", "name"]].rename(jk_id="id", join_key="name")
        e_s = e[["id", "project_id", "name"]].rename(entity_id="id", entity_name="name")
        p_s = p[["id", "name"]].rename(project_id="id", project_name="name")

        # Join graph mirrors the SQL used in DuckDB variant, now collision-free
        joined = (
            av_s.join(attr_s, av_s.attribute_id == attr_s.attr_id)
            .join(jkv_s, av_s.join_key_value_id == jkv_s.jkv_id)
            .join(jk_s, jkv_s.join_key_id == jk_s.jk_id)
            .join(e_s, jk_s.entity_id == e_s.entity_id)
            .join(p_s, e_s.project_id == p_s.project_id)
        )

        # Optional filters (apply on the joined relation before projection)
        conds: List[Any] = []
        if project_name:
            conds.append(p_s.project_name == project_name)
        if entity_name:
            conds.append(e_s.entity_name == entity_name)
        if feature_names:
            conds.append(attr_s.name.isin(feature_names))
        if join_keys:
            # rely on stringified comparison as in SQLAlchemy path
            conds.append(jkv_s.join_key_value.cast("string").isin(join_keys))
        if start_time:
            conds.append(av_s.timestamp >= start_time)
        if end_time:
            conds.append(av_s.timestamp <= end_time)
        if attr_type != "ALL":
            # Persisted values are typically lowercase ('feature'/'target').
            # Be tolerant to uppercase storage as well.
            conds.append(
                (attr_s.type == attr_type.value)
                | (attr_s.type == attr_type.value.upper())
            )

        joined_f = joined.filter(reduce(lambda a, b: a & b, conds)) if conds else joined

        data = joined_f.select(
            p_s.project_name,
            e_s.entity_name,
            jk_s.join_key,
            jkv_s.join_key_value,
            jkv_s.jkv_id,
            attr_s.attr_id.name("attribute_id"),
            attr_s.type.name("attribute"),
            attr_s.name,
            attr_s.data_type.name("type"),
            av_s.value,
            av_s.timestamp,
            av_s.av_id,
        )

        # Window for latest per (entity, join_key, jkv, attribute)
        w = ibis.window(
            group_by=[data["jkv_id"], data["attribute_id"]],
            order_by=[data["timestamp"].desc(), data["av_id"].desc()],
        )
        ranked = data.mutate(rownum=ibis.row_number().over(w))

        latest = ranked.filter(ranked.rownum == 1)

        # Execute and shape to wide format
        df = latest.select(
            "entity_name",
            "join_key",
            "join_key_value",
            "name",
            "value",
            "type",
            "timestamp",
        ).execute()

        if df.empty:
            return pd.DataFrame(columns=["join_key_value"])  # empty shape, consistent

        # Extract scalar from JSON-like dicts
        df["value"] = df["value"].apply(self._extract_single_value)
        df["join_key_value"] = df["join_key_value"].apply(self._extract_single_value)

        if include_timestamp:
            pivot = df.pivot(
                index="join_key_value",
                columns="name",
                values=["value", "type", "timestamp"],
            )
        else:
            pivot = df.pivot(index="join_key_value", columns="name", values="value")

        # Stable order by join_key_value
        pivot = pivot.sort_index()

        return pivot

    def build_latest_expr(
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
        Build and return the Ibis expression (table) that yields the latest
        records per (join_key_value, attribute) according to filters.

        Useful for debugging via `.compile()` to inspect generated SQL.
        """
        # Reuse the logic by constructing up to `latest` and return it
        av = self.con.table("attribute_values")
        attr = self.con.table("attributes")
        jkv = self.con.table("join_key_values")
        jk = self.con.table("join_keys")
        e = self.con.table("entities")
        p = self.con.table("projects")

        av_s = av[
            ["attribute_id", "join_key_value_id", "value", "timestamp", "id"]
        ].rename(av_id="id")
        attr_s = attr[["id", "type", "name", "data_type"]].rename(attr_id="id")
        jkv_s = jkv[["id", "join_key_id", "value"]].rename(
            jkv_id="id", join_key_value="value"
        )
        jk_s = jk[["id", "entity_id", "name"]].rename(jk_id="id", join_key="name")
        e_s = e[["id", "project_id", "name"]].rename(entity_id="id", entity_name="name")
        p_s = p[["id", "name"]].rename(project_id="id", project_name="name")

        joined = (
            av_s.join(attr_s, av_s.attribute_id == attr_s.attr_id)
            .join(jkv_s, av_s.join_key_value_id == jkv_s.jkv_id)
            .join(jk_s, jkv_s.join_key_id == jk_s.jk_id)
            .join(e_s, jk_s.entity_id == e_s.entity_id)
            .join(p_s, e_s.project_id == p_s.project_id)
        )

        conds: List[Any] = []
        if project_name:
            conds.append(p_s.project_name == project_name)
        if entity_name:
            conds.append(e_s.entity_name == entity_name)
        if feature_names:
            conds.append(attr_s.name.isin(feature_names))
        if join_keys:
            conds.append(jkv_s.join_key_value.cast("string").isin(join_keys))
        if start_time:
            conds.append(av_s.timestamp >= start_time)
        if end_time:
            conds.append(av_s.timestamp <= end_time)
        if attr_type != "ALL":
            conds.append(
                (attr_s.type == attr_type.value)
                | (attr_s.type == attr_type.value.upper())
            )

        joined_f = joined.filter(reduce(lambda a, b: a & b, conds)) if conds else joined

        data = joined_f.select(
            p_s.project_name,
            e_s.entity_name,
            jk_s.join_key,
            jkv_s.join_key_value,
            jkv_s.jkv_id,
            attr_s.attr_id.name("attribute_id"),
            attr_s.type.name("attribute"),
            attr_s.name,
            attr_s.data_type.name("type"),
            av_s.value,
            av_s.timestamp,
            av_s.av_id,
        )

        w = ibis.window(
            group_by=[data["jkv_id"], data["attribute_id"]],
            order_by=[data["timestamp"].desc(), data["av_id"].desc()],
        )

        ranked = data.mutate(rownum=ibis.row_number().over(w))
        latest = ranked.filter(ranked.rownum == 1)
        return latest

    def _connect_ibis_via_sqlalchemy_url(self, sqlalchemy_url: str | Engine):
        """
        Connect to a database with Ibis from a SQLAlchemy URL or Engine.

        Supports: sqlite (file-backed), postgresql, mysql. For in-memory sqlite
        URLs (sqlite:///:memory:), connectivity is not supported; provide a file
        path or use another backend.
        """
        url_str: str
        if isinstance(sqlalchemy_url, Engine):
            url_str = str(sqlalchemy_url.url)
        else:
            url_str = sqlalchemy_url

        url = make_url(url_str)
        driver = url.drivername

        if driver == "sqlite":
            # in-memory is not accessible cross-process
            if url.database in (None, ":memory:"):
                raise ValueError(
                    "Ibis cannot attach to in-memory SQLite via URL. "
                    "Use a file-backed SQLite database."
                )
            # ibis.sqlite.connect expects a filesystem path
            return ibis.sqlite.connect(url.database)

        if driver in ("postgresql", "postgresql+psycopg2"):
            return ibis.postgres.connect(
                host=url.host or "localhost",
                user=url.username or "",
                password=url.password or "",
                database=url.database or "postgres",
                port=url.port or 5432,
            )

        if driver in ("mysql", "mysql+pymysql"):
            return ibis.mysql.connect(
                host=url.host or "localhost",
                user=url.username or "",
                password=url.password or "",
                database=url.database or "",
                port=url.port or 3306,
            )

        raise ValueError(f"Unsupported SQLAlchemy URL driver for Ibis: {driver}")
