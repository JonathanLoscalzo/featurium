import json
from typing import Any, Counter, List

from sqlalchemy import CHAR, TEXT, cast, select
from sqlalchemy.orm import Session

from featurium.core.models import Entity, JoinKey, JoinKeyValue, Project


class RetrievalService:
    """Service to retrieve data from the database."""

    def __init__(self, db: Session):
        self.db = db

    @property
    def dialect(self) -> str:
        """
        Get the dialect of the database.
        """
        return self.db.get_bind().dialect.name

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
            select(Entity).where(Entity.name == entity_name, Entity.project_id == project.id)
        )
        if not entity:
            raise ValueError(f"Entity '{entity_name}' not found in project '{project.name}'")
        return entity

    def _extract_single_value(self, d: Any) -> Any:
        """
        Extract the simple value from the `feature_value` dictionary
        """
        if isinstance(d, dict) and len(d) == 1:
            return next(iter(d.values()))
        return d  # optional: pass if not a dict

    def _get_join_key_values(self, entity: Entity, join_keys: List[Any]) -> List[JoinKeyValue]:  # noqa: C901
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
                duplicates = [item for item, count in Counter(join_keys).items() if count > 1]
                raise ValueError(
                    f"join_keys must be a list of unique join key values. " f"Found duplicates: {duplicates}"
                )

            def normalize_join_keys(keys):
                import ast
                from functools import partial

                def to_sql_literal(value):
                    if isinstance(value, str):
                        escaped = value.replace('"', '""')
                        return f'"{escaped}"'
                    return str(value)

                json_dumps = partial(json.dumps, separators=(", ", ": "))

                normalized = []
                for k in keys:
                    if isinstance(k, list):
                        normalized.append(json_dumps(k))
                    elif isinstance(k, str) and isinstance(json.loads(k), dict):
                        try:
                            normalized.append(json_dumps(json.loads(k)))
                        except json.JSONDecodeError:
                            normalized.append(json_dumps(ast.literal_eval(k)))
                        except Exception:
                            normalized.append(json_dumps(k))
                    elif isinstance(k, str):
                        normalized.append(to_sql_literal(k))
                    else:
                        normalized.append(k)
                return normalized

            join_keys_parsed = normalize_join_keys(join_keys)
            col = JoinKeyValue.value

            if self.dialect == "mysql":
                expr = cast(col, CHAR)
            elif (self.dialect == "postgresql") or (self.dialect == "sqlite"):
                expr = cast(col, TEXT)
            else:
                raise NotImplementedError(f"Dialect {self.dialect} not supported")

            join_key_values = self.db.scalars(query.filter(expr.in_(set(join_keys_parsed)))).all()
            if len(join_key_values) != len(set(join_keys)):
                raise ValueError(
                    f"Some join keys not found for entity '{entity.name}'. "
                    f"Missing join keys: {set(join_keys) - set(j.value for j in join_key_values)}"  # noqa
                )
            return join_key_values

        return self.db.scalars(query).all()
