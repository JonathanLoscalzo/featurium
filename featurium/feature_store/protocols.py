"""
Feature Store Protocols

This module defines protocol interfaces for the services used by the Feature Store.
"""

from datetime import datetime
from typing import Any, List, Optional, Protocol

import pandas as pd

from featurium.core.models import (
    Attribute,
    AttributeEntities,
    AttributeValue,
    Entity,
    JoinKey,
    JoinKeyValue,
    Project,
)


class RegistrationServiceProtocol(Protocol):
    """
    Protocol for the Registration Service.

    Defines the interface that any registration service must implement
    to be compatible with the Feature Store.
    """

    def register_project(self, name: str, description: str = "") -> Project:
        """Register a single project."""
        ...

    def register_entity(self, name: str, project: Project, description: str = "") -> Entity:
        """Register a single entity."""
        ...

    def register_feature(
        self, name: str, project: Project, data_type: Any, description: str = ""
    ) -> Attribute:
        """Register a single feature."""
        ...

    def register_target(
        self, name: str, project: Project, data_type: Any, description: str = ""
    ) -> Attribute:
        """Register a single target."""
        ...

    def register_join_key(self, name: str, entity: Entity) -> JoinKey:
        """Register a single join key."""
        ...

    def register_join_key_value(self, join_key: JoinKey, value: Any) -> JoinKeyValue:
        """Register a single join key value."""
        ...

    def register_feature_value(
        self,
        feature: Attribute,
        join_key_value: JoinKeyValue,
        value: Any,
        metadata: Optional[dict] = None,
    ) -> AttributeValue:
        """Register a single feature value."""
        ...

    def associate_attribute_with_entity(self, attribute: Attribute, entity: Entity) -> AttributeEntities:
        """Associate an attribute with an entity."""
        ...

    def register_projects_bulk(self, projects: List[dict]) -> List[Project]:
        """Register multiple projects in bulk."""
        ...

    def register_entities_bulk(self, entities: List[dict]) -> List[Entity]:
        """Register multiple entities in bulk."""
        ...

    def register_attributes_bulk(self, attributes: List[dict]) -> List[Attribute]:
        """Register multiple attributes in bulk."""
        ...

    def register_join_keys_bulk(self, join_keys: List[dict]) -> List[JoinKey]:
        """Register multiple join keys in bulk."""
        ...

    def register_join_key_values_bulk(self, join_key_values: List[dict]) -> List[JoinKeyValue]:
        """Register multiple join key values in bulk."""
        ...

    def register_attribute_values_bulk(self, attribute_values: List[dict]) -> List[AttributeValue]:
        """Register multiple attribute values in bulk."""
        ...

    def associate_attributes_with_entities_bulk(self, associations: List[dict]) -> List[AttributeEntities]:
        """Associate multiple attributes with entities in bulk."""
        ...


class RetrievalServiceProtocol(Protocol):
    """
    Protocol for the Retrieval Service.

    Defines the interface that any retrieval service must implement
    to be compatible with the Feature Store.
    """

    def get_feature_values(
        self,
        project_name: str,
        entity_name: str,
        join_keys: Optional[List[Any]] = None,
        feature_names: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        include_timestamp: bool = False,
    ) -> pd.DataFrame:
        """
        Get feature values from the feature store.

        Args:
            project_name: The name of the project.
            entity_name: The name of the entity.
            join_keys: Optional list of join key values to filter.
            feature_names: Optional list of feature names to retrieve.
            start_time: Optional start time for filtering.
            end_time: Optional end time for filtering.
            include_timestamp: Whether to include timestamps in the result.

        Returns:
            A pandas DataFrame with the feature values.
        """
        ...

    def get_target_values(
        self,
        project_name: str,
        entity_name: str,
        join_keys: Optional[List[Any]] = None,
        feature_names: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        include_timestamp: bool = False,
    ) -> pd.DataFrame:
        """
        Get target values from the feature store.

        Args:
            project_name: The name of the project.
            entity_name: The name of the entity.
            join_keys: Optional list of join key values to filter.
            feature_names: Optional list of target names to retrieve.
            start_time: Optional start time for filtering.
            end_time: Optional end time for filtering.
            include_timestamp: Whether to include timestamps in the result.

        Returns:
            A pandas DataFrame with the target values.
        """
        ...

    def get_values(
        self,
        project_name: str,
        entity_name: str,
        join_keys: Optional[List[Any]] = None,
        feature_names: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        include_timestamp: bool = False,
    ) -> pd.DataFrame:
        """
        Get all attribute values (features and targets) from the feature store.

        Args:
            project_name: The name of the project.
            entity_name: The name of the entity.
            join_keys: Optional list of join key values to filter.
            feature_names: Optional list of attribute names to retrieve.
            start_time: Optional start time for filtering.
            end_time: Optional end time for filtering.
            include_timestamp: Whether to include timestamps in the result.

        Returns:
            A pandas DataFrame with all attribute values.
        """
        ...
