"""
Database validation utilities for integration tests.

This module provides helper functions to verify that entities are correctly
persisted in the database, including their relationships and data integrity.
"""

from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from featurium.core.models import (
    Attribute,
    AttributeEntities,
    AttributeType,
    AttributeValue,
    Entity,
    JoinKey,
    JoinKeyValue,
    Project,
)


class DBValidator:
    """Helper class to validate database state in integration tests."""

    def __init__(self, db: Session):
        """
        Initialize the validator with a database session.

        Args:
            db: SQLAlchemy database session.
        """
        self.db = db

    # region Project validations

    def verify_project_exists(self, name: str) -> Project:
        """
        Verify that a project exists in the database.

        Args:
            name: Name of the project.

        Returns:
            The project instance.

        Raises:
            AssertionError: If project doesn't exist.
        """
        project = self.db.scalar(select(Project).where(Project.name == name))
        assert project is not None, f"Project '{name}' not found in database"
        return project

    def verify_project_count(self, expected_count: int) -> None:
        """
        Verify the total number of projects in the database.

        Args:
            expected_count: Expected number of projects.

        Raises:
            AssertionError: If count doesn't match.
        """
        count = self.db.query(Project).count()
        assert count == expected_count, f"Expected {expected_count} projects, found {count}"

    def verify_project_metadata(self, name: str, expected_description: Optional[str] = None) -> None:
        """
        Verify project metadata.

        Args:
            name: Name of the project.
            expected_description: Expected description (if any).

        Raises:
            AssertionError: If metadata doesn't match.
        """
        project = self.verify_project_exists(name)
        if expected_description is not None:
            assert (
                project.description == expected_description
            ), f"Expected description '{expected_description}', got '{project.description}'"

    # endregion

    # region Entity validations

    def verify_entity_exists(self, name: str, project_name: Optional[str] = None) -> Entity:
        """
        Verify that an entity exists in the database.

        Args:
            name: Name of the entity.
            project_name: Optional project name to scope the search.

        Returns:
            The entity instance.

        Raises:
            AssertionError: If entity doesn't exist.
        """
        query = select(Entity).where(Entity.name == name)
        if project_name:
            project = self.verify_project_exists(project_name)
            query = query.where(Entity.project_id == project.id)

        entity = self.db.scalar(query)
        assert entity is not None, f"Entity '{name}' not found in database"
        return entity

    def verify_entity_count(self, project_name: Optional[str] = None, expected_count: int = 0) -> None:
        """
        Verify the number of entities in the database.

        Args:
            project_name: Optional project name to filter by.
            expected_count: Expected number of entities.

        Raises:
            AssertionError: If count doesn't match.
        """
        query = self.db.query(Entity)
        if project_name:
            project = self.verify_project_exists(project_name)
            query = query.filter(Entity.project_id == project.id)

        count = query.count()
        assert count == expected_count, f"Expected {expected_count} entities, found {count}"

    def verify_entity_belongs_to_project(self, entity_name: str, project_name: str) -> None:
        """
        Verify that an entity belongs to a specific project.

        Args:
            entity_name: Name of the entity.
            project_name: Name of the project.

        Raises:
            AssertionError: If entity doesn't belong to the project.
        """
        entity = self.verify_entity_exists(entity_name)
        project = self.verify_project_exists(project_name)
        assert (
            entity.project_id == project.id
        ), f"Entity '{entity_name}' doesn't belong to project '{project_name}'"

    # endregion

    # region Attribute (Feature/Target) validations

    def verify_attribute_exists(self, name: str, attr_type: Optional[AttributeType] = None) -> Attribute:
        """
        Verify that an attribute exists in the database.

        Args:
            name: Name of the attribute.
            attr_type: Optional type filter (FEATURE or TARGET).

        Returns:
            The attribute instance.

        Raises:
            AssertionError: If attribute doesn't exist.
        """
        query = select(Attribute).where(Attribute.name == name)
        if attr_type:
            query = query.where(Attribute.type == attr_type)

        attribute = self.db.scalar(query)
        assert attribute is not None, f"Attribute '{name}' not found in database"
        return attribute

    def verify_feature_count(self, project_name: Optional[str] = None, expected_count: int = 0) -> None:
        """
        Verify the number of features in the database.

        Args:
            project_name: Optional project name to filter by.
            expected_count: Expected number of features.

        Raises:
            AssertionError: If count doesn't match.
        """
        query = self.db.query(Attribute).filter(Attribute.type == AttributeType.FEATURE)
        if project_name:
            project = self.verify_project_exists(project_name)
            query = query.filter(Attribute.project_id == project.id)

        count = query.count()
        assert count == expected_count, f"Expected {expected_count} features, found {count}"

    def verify_target_count(self, project_name: Optional[str] = None, expected_count: int = 0) -> None:
        """
        Verify the number of targets in the database.

        Args:
            project_name: Optional project name to filter by.
            expected_count: Expected number of targets.

        Raises:
            AssertionError: If count doesn't match.
        """
        query = self.db.query(Attribute).filter(Attribute.type == AttributeType.TARGET)
        if project_name:
            project = self.verify_project_exists(project_name)
            query = query.filter(Attribute.project_id == project.id)

        count = query.count()
        assert count == expected_count, f"Expected {expected_count} targets, found {count}"

    def verify_attribute_associated_with_entity(self, attribute_name: str, entity_name: str) -> None:
        """
        Verify that an attribute is associated with an entity.

        Args:
            attribute_name: Name of the attribute.
            entity_name: Name of the entity.

        Raises:
            AssertionError: If association doesn't exist.
        """
        attribute = self.verify_attribute_exists(attribute_name)
        entity = self.verify_entity_exists(entity_name)

        association = self.db.scalar(
            select(AttributeEntities).where(
                AttributeEntities.attribute_id == attribute.id,
                AttributeEntities.entity_id == entity.id,
            )
        )
        assert (
            association is not None
        ), f"Attribute '{attribute_name}' not associated with entity '{entity_name}'"

    # endregion

    # region Join Key validations

    def verify_join_key_exists(self, name: str, entity_name: Optional[str] = None) -> JoinKey:
        """
        Verify that a join key exists in the database.

        Args:
            name: Name of the join key.
            entity_name: Optional entity name to scope the search.

        Returns:
            The join key instance.

        Raises:
            AssertionError: If join key doesn't exist.
        """
        query = select(JoinKey).where(JoinKey.name == name)
        if entity_name:
            entity = self.verify_entity_exists(entity_name)
            query = query.where(JoinKey.entity_id == entity.id)

        join_key = self.db.scalar(query)
        assert join_key is not None, f"Join key '{name}' not found in database"
        return join_key

    def verify_join_key_count(self, entity_name: Optional[str] = None, expected_count: int = 0) -> None:
        """
        Verify the number of join keys in the database.

        Args:
            entity_name: Optional entity name to filter by.
            expected_count: Expected number of join keys.

        Raises:
            AssertionError: If count doesn't match.
        """
        query = self.db.query(JoinKey)
        if entity_name:
            entity = self.verify_entity_exists(entity_name)
            query = query.filter(JoinKey.entity_id == entity.id)

        count = query.count()
        assert count == expected_count, f"Expected {expected_count} join keys, found {count}"

    def verify_join_key_value_exists(self, join_key_name: str, value: Dict[str, Any]) -> JoinKeyValue:
        """
        Verify that a join key value exists in the database.

        Args:
            join_key_name: Name of the join key.
            value: The value to check for.

        Returns:
            The join key value instance.

        Raises:
            AssertionError: If join key value doesn't exist.
        """
        join_key = self.verify_join_key_exists(join_key_name)
        jkv = self.db.scalar(
            select(JoinKeyValue).where(JoinKeyValue.join_key_id == join_key.id, JoinKeyValue.value == value)
        )
        assert jkv is not None, f"Join key value {value} not found for '{join_key_name}'"
        return jkv

    def verify_join_key_value_count(
        self, join_key_name: Optional[str] = None, expected_count: int = 0
    ) -> None:
        """
        Verify the number of join key values in the database.

        Args:
            join_key_name: Optional join key name to filter by.
            expected_count: Expected number of join key values.

        Raises:
            AssertionError: If count doesn't match.
        """
        query = self.db.query(JoinKeyValue)
        if join_key_name:
            join_key = self.verify_join_key_exists(join_key_name)
            query = query.filter(JoinKeyValue.join_key_id == join_key.id)

        count = query.count()
        assert count == expected_count, f"Expected {expected_count} join key values, found {count}"

    # endregion

    # region Attribute Value validations

    def verify_attribute_value_exists(
        self,
        attribute_name: str,
        join_key_value: Dict[str, Any],
        expected_value: Dict[str, Any],
    ) -> AttributeValue:
        """
        Verify that an attribute value exists with the expected value.

        Args:
            attribute_name: Name of the attribute.
            join_key_value: The join key value to filter by.
            expected_value: The expected attribute value.

        Returns:
            The attribute value instance.

        Raises:
            AssertionError: If value doesn't exist or doesn't match.
        """
        attribute = self.verify_attribute_exists(attribute_name)

        # Find the join key value
        jkv = self.db.scalar(select(JoinKeyValue).where(JoinKeyValue.value == join_key_value))
        assert jkv is not None, f"Join key value {join_key_value} not found"

        # Find the attribute value
        av = self.db.scalar(
            select(AttributeValue).where(
                AttributeValue.attribute_id == attribute.id,
                AttributeValue.join_key_value_id == jkv.id,
            )
        )
        assert av is not None, f"Attribute value not found for '{attribute_name}'"
        assert av.value == expected_value, f"Expected value {expected_value}, got {av.value}"
        return av

    def verify_attribute_value_count(
        self, attribute_name: Optional[str] = None, expected_count: int = 0
    ) -> None:
        """
        Verify the number of attribute values in the database.

        Args:
            attribute_name: Optional attribute name to filter by.
            expected_count: Expected number of attribute values.

        Raises:
            AssertionError: If count doesn't match.
        """
        query = self.db.query(AttributeValue)
        if attribute_name:
            attribute = self.verify_attribute_exists(attribute_name)
            query = query.filter(AttributeValue.attribute_id == attribute.id)

        count = query.count()
        assert count == expected_count, f"Expected {expected_count} attribute values, found {count}"

    # endregion

    # region Relationship validations

    def verify_entity_has_features(self, entity_name: str, expected_feature_names: List[str]) -> None:
        """
        Verify that an entity has the expected features associated.

        Args:
            entity_name: Name of the entity.
            expected_feature_names: List of expected feature names.

        Raises:
            AssertionError: If features don't match.
        """
        entity = self.verify_entity_exists(entity_name)
        feature_names = {f.name for f in entity.features}
        expected_set = set(expected_feature_names)

        assert feature_names == expected_set, (
            f"Entity '{entity_name}' features mismatch. " f"Expected: {expected_set}, Got: {feature_names}"
        )

    def verify_entity_has_targets(self, entity_name: str, expected_target_names: List[str]) -> None:
        """
        Verify that an entity has the expected targets associated.

        Args:
            entity_name: Name of the entity.
            expected_target_names: List of expected target names.

        Raises:
            AssertionError: If targets don't match.
        """
        entity = self.verify_entity_exists(entity_name)
        target_names = {t.name for t in entity.targets}
        expected_set = set(expected_target_names)

        assert target_names == expected_set, (
            f"Entity '{entity_name}' targets mismatch. " f"Expected: {expected_set}, Got: {target_names}"
        )

    def verify_project_has_entities(self, project_name: str, expected_entity_names: List[str]) -> None:
        """
        Verify that a project has the expected entities.

        Args:
            project_name: Name of the project.
            expected_entity_names: List of expected entity names.

        Raises:
            AssertionError: If entities don't match.
        """
        project = self.verify_project_exists(project_name)
        entity_names = {e.name for e in project.entities}
        expected_set = set(expected_entity_names)

        assert entity_names == expected_set, (
            f"Project '{project_name}' entities mismatch. " f"Expected: {expected_set}, Got: {entity_names}"
        )

    # endregion

    # region Integrity validations

    def verify_database_integrity(self) -> Dict[str, int]:
        """
        Verify overall database integrity and return entity counts.

        Returns:
            Dictionary with counts of each entity type.
        """
        counts = {
            "projects": self.db.query(Project).count(),
            "entities": self.db.query(Entity).count(),
            "attributes": self.db.query(Attribute).count(),
            "features": self.db.query(Attribute).filter(Attribute.type == AttributeType.FEATURE).count(),
            "targets": self.db.query(Attribute).filter(Attribute.type == AttributeType.TARGET).count(),
            "join_keys": self.db.query(JoinKey).count(),
            "join_key_values": self.db.query(JoinKeyValue).count(),
            "attribute_values": self.db.query(AttributeValue).count(),
            "associations": self.db.query(AttributeEntities).count(),
        }
        return counts

    def print_database_state(self) -> None:
        """Print a summary of the current database state (useful for debugging)."""
        counts = self.verify_database_integrity()
        print("\n" + "=" * 50)
        print("DATABASE STATE")
        print("=" * 50)
        for key, value in counts.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
        print("=" * 50 + "\n")

    # endregion
