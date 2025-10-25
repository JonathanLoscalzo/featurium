"""
Feature Store

This module provides the main FeatureStore class for managing and serving ML features.
The FeatureStore acts as a high-level interface that coordinates between registration
and retrieval services, using parameter objects for clean API design.
"""

from typing import List, Optional

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from featurium.core.models import AttributeType, Entity, Project
from featurium.feature_store.protocols import RegistrationServiceProtocol, RetrievalServiceProtocol
from featurium.feature_store.schemas import (
    AssociationInput,
    AssociationOutput,
    EntityInput,
    EntityOutput,
    FeatureInput,
    FeatureOutput,
    FeatureRetrievalInput,
    FeatureValueInput,
    FeatureValueOutput,
    JoinKeyInput,
    JoinKeyOutput,
    JoinKeyValueInput,
    JoinKeyValueOutput,
    ProjectInput,
    ProjectOutput,
    TargetInput,
    TargetOutput,
)


class FeatureStore:
    """
    FeatureStore is the main class for managing and serving ML features.

    This class provides a comprehensive interface for:
    - Registering projects, entities, features, and their values
    - Retrieving feature values for model training and inference
    - Managing relationships between entities and features
    - Materializing feature datasets

    The FeatureStore uses dependency injection to work with registration and retrieval
    services, making it flexible and testable.
    """

    def __init__(
        self,
        registration_service: RegistrationServiceProtocol,
        retrieval_service: RetrievalServiceProtocol,
        db: Session,
    ):
        """
        Initialize the FeatureStore with required services.

        Args:
            registration_service: Service for registering entities, features,
                and values.
            retrieval_service: Service for retrieving feature values.
            db: SQLAlchemy database session for direct queries.
        """
        self.registration_service = registration_service
        self.retrieval_service = retrieval_service
        self.db = db

    # region Registration Methods

    def register_projects(self, inputs: List[ProjectInput]) -> List[ProjectOutput]:
        """
        Register multiple projects in bulk.

        Args:
            inputs: List of project input specifications.

        Returns:
            List of created projects with their IDs and metadata.

        Example:
            >>> inputs = [
            ...     ProjectInput(name="my_project", description="My ML project"),
            ...     ProjectInput(name="another_project"),
            ... ]
            >>> projects = fs.register_projects(inputs)
        """
        projects_data = [input_obj.model_dump() for input_obj in inputs]
        projects = self.registration_service.register_projects_bulk(projects_data)
        return [ProjectOutput.model_validate(p) for p in projects]

    def register_entities(self, inputs: List[EntityInput]) -> List[EntityOutput]:
        """
        Register multiple entities in bulk.

        Args:
            inputs: List of entity input specifications.

        Returns:
            List of created entities with their IDs and metadata.

        Example:
            >>> inputs = [
            ...     EntityInput(name="user", project_id=1),
            ...     EntityInput(name="transaction", project_name="my_project"),
            ... ]
            >>> entities = fs.register_entities(inputs)
        """
        entities_data = []
        for input_obj in inputs:
            data = input_obj.model_dump(exclude_none=True)

            # Resolve project_name to project_id if needed
            if "project_name" in data and "project_id" not in data:
                project = self._get_project_by_name(data.pop("project_name"))
                data["project_id"] = project.id
            elif "project_name" in data:
                data.pop("project_name")

            entities_data.append(data)

        entities = self.registration_service.register_entities_bulk(entities_data)
        return [EntityOutput.model_validate(e) for e in entities]

    def register_features(self, inputs: List[FeatureInput]) -> List[FeatureOutput]:
        """
        Register multiple features in bulk.

        Args:
            inputs: List of feature input specifications.

        Returns:
            List of created features with their IDs and metadata.

        Example:
            >>> inputs = [
            ...     FeatureInput(
            ...         name="user_age",
            ...         project_id=1,
            ...         data_type=DataType.INTEGER,
            ...         entity_ids=[1]
            ...     ),
            ... ]
            >>> features = fs.register_features(inputs)
        """
        attributes_data = []
        entities_to_associate = []

        for input_obj in inputs:
            data = input_obj.model_dump(exclude_none=True)
            entity_ids = data.pop("entity_ids", None)

            # Resolve project_name to project_id if needed
            if "project_name" in data and "project_id" not in data:
                project = self._get_project_by_name(data.pop("project_name"))
                data["project_id"] = project.id
            elif "project_name" in data:
                data.pop("project_name")

            data["type"] = AttributeType.FEATURE
            attributes_data.append(data)

            if entity_ids:
                entities_to_associate.append(entity_ids)
            else:
                entities_to_associate.append([])

        features = self.registration_service.register_attributes_bulk(attributes_data)

        # Associate features with entities if specified
        associations = []
        for feature, entity_ids in zip(features, entities_to_associate):
            for entity_id in entity_ids:
                associations.append({"attribute_id": feature.id, "entity_id": entity_id})

        if associations:
            self.registration_service.associate_attributes_with_entities_bulk(associations)

        return [FeatureOutput.model_validate(f) for f in features]

    def register_targets(self, inputs: List[TargetInput]) -> List[TargetOutput]:
        """
        Register multiple targets in bulk.

        Args:
            inputs: List of target input specifications.

        Returns:
            List of created targets with their IDs and metadata.

        Example:
            >>> inputs = [
            ...     TargetInput(
            ...         name="conversion",
            ...         project_id=1,
            ...         data_type=DataType.BOOLEAN,
            ...         entity_ids=[1]
            ...     ),
            ... ]
            >>> targets = fs.register_targets(inputs)
        """
        attributes_data = []
        entities_to_associate = []

        for input_obj in inputs:
            data = input_obj.model_dump(exclude_none=True)
            entity_ids = data.pop("entity_ids", None)

            # Resolve project_name to project_id if needed
            if "project_name" in data and "project_id" not in data:
                project = self._get_project_by_name(data.pop("project_name"))
                data["project_id"] = project.id
            elif "project_name" in data:
                data.pop("project_name")

            data["type"] = AttributeType.TARGET
            data["is_label"] = True
            attributes_data.append(data)

            if entity_ids:
                entities_to_associate.append(entity_ids)
            else:
                entities_to_associate.append([])

        targets = self.registration_service.register_attributes_bulk(attributes_data)

        # Associate targets with entities if specified
        associations = []
        for target, entity_ids in zip(targets, entities_to_associate):
            for entity_id in entity_ids:
                associations.append({"attribute_id": target.id, "entity_id": entity_id})

        if associations:
            self.registration_service.associate_attributes_with_entities_bulk(associations)

        return [TargetOutput.model_validate(t) for t in targets]

    def register_join_keys(self, inputs: List[JoinKeyInput]) -> List[JoinKeyOutput]:
        """
        Register multiple join keys in bulk.

        Args:
            inputs: List of join key input specifications.

        Returns:
            List of created join keys with their IDs and metadata.

        Example:
            >>> inputs = [
            ...     JoinKeyInput(name="user_id", entity_id=1),
            ...     JoinKeyInput(name="transaction_id", entity_name="transaction"),
            ... ]
            >>> join_keys = fs.register_join_keys(inputs)
        """
        join_keys_data = []

        for input_obj in inputs:
            data = input_obj.model_dump(exclude_none=True)

            # Resolve entity_name to entity_id if needed
            if "entity_name" in data and "entity_id" not in data:
                entity = self._get_entity_by_name(data.pop("entity_name"))
                data["entity_id"] = entity.id
            elif "entity_name" in data:
                data.pop("entity_name")

            join_keys_data.append(data)

        join_keys = self.registration_service.register_join_keys_bulk(join_keys_data)
        return [JoinKeyOutput.model_validate(jk) for jk in join_keys]

    def register_join_key_values(self, inputs: List[JoinKeyValueInput]) -> List[JoinKeyValueOutput]:
        """
        Register multiple join key values in bulk.

        Args:
            inputs: List of join key value input specifications.

        Returns:
            List of created join key values with their IDs and metadata.

        Example:
            >>> inputs = [
            ...     JoinKeyValueInput(join_key_id=1, value=123),
            ...     JoinKeyValueInput(join_key_name="user_id", value=456),
            ... ]
            >>> jkv = fs.register_join_key_values(inputs)
        """
        jkv_data = []

        for input_obj in inputs:
            data = input_obj.model_dump(exclude_none=True)

            # Resolve join_key_name to join_key_id if needed
            if "join_key_name" in data and "join_key_id" not in data:
                join_key = self._get_join_key_by_name(data.pop("join_key_name"))
                data["join_key_id"] = join_key.id
            elif "join_key_name" in data:
                data.pop("join_key_name")

            jkv_data.append(data)

        join_key_values = self.registration_service.register_join_key_values_bulk(jkv_data)
        return [JoinKeyValueOutput.model_validate(jkv) for jkv in join_key_values]

    def register_feature_values(self, inputs: List[FeatureValueInput]) -> List[FeatureValueOutput]:
        """
        Register multiple feature/attribute values in bulk.

        Args:
            inputs: List of feature value input specifications.

        Returns:
            List of created feature values with their IDs and metadata.

        Example:
            >>> inputs = [
            ...     FeatureValueInput(
            ...         attribute_id=1,
            ...         join_key_value_id=1,
            ...         value=25
            ...     ),
            ... ]
            >>> values = fs.register_feature_values(inputs)
        """
        av_data = []

        for input_obj in inputs:
            data = input_obj.model_dump(exclude_none=True)

            # Resolve attribute_name to attribute_id if needed
            if "attribute_name" in data and "attribute_id" not in data:
                attribute = self._get_attribute_by_name(data.pop("attribute_name"))
                data["attribute_id"] = attribute.id
            elif "attribute_name" in data:
                data.pop("attribute_name")

            av_data.append(data)

        attribute_values = self.registration_service.register_attribute_values_bulk(av_data)
        return [FeatureValueOutput.model_validate(av) for av in attribute_values]

    def associate_features_with_entities(self, inputs: List[AssociationInput]) -> List[AssociationOutput]:
        """
        Associate multiple features with entities in bulk.

        Args:
            inputs: List of association input specifications.

        Returns:
            List of created associations with their metadata.

        Example:
            >>> inputs = [
            ...     AssociationInput(attribute_id=1, entity_id=1),
            ...     AssociationInput(attribute_id=2, entity_id=1),
            ... ]
            >>> associations = fs.associate_features_with_entities(inputs)
        """
        associations_data = [input_obj.model_dump() for input_obj in inputs]
        associations = self.registration_service.associate_attributes_with_entities_bulk(associations_data)
        return [AssociationOutput.model_validate(a) for a in associations]

    # endregion

    # region Retrieval Methods

    def get_feature_values(self, input_obj: FeatureRetrievalInput) -> pd.DataFrame:
        """
        Retrieve feature values from the feature store.

        Args:
            input_obj: Specification of what features to retrieve and how to filter.

        Returns:
            A pandas DataFrame with feature values indexed by join key values.

        Example:
            >>> input_obj = FeatureRetrievalInput(
            ...     project_name="my_project",
            ...     entity_name="user",
            ...     join_keys=[123, 456],
            ...     feature_names=["age", "country"]
            ... )
            >>> df = fs.get_feature_values(input_obj)
        """
        return self.retrieval_service.get_feature_values(
            project_name=input_obj.project_name,
            entity_name=input_obj.entity_name,
            join_keys=input_obj.join_keys,
            feature_names=input_obj.feature_names,
            start_time=input_obj.start_time,
            end_time=input_obj.end_time,
            include_timestamp=input_obj.include_timestamp,
        )

    def get_target_values(self, input_obj: FeatureRetrievalInput) -> pd.DataFrame:
        """
        Retrieve target values from the feature store.

        Args:
            input_obj: Specification of what targets to retrieve and how to filter.

        Returns:
            A pandas DataFrame with target values indexed by join key values.

        Example:
            >>> input_obj = FeatureRetrievalInput(
            ...     project_name="my_project",
            ...     entity_name="user",
            ...     join_keys=[123, 456],
            ...     feature_names=["conversion", "churn"]
            ... )
            >>> df = fs.get_target_values(input_obj)
        """
        return self.retrieval_service.get_target_values(
            project_name=input_obj.project_name,
            entity_name=input_obj.entity_name,
            join_keys=input_obj.join_keys,
            feature_names=input_obj.feature_names,
            start_time=input_obj.start_time,
            end_time=input_obj.end_time,
            include_timestamp=input_obj.include_timestamp,
        )

    def get_all_values(self, input_obj: FeatureRetrievalInput) -> pd.DataFrame:
        """
        Retrieve all attribute values (features and targets) from the feature store.

        Args:
            input_obj: Specification of what attributes to retrieve and how to filter.

        Returns:
            A pandas DataFrame with all attribute values indexed by join key values.

        Example:
            >>> input_obj = FeatureRetrievalInput(
            ...     project_name="my_project",
            ...     entity_name="user",
            ...     join_keys=[123, 456]
            ... )
            >>> df = fs.get_all_values(input_obj)
        """
        return self.retrieval_service.get_values(
            project_name=input_obj.project_name,
            entity_name=input_obj.entity_name,
            join_keys=input_obj.join_keys,
            feature_names=input_obj.feature_names,
            start_time=input_obj.start_time,
            end_time=input_obj.end_time,
            include_timestamp=input_obj.include_timestamp,
        )

    # endregion

    # region Query/Listing Methods

    def list_projects(self) -> List[str]:
        """
        List all projects in the feature store.

        Returns:
            List of project names.

        Example:
            >>> projects = fs.list_projects()
            >>> print(projects)
            ['my_project', 'another_project']
        """
        return [p[0] for p in self.db.query(Project.name).distinct().all()]

    def list_entities(self, project_name: Optional[str] = None) -> List[str]:
        """
        List all entities in the feature store, optionally filtered by project.

        Args:
            project_name: Optional project name to filter entities.

        Returns:
            List of entity names.

        Example:
            >>> entities = fs.list_entities(project_name="my_project")
            >>> print(entities)
            ['user', 'transaction']
        """
        query = self.db.query(Entity.name)
        if project_name:
            query = query.join(Project).filter(Project.name == project_name)
        return [e[0] for e in query.distinct().all()]

    def list_features(self, project_name: str, entity_name: Optional[str] = None) -> List[str]:
        """
        List all features for a project, optionally filtered by entity.

        Args:
            project_name: Name of the project.
            entity_name: Optional entity name to filter features.

        Returns:
            List of feature names.

        Example:
            >>> features = fs.list_features("my_project", entity_name="user")
            >>> print(features)
            ['age', 'country', 'signup_date']
        """
        project = self._get_project_by_name(project_name)

        if entity_name:
            entity = self._get_entity_by_name(entity_name, project_id=project.id)
            return [f.name for f in entity.features]
        else:
            return [f.name for f in project.features]

    def list_targets(self, project_name: str, entity_name: Optional[str] = None) -> List[str]:
        """
        List all targets for a project, optionally filtered by entity.

        Args:
            project_name: Name of the project.
            entity_name: Optional entity name to filter targets.

        Returns:
            List of target names.

        Example:
            >>> targets = fs.list_targets("my_project", entity_name="user")
            >>> print(targets)
            ['conversion', 'churn']
        """
        project = self._get_project_by_name(project_name)

        if entity_name:
            entity = self._get_entity_by_name(entity_name, project_id=project.id)
            return [t.name for t in entity.targets]
        else:
            return [t.name for t in project.targets]

    # endregion

    # region Materialization Methods

    def materialize(self) -> None:
        """
        Materialize feature datasets for batch serving.

        This method will be implemented in the future to support:
        - Pre-computing feature values for batch inference
        - Exporting features to data warehouses
        - Creating feature snapshots at specific timestamps
        - Optimizing feature retrieval performance

        Implementation pending.
        """
        pass

    # endregion

    # region Private Helper Methods

    def _get_project_by_name(self, name: str) -> Project:
        """
        Get a project by name.

        Args:
            name: Name of the project.

        Returns:
            Project instance.

        Raises:
            ValueError: If project is not found.
        """
        project = self.db.scalar(select(Project).where(Project.name == name))
        if not project:
            raise ValueError(f"Project '{name}' not found")
        return project

    def _get_entity_by_name(self, name: str, project_id: Optional[int] = None) -> Entity:
        """
        Get an entity by name.

        Args:
            name: Name of the entity.
            project_id: Optional project ID to filter.

        Returns:
            Entity instance.

        Raises:
            ValueError: If entity is not found.
        """
        query = select(Entity).where(Entity.name == name)
        if project_id:
            query = query.where(Entity.project_id == project_id)

        entity = self.db.scalar(query)
        if not entity:
            raise ValueError(f"Entity '{name}' not found")
        return entity

    def _get_attribute_by_name(self, name: str):
        """
        Get an attribute by name.

        Args:
            name: Name of the attribute.

        Returns:
            Attribute instance.

        Raises:
            ValueError: If attribute is not found.
        """
        from featurium.core.models import Attribute

        attribute = self.db.scalar(select(Attribute).where(Attribute.name == name))
        if not attribute:
            raise ValueError(f"Attribute '{name}' not found")
        return attribute

    def _get_join_key_by_name(self, name: str):
        """
        Get a join key by name.

        Args:
            name: Name of the join key.

        Returns:
            JoinKey instance.

        Raises:
            ValueError: If join key is not found.
        """
        from featurium.core.models import JoinKey

        join_key = self.db.scalar(select(JoinKey).where(JoinKey.name == name))
        if not join_key:
            raise ValueError(f"JoinKey '{name}' not found")
        return join_key

    # endregion
