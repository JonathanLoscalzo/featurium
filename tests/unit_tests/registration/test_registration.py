from typing import Any

import pytest
from sqlalchemy.orm import Session

from featurium.core.models import AttributeType, AttributeValue, DataType, Project
from featurium.services.registration.registration import RegistrationService


@pytest.mark.usefixtures("cleanup")
class TestRegistrationServiceProjectCreation:
    """Test project creation methods in RegistrationService"""

    def test_register_project_basic(self, db: Session) -> None:
        """Test basic project registration"""
        service = RegistrationService(db)

        project = service.register_project("Test Project", "Test Description")

        assert project.id is not None
        assert project.name == "Test Project"
        assert project.description == "Test Description"
        assert project.created_at is not None
        assert project.updated_at is not None

    def test_register_project_minimal(self, db: Session) -> None:
        """Test project registration with minimal parameters"""
        service = RegistrationService(db)

        project = service.register_project("Minimal Project")

        assert project.id is not None
        assert project.name == "Minimal Project"
        assert project.description == ""
        assert project.created_at is not None

    def test_register_project_with_meta(self, db: Session) -> None:
        """Test project registration with metadata"""
        # Create project directly to test meta field
        project = Project(
            name="Project with Meta",
            description="Test Description",
            meta={"version": "1.0", "environment": "test"},
        )
        db.add(project)
        db.commit()

        assert project.id is not None
        assert project.name == "Project with Meta"
        assert project.meta == {"version": "1.0", "environment": "test"}


@pytest.mark.usefixtures("cleanup")
class TestRegistrationServiceEntityCreation:
    """Test entity creation methods in RegistrationService"""

    def test_register_entity_basic(self, db: Session) -> None:
        """Test basic entity registration"""
        service = RegistrationService(db)

        # First create a project
        project = service.register_project("Test Project")

        entity = service.register_entity("Test Entity", project, "Test Description")

        assert entity.id is not None
        assert entity.name == "Test Entity"
        assert entity.description == "Test Description"
        assert entity.project_id == project.id
        assert entity.created_at is not None

    def test_register_entity_minimal(self, db: Session) -> None:
        """Test entity registration with minimal parameters"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Minimal Entity", project)

        assert entity.id is not None
        assert entity.name == "Minimal Entity"
        assert entity.description == ""
        assert entity.project_id == project.id


@pytest.mark.usefixtures("cleanup")
class TestRegistrationServiceFeatureCreation:
    """Test feature creation methods in RegistrationService"""

    def test_register_feature_basic(self, db: Session) -> None:
        """Test basic feature registration"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        feature = service.register_feature(
            "Test Feature", project, DataType.FLOAT, "Test Description"
        )

        assert feature.id is not None
        assert feature.name == "Test Feature"
        assert feature.description == "Test Description"
        assert feature.project_id == project.id
        assert feature.type == AttributeType.FEATURE
        assert feature.data_type == DataType.FLOAT
        assert feature.is_label is False

    def test_register_feature_minimal(self, db: Session) -> None:
        """Test feature registration with minimal parameters"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        feature = service.register_feature("Minimal Feature", project, DataType.STRING)

        assert feature.id is not None
        assert feature.name == "Minimal Feature"
        assert feature.description == ""
        assert feature.type == AttributeType.FEATURE
        assert feature.data_type == DataType.STRING

    def test_register_feature_all_data_types(self, db: Session) -> None:
        """Test feature registration with all data types"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")

        data_types = [
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.STRING,
            DataType.BOOLEAN,
            DataType.DATETIME,
            DataType.JSON,
        ]

        for data_type in data_types:
            feature = service.register_feature(
                f"Feature_{data_type.value}", project, data_type
            )
            assert feature.data_type == data_type
            assert feature.type == AttributeType.FEATURE


@pytest.mark.usefixtures("cleanup")
class TestRegistrationServiceTargetCreation:
    """Test target creation methods in RegistrationService"""

    def test_register_target_basic(self, db: Session) -> None:
        """Test basic target registration"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        target = service.register_target(
            "Test Target", project, DataType.FLOAT, "Test Description"
        )

        assert target.id is not None
        assert target.name == "Test Target"
        assert target.description == "Test Description"
        assert target.project_id == project.id
        assert target.type == AttributeType.TARGET
        assert target.data_type == DataType.FLOAT
        assert target.is_label is True

    def test_register_target_minimal(self, db: Session) -> None:
        """Test target registration with minimal parameters"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        target = service.register_target("Minimal Target", project, DataType.STRING)

        assert target.id is not None
        assert target.name == "Minimal Target"
        assert target.description == ""
        assert target.type == AttributeType.TARGET
        assert target.data_type == DataType.STRING
        assert target.is_label is True


@pytest.mark.usefixtures("cleanup")
class TestRegistrationServiceJoinKeyCreation:
    """Test join key creation methods in RegistrationService"""

    def test_register_join_key_basic(self, db: Session) -> None:
        """Test basic join key registration"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Test Entity", project)

        join_key = service.register_join_key("Test Join Key", entity)

        assert join_key.id is not None
        assert join_key.name == "Test Join Key"
        assert join_key.entity_id == entity.id
        assert join_key.created_at is not None

    def test_register_join_key_value_basic(self, db: Session) -> None:
        """Test basic join key value registration"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Test Entity", project)
        join_key = service.register_join_key("Test Join Key", entity)

        join_key_value = service.register_join_key_value(
            join_key, {"string": "test_value"}
        )

        assert join_key_value.id is not None
        assert join_key_value.value == {"string": "test_value"}
        assert join_key_value.join_key_id == join_key.id
        assert join_key_value.created_at is not None

    def test_register_join_key_value_different_types(self, db: Session) -> None:
        """Test join key value registration with different value types"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Test Entity", project)
        join_key = service.register_join_key("Test Join Key", entity)

        # Test different value types
        test_values = [
            {"string": "test_string"},
            {"integer": 123},
            {"float": 45.67},
            {"boolean": True},
            {"json": {"nested": "object"}},
        ]

        for value in test_values:
            join_key_value = service.register_join_key_value(join_key, value)
            assert join_key_value.value == value


@pytest.mark.usefixtures("cleanup")
class TestRegistrationServiceAssociations:
    """Test association methods in RegistrationService"""

    def test_associate_attribute_with_entity(self, db: Session) -> None:
        """Test associating an attribute with an entity"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Test Entity", project)
        feature = service.register_feature("Test Feature", project, DataType.FLOAT)

        association = service.associate_attribute_with_entity(feature, entity)

        assert association.attribute_id == feature.id
        assert association.entity_id == entity.id

    def test_associate_attribute_value_with_join_key_value(self, db: Session) -> None:
        """Test associating an attribute value with a join key value"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Test Entity", project)
        feature = service.register_feature("Test Feature", project, DataType.FLOAT)
        join_key = service.register_join_key("Test Join Key", entity)
        join_key_value = service.register_join_key_value(
            join_key, {"string": "test_value"}
        )

        # Create attribute value first
        attribute_value = AttributeValue(attribute=feature, value={"float": 123.45})
        db.add(attribute_value)
        db.commit()

        # Associate with join key value
        result = service.associate_attribute_value_with_join_key_value(
            attribute_value, join_key_value
        )

        assert result.join_key_value_id == join_key_value.id
        assert result.attribute_id == feature.id


@pytest.mark.usefixtures("cleanup")
class TestRegistrationServiceValueCreation:
    """Test value creation methods in RegistrationService"""

    def test_register_feature_value_basic(self, db: Session) -> None:
        """Test basic feature value registration"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Test Entity", project)
        feature = service.register_feature("Test Feature", project, DataType.FLOAT)
        join_key = service.register_join_key("Test Join Key", entity)
        join_key_value = service.register_join_key_value(
            join_key, {"string": "test_value"}
        )

        feature_value = service.register_feature_value(
            feature, join_key_value, {"float": 123.45}, {"source": "test"}
        )

        assert feature_value.id is not None
        assert feature_value.attribute_id == feature.id
        assert feature_value.join_key_value_id == join_key_value.id
        assert feature_value.value == {"float": 123.45}
        assert feature_value.meta == {"source": "test"}

    def test_register_target_value_basic(self, db: Session) -> None:
        """Test basic target value registration"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Test Entity", project)
        target = service.register_target("Test Target", project, DataType.FLOAT)
        join_key = service.register_join_key("Test Join Key", entity)
        join_key_value = service.register_join_key_value(
            join_key, {"string": "test_value"}
        )

        target_value = service.register_target_value(
            target, join_key_value, {"float": 4.5}, {"source": "test"}
        )

        assert target_value.id is not None
        assert target_value.attribute_id == target.id
        assert target_value.join_key_value_id == join_key_value.id
        assert target_value.value == {"float": 4.5}
        assert target_value.meta == {"source": "test"}

    def test_register_feature_value_minimal(self, db: Session) -> None:
        """Test feature value registration with minimal parameters"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Test Entity", project)
        feature = service.register_feature("Test Feature", project, DataType.STRING)
        join_key = service.register_join_key("Test Join Key", entity)
        join_key_value = service.register_join_key_value(
            join_key, {"string": "test_value"}
        )

        feature_value = service.register_feature_value(
            feature, join_key_value, {"string": "test_value"}
        )

        assert feature_value.id is not None
        assert feature_value.value == {"string": "test_value"}
        assert feature_value.meta == {}


@pytest.mark.usefixtures("cleanup")
class TestRegistrationServiceBulkOperations:
    """Test bulk operations in RegistrationService"""

    def test_register_projects_bulk(self, db: Session) -> None:
        """Test bulk project registration"""
        service = RegistrationService(db)

        projects_data: list[dict[str, Any]] = [
            {"name": "Project 1", "description": "Description 1"},
            {
                "name": "Project 2",
                "description": "Description 2",
                "meta": {"version": "1.0"},
            },
            {"name": "Project 3"},
        ]

        projects = service.register_projects_bulk(projects_data)

        assert len(projects) == 3
        assert projects[0].name == "Project 1"
        assert projects[0].description == "Description 1"
        assert projects[1].name == "Project 2"
        assert projects[1].meta == {"version": "1.0"}
        assert projects[2].name == "Project 3"
        assert projects[2].description == ""

    def test_register_entities_bulk(self, db: Session) -> None:
        """Test bulk entity registration"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")

        entities_data = [
            {
                "name": "Entity 1",
                "project_id": project.id,
                "description": "Description 1",
            },
            {"name": "Entity 2", "project_id": project.id, "meta": {"version": "1.0"}},
            {"name": "Entity 3", "project_id": project.id},
        ]

        entities = service.register_entities_bulk(entities_data)

        assert len(entities) == 3
        assert entities[0].name == "Entity 1"
        assert entities[0].project_id == project.id
        assert entities[1].name == "Entity 2"
        assert entities[1].meta == {"version": "1.0"}

    def test_register_attributes_bulk(self, db: Session) -> None:
        """Test bulk attribute registration"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")

        attributes_data = [
            {
                "name": "Feature 1",
                "project_id": project.id,
                "type": AttributeType.FEATURE,
                "data_type": DataType.FLOAT,
                "description": "Description 1",
            },
            {
                "name": "Target 1",
                "project_id": project.id,
                "type": AttributeType.TARGET,
                "data_type": DataType.STRING,
                "is_label": True,
                "meta": {"version": "1.0"},
            },
        ]

        attributes = service.register_attributes_bulk(attributes_data)

        assert len(attributes) == 2
        assert attributes[0].name == "Feature 1"
        assert attributes[0].type == AttributeType.FEATURE
        assert attributes[1].name == "Target 1"
        assert attributes[1].type == AttributeType.TARGET
        assert attributes[1].is_label is True

    def test_register_join_keys_bulk(self, db: Session) -> None:
        """Test bulk join key registration"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Test Entity", project)

        join_keys_data = [
            {
                "name": "Join Key 1",
                "entity_id": entity.id,
                "description": "Description 1",
            },
            {"name": "Join Key 2", "entity_id": entity.id, "meta": {"version": "1.0"}},
            {"name": "Join Key 3", "entity_id": entity.id},
        ]

        join_keys = service.register_join_keys_bulk(join_keys_data)

        assert len(join_keys) == 3
        assert join_keys[0].name == "Join Key 1"
        assert join_keys[0].entity_id == entity.id
        assert join_keys[1].name == "Join Key 2"
        assert join_keys[1].meta == {"version": "1.0"}

    def test_register_join_key_values_bulk(self, db: Session) -> None:
        """Test bulk join key value registration"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Test Entity", project)
        join_key = service.register_join_key("Test Join Key", entity)

        join_key_values_data = [
            {"join_key_id": join_key.id, "value": {"string": "value1"}},
            {
                "join_key_id": join_key.id,
                "value": {"string": "value2"},
                "meta": {"version": "1.0"},
            },
            {"join_key_id": join_key.id, "value": {"integer": 123}},
        ]

        join_key_values = service.register_join_key_values_bulk(join_key_values_data)

        assert len(join_key_values) == 3
        assert join_key_values[0].value == {"string": "value1"}
        assert join_key_values[1].value == {"string": "value2"}
        assert join_key_values[1].meta == {"version": "1.0"}
        assert join_key_values[2].value == {"integer": 123}

    def test_register_attribute_values_bulk(self, db: Session) -> None:
        """Test bulk attribute value registration"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Test Entity", project)
        feature = service.register_feature("Test Feature", project, DataType.FLOAT)
        join_key = service.register_join_key("Test Join Key", entity)
        join_key_value = service.register_join_key_value(
            join_key, {"string": "test_value"}
        )

        attribute_values_data = [
            {
                "attribute_id": feature.id,
                "join_key_value_id": join_key_value.id,
                "value": {"float": 1.0},
            },
            {
                "attribute_id": feature.id,
                "join_key_value_id": join_key_value.id,
                "value": {"float": 2.0},
                "meta": {"source": "test"},
            },
        ]

        attribute_values = service.register_attribute_values_bulk(attribute_values_data)

        assert len(attribute_values) == 2
        assert attribute_values[0].value == {"float": 1.0}
        assert attribute_values[1].value == {"float": 2.0}
        assert attribute_values[1].meta == {"source": "test"}

    def test_associate_attributes_with_entities_bulk(self, db: Session) -> None:
        """Test bulk attribute-entity association"""
        service = RegistrationService(db)

        project = service.register_project("Test Project")
        entity = service.register_entity("Test Entity", project)
        feature1 = service.register_feature("Feature 1", project, DataType.FLOAT)
        feature2 = service.register_feature("Feature 2", project, DataType.STRING)

        associations_data = [
            {"attribute_id": feature1.id, "entity_id": entity.id},
            {"attribute_id": feature2.id, "entity_id": entity.id},
        ]

        associations = service.associate_attributes_with_entities_bulk(
            associations_data
        )

        assert len(associations) == 2
        assert associations[0].attribute_id == feature1.id
        assert associations[0].entity_id == entity.id
        assert associations[1].attribute_id == feature2.id
        assert associations[1].entity_id == entity.id


@pytest.mark.usefixtures("cleanup")
class TestRegistrationServiceIntegration:
    """Test integration scenarios in RegistrationService"""

    def test_complete_workflow(self, db: Session) -> None:
        """Test a complete workflow from project to feature values"""
        service = RegistrationService(db)

        # 1. Create project
        project = service.register_project("Integration Test Project", "Test workflow")

        # 2. Create entity
        entity = service.register_entity("Test Entity", project, "Test entity")

        # 3. Create features and targets
        feature = service.register_feature(
            "Test Feature", project, DataType.FLOAT, "Test feature"
        )
        target = service.register_target(
            "Test Target", project, DataType.FLOAT, "Test target"
        )

        # 4. Create join key
        join_key = service.register_join_key("Test Join Key", entity)

        # 5. Create join key value
        join_key_value = service.register_join_key_value(
            join_key, {"string": "test_id"}
        )

        # 6. Associate features with entity
        association = service.associate_attribute_with_entity(feature, entity)

        # 7. Create feature and target values
        feature_value = service.register_feature_value(
            feature, join_key_value, {"float": 123.45}
        )
        target_value = service.register_target_value(
            target, join_key_value, {"float": 4.5}
        )

        # Verify everything is connected correctly
        assert project.id is not None
        assert entity.project_id == project.id
        assert feature.project_id == project.id
        assert target.project_id == project.id
        assert join_key.entity_id == entity.id
        assert join_key_value.join_key_id == join_key.id
        assert association.attribute_id == feature.id
        assert association.entity_id == entity.id
        assert feature_value.attribute_id == feature.id
        assert feature_value.join_key_value_id == join_key_value.id
        assert target_value.attribute_id == target.id
        assert target_value.join_key_value_id == join_key_value.id
