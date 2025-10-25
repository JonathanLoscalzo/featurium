"""
Unit tests for FeatureStore registration methods.

These tests use mocks to verify that the FeatureStore correctly delegates
to the registration service and properly converts between Pydantic and SQLAlchemy models.
"""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from featurium.core.models import (
    Attribute,
    AttributeEntities,
    AttributeType,
    AttributeValue,
    DataType,
    Entity,
    JoinKey,
    JoinKeyValue,
    Project,
)
from featurium.feature_store import (
    AssociationInput,
    EntityInput,
    FeatureInput,
    FeatureStore,
    FeatureValueInput,
    JoinKeyInput,
    JoinKeyValueInput,
    ProjectInput,
    RegistrationServiceProtocol,
    RetrievalServiceProtocol,
    TargetInput,
)


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    return MagicMock(spec=Session)


@pytest.fixture
def mock_registration_service() -> RegistrationServiceProtocol:
    """Create a mock registration service."""
    return MagicMock(spec=RegistrationServiceProtocol)


@pytest.fixture
def mock_retrieval_service() -> RetrievalServiceProtocol:
    """Create a mock retrieval service."""
    return MagicMock(spec=RetrievalServiceProtocol)


@pytest.fixture
def feature_store(
    mock_registration_service: RegistrationServiceProtocol,
    mock_retrieval_service: RetrievalServiceProtocol,
    mock_db: Session,
) -> FeatureStore:
    """Create a FeatureStore instance with mocked dependencies."""
    return FeatureStore(
        registration_service=mock_registration_service,
        retrieval_service=mock_retrieval_service,
        db=mock_db,
    )


class TestRegisterProjects:
    """Tests for register_projects method."""

    def test_register_single_project(self, feature_store: FeatureStore, mock_registration_service: MagicMock):
        """Test registering a single project."""
        # Arrange
        project_input = ProjectInput(name="test_project", description="Test")
        mock_project = Project(
            id=1,
            name="test_project",
            description="Test",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        mock_registration_service.register_projects_bulk.return_value = [mock_project]

        # Act
        result = feature_store.register_projects([project_input])

        # Assert
        assert len(result) == 1
        assert result[0].id == 1
        assert result[0].name == "test_project"
        mock_registration_service.register_projects_bulk.assert_called_once()
        call_args = mock_registration_service.register_projects_bulk.call_args[0][0]
        assert call_args[0]["name"] == "test_project"
        assert call_args[0]["description"] == "Test"

    def test_register_multiple_projects(
        self, feature_store: FeatureStore, mock_registration_service: MagicMock
    ):
        """Test registering multiple projects in bulk."""
        # Arrange
        inputs = [
            ProjectInput(name="project1", description="Project 1"),
            ProjectInput(name="project2", description="Project 2"),
            ProjectInput(name="project3"),
        ]
        mock_projects = [
            Project(
                id=i + 1,
                name=f"project{i + 1}",
                description=inputs[i].description,
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            for i in range(3)
        ]
        mock_registration_service.register_projects_bulk.return_value = mock_projects

        # Act
        result = feature_store.register_projects(inputs)

        # Assert
        assert len(result) == 3
        assert result[0].name == "project1"
        assert result[1].name == "project2"
        assert result[2].name == "project3"
        mock_registration_service.register_projects_bulk.assert_called_once()


class TestRegisterEntities:
    """Tests for register_entities method."""

    def test_register_entity_with_project_id(
        self, feature_store: FeatureStore, mock_registration_service: MagicMock
    ):
        """Test registering an entity with project_id."""
        # Arrange
        entity_input = EntityInput(name="user", project_id=1, description="User entity")
        mock_entity = Entity(
            id=1,
            name="user",
            project_id=1,
            description="User entity",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        mock_registration_service.register_entities_bulk.return_value = [mock_entity]

        # Act
        result = feature_store.register_entities([entity_input])

        # Assert
        assert len(result) == 1
        assert result[0].name == "user"
        assert result[0].project_id == 1
        mock_registration_service.register_entities_bulk.assert_called_once()

    def test_register_entity_with_project_name(
        self,
        feature_store: FeatureStore,
        mock_registration_service: MagicMock,
        mock_db: MagicMock,
    ):
        """Test registering an entity with project_name (resolves to ID)."""
        # Arrange
        entity_input = EntityInput(name="user", project_name="test_project", description="User entity")
        mock_project = Project(id=1, name="test_project")
        mock_db.scalar.return_value = mock_project

        mock_entity = Entity(
            id=1,
            name="user",
            project_id=1,
            description="User entity",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        mock_registration_service.register_entities_bulk.return_value = [mock_entity]

        # Act
        result = feature_store.register_entities([entity_input])

        # Assert
        assert len(result) == 1
        assert result[0].name == "user"
        assert result[0].project_id == 1
        mock_db.scalar.assert_called_once()  # Called to resolve project name

    def test_register_entity_with_nonexistent_project_name(
        self, feature_store: FeatureStore, mock_db: MagicMock
    ):
        """Test that registering with nonexistent project name raises error."""
        # Arrange
        entity_input = EntityInput(name="user", project_name="nonexistent", description="User entity")
        mock_db.scalar.return_value = None

        # Act & Assert
        with pytest.raises(ValueError, match="Project 'nonexistent' not found"):
            feature_store.register_entities([entity_input])


class TestRegisterFeatures:
    """Tests for register_features method."""

    def test_register_feature_without_entities(
        self, feature_store: FeatureStore, mock_registration_service: MagicMock
    ):
        """Test registering a feature without entity associations."""
        # Arrange
        feature_input = FeatureInput(
            name="age",
            project_id=1,
            data_type=DataType.INTEGER,
            description="User age",
        )
        mock_feature = Attribute(
            id=1,
            name="age",
            project_id=1,
            data_type=DataType.INTEGER,
            type=AttributeType.FEATURE,
            is_label=False,
            description="User age",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        mock_registration_service.register_attributes_bulk.return_value = [mock_feature]

        # Act
        result = feature_store.register_features([feature_input])

        # Assert
        assert len(result) == 1
        assert result[0].name == "age"
        assert result[0].type == AttributeType.FEATURE
        mock_registration_service.register_attributes_bulk.assert_called_once()
        # Should not call association method
        mock_registration_service.associate_attributes_with_entities_bulk.assert_not_called()

    def test_register_feature_with_entity_associations(
        self, feature_store: FeatureStore, mock_registration_service: MagicMock
    ):
        """Test registering a feature with entity associations."""
        # Arrange
        feature_input = FeatureInput(
            name="age",
            project_id=1,
            data_type=DataType.INTEGER,
            entity_ids=[1, 2],
        )
        mock_feature = Attribute(
            id=1,
            name="age",
            project_id=1,
            data_type=DataType.INTEGER,
            type=AttributeType.FEATURE,
            is_label=False,
            description="",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        mock_registration_service.register_attributes_bulk.return_value = [mock_feature]
        mock_registration_service.associate_attributes_with_entities_bulk.return_value = []

        # Act
        result = feature_store.register_features([feature_input])

        # Assert
        assert len(result) == 1
        mock_registration_service.associate_attributes_with_entities_bulk.assert_called_once()
        # Verify associations were created correctly
        call_args = mock_registration_service.associate_attributes_with_entities_bulk.call_args[0][0]
        assert len(call_args) == 2
        assert call_args[0]["attribute_id"] == 1
        assert call_args[0]["entity_id"] == 1
        assert call_args[1]["entity_id"] == 2

    def test_register_multiple_features_with_mixed_associations(
        self, feature_store: FeatureStore, mock_registration_service: MagicMock
    ):
        """Test registering multiple features with mixed associations."""
        # Arrange
        inputs = [
            FeatureInput(
                name="age",
                project_id=1,
                data_type=DataType.INTEGER,
                entity_ids=[1],
            ),
            FeatureInput(
                name="country",
                project_id=1,
                data_type=DataType.STRING,
                entity_ids=[1, 2],
            ),
            FeatureInput(name="score", project_id=1, data_type=DataType.FLOAT),  # No associations
        ]
        mock_features = [
            Attribute(
                id=i + 1,
                name=inputs[i].name,
                project_id=1,
                data_type=inputs[i].data_type,
                type=AttributeType.FEATURE,
                is_label=False,
                description="",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            for i in range(3)
        ]
        mock_registration_service.register_attributes_bulk.return_value = mock_features
        mock_registration_service.associate_attributes_with_entities_bulk.return_value = []

        # Act
        result = feature_store.register_features(inputs)

        # Assert
        assert len(result) == 3
        # Should create associations for first two features
        call_args = mock_registration_service.associate_attributes_with_entities_bulk.call_args[0][0]
        assert len(call_args) == 3  # 1 + 2 + 0 associations


class TestRegisterTargets:
    """Tests for register_targets method."""

    def test_register_target(self, feature_store: FeatureStore, mock_registration_service: MagicMock):
        """Test registering a target."""
        # Arrange
        target_input = TargetInput(
            name="conversion",
            project_id=1,
            data_type=DataType.BOOLEAN,
            description="User converted",
        )
        mock_target = Attribute(
            id=1,
            name="conversion",
            project_id=1,
            data_type=DataType.BOOLEAN,
            type=AttributeType.TARGET,
            is_label=True,
            description="User converted",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        mock_registration_service.register_attributes_bulk.return_value = [mock_target]

        # Act
        result = feature_store.register_targets([target_input])

        # Assert
        assert len(result) == 1
        assert result[0].name == "conversion"
        assert result[0].type == AttributeType.TARGET
        assert result[0].is_label is True
        # Verify that type and is_label were set correctly
        call_args = mock_registration_service.register_attributes_bulk.call_args[0][0]
        assert call_args[0]["type"] == AttributeType.TARGET
        assert call_args[0]["is_label"] is True


class TestRegisterJoinKeys:
    """Tests for register_join_keys method."""

    def test_register_join_key_with_entity_id(
        self, feature_store: FeatureStore, mock_registration_service: MagicMock
    ):
        """Test registering a join key with entity_id."""
        # Arrange
        join_key_input = JoinKeyInput(name="user_id", entity_id=1)
        mock_join_key = JoinKey(
            id=1,
            name="user_id",
            entity_id=1,
            description="",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        mock_registration_service.register_join_keys_bulk.return_value = [mock_join_key]

        # Act
        result = feature_store.register_join_keys([join_key_input])

        # Assert
        assert len(result) == 1
        assert result[0].name == "user_id"
        assert result[0].entity_id == 1

    def test_register_join_key_with_entity_name(
        self,
        feature_store: FeatureStore,
        mock_registration_service: MagicMock,
        mock_db: MagicMock,
    ):
        """Test registering a join key with entity_name."""
        # Arrange
        join_key_input = JoinKeyInput(name="user_id", entity_name="user")
        mock_entity = Entity(id=1, name="user")
        mock_db.scalar.return_value = mock_entity

        mock_join_key = JoinKey(
            id=1,
            name="user_id",
            entity_id=1,
            description="",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        mock_registration_service.register_join_keys_bulk.return_value = [mock_join_key]

        # Act
        result = feature_store.register_join_keys([join_key_input])

        # Assert
        assert len(result) == 1
        mock_db.scalar.assert_called_once()  # Called to resolve entity name


class TestRegisterJoinKeyValues:
    """Tests for register_join_key_values method."""

    def test_register_join_key_value_with_id(
        self, feature_store: FeatureStore, mock_registration_service: MagicMock
    ):
        """Test registering a join key value with join_key_id."""
        # Arrange
        jkv_input = JoinKeyValueInput(join_key_id=1, value=123)
        mock_jkv = JoinKeyValue(
            id=1,
            join_key_id=1,
            value={"integer": 123},
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        mock_registration_service.register_join_key_values_bulk.return_value = [mock_jkv]

        # Act
        result = feature_store.register_join_key_values([jkv_input])

        # Assert
        assert len(result) == 1
        assert result[0].join_key_id == 1

    def test_register_join_key_value_with_name(
        self,
        feature_store: FeatureStore,
        mock_registration_service: MagicMock,
        mock_db: MagicMock,
    ):
        """Test registering a join key value with join_key_name."""
        # Arrange
        jkv_input = JoinKeyValueInput(join_key_name="user_id", value=123)
        mock_join_key = JoinKey(id=1, name="user_id")
        mock_db.scalar.return_value = mock_join_key

        mock_jkv = JoinKeyValue(
            id=1,
            join_key_id=1,
            value={"integer": 123},
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        mock_registration_service.register_join_key_values_bulk.return_value = [mock_jkv]

        # Act
        result = feature_store.register_join_key_values([jkv_input])

        # Assert
        assert len(result) == 1
        mock_db.scalar.assert_called_once()


class TestRegisterFeatureValues:
    """Tests for register_feature_values method."""

    def test_register_feature_value_with_attribute_id(
        self, feature_store: FeatureStore, mock_registration_service: MagicMock
    ):
        """Test registering a feature value with attribute_id."""
        # Arrange
        fv_input = FeatureValueInput(attribute_id=1, join_key_value_id=1, value=25)
        mock_fv = AttributeValue(
            id=1,
            attribute_id=1,
            join_key_value_id=1,
            value={"integer": 25},
            timestamp=datetime.now(),
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        mock_registration_service.register_attribute_values_bulk.return_value = [mock_fv]

        # Act
        result = feature_store.register_feature_values([fv_input])

        # Assert
        assert len(result) == 1
        assert result[0].attribute_id == 1
        assert result[0].join_key_value_id == 1

    def test_register_feature_value_with_attribute_name(
        self,
        feature_store: FeatureStore,
        mock_registration_service: MagicMock,
        mock_db: MagicMock,
    ):
        """Test registering a feature value with attribute_name."""
        # Arrange
        fv_input = FeatureValueInput(attribute_name="age", join_key_value_id=1, value=25)
        mock_attribute = Attribute(id=1, name="age")
        mock_db.scalar.return_value = mock_attribute

        mock_fv = AttributeValue(
            id=1,
            attribute_id=1,
            join_key_value_id=1,
            value={"integer": 25},
            timestamp=datetime.now(),
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        mock_registration_service.register_attribute_values_bulk.return_value = [mock_fv]

        # Act
        result = feature_store.register_feature_values([fv_input])

        # Assert
        assert len(result) == 1
        mock_db.scalar.assert_called_once()


class TestAssociateFeatures:
    """Tests for associate_features_with_entities method."""

    def test_associate_features_with_entities(
        self, feature_store: FeatureStore, mock_registration_service: MagicMock
    ):
        """Test associating features with entities."""
        # Arrange
        inputs = [
            AssociationInput(attribute_id=1, entity_id=1),
            AssociationInput(attribute_id=2, entity_id=1),
        ]
        mock_associations = [
            AttributeEntities(
                attribute_id=1,
                entity_id=1,
                created_at=datetime.now(),
                updated_at=datetime.now(),
            ),
            AttributeEntities(
                attribute_id=2,
                entity_id=1,
                created_at=datetime.now(),
                updated_at=datetime.now(),
            ),
        ]
        mock_registration_service.associate_attributes_with_entities_bulk.return_value = mock_associations

        # Act
        result = feature_store.associate_features_with_entities(inputs)

        # Assert
        assert len(result) == 2
        assert result[0].attribute_id == 1
        assert result[1].attribute_id == 2
        mock_registration_service.associate_attributes_with_entities_bulk.assert_called_once()
