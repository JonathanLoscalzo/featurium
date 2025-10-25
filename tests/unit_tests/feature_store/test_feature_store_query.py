"""
Unit tests for FeatureStore query and listing methods.

These tests use mocks to verify that the FeatureStore correctly queries
and lists projects, entities, features, and targets.
"""

from unittest.mock import Mock

import pytest

from featurium.core.models import Attribute, Entity, Project
from featurium.feature_store import FeatureStore


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = Mock()
    # Setup default query behavior
    db.query.return_value.distinct.return_value.all.return_value = []
    return db


@pytest.fixture
def mock_registration_service():
    """Create a mock registration service."""
    return Mock()


@pytest.fixture
def mock_retrieval_service():
    """Create a mock retrieval service."""
    return Mock()


@pytest.fixture
def feature_store(mock_registration_service, mock_retrieval_service, mock_db):
    """Create a FeatureStore instance with mocked dependencies."""
    return FeatureStore(
        registration_service=mock_registration_service,
        retrieval_service=mock_retrieval_service,
        db=mock_db,
    )


class TestListProjects:
    """Tests for list_projects method."""

    def test_list_projects_empty(self, feature_store, mock_db):
        """Test listing projects when none exist."""
        # Arrange
        mock_db.query.return_value.distinct.return_value.all.return_value = []

        # Act
        result = feature_store.list_projects()

        # Assert
        assert result == []
        mock_db.query.assert_called_once()

    def test_list_projects_multiple(self, feature_store, mock_db):
        """Test listing multiple projects."""
        # Arrange
        mock_db.query.return_value.distinct.return_value.all.return_value = [
            ("project1",),
            ("project2",),
            ("project3",),
        ]

        # Act
        result = feature_store.list_projects()

        # Assert
        assert result == ["project1", "project2", "project3"]

    def test_list_projects_single(self, feature_store, mock_db):
        """Test listing a single project."""
        # Arrange
        mock_db.query.return_value.distinct.return_value.all.return_value = [("my_project",)]

        # Act
        result = feature_store.list_projects()

        # Assert
        assert result == ["my_project"]


class TestListEntities:
    """Tests for list_entities method."""

    def test_list_entities_all(self, feature_store, mock_db):
        """Test listing all entities without project filter."""
        # Arrange
        mock_query = Mock()
        mock_db.query.return_value = mock_query
        mock_query.distinct.return_value.all.return_value = [
            ("user",),
            ("transaction",),
            ("session",),
        ]

        # Act
        result = feature_store.list_entities()

        # Assert
        assert result == ["user", "transaction", "session"]
        mock_db.query.assert_called_once()
        # Should not join with Project when no filter
        mock_query.join.assert_not_called()

    def test_list_entities_by_project(self, feature_store, mock_db):
        """Test listing entities filtered by project."""
        # Arrange
        mock_query = Mock()
        mock_db.query.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.distinct.return_value.all.return_value = [
            ("user",),
            ("transaction",),
        ]

        # Act
        result = feature_store.list_entities(project_name="test_project")

        # Assert
        assert result == ["user", "transaction"]
        mock_query.join.assert_called_once()
        mock_query.filter.assert_called_once()

    def test_list_entities_empty(self, feature_store, mock_db):
        """Test listing entities when none exist."""
        # Arrange
        mock_query = Mock()
        mock_db.query.return_value = mock_query
        mock_query.distinct.return_value.all.return_value = []

        # Act
        result = feature_store.list_entities()

        # Assert
        assert result == []


class TestListFeatures:
    """Tests for list_features method."""

    def test_list_features_for_project(self, feature_store, mock_db):
        """Test listing features for a project."""
        # Arrange
        mock_project = Mock(spec=Project)
        mock_project.name = "test_project"
        mock_feature1 = Mock(spec=Attribute)
        mock_feature1.name = "age"
        mock_feature2 = Mock(spec=Attribute)
        mock_feature2.name = "country"
        mock_project.features = [mock_feature1, mock_feature2]
        mock_db.scalar.return_value = mock_project

        # Act
        result = feature_store.list_features(project_name="test_project")

        # Assert
        assert result == ["age", "country"]
        mock_db.scalar.assert_called_once()

    def test_list_features_for_entity(self, feature_store, mock_db):
        """Test listing features for a specific entity."""
        # Arrange
        mock_project = Mock(spec=Project)
        mock_project.name = "test_project"
        mock_project.id = 1

        mock_entity = Mock(spec=Entity)
        mock_entity.name = "user"
        mock_feature1 = Mock(spec=Attribute)
        mock_feature1.name = "age"
        mock_feature2 = Mock(spec=Attribute)
        mock_feature2.name = "email"
        mock_entity.features = [mock_feature1, mock_feature2]

        # First call returns project, second returns entity
        mock_db.scalar.side_effect = [mock_project, mock_entity]

        # Act
        result = feature_store.list_features(project_name="test_project", entity_name="user")

        # Assert
        assert result == ["age", "email"]
        assert mock_db.scalar.call_count == 2

    def test_list_features_empty(self, feature_store, mock_db):
        """Test listing features when none exist."""
        # Arrange
        mock_project = Mock(spec=Project)
        mock_project.features = []
        mock_db.scalar.return_value = mock_project

        # Act
        result = feature_store.list_features(project_name="test_project")

        # Assert
        assert result == []

    def test_list_features_nonexistent_project(self, feature_store, mock_db):
        """Test listing features for nonexistent project."""
        # Arrange
        mock_db.scalar.return_value = None

        # Act & Assert
        with pytest.raises(ValueError, match="Project 'nonexistent' not found"):
            feature_store.list_features(project_name="nonexistent")

    def test_list_features_nonexistent_entity(self, feature_store, mock_db):
        """Test listing features for nonexistent entity."""
        # Arrange
        mock_project = Mock(spec=Project)
        mock_project.id = 1
        # First call returns project, second returns None (entity not found)
        mock_db.scalar.side_effect = [mock_project, None]

        # Act & Assert
        with pytest.raises(ValueError, match="Entity 'nonexistent' not found"):
            feature_store.list_features(project_name="test_project", entity_name="nonexistent")


class TestListTargets:
    """Tests for list_targets method."""

    def test_list_targets_for_project(self, feature_store, mock_db):
        """Test listing targets for a project."""
        # Arrange
        mock_project = Mock(spec=Project)
        mock_target1 = Mock(spec=Attribute)
        mock_target1.name = "conversion"
        mock_target2 = Mock(spec=Attribute)
        mock_target2.name = "churn"
        mock_project.targets = [mock_target1, mock_target2]
        mock_db.scalar.return_value = mock_project

        # Act
        result = feature_store.list_targets(project_name="test_project")

        # Assert
        assert result == ["conversion", "churn"]

    def test_list_targets_for_entity(self, feature_store, mock_db):
        """Test listing targets for a specific entity."""
        # Arrange
        mock_project = Mock(spec=Project)
        mock_project.id = 1

        mock_entity = Mock(spec=Entity)
        mock_entity.name = "user"
        mock_target = Mock(spec=Attribute)
        mock_target.name = "conversion"
        mock_entity.targets = [mock_target]

        mock_db.scalar.side_effect = [mock_project, mock_entity]

        # Act
        result = feature_store.list_targets(project_name="test_project", entity_name="user")

        # Assert
        assert result == ["conversion"]

    def test_list_targets_empty(self, feature_store, mock_db):
        """Test listing targets when none exist."""
        # Arrange
        mock_project = Mock(spec=Project)
        mock_project.targets = []
        mock_db.scalar.return_value = mock_project

        # Act
        result = feature_store.list_targets(project_name="test_project")

        # Assert
        assert result == []

    def test_list_targets_nonexistent_project(self, feature_store, mock_db):
        """Test listing targets for nonexistent project."""
        # Arrange
        mock_db.scalar.return_value = None

        # Act & Assert
        with pytest.raises(ValueError, match="Project 'nonexistent' not found"):
            feature_store.list_targets(project_name="nonexistent")


class TestPrivateHelperMethods:
    """Tests for private helper methods."""

    def test_get_project_by_name_success(self, feature_store, mock_db):
        """Test getting project by name successfully."""
        # Arrange
        mock_project = Mock(spec=Project)
        mock_project.name = "test_project"
        mock_db.scalar.return_value = mock_project

        # Act
        result = feature_store._get_project_by_name("test_project")

        # Assert
        assert result == mock_project

    def test_get_project_by_name_not_found(self, feature_store, mock_db):
        """Test getting nonexistent project raises error."""
        # Arrange
        mock_db.scalar.return_value = None

        # Act & Assert
        with pytest.raises(ValueError, match="Project 'nonexistent' not found"):
            feature_store._get_project_by_name("nonexistent")

    def test_get_entity_by_name_success(self, feature_store, mock_db):
        """Test getting entity by name successfully."""
        # Arrange
        mock_entity = Mock(spec=Entity)
        mock_entity.name = "user"
        mock_db.scalar.return_value = mock_entity

        # Act
        result = feature_store._get_entity_by_name("user")

        # Assert
        assert result == mock_entity

    def test_get_entity_by_name_not_found(self, feature_store, mock_db):
        """Test getting nonexistent entity raises error."""
        # Arrange
        mock_db.scalar.return_value = None

        # Act & Assert
        with pytest.raises(ValueError, match="Entity 'nonexistent' not found"):
            feature_store._get_entity_by_name("nonexistent")

    def test_get_entity_by_name_with_project_filter(self, feature_store, mock_db):
        """Test getting entity by name with project filter."""
        # Arrange
        mock_entity = Mock(spec=Entity)
        mock_entity.name = "user"
        mock_entity.project_id = 1
        mock_db.scalar.return_value = mock_entity

        # Act
        result = feature_store._get_entity_by_name("user", project_id=1)

        # Assert
        assert result == mock_entity
        # Verify that project_id filter was applied
        mock_db.scalar.assert_called_once()

    def test_get_attribute_by_name_success(self, feature_store, mock_db):
        """Test getting attribute by name successfully."""
        # Arrange
        mock_attribute = Mock(spec=Attribute)
        mock_attribute.name = "age"
        mock_db.scalar.return_value = mock_attribute

        # Act
        result = feature_store._get_attribute_by_name("age")

        # Assert
        assert result == mock_attribute

    def test_get_attribute_by_name_not_found(self, feature_store, mock_db):
        """Test getting nonexistent attribute raises error."""
        # Arrange
        mock_db.scalar.return_value = None

        # Act & Assert
        with pytest.raises(ValueError, match="Attribute 'nonexistent' not found"):
            feature_store._get_attribute_by_name("nonexistent")


class TestMaterialize:
    """Tests for materialize method."""

    def test_materialize_does_nothing(self, feature_store):
        """Test that materialize method exists but does nothing (pass)."""
        # Act - should not raise any errors
        feature_store.materialize()

        # No assertions needed - just verify it doesn't crash
