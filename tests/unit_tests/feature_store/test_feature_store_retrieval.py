"""
Unit tests for FeatureStore retrieval methods.

These tests use mocks to verify that the FeatureStore correctly delegates
to the retrieval service and properly handles input/output conversions.
"""

from datetime import datetime
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

from featurium.feature_store import FeatureRetrievalInput, FeatureStore, RegistrationServiceProtocol
from featurium.feature_store.protocols import RetrievalServiceProtocol


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    return Mock()


@pytest.fixture
def mock_registration_service() -> RegistrationServiceProtocol:
    """Create a mock registration service."""
    return MagicMock(spec=RegistrationServiceProtocol)


@pytest.fixture
def mock_retrieval_service() -> RetrievalServiceProtocol:
    """Create a mock retrieval service."""
    return MagicMock(spec=RetrievalServiceProtocol)


@pytest.fixture
def feature_store(mock_registration_service, mock_retrieval_service, mock_db) -> FeatureStore:
    """Create a FeatureStore instance with mocked dependencies."""
    return FeatureStore(
        registration_service=mock_registration_service,
        retrieval_service=mock_retrieval_service,
        db=mock_db,
    )


class TestGetFeatureValues:
    """Tests for get_feature_values method."""

    def test_get_feature_values_basic(
        self,
        feature_store: FeatureStore,
        mock_retrieval_service: MagicMock,
    ):
        """Test getting feature values with basic parameters."""
        # Arrange
        retrieval_input = FeatureRetrievalInput(
            project_name="test_project",
            entity_name="user",
            join_keys=[1, 2, 3],
            feature_names=["age", "country"],
        )
        mock_df = pd.DataFrame(
            {
                "age": [25, 30, 35],
                "country": ["US", "UK", "DE"],
            },
            index=[1, 2, 3],
        )
        mock_retrieval_service.get_feature_values.return_value = mock_df

        # Act
        result = feature_store.get_feature_values(retrieval_input)

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert list(result.columns) == ["age", "country"]
        mock_retrieval_service.get_feature_values.assert_called_once_with(
            project_name="test_project",
            entity_name="user",
            join_keys=[1, 2, 3],
            feature_names=["age", "country"],
            start_time=None,
            end_time=None,
            include_timestamp=False,
        )

    def test_get_feature_values_with_time_range(
        self, feature_store: FeatureStore, mock_retrieval_service: MagicMock
    ):
        """Test getting feature values with time range filtering."""
        # Arrange
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 12, 31)
        retrieval_input = FeatureRetrievalInput(
            project_name="test_project",
            entity_name="user",
            start_time=start_time,
            end_time=end_time,
        )
        mock_df = pd.DataFrame({"age": [25, 30]})
        mock_retrieval_service.get_feature_values.return_value = mock_df

        # Act
        _ = feature_store.get_feature_values(retrieval_input)

        # Assert
        mock_retrieval_service.get_feature_values.assert_called_once()
        call_kwargs = mock_retrieval_service.get_feature_values.call_args[1]
        assert call_kwargs["start_time"] == start_time
        assert call_kwargs["end_time"] == end_time

    def test_get_feature_values_with_timestamp(
        self,
        feature_store: FeatureStore,
        mock_retrieval_service: MagicMock,
    ):
        """Test getting feature values with timestamps included."""
        # Arrange
        retrieval_input = FeatureRetrievalInput(
            project_name="test_project",
            entity_name="user",
            include_timestamp=True,
        )
        mock_df = pd.DataFrame(
            {
                "value": {"age": [25, 30]},
                "timestamp": {"age": [datetime.now(), datetime.now()]},
            }
        )
        mock_retrieval_service.get_feature_values.return_value = mock_df

        # Act
        _ = feature_store.get_feature_values(retrieval_input)

        # Assert
        mock_retrieval_service.get_feature_values.assert_called_once()
        call_kwargs = mock_retrieval_service.get_feature_values.call_args[1]
        assert call_kwargs["include_timestamp"] is True

    def test_get_feature_values_no_filters(
        self, feature_store: FeatureStore, mock_retrieval_service: MagicMock
    ):
        """Test getting all feature values without filters."""
        # Arrange
        retrieval_input = FeatureRetrievalInput(project_name="test_project", entity_name="user")
        mock_df = pd.DataFrame({"age": [25], "country": ["US"], "score": [0.8]})
        mock_retrieval_service.get_feature_values.return_value = mock_df

        # Act
        result = feature_store.get_feature_values(retrieval_input)

        # Assert
        assert len(result.columns) == 3
        mock_retrieval_service.get_feature_values.assert_called_once_with(
            project_name="test_project",
            entity_name="user",
            join_keys=None,
            feature_names=None,
            start_time=None,
            end_time=None,
            include_timestamp=False,
        )


class TestGetTargetValues:
    """Tests for get_target_values method."""

    def test_get_target_values_basic(self, feature_store: FeatureStore, mock_retrieval_service: MagicMock):
        """Test getting target values."""
        # Arrange
        retrieval_input = FeatureRetrievalInput(
            project_name="test_project",
            entity_name="user",
            join_keys=[1, 2],
            feature_names=["conversion", "churn"],
        )
        mock_df = pd.DataFrame({"conversion": [True, False], "churn": [False, True]}, index=[1, 2])
        mock_retrieval_service.get_target_values.return_value = mock_df

        # Act
        result = feature_store.get_target_values(retrieval_input)

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        mock_retrieval_service.get_target_values.assert_called_once_with(
            project_name="test_project",
            entity_name="user",
            join_keys=[1, 2],
            feature_names=["conversion", "churn"],
            start_time=None,
            end_time=None,
            include_timestamp=False,
        )

    def test_get_target_values_with_filters(
        self, feature_store: FeatureStore, mock_retrieval_service: MagicMock
    ):
        """Test getting target values with various filters."""
        # Arrange
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 6, 30)
        retrieval_input = FeatureRetrievalInput(
            project_name="test_project",
            entity_name="user",
            join_keys=[1, 2, 3],
            feature_names=["conversion"],
            start_time=start_time,
            end_time=end_time,
            include_timestamp=True,
        )
        mock_df = pd.DataFrame({"conversion": [True, False, True]})
        mock_retrieval_service.get_target_values.return_value = mock_df

        # Act
        _ = feature_store.get_target_values(retrieval_input)

        # Assert
        mock_retrieval_service.get_target_values.assert_called_once()
        call_kwargs = mock_retrieval_service.get_target_values.call_args[1]
        assert call_kwargs["start_time"] == start_time
        assert call_kwargs["end_time"] == end_time
        assert call_kwargs["include_timestamp"] is True


class TestGetAllValues:
    """Tests for get_all_values method."""

    def test_get_all_values_basic(self, feature_store: FeatureStore, mock_retrieval_service: MagicMock):
        """Test getting all values (features and targets)."""
        # Arrange
        retrieval_input = FeatureRetrievalInput(
            project_name="test_project",
            entity_name="user",
            join_keys=[1, 2],
        )
        mock_df = pd.DataFrame(
            {
                "age": [25, 30],
                "country": ["US", "UK"],
                "conversion": [True, False],
            },
            index=[1, 2],
        )
        mock_retrieval_service.get_values.return_value = mock_df

        # Act
        result = feature_store.get_all_values(retrieval_input)

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert len(result.columns) == 3
        mock_retrieval_service.get_values.assert_called_once_with(
            project_name="test_project",
            entity_name="user",
            join_keys=[1, 2],
            feature_names=None,
            start_time=None,
            end_time=None,
            include_timestamp=False,
        )

    def test_get_all_values_with_filters(
        self, feature_store: FeatureStore, mock_retrieval_service: MagicMock
    ):
        """Test getting all values with specific attribute names."""
        # Arrange
        retrieval_input = FeatureRetrievalInput(
            project_name="test_project",
            entity_name="user",
            feature_names=["age", "conversion"],  # Mix of feature and target
        )
        mock_df = pd.DataFrame({"age": [25, 30], "conversion": [True, False]})
        mock_retrieval_service.get_values.return_value = mock_df

        # Act
        result = feature_store.get_all_values(retrieval_input)

        # Assert
        assert len(result.columns) == 2
        mock_retrieval_service.get_values.assert_called_once()


class TestRetrievalErrorHandling:
    """Tests for error handling in retrieval methods."""

    def test_get_feature_values_propagates_errors(
        self, feature_store: FeatureStore, mock_retrieval_service: MagicMock
    ):
        """Test that errors from retrieval service are propagated."""
        # Arrange
        retrieval_input = FeatureRetrievalInput(project_name="nonexistent", entity_name="user")
        mock_retrieval_service.get_feature_values.side_effect = ValueError("Project 'nonexistent' not found")

        # Act & Assert
        with pytest.raises(ValueError, match="Project 'nonexistent' not found"):
            feature_store.get_feature_values(retrieval_input)

    def test_get_target_values_propagates_errors(self, feature_store, mock_retrieval_service):
        """Test that errors from retrieval service are propagated."""
        # Arrange
        retrieval_input = FeatureRetrievalInput(project_name="test_project", entity_name="nonexistent")
        mock_retrieval_service.get_target_values.side_effect = ValueError("Entity 'nonexistent' not found")

        # Act & Assert
        with pytest.raises(ValueError, match="Entity 'nonexistent' not found"):
            feature_store.get_target_values(retrieval_input)
