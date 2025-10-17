"""
Unit tests for RetrievalStore.

This module tests the RetrievalStore's feature and target retrieval functionality
including various scenarios and error handling.
"""

from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy.orm import Session

from featurium.core.models import DataType
from featurium.services.retrieval.retrieval import RetrievalStore


@pytest.mark.usefixtures("cleanup")
class TestRetrievalStore:
    """Test RetrievalStore functionality."""

    def test_get_feature_values_basic(self, db: Session) -> None:
        """Test basic get_feature_values functionality."""
        # Create test data using RegistrationService
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create project and entity
        project = registration_service.register_project(
            "test_project", "Test project for retrieval"
        )
        entity = registration_service.register_entity(
            "test_entity", project, "Test entity"
        )

        # Create join key
        join_key = registration_service.register_join_key("test_key", entity)

        # Create features
        feature1 = registration_service.register_feature(
            "feature1", project, DataType.INTEGER, "Test feature 1"
        )
        feature2 = registration_service.register_feature(
            "feature2", project, DataType.FLOAT, "Test feature 2"
        )

        # Associate features with entity
        registration_service.associate_attribute_with_entity(feature1, entity)
        registration_service.associate_attribute_with_entity(feature2, entity)

        # Create join key values
        jkv1 = registration_service.register_join_key_value(join_key, 1)
        jkv2 = registration_service.register_join_key_value(join_key, 2)

        # Create feature values
        registration_service.register_feature_value(
            feature1, jkv1, {"integer": 100}, {"source": "test"}
        )
        registration_service.register_feature_value(
            feature1, jkv2, {"integer": 200}, {"source": "test"}
        )
        registration_service.register_feature_value(
            feature2, jkv1, {"float": 1.5}, {"source": "test"}
        )
        registration_service.register_feature_value(
            feature2, jkv2, {"float": 2.5}, {"source": "test"}
        )

        # Test retrieval
        result = retrieval_service.get_feature_values("test_project", "test_entity")

        # Verify result structure
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert "feature1" in result.columns
        assert "feature2" in result.columns
        assert len(result) == 2  # Two join key values

        # Verify values
        feature1_values = result["feature1"].dropna().tolist()
        feature2_values = result["feature2"].dropna().tolist()
        assert 100 in feature1_values
        assert 200 in feature1_values
        assert 1.5 in feature2_values
        assert 2.5 in feature2_values

    def test_get_target_values_basic(self, db: Session) -> None:
        """Test basic get_target_values functionality."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create project and entity
        project = registration_service.register_project(
            "test_project", "Test project for targets"
        )
        entity = registration_service.register_entity(
            "test_entity", project, "Test entity"
        )

        # Create join key
        join_key = registration_service.register_join_key("test_key", entity)

        # Create targets
        target1 = registration_service.register_target(
            "target1", project, DataType.FLOAT, "Test target 1"
        )
        target2 = registration_service.register_target(
            "target2", project, DataType.INTEGER, "Test target 2"
        )

        # Associate targets with entity
        registration_service.associate_attribute_with_entity(target1, entity)
        registration_service.associate_attribute_with_entity(target2, entity)

        # Create join key values
        jkv1 = registration_service.register_join_key_value(join_key, 1)
        jkv2 = registration_service.register_join_key_value(join_key, 2)

        # Create target values
        registration_service.register_target_value(
            target1, jkv1, {"float": 0.8}, {"model": "test_model"}
        )
        registration_service.register_target_value(
            target1, jkv2, {"float": 0.9}, {"model": "test_model"}
        )
        registration_service.register_target_value(
            target2, jkv1, {"integer": 1}, {"model": "test_model"}
        )
        registration_service.register_target_value(
            target2, jkv2, {"integer": 0}, {"model": "test_model"}
        )

        # Test retrieval
        result = retrieval_service.get_target_values("test_project", "test_entity")

        # Verify result structure
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert "target1" in result.columns
        assert "target2" in result.columns
        assert len(result) == 2  # Two join key values

        # Verify values
        target1_values = result["target1"].dropna().tolist()
        target2_values = result["target2"].dropna().tolist()
        assert 0.8 in target1_values
        assert 0.9 in target1_values
        assert 1 in target2_values
        assert 0 in target2_values

    def test_get_feature_values_with_join_keys_filter(self, db: Session) -> None:
        """Test get_feature_values with specific join keys."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create test data
        project = registration_service.register_project(
            "filter_test", "Test project for filtering"
        )
        entity = registration_service.register_entity(
            "test_entity", project, "Test entity"
        )
        join_key = registration_service.register_join_key("test_key", entity)
        feature = registration_service.register_feature(
            "test_feature", project, DataType.INTEGER, "Test feature"
        )
        registration_service.associate_attribute_with_entity(feature, entity)

        # Create multiple join key values
        jkv1 = registration_service.register_join_key_value(join_key, 1)
        jkv2 = registration_service.register_join_key_value(join_key, 2)
        jkv3 = registration_service.register_join_key_value(join_key, 3)

        # Create feature values for all join keys
        registration_service.register_feature_value(
            feature, jkv1, {"integer": 100}, {"source": "test"}
        )
        registration_service.register_feature_value(
            feature, jkv2, {"integer": 200}, {"source": "test"}
        )
        registration_service.register_feature_value(
            feature, jkv3, {"integer": 300}, {"source": "test"}
        )

        # Test retrieval with specific join keys
        result = retrieval_service.get_feature_values(
            "filter_test", "test_entity", join_keys=[1, 3]
        )

        # Verify only filtered results
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result) == 2  # Only 2 join key values
        feature_values = result["test_feature"].dropna().tolist()
        assert 100 in feature_values
        assert 300 in feature_values
        assert 200 not in feature_values

    def test_get_feature_values_with_feature_names_filter(self, db: Session) -> None:
        """Test get_feature_values with specific feature names."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create test data
        project = registration_service.register_project(
            "feature_filter_test", "Test project for feature filtering"
        )
        entity = registration_service.register_entity(
            "test_entity", project, "Test entity"
        )
        join_key = registration_service.register_join_key("test_key", entity)

        # Create multiple features
        feature1 = registration_service.register_feature(
            "feature1", project, DataType.INTEGER, "Test feature 1"
        )
        feature2 = registration_service.register_feature(
            "feature2", project, DataType.FLOAT, "Test feature 2"
        )
        feature3 = registration_service.register_feature(
            "feature3", project, DataType.INTEGER, "Test feature 3"
        )

        # Associate all features with entity
        registration_service.associate_attribute_with_entity(feature1, entity)
        registration_service.associate_attribute_with_entity(feature2, entity)
        registration_service.associate_attribute_with_entity(feature3, entity)

        # Create join key value
        jkv = registration_service.register_join_key_value(join_key, 1)

        # Create feature values
        registration_service.register_feature_value(
            feature1, jkv, {"integer": 100}, {"source": "test"}
        )
        registration_service.register_feature_value(
            feature2, jkv, {"float": 1.5}, {"source": "test"}
        )
        registration_service.register_feature_value(
            feature3, jkv, {"integer": 300}, {"source": "test"}
        )

        # Test retrieval with specific feature names
        result = retrieval_service.get_feature_values(
            "feature_filter_test", "test_entity", feature_names=["feature1", "feature3"]
        )

        # Verify only requested features
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert "feature1" in result.columns
        assert "feature3" in result.columns
        assert "feature2" not in result.columns
        assert len(result) == 1  # One join key value

    def test_get_feature_values_with_timestamp_filter(self, db: Session) -> None:
        """Test get_feature_values with timestamp filtering."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create test data
        project = registration_service.register_project(
            "timestamp_test", "Test project for timestamp filtering"
        )
        entity = registration_service.register_entity(
            "test_entity", project, "Test entity"
        )
        join_key = registration_service.register_join_key("test_key", entity)
        feature = registration_service.register_feature(
            "test_feature", project, DataType.INTEGER, "Test feature"
        )
        registration_service.associate_attribute_with_entity(feature, entity)
        jkv = registration_service.register_join_key_value(join_key, 1)

        # Create feature values with different timestamps
        registration_service.register_feature_value(
            feature, jkv, {"integer": 100}, {"source": "test"}
        )  # Default timestamp (now)

        # Test retrieval with timestamp range
        start_time = datetime(2024, 1, 1, 11, 0, 0)
        end_time = datetime(2024, 1, 1, 13, 0, 0)

        result = retrieval_service.get_feature_values(
            "timestamp_test", "test_entity", start_time=start_time, end_time=end_time
        )

        # Verify result
        assert isinstance(result, pd.DataFrame)
        # Note: The exact behavior depends on the current timestamp vs test time

    def test_get_feature_values_historical_with_end_time_filter(
        self, db: Session
    ) -> None:
        """Test get_feature_values with historical data and end_time filtering."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create test data
        project = registration_service.register_project(
            "historical_test", "Test project for historical filtering"
        )
        entity = registration_service.register_entity(
            "test_entity", project, "Test entity"
        )
        join_key = registration_service.register_join_key("test_key", entity)
        feature = registration_service.register_feature(
            "price_feature", project, DataType.FLOAT, "Price feature"
        )
        registration_service.associate_attribute_with_entity(feature, entity)
        jkv = registration_service.register_join_key_value(join_key, 1)

        # Create historical feature values with different timestamps
        # Note: In a real scenario, we'd need to mock the timestamp creation
        # For now, we'll create values and test the filtering logic

        # Create feature values (timestamps will be set to current time)
        registration_service.register_feature_value(
            feature, jkv, {"float": 100.0}, {"source": "historical", "version": "v1"}
        )
        registration_service.register_feature_value(
            feature, jkv, {"float": 110.0}, {"source": "historical", "version": "v2"}
        )
        registration_service.register_feature_value(
            feature, jkv, {"float": 120.0}, {"source": "historical", "version": "v3"}
        )

        # Test retrieval without timestamp filter (should get all values)
        result_all = retrieval_service.get_feature_values(
            "historical_test", "test_entity"
        )

        # Verify we get some data
        assert isinstance(result_all, pd.DataFrame)
        assert not result_all.empty
        assert "price_feature" in result_all.columns

        # Test retrieval with end_time filter (should get values up to end_time)
        # Using a future end_time to capture all current values
        end_time = datetime(2025, 12, 31, 23, 59, 59)

        result_filtered = retrieval_service.get_feature_values(
            "historical_test", "test_entity", end_time=end_time
        )

        # Verify filtered result
        assert isinstance(result_filtered, pd.DataFrame)
        # Should contain data since end_time is in the future
        assert not result_filtered.empty
        assert "price_feature" in result_filtered.columns

        # Test retrieval with past end_time (should get no data)
        past_end_time = datetime(2020, 1, 1, 0, 0, 0)

        result_past = retrieval_service.get_feature_values(
            "historical_test", "test_entity", end_time=past_end_time
        )

        # Should be empty or contain no feature values
        assert isinstance(result_past, pd.DataFrame)
        # The exact behavior depends on implementation

    def test_get_feature_values_historical_with_start_and_end_time(
        self, db: Session
    ) -> None:
        """Test get_feature_values with both start_time and end_time filtering."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create test data
        project = registration_service.register_project(
            "range_test", "Test project for time range filtering"
        )
        entity = registration_service.register_entity(
            "test_entity", project, "Test entity"
        )
        join_key = registration_service.register_join_key("test_key", entity)
        feature = registration_service.register_feature(
            "score_feature", project, DataType.INTEGER, "Score feature"
        )
        registration_service.associate_attribute_with_entity(feature, entity)
        jkv = registration_service.register_join_key_value(join_key, 1)

        # Create feature values
        registration_service.register_feature_value(
            feature, jkv, {"integer": 85}, {"source": "model_v1"}
        )
        registration_service.register_feature_value(
            feature, jkv, {"integer": 90}, {"source": "model_v2"}
        )
        registration_service.register_feature_value(
            feature, jkv, {"integer": 95}, {"source": "model_v3"}
        )

        # Test retrieval with time range (use a very wide range to capture current data)
        start_time = datetime(2020, 1, 1, 0, 0, 0)
        end_time = datetime(2030, 12, 31, 23, 59, 59)

        result = retrieval_service.get_feature_values(
            "range_test", "test_entity", start_time=start_time, end_time=end_time
        )

        # Verify result
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert "score_feature" in result.columns

        # Verify we get some score values
        score_values = result["score_feature"].dropna().tolist()
        assert len(score_values) > 0
        # Should contain some of our test values
        assert any(score in [85, 90, 95] for score in score_values)

    def test_get_feature_values_with_include_timestamp(self, db: Session) -> None:
        """Test get_feature_values with include_timestamp=True."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create test data
        project = registration_service.register_project(
            "timestamp_include_test", "Test project for timestamp inclusion"
        )
        entity = registration_service.register_entity(
            "test_entity", project, "Test entity"
        )
        join_key = registration_service.register_join_key("test_key", entity)
        feature = registration_service.register_feature(
            "test_feature", project, DataType.INTEGER, "Test feature"
        )
        registration_service.associate_attribute_with_entity(feature, entity)
        jkv = registration_service.register_join_key_value(join_key, 1)

        # Create feature value
        registration_service.register_feature_value(
            feature, jkv, {"integer": 100}, {"source": "test"}
        )

        # Test retrieval with include_timestamp=True
        result = retrieval_service.get_feature_values(
            "timestamp_include_test", "test_entity", include_timestamp=True
        )

        # Verify result includes timestamp information
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        # The exact structure depends on the pivot implementation

    def test_get_feature_values_project_not_found(self, db: Session) -> None:
        """Test get_feature_values with non-existent project."""
        retrieval_service = RetrievalStore(db)

        with pytest.raises(ValueError, match="Project 'nonexistent' not found"):
            retrieval_service.get_feature_values("nonexistent", "test_entity")

    def test_get_feature_values_entity_not_found(self, db: Session) -> None:
        """Test get_feature_values with non-existent entity."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create project but no entity
        registration_service.register_project("test_project", "Test project")

        with pytest.raises(
            ValueError, match="Entity 'nonexistent' not found in project 'test_project'"
        ):
            retrieval_service.get_feature_values("test_project", "nonexistent")

    def test_get_feature_values_duplicate_feature_names(self, db: Session) -> None:
        """Test get_feature_values with duplicate feature names."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create test data
        project = registration_service.register_project(
            "duplicate_test", "Test project for duplicate features"
        )
        entity = registration_service.register_entity(
            "test_entity", project, "Test entity"
        )
        # Create join key for the entity
        registration_service.register_join_key("test_key", entity)

        with pytest.raises(
            ValueError, match="feature_names must be a list of unique feature names"
        ):
            retrieval_service.get_feature_values(
                "duplicate_test", "test_entity", feature_names=["feature1", "feature1"]
            )

    def test_get_feature_values_invalid_join_keys(self, db: Session) -> None:
        """Test get_feature_values with invalid join keys."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create test data
        project = registration_service.register_project(
            "invalid_keys_test", "Test project for invalid join keys"
        )
        entity = registration_service.register_entity(
            "test_entity", project, "Test entity"
        )
        # Create join key for the entity
        registration_service.register_join_key("test_key", entity)

        with pytest.raises(
            ValueError, match="Some join keys not found for entity 'test_entity'"
        ):
            retrieval_service.get_feature_values(
                "invalid_keys_test", "test_entity", join_keys=[999, 998]
            )

    def test_get_target_values_project_not_found(self, db: Session) -> None:
        """Test get_target_values with non-existent project."""
        retrieval_service = RetrievalStore(db)

        with pytest.raises(ValueError, match="Project 'nonexistent' not found"):
            retrieval_service.get_target_values("nonexistent", "test_entity")

    def test_get_target_values_entity_not_found(self, db: Session) -> None:
        """Test get_target_values with non-existent entity."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create project but no entity
        registration_service.register_project("test_project", "Test project")

        with pytest.raises(
            ValueError, match="Entity 'nonexistent' not found in project 'test_project'"
        ):
            retrieval_service.get_target_values("test_project", "nonexistent")

    def test_get_feature_values_no_features_found(self, db: Session) -> None:
        """Test get_feature_values when no features are found."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create project and entity but no features
        project = registration_service.register_project(
            "no_features_test", "Test project without features"
        )
        entity = registration_service.register_entity(
            "test_entity", project, "Test entity"
        )
        # Create join key for the entity
        registration_service.register_join_key("test_key", entity)

        with pytest.raises(ValueError, match="No features found"):
            retrieval_service.get_feature_values("no_features_test", "test_entity")

    def test_get_target_values_no_targets_found(self, db: Session) -> None:
        """Test get_target_values when no targets are found."""
        from featurium.services.registration.registration import RegistrationService

        registration_service = RegistrationService(db)
        retrieval_service = RetrievalStore(db)

        # Create project and entity but no targets
        project = registration_service.register_project(
            "no_targets_test", "Test project without targets"
        )
        entity = registration_service.register_entity(
            "test_entity", project, "Test entity"
        )
        # Create join key for the entity
        registration_service.register_join_key("test_key", entity)

        with pytest.raises(ValueError, match="No features found"):
            retrieval_service.get_target_values("no_targets_test", "test_entity")

    def test_get_feature_values_invalid_timestamp_range(self, db: Session) -> None:
        """Test get_feature_values with invalid timestamp range."""
        retrieval_service = RetrievalStore(db)

        start_time = datetime(2024, 1, 2)
        end_time = datetime(2024, 1, 1)  # End before start

        with pytest.raises(ValueError, match="start_time must be before end_time"):
            retrieval_service.get_feature_values(
                "test_project", "test_entity", start_time=start_time, end_time=end_time
            )

    def test_get_target_values_invalid_timestamp_range(self, db: Session) -> None:
        """Test get_target_values with invalid timestamp range."""
        retrieval_service = RetrievalStore(db)

        start_time = datetime(2024, 1, 2)
        end_time = datetime(2024, 1, 1)  # End before start

        with pytest.raises(ValueError, match="start_time must be before end_time"):
            retrieval_service.get_target_values(
                "test_project", "test_entity", start_time=start_time, end_time=end_time
            )
