"""
Unit tests for FeatureStore Pydantic schemas.

These tests verify that the Pydantic models correctly validate inputs
and handle edge cases.
"""

from datetime import datetime

import pytest
from pydantic import ValidationError

from featurium.core.models import DataType
from featurium.feature_store.schemas import (
    AssociationInput,
    EntityInput,
    FeatureInput,
    FeatureRetrievalInput,
    FeatureValueInput,
    JoinKeyInput,
    JoinKeyValueInput,
    ProjectInput,
    TargetInput,
)


class TestProjectInput:
    """Tests for ProjectInput schema."""

    def test_valid_project_input(self):
        """Test creating a valid project input."""
        project = ProjectInput(name="test_project", description="Test project")
        assert project.name == "test_project"
        assert project.description == "Test project"
        assert project.meta is None

    def test_project_input_with_meta(self):
        """Test creating a project input with metadata."""
        project = ProjectInput(name="test_project", meta={"owner": "team1", "cost_center": "ml"})
        assert project.meta == {"owner": "team1", "cost_center": "ml"}

    def test_project_input_minimal(self):
        """Test creating a project input with minimal data."""
        project = ProjectInput(name="test_project")
        assert project.name == "test_project"
        assert project.description == ""
        assert project.meta is None


class TestEntityInput:
    """Tests for EntityInput schema."""

    def test_entity_input_with_project_id(self):
        """Test creating entity input with project_id."""
        entity = EntityInput(name="user", project_id=1)
        assert entity.name == "user"
        assert entity.project_id == 1
        assert entity.project_name is None

    def test_entity_input_with_project_name(self):
        """Test creating entity input with project_name."""
        entity = EntityInput(name="user", project_name="test_project")
        assert entity.name == "user"
        assert entity.project_name == "test_project"
        assert entity.project_id is None

    def test_entity_input_with_both_project_references(self):
        """Test entity input with both project_id and project_name."""
        # Should be valid - validation happens at the validator level
        entity = EntityInput(name="user", project_id=1, project_name="test_project")
        assert entity.project_id == 1
        assert entity.project_name == "test_project"

    def test_entity_input_without_project_reference(self):
        """Test entity input without any project reference."""
        # Should be valid at schema level (validation happens in FS)
        entity = EntityInput(name="user")
        assert entity.name == "user"
        assert entity.project_id is None
        assert entity.project_name is None


class TestFeatureInput:
    """Tests for FeatureInput schema."""

    def test_feature_input_valid(self):
        """Test creating a valid feature input."""
        feature = FeatureInput(
            name="age",
            project_id=1,
            data_type=DataType.INTEGER,
            description="User age",
        )
        assert feature.name == "age"
        assert feature.project_id == 1
        assert feature.data_type == DataType.INTEGER
        assert feature.description == "User age"

    def test_feature_input_with_entities(self):
        """Test feature input with entity associations."""
        feature = FeatureInput(
            name="age",
            project_id=1,
            data_type=DataType.INTEGER,
            entity_ids=[1, 2, 3],
        )
        assert feature.entity_ids == [1, 2, 3]

    def test_feature_input_with_project_name(self):
        """Test feature input with project_name instead of ID."""
        feature = FeatureInput(name="age", project_name="test_project", data_type=DataType.INTEGER)
        assert feature.project_name == "test_project"
        assert feature.project_id is None

    def test_feature_input_all_data_types(self):
        """Test feature input with different data types."""
        for data_type in DataType:
            feature = FeatureInput(name="test_feature", project_id=1, data_type=data_type)
            assert feature.data_type == data_type


class TestTargetInput:
    """Tests for TargetInput schema."""

    def test_target_input_valid(self):
        """Test creating a valid target input."""
        target = TargetInput(
            name="conversion",
            project_id=1,
            data_type=DataType.BOOLEAN,
            description="User conversion",
        )
        assert target.name == "conversion"
        assert target.data_type == DataType.BOOLEAN

    def test_target_input_with_entities(self):
        """Test target input with entity associations."""
        target = TargetInput(
            name="conversion",
            project_id=1,
            data_type=DataType.BOOLEAN,
            entity_ids=[1],
        )
        assert target.entity_ids == [1]


class TestJoinKeyInput:
    """Tests for JoinKeyInput schema."""

    def test_join_key_input_with_entity_id(self):
        """Test creating join key input with entity_id."""
        join_key = JoinKeyInput(name="user_id", entity_id=1)
        assert join_key.name == "user_id"
        assert join_key.entity_id == 1
        assert join_key.entity_name is None

    def test_join_key_input_with_entity_name(self):
        """Test creating join key input with entity_name."""
        join_key = JoinKeyInput(name="user_id", entity_name="user")
        assert join_key.name == "user_id"
        assert join_key.entity_name == "user"
        assert join_key.entity_id is None

    def test_join_key_input_with_description(self):
        """Test join key input with description."""
        join_key = JoinKeyInput(name="user_id", entity_id=1, description="User identifier")
        assert join_key.description == "User identifier"


class TestJoinKeyValueInput:
    """Tests for JoinKeyValueInput schema."""

    def test_join_key_value_input_with_id(self):
        """Test creating join key value input with join_key_id."""
        jkv = JoinKeyValueInput(join_key_id=1, value=123)
        assert jkv.join_key_id == 1
        assert jkv.value == 123

    def test_join_key_value_input_with_name(self):
        """Test creating join key value input with join_key_name."""
        jkv = JoinKeyValueInput(join_key_name="user_id", value=123)
        assert jkv.join_key_name == "user_id"
        assert jkv.value == 123

    def test_join_key_value_input_various_value_types(self):
        """Test join key value input with different value types."""
        # Integer
        jkv1 = JoinKeyValueInput(join_key_id=1, value=123)
        assert jkv1.value == 123

        # String
        jkv2 = JoinKeyValueInput(join_key_id=1, value="abc")
        assert jkv2.value == "abc"

        # Float
        jkv3 = JoinKeyValueInput(join_key_id=1, value=123.45)
        assert jkv3.value == 123.45

        # Dict
        jkv4 = JoinKeyValueInput(join_key_id=1, value={"key": "value"})
        assert jkv4.value == {"key": "value"}


class TestFeatureValueInput:
    """Tests for FeatureValueInput schema."""

    def test_feature_value_input_with_attribute_id(self):
        """Test creating feature value input with attribute_id."""
        fv = FeatureValueInput(attribute_id=1, join_key_value_id=1, value=25)
        assert fv.attribute_id == 1
        assert fv.join_key_value_id == 1
        assert fv.value == 25

    def test_feature_value_input_with_attribute_name(self):
        """Test creating feature value input with attribute_name."""
        fv = FeatureValueInput(attribute_name="age", join_key_value_id=1, value=25)
        assert fv.attribute_name == "age"
        assert fv.join_key_value_id == 1

    def test_feature_value_input_with_metadata(self):
        """Test feature value input with metadata."""
        fv = FeatureValueInput(
            attribute_id=1,
            join_key_value_id=1,
            value=25,
            meta={"source": "api", "quality": "high"},
        )
        assert fv.meta == {"source": "api", "quality": "high"}

    def test_feature_value_input_with_timestamp(self):
        """Test feature value input with custom timestamp."""
        ts = datetime(2024, 1, 15, 10, 30, 0)
        fv = FeatureValueInput(attribute_id=1, join_key_value_id=1, value=25, timestamp=ts)
        assert fv.timestamp == ts

    def test_feature_value_input_without_timestamp(self):
        """Test feature value input without timestamp."""
        fv = FeatureValueInput(attribute_id=1, join_key_value_id=1, value=25)
        assert fv.timestamp is None


class TestAssociationInput:
    """Tests for AssociationInput schema."""

    def test_association_input_valid(self):
        """Test creating a valid association input."""
        assoc = AssociationInput(attribute_id=1, entity_id=2)
        assert assoc.attribute_id == 1
        assert assoc.entity_id == 2

    def test_association_input_missing_fields(self):
        """Test that association input requires both fields."""
        with pytest.raises(ValidationError):
            AssociationInput(attribute_id=1)

        with pytest.raises(ValidationError):
            AssociationInput(entity_id=2)


class TestFeatureRetrievalInput:
    """Tests for FeatureRetrievalInput schema."""

    def test_retrieval_input_minimal(self):
        """Test creating minimal retrieval input."""
        retrieval = FeatureRetrievalInput(project_name="test_project", entity_name="user")
        assert retrieval.project_name == "test_project"
        assert retrieval.entity_name == "user"
        assert retrieval.join_keys is None
        assert retrieval.feature_names is None
        assert retrieval.start_time is None
        assert retrieval.end_time is None
        assert retrieval.include_timestamp is False

    def test_retrieval_input_with_filters(self):
        """Test retrieval input with all filters."""
        start = datetime(2024, 1, 1)
        end = datetime(2024, 12, 31)
        retrieval = FeatureRetrievalInput(
            project_name="test_project",
            entity_name="user",
            join_keys=[1, 2, 3],
            feature_names=["age", "country"],
            start_time=start,
            end_time=end,
            include_timestamp=True,
        )
        assert retrieval.join_keys == [1, 2, 3]
        assert retrieval.feature_names == ["age", "country"]
        assert retrieval.start_time == start
        assert retrieval.end_time == end
        assert retrieval.include_timestamp is True

    def test_retrieval_input_invalid_time_range(self):
        """Test that invalid time range raises validation error."""
        start = datetime(2024, 12, 31)
        end = datetime(2024, 1, 1)

        with pytest.raises(ValidationError, match="start_time must be before end_time"):
            FeatureRetrievalInput(
                project_name="test_project",
                entity_name="user",
                start_time=start,
                end_time=end,
            )

    def test_retrieval_input_valid_time_range(self):
        """Test that valid time range passes validation."""
        start = datetime(2024, 1, 1)
        end = datetime(2024, 12, 31)
        retrieval = FeatureRetrievalInput(
            project_name="test_project",
            entity_name="user",
            start_time=start,
            end_time=end,
        )
        assert retrieval.start_time == start
        assert retrieval.end_time == end

    def test_retrieval_input_only_start_time(self):
        """Test retrieval input with only start_time."""
        start = datetime(2024, 1, 1)
        retrieval = FeatureRetrievalInput(
            project_name="test_project",
            entity_name="user",
            start_time=start,
        )
        assert retrieval.start_time == start
        assert retrieval.end_time is None

    def test_retrieval_input_only_end_time(self):
        """Test retrieval input with only end_time."""
        end = datetime(2024, 12, 31)
        retrieval = FeatureRetrievalInput(
            project_name="test_project",
            entity_name="user",
            end_time=end,
        )
        assert retrieval.start_time is None
        assert retrieval.end_time == end


class TestOutputSchemas:
    """Tests for output schemas (from_attributes conversion)."""

    def test_output_schemas_have_config(self):
        """Test that output schemas have proper Pydantic config."""
        from featurium.feature_store.schemas import EntityOutput, FeatureOutput, ProjectOutput

        # Verify Config class exists
        assert hasattr(ProjectOutput, "model_config")
        assert hasattr(EntityOutput, "model_config")
        assert hasattr(FeatureOutput, "model_config")
