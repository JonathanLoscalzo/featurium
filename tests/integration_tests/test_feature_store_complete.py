"""
Integration tests for FeatureStore class.

These tests verify the complete end-to-end functionality of the FeatureStore
using SQLite in-memory database. They test the full workflow from registration
to retrieval using the high-level FeatureStore API.
"""

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy.orm import Session

from featurium.core.models import DataType
from featurium.feature_store.feature_store import FeatureStore
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
from featurium.services.registration.registration import RegistrationService
from featurium.services.retrieval.retrieval import RetrievalStore
from tests.integration_tests.db_validator import DBValidator


@pytest.fixture
def feature_store(db: Session) -> FeatureStore:
    """Create a FeatureStore instance with all dependencies."""
    registration_service = RegistrationService(db)
    retrieval_service = RetrievalStore(db)
    return FeatureStore(registration_service, retrieval_service, db)


@pytest.mark.usefixtures("cleanup")
class TestFeatureStoreRegistration:
    """Test FeatureStore registration methods."""

    def test_register_projects(self, feature_store: FeatureStore):
        """Test registering multiple projects."""
        inputs = [
            ProjectInput(name="project_1", description="First project"),
            ProjectInput(name="project_2", description="Second project"),
        ]

        projects = feature_store.register_projects(inputs)

        assert len(projects) == 2
        assert projects[0].name == "project_1"
        assert projects[1].name == "project_2"
        assert all(p.id is not None for p in projects)

    def test_register_entities_with_project_id(self, feature_store: FeatureStore):
        """Test registering entities using project_id."""
        # Setup: Create a project
        project = feature_store.register_projects([ProjectInput(name="test_project")])[0]

        # Test: Register entities
        inputs = [
            EntityInput(name="user", project_id=project.id, description="User entity"),
            EntityInput(name="product", project_id=project.id, description="Product entity"),
        ]

        entities = feature_store.register_entities(inputs)

        assert len(entities) == 2
        assert entities[0].name == "user"
        assert entities[1].name == "product"
        assert all(e.project_id == project.id for e in entities)

    def test_register_entities_with_project_name(self, feature_store: FeatureStore):
        """Test registering entities using project_name."""
        # Setup: Create a project
        feature_store.register_projects([ProjectInput(name="test_project")])

        # Test: Register entities using project_name
        inputs = [
            EntityInput(name="customer", project_name="test_project"),
            EntityInput(name="order", project_name="test_project"),
        ]

        entities = feature_store.register_entities(inputs)

        assert len(entities) == 2
        assert entities[0].name == "customer"
        assert entities[1].name == "order"

    def test_register_features_with_entities(self, feature_store: FeatureStore):
        """Test registering features and associating with entities."""
        # Setup: Create project and entities
        project = feature_store.register_projects([ProjectInput(name="ml_project")])[0]
        entities = feature_store.register_entities([EntityInput(name="user", project_id=project.id)])
        entity_id = entities[0].id

        # Test: Register features with entity associations
        inputs = [
            FeatureInput(
                name="age",
                project_id=project.id,
                data_type=DataType.INTEGER,
                entity_ids=[entity_id],
                description="User age",
            ),
            FeatureInput(
                name="country",
                project_id=project.id,
                data_type=DataType.STRING,
                entity_ids=[entity_id],
                description="User country",
            ),
        ]

        features = feature_store.register_features(inputs)

        assert len(features) == 2
        assert features[0].name == "age"
        assert features[0].data_type == DataType.INTEGER
        assert features[1].name == "country"
        assert features[1].data_type == DataType.STRING

    def test_register_targets(self, feature_store: FeatureStore):
        """Test registering targets."""
        # Setup: Create project and entity
        project = feature_store.register_projects([ProjectInput(name="ml_project")])[0]
        entity = feature_store.register_entities([EntityInput(name="user", project_id=project.id)])[0]

        # Test: Register targets
        inputs = [
            TargetInput(
                name="churn",
                project_id=project.id,
                data_type=DataType.BOOLEAN,
                entity_ids=[entity.id],
                description="Churn prediction",
            ),
            TargetInput(
                name="conversion_probability",
                project_id=project.id,
                data_type=DataType.FLOAT,
                entity_ids=[entity.id],
                description="Conversion probability",
            ),
        ]

        targets = feature_store.register_targets(inputs)

        assert len(targets) == 2
        assert targets[0].name == "churn"
        assert targets[0].is_label is True
        assert targets[1].name == "conversion_probability"

    def test_register_join_keys(self, feature_store: FeatureStore):
        """Test registering join keys."""
        # Setup: Create project and entities
        project = feature_store.register_projects([ProjectInput(name="test_project")])[0]
        entities = feature_store.register_entities(
            [
                EntityInput(name="user", project_id=project.id),
                EntityInput(name="order", project_id=project.id),
            ]
        )

        # Test: Register join keys
        inputs = [
            JoinKeyInput(name="user_id", entity_id=entities[0].id),
            JoinKeyInput(name="order_id", entity_id=entities[1].id),
        ]

        join_keys = feature_store.register_join_keys(inputs)

        assert len(join_keys) == 2
        assert join_keys[0].name == "user_id"
        assert join_keys[1].name == "order_id"

    def test_register_join_key_values(self, feature_store: FeatureStore):
        """Test registering join key values."""
        # Setup: Create complete hierarchy
        project = feature_store.register_projects([ProjectInput(name="test_project")])[0]
        entity = feature_store.register_entities([EntityInput(name="user", project_id=project.id)])[0]
        join_key = feature_store.register_join_keys([JoinKeyInput(name="user_id", entity_id=entity.id)])[0]

        # Test: Register join key values
        inputs = [
            JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 123}),
            JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 456}),
            JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 789}),
        ]

        values = feature_store.register_join_key_values(inputs)

        assert len(values) == 3
        assert values[0].value == {"integer": 123}
        assert values[1].value == {"integer": 456}
        assert values[2].value == {"integer": 789}

    def test_register_feature_values(self, feature_store: FeatureStore):
        """Test registering feature values."""
        # Setup: Create complete structure
        project = feature_store.register_projects([ProjectInput(name="test_project")])[0]
        entity = feature_store.register_entities([EntityInput(name="user", project_id=project.id)])[0]
        feature = feature_store.register_features(
            [
                FeatureInput(
                    name="age",
                    project_id=project.id,
                    data_type=DataType.INTEGER,
                    entity_ids=[entity.id],
                )
            ]
        )[0]
        join_key = feature_store.register_join_keys([JoinKeyInput(name="user_id", entity_id=entity.id)])[0]
        join_key_value = feature_store.register_join_key_values(
            [JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 123})]
        )[0]

        # Test: Register feature values
        inputs = [
            FeatureValueInput(
                attribute_id=feature.id,
                join_key_value_id=join_key_value.id,
                value={"integer": 25},
            )
        ]

        feature_values = feature_store.register_feature_values(inputs)

        assert len(feature_values) == 1
        assert feature_values[0].value == {"integer": 25}

    def test_associate_features_with_entities(self, feature_store: FeatureStore):
        """Test associating features with entities."""
        # Setup: Create features and entities without associations
        project = feature_store.register_projects([ProjectInput(name="test_project")])[0]
        entity = feature_store.register_entities([EntityInput(name="user", project_id=project.id)])[0]

        # Register features without entity associations
        features = feature_store.register_features(
            [
                FeatureInput(
                    name="feature_1",
                    project_id=project.id,
                    data_type=DataType.INTEGER,
                ),
                FeatureInput(
                    name="feature_2",
                    project_id=project.id,
                    data_type=DataType.FLOAT,
                ),
            ]
        )

        # Test: Associate features with entity
        inputs = [
            AssociationInput(attribute_id=features[0].id, entity_id=entity.id),
            AssociationInput(attribute_id=features[1].id, entity_id=entity.id),
        ]

        associations = feature_store.associate_features_with_entities(inputs)

        assert len(associations) == 2
        assert associations[0].attribute_id == features[0].id
        assert associations[1].attribute_id == features[1].id


@pytest.mark.usefixtures("cleanup")
class TestFeatureStoreRetrieval:
    """Test FeatureStore retrieval methods."""

    def setup_ecommerce_data(self, feature_store: FeatureStore):
        """Helper method to setup a complete e-commerce scenario."""
        # Create project
        project = feature_store.register_projects(
            [ProjectInput(name="ecommerce", description="E-commerce ML project")]
        )[0]

        # Create entities
        entities = feature_store.register_entities([EntityInput(name="customer", project_id=project.id)])
        entity = entities[0]

        # Create features
        features = feature_store.register_features(
            [
                FeatureInput(
                    name="age",
                    project_id=project.id,
                    data_type=DataType.INTEGER,
                    entity_ids=[entity.id],
                ),
                FeatureInput(
                    name="total_spent",
                    project_id=project.id,
                    data_type=DataType.FLOAT,
                    entity_ids=[entity.id],
                ),
                FeatureInput(
                    name="country",
                    project_id=project.id,
                    data_type=DataType.STRING,
                    entity_ids=[entity.id],
                ),
            ]
        )

        # Create targets
        targets = feature_store.register_targets(
            [
                TargetInput(
                    name="churn",
                    project_id=project.id,
                    data_type=DataType.BOOLEAN,
                    entity_ids=[entity.id],
                )
            ]
        )

        # Create join key
        join_key = feature_store.register_join_keys([JoinKeyInput(name="customer_id", entity_id=entity.id)])[
            0
        ]

        # Create join key values
        jkvs = feature_store.register_join_key_values(
            [
                JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 1}),
                JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 2}),
                JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 3}),
            ]
        )

        # Create feature values
        base_time = datetime.now(UTC)
        feature_store.register_feature_values(
            [
                # Customer 1
                FeatureValueInput(
                    attribute_id=features[0].id,
                    join_key_value_id=jkvs[0].id,
                    value={"integer": 25},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[1].id,
                    join_key_value_id=jkvs[0].id,
                    value={"float": 1500.50},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[2].id,
                    join_key_value_id=jkvs[0].id,
                    value={"string": "USA"},
                    timestamp=base_time,
                ),
                # Customer 2
                FeatureValueInput(
                    attribute_id=features[0].id,
                    join_key_value_id=jkvs[1].id,
                    value={"integer": 30},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[1].id,
                    join_key_value_id=jkvs[1].id,
                    value={"float": 2500.75},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[2].id,
                    join_key_value_id=jkvs[1].id,
                    value={"string": "Canada"},
                    timestamp=base_time,
                ),
                # Customer 3
                FeatureValueInput(
                    attribute_id=features[0].id,
                    join_key_value_id=jkvs[2].id,
                    value={"integer": 45},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[1].id,
                    join_key_value_id=jkvs[2].id,
                    value={"float": 3200.00},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[2].id,
                    join_key_value_id=jkvs[2].id,
                    value={"string": "UK"},
                    timestamp=base_time,
                ),
            ]
        )

        # Create target values
        feature_store.register_feature_values(
            [
                FeatureValueInput(
                    attribute_id=targets[0].id,
                    join_key_value_id=jkvs[0].id,
                    value={"boolean": False},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=targets[0].id,
                    join_key_value_id=jkvs[1].id,
                    value={"boolean": False},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=targets[0].id,
                    join_key_value_id=jkvs[2].id,
                    value={"boolean": True},
                    timestamp=base_time,
                ),
            ]
        )

        return project, entity, features, targets, jkvs

    def test_get_feature_values_all(self, feature_store: FeatureStore):
        """Test retrieving all feature values for an entity."""
        # Setup
        self.setup_ecommerce_data(feature_store)

        # Test: Get all feature values
        input_obj = FeatureRetrievalInput(
            project_name="ecommerce",
            entity_name="customer",
            join_keys=['{"integer": 1}', '{"integer": 2}', '{"integer": 3}'],
        )

        df = feature_store.get_feature_values(input_obj)

        # Verify
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3  # 3 customers
        assert "age" in df.columns
        assert "total_spent" in df.columns
        assert "country" in df.columns

    def test_get_feature_values_specific_features(self, feature_store: FeatureStore):
        """Test retrieving specific features."""
        # Setup
        self.setup_ecommerce_data(feature_store)

        # Test: Get only specific features
        input_obj = FeatureRetrievalInput(
            project_name="ecommerce",
            entity_name="customer",
            join_keys=['{"integer": 1}', '{"integer": 2}'],
            feature_names=["age", "country"],
        )

        df = feature_store.get_feature_values(input_obj)

        # Verify
        assert len(df) == 2
        assert "age" in df.columns
        assert "country" in df.columns
        assert "total_spent" not in df.columns

    def test_get_feature_values_specific_join_keys(self, feature_store: FeatureStore):
        """Test retrieving features for specific join keys."""
        # Setup
        self.setup_ecommerce_data(feature_store)

        # Test: Get features for specific customers only
        input_obj = FeatureRetrievalInput(
            project_name="ecommerce",
            entity_name="customer",
            join_keys=['{"integer": 1}', '{"integer": 3}'],
        )

        df = feature_store.get_feature_values(input_obj)

        # Verify
        assert len(df) == 2
        # Check that we got the right customers
        assert df.index.astype(str).tolist() == ["1", "3"]

    def test_get_target_values(self, feature_store: FeatureStore):
        """Test retrieving target values."""
        # Setup
        self.setup_ecommerce_data(feature_store)

        # Test: Get target values
        input_obj = FeatureRetrievalInput(
            project_name="ecommerce",
            entity_name="customer",
            join_keys=['{"integer": 1}', '{"integer": 2}', '{"integer": 3}'],
            feature_names=["churn"],
        )

        df = feature_store.get_target_values(input_obj)

        # Verify
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "churn" in df.columns

    def test_get_all_values(self, feature_store: FeatureStore):
        """Test retrieving all values (features + targets)."""
        # Setup
        self.setup_ecommerce_data(feature_store)

        # Test: Get all values
        input_obj = FeatureRetrievalInput(
            project_name="ecommerce",
            entity_name="customer",
            join_keys=['{"integer": 1}', '{"integer": 2}', '{"integer": 3}'],
        )

        df = feature_store.get_all_values(input_obj)

        # Verify
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        # Should have both features and targets
        assert "age" in df.columns
        assert "total_spent" in df.columns
        assert "country" in df.columns
        assert "churn" in df.columns

    def test_get_feature_values_with_timestamp(self, feature_store: FeatureStore):
        """Test retrieving feature values with timestamp information."""
        # Setup
        self.setup_ecommerce_data(feature_store)

        # Test: Get features with timestamp
        input_obj = FeatureRetrievalInput(
            project_name="ecommerce",
            entity_name="customer",
            join_keys=['{"integer": 1}', '{"integer": 2}'],
            feature_names=["age"],
            include_timestamp=True,
        )

        df = feature_store.get_feature_values(input_obj)

        # Verify - with timestamp, we get multi-level columns
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_get_feature_values_time_range(self, feature_store: FeatureStore):
        """Test retrieving feature values within a time range."""
        # Setup: Create data with different timestamps
        project = feature_store.register_projects([ProjectInput(name="time_test")])[0]

        entity = feature_store.register_entities([EntityInput(name="user", project_id=project.id)])[0]

        feature = feature_store.register_features(
            [
                FeatureInput(
                    name="score",
                    project_id=project.id,
                    data_type=DataType.INTEGER,
                    entity_ids=[entity.id],
                )
            ]
        )[0]

        join_key = feature_store.register_join_keys([JoinKeyInput(name="user_id", entity_id=entity.id)])[0]

        jkv = feature_store.register_join_key_values(
            [JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 1})]
        )[0]

        # Create values at different times
        base_time = datetime.now(UTC)
        feature_store.register_feature_values(
            [
                FeatureValueInput(
                    attribute_id=feature.id,
                    join_key_value_id=jkv.id,
                    value={"integer": 10},
                    timestamp=base_time - timedelta(days=2),
                ),
                FeatureValueInput(
                    attribute_id=feature.id,
                    join_key_value_id=jkv.id,
                    value={"integer": 20},
                    timestamp=base_time - timedelta(days=1),
                ),
                FeatureValueInput(
                    attribute_id=feature.id,
                    join_key_value_id=jkv.id,
                    value={"integer": 30},
                    timestamp=base_time,
                ),
            ]
        )

        # Test: Get values within time range (should get most recent)
        input_obj = FeatureRetrievalInput(
            project_name="time_test",
            entity_name="user",
            join_keys=['{"integer": 1}'],
            start_time=base_time - timedelta(days=1, hours=12),
            end_time=base_time + timedelta(hours=1),
        )

        df = feature_store.get_feature_values(input_obj)

        # Verify - should get the most recent value within the time range
        assert len(df) == 1
        assert "score" in df.columns


@pytest.mark.usefixtures("cleanup")
class TestFeatureStoreQueryMethods:
    """Test FeatureStore query and listing methods."""

    def test_list_projects(self, feature_store: FeatureStore):
        """Test listing all projects."""
        # Setup: Create multiple projects
        feature_store.register_projects(
            [
                ProjectInput(name="project_a"),
                ProjectInput(name="project_b"),
                ProjectInput(name="project_c"),
            ]
        )

        # Test
        projects = feature_store.list_projects()

        # Verify
        assert len(projects) == 3
        assert "project_a" in projects
        assert "project_b" in projects
        assert "project_c" in projects

    def test_list_entities_all(self, feature_store: FeatureStore):
        """Test listing all entities."""
        # Setup
        project = feature_store.register_projects([ProjectInput(name="test_project")])[0]

        feature_store.register_entities(
            [
                EntityInput(name="user", project_id=project.id),
                EntityInput(name="product", project_id=project.id),
                EntityInput(name="order", project_id=project.id),
            ]
        )

        # Test
        entities = feature_store.list_entities()

        # Verify
        assert len(entities) == 3
        assert "user" in entities
        assert "product" in entities
        assert "order" in entities

    def test_list_entities_by_project(self, feature_store: FeatureStore):
        """Test listing entities filtered by project."""
        # Setup: Create two projects with different entities
        projects = feature_store.register_projects(
            [
                ProjectInput(name="project_1"),
                ProjectInput(name="project_2"),
            ]
        )

        feature_store.register_entities(
            [
                EntityInput(name="entity_a", project_id=projects[0].id),
                EntityInput(name="entity_b", project_id=projects[0].id),
                EntityInput(name="entity_c", project_id=projects[1].id),
            ]
        )

        # Test
        entities_p1 = feature_store.list_entities(project_name="project_1")
        entities_p2 = feature_store.list_entities(project_name="project_2")

        # Verify
        assert len(entities_p1) == 2
        assert "entity_a" in entities_p1
        assert "entity_b" in entities_p1

        assert len(entities_p2) == 1
        assert "entity_c" in entities_p2

    def test_list_features_by_project(self, feature_store: FeatureStore):
        """Test listing features for a project."""
        # Setup
        project = feature_store.register_projects([ProjectInput(name="ml_project")])[0]

        entity = feature_store.register_entities([EntityInput(name="user", project_id=project.id)])[0]

        feature_store.register_features(
            [
                FeatureInput(
                    name="age",
                    project_id=project.id,
                    data_type=DataType.INTEGER,
                    entity_ids=[entity.id],
                ),
                FeatureInput(
                    name="income",
                    project_id=project.id,
                    data_type=DataType.FLOAT,
                    entity_ids=[entity.id],
                ),
                FeatureInput(
                    name="country",
                    project_id=project.id,
                    data_type=DataType.STRING,
                    entity_ids=[entity.id],
                ),
            ]
        )

        # Test
        features = feature_store.list_features("ml_project")

        # Verify
        assert len(features) == 3
        assert "age" in features
        assert "income" in features
        assert "country" in features

    def test_list_features_by_project_and_entity(self, feature_store: FeatureStore):
        """Test listing features filtered by project and entity."""
        # Setup
        project = feature_store.register_projects([ProjectInput(name="ml_project")])[0]

        entities = feature_store.register_entities(
            [
                EntityInput(name="user", project_id=project.id),
                EntityInput(name="product", project_id=project.id),
            ]
        )

        feature_store.register_features(
            [
                FeatureInput(
                    name="user_age",
                    project_id=project.id,
                    data_type=DataType.INTEGER,
                    entity_ids=[entities[0].id],
                ),
                FeatureInput(
                    name="user_country",
                    project_id=project.id,
                    data_type=DataType.STRING,
                    entity_ids=[entities[0].id],
                ),
                FeatureInput(
                    name="product_price",
                    project_id=project.id,
                    data_type=DataType.FLOAT,
                    entity_ids=[entities[1].id],
                ),
            ]
        )

        # Test
        user_features = feature_store.list_features("ml_project", entity_name="user")
        product_features = feature_store.list_features("ml_project", entity_name="product")

        # Verify
        assert len(user_features) == 2
        assert "user_age" in user_features
        assert "user_country" in user_features

        assert len(product_features) == 1
        assert "product_price" in product_features

    def test_list_targets_by_project(self, feature_store: FeatureStore):
        """Test listing targets for a project."""
        # Setup
        project = feature_store.register_projects([ProjectInput(name="ml_project")])[0]

        entity = feature_store.register_entities([EntityInput(name="user", project_id=project.id)])[0]

        feature_store.register_targets(
            [
                TargetInput(
                    name="churn",
                    project_id=project.id,
                    data_type=DataType.BOOLEAN,
                    entity_ids=[entity.id],
                ),
                TargetInput(
                    name="conversion",
                    project_id=project.id,
                    data_type=DataType.BOOLEAN,
                    entity_ids=[entity.id],
                ),
            ]
        )

        # Test
        targets = feature_store.list_targets("ml_project")

        # Verify
        assert len(targets) == 2
        assert "churn" in targets
        assert "conversion" in targets

    def test_list_targets_by_project_and_entity(self, feature_store: FeatureStore):
        """Test listing targets filtered by project and entity."""
        # Setup
        project = feature_store.register_projects([ProjectInput(name="ml_project")])[0]

        entities = feature_store.register_entities(
            [
                EntityInput(name="user", project_id=project.id),
                EntityInput(name="transaction", project_id=project.id),
            ]
        )

        feature_store.register_targets(
            [
                TargetInput(
                    name="user_churn",
                    project_id=project.id,
                    data_type=DataType.BOOLEAN,
                    entity_ids=[entities[0].id],
                ),
                TargetInput(
                    name="fraud",
                    project_id=project.id,
                    data_type=DataType.BOOLEAN,
                    entity_ids=[entities[1].id],
                ),
            ]
        )

        # Test
        user_targets = feature_store.list_targets("ml_project", entity_name="user")
        transaction_targets = feature_store.list_targets("ml_project", entity_name="transaction")

        # Verify
        assert len(user_targets) == 1
        assert "user_churn" in user_targets

        assert len(transaction_targets) == 1
        assert "fraud" in transaction_targets


@pytest.mark.usefixtures("cleanup")
class TestFeatureStoreErrorHandling:
    """Test FeatureStore error handling."""

    def test_register_entity_with_invalid_project(self, feature_store: FeatureStore):
        """Test registering entity with non-existent project name."""
        with pytest.raises(ValueError, match="Project 'nonexistent' not found"):
            feature_store.register_entities([EntityInput(name="user", project_name="nonexistent")])

    def test_get_features_with_invalid_project(self, feature_store: FeatureStore):
        """Test retrieving features with non-existent project."""
        input_obj = FeatureRetrievalInput(
            project_name="nonexistent", entity_name="user", join_keys=['{"integer": 1}']
        )

        with pytest.raises(ValueError):
            feature_store.get_feature_values(input_obj)

    def test_get_features_with_invalid_entity(self, feature_store: FeatureStore):
        """Test retrieving features with non-existent entity."""
        # Setup: Create project only
        feature_store.register_projects([ProjectInput(name="test_project")])

        input_obj = FeatureRetrievalInput(
            project_name="test_project",
            entity_name="nonexistent_entity",
            join_keys=['{"integer": 1}'],
        )

        with pytest.raises(ValueError):
            feature_store.get_feature_values(input_obj)

    def test_list_features_with_invalid_project(self, feature_store: FeatureStore):
        """Test listing features with non-existent project."""
        with pytest.raises(ValueError, match="Project 'nonexistent' not found"):
            feature_store.list_features("nonexistent")


@pytest.mark.usefixtures("cleanup")
class TestFeatureStoreCompleteWorkflow:
    """Test complete end-to-end workflows."""

    def test_complete_ml_pipeline_workflow(self, feature_store: FeatureStore):
        """Test a complete ML pipeline from registration to retrieval."""
        # Step 1: Register project
        project = feature_store.register_projects(
            [ProjectInput(name="credit_scoring", description="Credit scoring ML project")]
        )[0]

        # Step 2: Register entity
        entity = feature_store.register_entities(
            [
                EntityInput(
                    name="applicant",
                    project_id=project.id,
                    description="Loan applicant",
                )
            ]
        )[0]

        # Step 3: Register features
        features = feature_store.register_features(
            [
                FeatureInput(
                    name="annual_income",
                    project_id=project.id,
                    data_type=DataType.FLOAT,
                    entity_ids=[entity.id],
                    description="Annual income in USD",
                ),
                FeatureInput(
                    name="credit_score",
                    project_id=project.id,
                    data_type=DataType.INTEGER,
                    entity_ids=[entity.id],
                    description="Credit score (300-850)",
                ),
                FeatureInput(
                    name="employment_length",
                    project_id=project.id,
                    data_type=DataType.INTEGER,
                    entity_ids=[entity.id],
                    description="Years of employment",
                ),
            ]
        )

        # Step 4: Register target
        target = feature_store.register_targets(
            [
                TargetInput(
                    name="default_risk",
                    project_id=project.id,
                    data_type=DataType.FLOAT,
                    entity_ids=[entity.id],
                    description="Probability of default",
                )
            ]
        )[0]

        # Step 5: Register join key
        join_key = feature_store.register_join_keys([JoinKeyInput(name="applicant_id", entity_id=entity.id)])[
            0
        ]

        # Step 6: Register join key values
        jkvs = feature_store.register_join_key_values(
            [
                JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 1001}),
                JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 1002}),
                JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 1003}),
            ]
        )

        # Step 7: Register feature values
        base_time = datetime.now(UTC)
        feature_store.register_feature_values(
            [
                # Applicant 1001
                FeatureValueInput(
                    attribute_id=features[0].id,
                    join_key_value_id=jkvs[0].id,
                    value={"float": 75000.0},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[1].id,
                    join_key_value_id=jkvs[0].id,
                    value={"integer": 720},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[2].id,
                    join_key_value_id=jkvs[0].id,
                    value={"integer": 5},
                    timestamp=base_time,
                ),
                # Applicant 1002
                FeatureValueInput(
                    attribute_id=features[0].id,
                    join_key_value_id=jkvs[1].id,
                    value={"float": 50000.0},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[1].id,
                    join_key_value_id=jkvs[1].id,
                    value={"integer": 650},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[2].id,
                    join_key_value_id=jkvs[1].id,
                    value={"integer": 2},
                    timestamp=base_time,
                ),
                # Applicant 1003
                FeatureValueInput(
                    attribute_id=features[0].id,
                    join_key_value_id=jkvs[2].id,
                    value={"float": 120000.0},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[1].id,
                    join_key_value_id=jkvs[2].id,
                    value={"integer": 800},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[2].id,
                    join_key_value_id=jkvs[2].id,
                    value={"integer": 10},
                    timestamp=base_time,
                ),
            ]
        )

        # Step 8: Register target values
        feature_store.register_feature_values(
            [
                FeatureValueInput(
                    attribute_id=target.id,
                    join_key_value_id=jkvs[0].id,
                    value={"float": 0.15},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=target.id,
                    join_key_value_id=jkvs[1].id,
                    value={"float": 0.35},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=target.id,
                    join_key_value_id=jkvs[2].id,
                    value={"float": 0.05},
                    timestamp=base_time,
                ),
            ]
        )

        # Step 9: Retrieve training data
        training_input = FeatureRetrievalInput(
            project_name="credit_scoring",
            entity_name="applicant",
            join_keys=['{"integer": 1001}', '{"integer": 1002}', '{"integer": 1003}'],
        )

        features_df = feature_store.get_feature_values(training_input)
        targets_df = feature_store.get_target_values(training_input)

        # Step 10: Verify the complete workflow
        assert len(features_df) == 3
        assert "annual_income" in features_df.columns
        assert "credit_score" in features_df.columns
        assert "employment_length" in features_df.columns

        assert len(targets_df) == 3
        assert "default_risk" in targets_df.columns

        # Step 11: Test listing methods
        assert "credit_scoring" in feature_store.list_projects()
        assert "applicant" in feature_store.list_entities()

        features_list = feature_store.list_features("credit_scoring")
        assert len(features_list) == 3

        targets_list = feature_store.list_targets("credit_scoring")
        assert len(targets_list) == 1


@pytest.mark.usefixtures("cleanup")
class TestFeatureStorePersistence:
    """Test FeatureStore with database validation."""

    def test_registration_with_db_validation(self, feature_store: FeatureStore, db_validator: DBValidator):
        """Test registration and verify database state."""
        # Step 1: Register project
        projects = feature_store.register_projects(
            [ProjectInput(name="validated_project", description="Test project with validation")]
        )

        # Validate project in DB
        db_validator.verify_project_exists("validated_project")
        db_validator.verify_project_count(1)
        db_validator.verify_project_metadata(
            "validated_project", expected_description="Test project with validation"
        )

        # Step 2: Register entities
        entities = feature_store.register_entities(
            [
                EntityInput(name="customer", project_id=projects[0].id),
                EntityInput(name="product", project_id=projects[0].id),
            ]
        )

        # Validate entities in DB
        db_validator.verify_entity_exists("customer")
        db_validator.verify_entity_exists("product")
        db_validator.verify_entity_count(expected_count=2)
        db_validator.verify_entity_count(project_name="validated_project", expected_count=2)
        db_validator.verify_entity_belongs_to_project("customer", "validated_project")
        db_validator.verify_entity_belongs_to_project("product", "validated_project")
        db_validator.verify_project_has_entities("validated_project", ["customer", "product"])

        # Step 3: Register features
        features = feature_store.register_features(
            [
                FeatureInput(
                    name="customer_age",
                    project_id=projects[0].id,
                    data_type=DataType.INTEGER,
                    entity_ids=[entities[0].id],
                ),
                FeatureInput(
                    name="customer_country",
                    project_id=projects[0].id,
                    data_type=DataType.STRING,
                    entity_ids=[entities[0].id],
                ),
                FeatureInput(
                    name="product_price",
                    project_id=projects[0].id,
                    data_type=DataType.FLOAT,
                    entity_ids=[entities[1].id],
                ),
            ]
        )

        # Validate features in DB
        db_validator.verify_attribute_exists("customer_age")
        db_validator.verify_attribute_exists("customer_country")
        db_validator.verify_attribute_exists("product_price")
        db_validator.verify_feature_count(expected_count=3)
        db_validator.verify_feature_count(project_name="validated_project", expected_count=3)

        # Validate associations
        db_validator.verify_attribute_associated_with_entity("customer_age", "customer")
        db_validator.verify_attribute_associated_with_entity("customer_country", "customer")
        db_validator.verify_attribute_associated_with_entity("product_price", "product")
        db_validator.verify_entity_has_features("customer", ["customer_age", "customer_country"])
        db_validator.verify_entity_has_features("product", ["product_price"])

        # Step 4: Register targets
        targets = feature_store.register_targets(
            [
                TargetInput(
                    name="churn_risk",
                    project_id=projects[0].id,
                    data_type=DataType.FLOAT,
                    entity_ids=[entities[0].id],
                )
            ]
        )

        # Validate targets in DB
        db_validator.verify_attribute_exists("churn_risk")
        db_validator.verify_target_count(expected_count=1)
        db_validator.verify_target_count(project_name="validated_project", expected_count=1)
        db_validator.verify_entity_has_targets("customer", ["churn_risk"])

        # Step 5: Register join keys
        join_keys = feature_store.register_join_keys(
            [
                JoinKeyInput(name="customer_id", entity_id=entities[0].id),
                JoinKeyInput(name="product_id", entity_id=entities[1].id),
            ]
        )

        # Validate join keys in DB
        db_validator.verify_join_key_exists("customer_id")
        db_validator.verify_join_key_exists("product_id")
        db_validator.verify_join_key_count(expected_count=2)
        db_validator.verify_join_key_count(entity_name="customer", expected_count=1)
        db_validator.verify_join_key_count(entity_name="product", expected_count=1)

        # Step 6: Register join key values
        jkvs = feature_store.register_join_key_values(
            [
                JoinKeyValueInput(join_key_id=join_keys[0].id, value={"integer": 100}),
                JoinKeyValueInput(join_key_id=join_keys[0].id, value={"integer": 200}),
                JoinKeyValueInput(join_key_id=join_keys[1].id, value={"string": "prod_001"}),
            ]
        )

        # Validate join key values in DB
        db_validator.verify_join_key_value_exists("customer_id", {"integer": 100})
        db_validator.verify_join_key_value_exists("customer_id", {"integer": 200})
        db_validator.verify_join_key_value_exists("product_id", {"string": "prod_001"})
        db_validator.verify_join_key_value_count(expected_count=3)
        db_validator.verify_join_key_value_count(join_key_name="customer_id", expected_count=2)
        db_validator.verify_join_key_value_count(join_key_name="product_id", expected_count=1)

        # Step 7: Register attribute values
        base_time = datetime.now(UTC)
        feature_store.register_feature_values(
            [
                FeatureValueInput(
                    attribute_id=features[0].id,
                    join_key_value_id=jkvs[0].id,
                    value={"integer": 25},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[1].id,
                    join_key_value_id=jkvs[0].id,
                    value={"string": "USA"},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[0].id,
                    join_key_value_id=jkvs[1].id,
                    value={"integer": 35},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=targets[0].id,
                    join_key_value_id=jkvs[0].id,
                    value={"float": 0.25},
                    timestamp=base_time,
                ),
            ]
        )

        # Validate attribute values in DB
        db_validator.verify_attribute_value_exists("customer_age", {"integer": 100}, {"integer": 25})
        db_validator.verify_attribute_value_exists("customer_country", {"integer": 100}, {"string": "USA"})
        db_validator.verify_attribute_value_exists("customer_age", {"integer": 200}, {"integer": 35})
        db_validator.verify_attribute_value_exists("churn_risk", {"integer": 100}, {"float": 0.25})
        db_validator.verify_attribute_value_count(expected_count=4)
        db_validator.verify_attribute_value_count(attribute_name="customer_age", expected_count=2)
        db_validator.verify_attribute_value_count(attribute_name="customer_country", expected_count=1)

        # Step 8: Verify overall database integrity
        counts = db_validator.verify_database_integrity()
        assert counts["projects"] == 1
        assert counts["entities"] == 2
        assert counts["features"] == 3
        assert counts["targets"] == 1
        assert counts["join_keys"] == 2
        assert counts["join_key_values"] == 3
        assert counts["attribute_values"] == 4
        assert counts["associations"] == 4  # 3 features + 1 target associated

        # Optional: Print database state for debugging
        # db_validator.print_database_state()

    def test_complete_workflow_with_validation(self, feature_store: FeatureStore, db_validator: DBValidator):
        """Test complete workflow including retrieval with DB validation."""
        # Setup: Register everything
        project = feature_store.register_projects(
            [ProjectInput(name="ml_pipeline", description="ML Pipeline Test")]
        )[0]

        entity = feature_store.register_entities([EntityInput(name="user", project_id=project.id)])[0]

        features = feature_store.register_features(
            [
                FeatureInput(
                    name="age",
                    project_id=project.id,
                    data_type=DataType.INTEGER,
                    entity_ids=[entity.id],
                ),
                FeatureInput(
                    name="score",
                    project_id=project.id,
                    data_type=DataType.FLOAT,
                    entity_ids=[entity.id],
                ),
            ]
        )

        join_key = feature_store.register_join_keys([JoinKeyInput(name="user_id", entity_id=entity.id)])[0]

        jkvs = feature_store.register_join_key_values(
            [
                JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 1}),
                JoinKeyValueInput(join_key_id=join_key.id, value={"integer": 2}),
            ]
        )

        base_time = datetime.now(UTC)
        feature_store.register_feature_values(
            [
                FeatureValueInput(
                    attribute_id=features[0].id,
                    join_key_value_id=jkvs[0].id,
                    value={"integer": 25},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[1].id,
                    join_key_value_id=jkvs[0].id,
                    value={"float": 85.5},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[0].id,
                    join_key_value_id=jkvs[1].id,
                    value={"integer": 30},
                    timestamp=base_time,
                ),
                FeatureValueInput(
                    attribute_id=features[1].id,
                    join_key_value_id=jkvs[1].id,
                    value={"float": 92.3},
                    timestamp=base_time,
                ),
            ]
        )

        # Verify database state before retrieval
        db_validator.verify_project_exists("ml_pipeline")
        db_validator.verify_entity_exists("user", project_name="ml_pipeline")
        db_validator.verify_feature_count(project_name="ml_pipeline", expected_count=2)
        db_validator.verify_join_key_value_count(join_key_name="user_id", expected_count=2)
        db_validator.verify_attribute_value_count(expected_count=4)

        # Test retrieval
        input_obj = FeatureRetrievalInput(
            project_name="ml_pipeline",
            entity_name="user",
            join_keys=['{"integer": 1}', '{"integer": 2}'],
        )

        df = feature_store.get_feature_values(input_obj)

        # Verify retrieval results
        assert len(df) == 2
        assert "age" in df.columns
        assert "score" in df.columns

        # Verify database integrity after retrieval
        counts = db_validator.verify_database_integrity()
        assert counts["projects"] == 1
        assert counts["entities"] == 1
        assert counts["features"] == 2
        assert counts["join_key_values"] == 2
        assert counts["attribute_values"] == 4
