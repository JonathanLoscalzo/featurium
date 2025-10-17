"""
Integration test for complete feature store workflow.

This test demonstrates a complete end-to-end workflow using the actual
RegistrationService API, showing how all components work together.
"""

import pytest
from sqlalchemy.orm import Session

from featurium.core.models import AttributeType, DataType
from featurium.services.registration.registration import RegistrationService


@pytest.mark.usefixtures("cleanup")
class TestCompleteFeatureStoreWorkflow:
    """Test complete feature store workflow from project creation to data retrieval."""

    def test_complete_ecommerce_workflow(self, db: Session) -> None:
        """Test a complete e-commerce feature store workflow."""
        service = RegistrationService(db)

        # 1. Create a project
        project = service.register_project(
            "ecommerce_analytics", "E-commerce feature store for customer analytics"
        )
        assert project.name == "ecommerce_analytics"
        assert project.id is not None

        # 2. Create entities
        user_entity = service.register_entity(
            "user", project, "Customer entity for user analytics"
        )
        order_entity = service.register_entity(
            "order", project, "Order entity for transaction analytics"
        )

        assert user_entity.name == "user"
        assert order_entity.name == "order"
        assert user_entity.project_id == project.id
        assert order_entity.project_id == project.id

        # 3. Create join keys for entities
        user_id_key = service.register_join_key("user_id", user_entity)
        order_id_key = service.register_join_key("order_id", order_entity)

        assert user_id_key.name == "user_id"
        assert order_id_key.name == "order_id"
        assert user_id_key.entity_id == user_entity.id
        assert order_id_key.entity_id == order_entity.id

        # 4. Create features
        user_age_feature = service.register_feature(
            "user_age", project, DataType.INTEGER, "User age in years"
        )
        user_income_feature = service.register_feature(
            "user_income", project, DataType.FLOAT, "User annual income"
        )
        order_amount_feature = service.register_feature(
            "order_amount", project, DataType.FLOAT, "Order total amount"
        )
        order_item_count_feature = service.register_feature(
            "order_item_count", project, DataType.INTEGER, "Number of items in order"
        )

        # Verify features
        assert user_age_feature.type == AttributeType.FEATURE
        assert user_income_feature.type == AttributeType.FEATURE
        assert order_amount_feature.type == AttributeType.FEATURE
        assert order_item_count_feature.type == AttributeType.FEATURE

        # 5. Create targets
        purchase_probability_target = service.register_target(
            "purchase_probability",
            project,
            DataType.FLOAT,
            "Probability of making a purchase",
        )
        churn_risk_target = service.register_target(
            "churn_risk", project, DataType.FLOAT, "Risk of customer churn"
        )

        # Verify targets
        assert purchase_probability_target.type == AttributeType.TARGET
        assert churn_risk_target.type == AttributeType.TARGET
        assert purchase_probability_target.is_label is True
        assert churn_risk_target.is_label is True

        # 6. Associate features with entities
        user_age_association = service.associate_attribute_with_entity(
            user_age_feature, user_entity
        )
        user_income_association = service.associate_attribute_with_entity(
            user_income_feature, user_entity
        )

        # Verify associations
        assert user_age_association.attribute_id == user_age_feature.id
        assert user_age_association.entity_id == user_entity.id
        assert user_income_association.attribute_id == user_income_feature.id
        assert user_income_association.entity_id == user_entity.id

        # 7. Create join key values
        user_123_jkv = service.register_join_key_value(user_id_key, {"integer": 123})
        user_456_jkv = service.register_join_key_value(user_id_key, {"integer": 456})
        order_001_jkv = service.register_join_key_value(
            order_id_key, {"string": "order_001"}
        )
        order_002_jkv = service.register_join_key_value(
            order_id_key, {"string": "order_002"}
        )

        # Verify join key values
        assert user_123_jkv.value == {"integer": 123}
        assert user_456_jkv.value == {"integer": 456}
        assert order_001_jkv.value == {"string": "order_001"}
        assert order_002_jkv.value == {"string": "order_002"}

        # 8. Create feature values
        # User 123 features
        user_123_age_value = service.register_feature_value(
            user_age_feature, user_123_jkv, {"integer": 28}, {"source": "user_profile"}
        )
        user_123_income_value = service.register_feature_value(
            user_income_feature,
            user_123_jkv,
            {"float": 75000.0},
            {"source": "user_profile"},
        )

        # User 456 features
        service.register_feature_value(
            user_age_feature, user_456_jkv, {"integer": 35}, {"source": "user_profile"}
        )
        service.register_feature_value(
            user_income_feature,
            user_456_jkv,
            {"float": 95000.0},
            {"source": "user_profile"},
        )

        # Order features
        order_001_amount_value = service.register_feature_value(
            order_amount_feature,
            order_001_jkv,
            {"float": 299.99},
            {"source": "order_system"},
        )
        order_001_item_count_value = service.register_feature_value(
            order_item_count_feature,
            order_001_jkv,
            {"integer": 3},
            {"source": "order_system"},
        )

        # Verify feature values
        assert user_123_age_value.value == {"integer": 28}
        assert user_123_income_value.value == {"float": 75000.0}
        assert order_001_amount_value.value == {"float": 299.99}
        assert order_001_item_count_value.value == {"integer": 3}

        # 9. Create target values
        user_123_purchase_prob = service.register_target_value(
            purchase_probability_target,
            user_123_jkv,
            {"float": 0.85},
            {"model": "purchase_predictor_v1"},
        )
        user_123_churn_risk = service.register_target_value(
            churn_risk_target,
            user_123_jkv,
            {"float": 0.15},
            {"model": "churn_predictor_v1"},
        )

        user_456_purchase_prob = service.register_target_value(
            purchase_probability_target,
            user_456_jkv,
            {"float": 0.92},
            {"model": "purchase_predictor_v1"},
        )
        user_456_churn_risk = service.register_target_value(
            churn_risk_target,
            user_456_jkv,
            {"float": 0.08},
            {"model": "churn_predictor_v1"},
        )

        # Verify target values
        assert user_123_purchase_prob.value == {"float": 0.85}
        assert user_123_churn_risk.value == {"float": 0.15}
        assert user_456_purchase_prob.value == {"float": 0.92}
        assert user_456_churn_risk.value == {"float": 0.08}

        # 10. Verify the complete setup
        # All entities should be properly linked
        entity_names = [entity.name for entity in project.entities]
        assert "user" in entity_names
        assert "order" in entity_names

        # All features should be properly linked to project
        feature_names = [attr.name for attr in project.attributes]
        assert "user_age" in feature_names
        assert "user_income" in feature_names
        assert "order_amount" in feature_names
        assert "order_item_count" in feature_names
        assert "purchase_probability" in feature_names
        assert "churn_risk" in feature_names

        # Verify join keys are properly linked to entities
        assert user_entity.join_key.name == "user_id"
        assert order_entity.join_key.name == "order_id"

        print("✅ Complete e-commerce workflow test passed!")
        print(f"   - Project: {project.name}")
        print(f"   - Entities: {len(project.entities)}")
        features_count = len(
            [a for a in project.attributes if a.type == AttributeType.FEATURE]
        )
        targets_count = len(
            [a for a in project.attributes if a.type == AttributeType.TARGET]
        )
        print(f"   - Features: {features_count}")
        print(f"   - Targets: {targets_count}")
        print("   - Join Keys: 2")
        print("   - Feature Values: 6")
        print("   - Target Values: 4")

    def test_bulk_operations_workflow(self, db: Session) -> None:
        """Test bulk operations workflow."""
        service = RegistrationService(db)

        # 1. Create project
        project = service.register_project(
            "bulk_test_project", "Testing bulk operations"
        )

        # 2. Bulk create entities
        entities_data = [
            {
                "name": "customer",
                "project_id": project.id,
                "description": "Customer entity",
            },
            {
                "name": "product",
                "project_id": project.id,
                "description": "Product entity",
            },
            {
                "name": "transaction",
                "project_id": project.id,
                "description": "Transaction entity",
            },
        ]
        entities = service.register_entities_bulk(entities_data)
        assert len(entities) == 3

        # 3. Bulk create features
        features_data = [
            {
                "name": "customer_age",
                "project_id": project.id,
                "type": AttributeType.FEATURE,
                "data_type": DataType.INTEGER,
                "description": "Customer age",
            },
            {
                "name": "product_price",
                "project_id": project.id,
                "type": AttributeType.FEATURE,
                "data_type": DataType.FLOAT,
                "description": "Product price",
            },
            {
                "name": "transaction_amount",
                "project_id": project.id,
                "type": AttributeType.FEATURE,
                "data_type": DataType.FLOAT,
                "description": "Transaction amount",
            },
        ]
        features = service.register_attributes_bulk(features_data)
        assert len(features) == 3

        # 4. Bulk create join keys
        join_keys_data = [
            {
                "name": "customer_id",
                "entity_id": entities[0].id,
                "description": "Customer ID",
            },
            {
                "name": "product_id",
                "entity_id": entities[1].id,
                "description": "Product ID",
            },
            {
                "name": "transaction_id",
                "entity_id": entities[2].id,
                "description": "Transaction ID",
            },
        ]
        join_keys = service.register_join_keys_bulk(join_keys_data)
        assert len(join_keys) == 3

        # 5. Bulk create join key values
        join_key_values_data = [
            {"join_key_id": join_keys[0].id, "value": {"integer": 1}},
            {"join_key_id": join_keys[0].id, "value": {"integer": 2}},
            {"join_key_id": join_keys[1].id, "value": {"string": "prod_001"}},
            {"join_key_id": join_keys[1].id, "value": {"string": "prod_002"}},
            {"join_key_id": join_keys[2].id, "value": {"string": "txn_001"}},
            {"join_key_id": join_keys[2].id, "value": {"string": "txn_002"}},
        ]
        join_key_values = service.register_join_key_values_bulk(join_key_values_data)
        assert len(join_key_values) == 6

        # 6. Bulk create feature values
        feature_values_data = [
            {
                "attribute_id": features[0].id,
                "join_key_value_id": join_key_values[0].id,
                "value": {"integer": 25},
            },
            {
                "attribute_id": features[0].id,
                "join_key_value_id": join_key_values[1].id,
                "value": {"integer": 30},
            },
            {
                "attribute_id": features[1].id,
                "join_key_value_id": join_key_values[2].id,
                "value": {"float": 99.99},
            },
            {
                "attribute_id": features[1].id,
                "join_key_value_id": join_key_values[3].id,
                "value": {"float": 149.99},
            },
            {
                "attribute_id": features[2].id,
                "join_key_value_id": join_key_values[4].id,
                "value": {"float": 199.98},
            },
            {
                "attribute_id": features[2].id,
                "join_key_value_id": join_key_values[5].id,
                "value": {"float": 299.97},
            },
        ]
        feature_values = service.register_attribute_values_bulk(feature_values_data)
        assert len(feature_values) == 6

        print("✅ Bulk operations workflow test passed!")
        print(f"   - Project: {project.name}")
        print(f"   - Entities: {len(entities)}")
        print(f"   - Features: {len(features)}")
        print(f"   - Join Keys: {len(join_keys)}")
        print(f"   - Join Key Values: {len(join_key_values)}")
        print(f"   - Feature Values: {len(feature_values)}")
