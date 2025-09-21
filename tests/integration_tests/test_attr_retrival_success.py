from datetime import datetime, timedelta

import pytest
from sqlalchemy.orm import Session

from featurium.core.models import DataType
from featurium.services.retrieval import RetrivalStore
from tests.integration_tests.utils import BaseTestCase, TestCase
from tests.utils.factories import (
    create_entity,
    create_feature,
    create_feature_value,
    create_join_key,
    create_join_key_value,
    create_project,
    create_target,
    create_target_value,
)


def _setup_data(db: Session):
    """Setup the data"""
    project = create_project(db, "project:animals")

    # entity dog
    entity = create_entity(db, "entity:dog", project)

    join_key = create_join_key(db, "dog:name", entity)
    join_key_value_1 = create_join_key_value(db, join_key, {"name": "firulais"})
    join_key_value_2 = create_join_key_value(db, join_key, {"name": "chiquito"})
    _ = create_join_key_value(db, join_key, {"name": "chiquito", "quien": "canichin"})

    target = create_target(db, "target:dog:is_good", project, DataType.BOOLEAN)
    target.entities.append(entity)

    feature = create_feature(db, "feature:dog:avg_legs", project, DataType.INTEGER)
    feature.entities.append(entity)
    feature2 = create_feature(db, "feature:dog:long_hair", project, DataType.BOOLEAN)
    feature2.entities.append(entity)

    # firulais
    feature_value_1_1 = create_feature_value(
        db, feature, {"integer": 3}, datetime.now() - timedelta(days=30)
    )
    feature_value_1_2 = create_feature_value(
        db, feature, {"integer": 4}, datetime.now() - timedelta(days=60)
    )
    join_key_value_1.attribute_values.append(feature_value_1_1)
    join_key_value_1.attribute_values.append(feature_value_1_2)

    feature_value_a_1 = create_feature_value(
        db, feature2, {"boolean": True}, datetime.now() - timedelta(days=28)
    )
    feature_value_a_2 = create_feature_value(
        db, feature2, {"boolean": False}, datetime.now() - timedelta(days=59)
    )
    join_key_value_1.attribute_values.append(feature_value_a_1)
    join_key_value_1.attribute_values.append(feature_value_a_2)
    target_value = create_target_value(
        db, target, {"boolean": True}, datetime.now() - timedelta(days=30)
    )
    join_key_value_1.attribute_values.append(target_value)

    # chiquito
    feature_value_2_1 = create_feature_value(
        db, feature, {"integer": 2}, datetime.now() - timedelta(days=10)
    )
    feature_value_2_2 = create_feature_value(
        db, feature, {"integer": 3}, datetime.now() - timedelta(days=20)
    )
    join_key_value_2.attribute_values.append(feature_value_2_1)
    join_key_value_2.attribute_values.append(feature_value_2_2)

    feature_value_b_1 = create_feature_value(
        db, feature2, {"boolean": True}, datetime.now() - timedelta(days=5)
    )
    feature_value_b_2 = create_feature_value(
        db, feature2, {"boolean": False}, datetime.now() - timedelta(days=15)
    )
    join_key_value_2.attribute_values.append(feature_value_b_1)
    join_key_value_2.attribute_values.append(feature_value_b_2)
    target_value = create_target_value(
        db, target, {"boolean": False}, datetime.now() - timedelta(days=10)
    )
    join_key_value_2.attribute_values.append(target_value)

    # ======================================================================
    # entity cat
    entity_cat = create_entity(db, "entity:cat", project)
    join_key_cat = create_join_key(db, "cat:name", entity_cat)
    join_key_value_cat_1 = create_join_key_value(db, join_key_cat, "michi")
    join_key_value_cat_2 = create_join_key_value(db, join_key_cat, "garfield")

    feature_cat_legs = create_feature(
        db, "feature:cat:avg_legs", project, DataType.INTEGER
    )
    feature_cat_legs.entities.append(entity_cat)
    feature_cat_hair = create_feature(
        db, "feature:cat:long_hair", project, DataType.BOOLEAN
    )
    feature_cat_hair.entities.append(entity_cat)
    feature_cat_color = create_feature(
        db, "feature:cat:color", project, DataType.STRING
    )
    feature_cat_color.entities.append(entity_cat)

    # michi
    feature_value_cat_1_1 = create_feature_value(
        db, feature_cat_legs, {"integer": 4}, datetime.now() - timedelta(days=12)
    )
    feature_value_cat_1_2 = create_feature_value(
        db, feature_cat_legs, {"integer": 3}, datetime.now() - timedelta(days=40)
    )
    feature_value_cat_1_3 = create_feature_value(
        db,
        feature_cat_color,
        {"string": "black"},
        datetime.now() - timedelta(days=45),
    )
    join_key_value_cat_1.attribute_values.append(feature_value_cat_1_1)
    join_key_value_cat_1.attribute_values.append(feature_value_cat_1_2)
    join_key_value_cat_1.attribute_values.append(feature_value_cat_1_3)

    feature_value_cat_a_1 = create_feature_value(
        db, feature_cat_hair, {"boolean": True}, datetime.now() - timedelta(days=10)
    )
    feature_value_cat_a_2 = create_feature_value(
        db,
        feature_cat_hair,
        {"boolean": False},
        datetime.now() - timedelta(days=35),
    )

    join_key_value_cat_1.attribute_values.append(feature_value_cat_a_1)
    join_key_value_cat_1.attribute_values.append(feature_value_cat_a_2)

    # garfield
    feature_value_cat_2_1 = create_feature_value(
        db, feature_cat_legs, {"integer": 4}, datetime.now() - timedelta(days=8)
    )
    feature_value_cat_2_2 = create_feature_value(
        db, feature_cat_legs, {"integer": 4}, datetime.now() - timedelta(days=18)
    )
    feature_value_cat_2_3 = create_feature_value(
        db,
        feature_cat_color,
        {"string": "orange"},
        datetime.now() - timedelta(days=18),
    )
    join_key_value_cat_2.attribute_values.append(feature_value_cat_2_1)
    join_key_value_cat_2.attribute_values.append(feature_value_cat_2_2)
    join_key_value_cat_2.attribute_values.append(feature_value_cat_2_3)

    feature_value_cat_b_1 = create_feature_value(
        db, feature_cat_hair, {"boolean": False}, datetime.now() - timedelta(days=7)
    )
    feature_value_cat_b_2 = create_feature_value(
        db, feature_cat_hair, {"boolean": True}, datetime.now() - timedelta(days=25)
    )
    join_key_value_cat_2.attribute_values.append(feature_value_cat_b_1)
    join_key_value_cat_2.attribute_values.append(feature_value_cat_b_2)

    db.commit()


class TestFeatureRetrievalSuccess(BaseTestCase):
    """Test the feature retrieval"""

    test_cases = [
        TestCase(
            test_case_id="retrieve_all_features_and_entities",
            description="Retrieve all features for an entity",
            project_name="project:animals",
            entity_name="entity:dog",
            join_keys=None,
            feature_names=None,
            expected_features=["feature:dog:avg_legs", "feature:dog:long_hair"],
            expected_entities=[
                "{'name': 'chiquito', 'quien': 'canichin'}",
                "firulais",
                "chiquito",
            ],
            expected_row_count=3,
        ),
        TestCase(
            test_case_id="filter_by_join_key",
            description="Filter by specific join key",
            project_name="project:animals",
            entity_name="entity:dog",
            join_keys=['{"name": "chiquito"}'],
            feature_names=None,
            expected_features=["feature:dog:avg_legs", "feature:dog:long_hair"],
            expected_entities=["chiquito"],
            expected_row_count=1,
        ),
        TestCase(
            test_case_id="filter_by_join_keys_and_feature_names",
            description="Filter by specific feature names",
            project_name="project:animals",
            entity_name="entity:dog",
            join_keys=['{"name": "firulais"}', '{"name": "chiquito"}'],
            feature_names=["feature:dog:avg_legs"],
            expected_features=["feature:dog:avg_legs"],
            expected_entities=["firulais", "chiquito"],
            expected_row_count=2,
        ),
        TestCase(
            test_case_id="filter_by_date_range",
            description="Filter by date range",
            project_name="project:animals",
            entity_name="entity:dog",
            join_keys=['{"name": "firulais"}', '{"name": "chiquito"}'],
            feature_names=None,
            start_time=datetime.now() - timedelta(days=25),
            end_time=datetime.now() - timedelta(days=5),
            expected_features=["feature:dog:avg_legs", "feature:dog:long_hair"],
            expected_entities=["firulais", "chiquito"],
            expected_row_count=2,
        ),
    ]

    @pytest.fixture(autouse=True)
    def setup_data(self, db: Session):
        """Setup the data"""
        _setup_data(db)

    @pytest.mark.parametrize("test_case", test_cases[:3])
    def test_feature_retrieval(self, db: Session, test_case: TestCase):
        """Test the feature retrieval"""

        feature_retrieval = RetrivalStore(db)

        result = feature_retrieval.get_feature_values(
            project_name=test_case.project_name,
            entity_name=test_case.entity_name,
            join_keys=test_case.join_keys,
            feature_names=test_case.feature_names,
            start_time=test_case.start_time,
            end_time=test_case.end_time,
            include_timestamp=test_case.include_timestamp,
        )

        self.validate_result_structure(result, test_case)
        self.validate_expected_features(result, test_case)
        self.validate_expected_entities(result, test_case)
        self.validate_row_count(result, test_case)
        self.validate_date_range(result, test_case)


class TestTargetRetrievalSuccess(BaseTestCase):

    test_cases = [
        TestCase(
            test_case_id="retrieve_all_targets_and_entities",
            description="Retrieve all targets for an entity",
            project_name="project:animals",
            entity_name="entity:dog",
            join_keys=None,
            feature_names=None,
            expected_features=["target:dog:is_good"],
            expected_entities=[
                "firulais",
                "chiquito",
                "{'name': 'chiquito', 'quien': 'canichin'}",
            ],
            expected_row_count=3,
        ),
        TestCase(
            test_case_id="filter_by_join_key",
            description="Filter by specific join key",
            project_name="project:animals",
            entity_name="entity:dog",
            join_keys=['{"name": "chiquito"}'],
            expected_features=["target:dog:is_good"],
            expected_entities=["chiquito"],
            expected_row_count=1,
        ),
        TestCase(
            test_case_id="filter_by_join_keys_and_feature_names",
            description="Filter by specific feature names",
            project_name="project:animals",
            entity_name="entity:dog",
            join_keys=['{"name": "firulais"}', '{"name": "chiquito"}'],
            expected_features=["target:dog:is_good"],
            expected_entities=["firulais", "chiquito"],
            expected_row_count=2,
        ),
    ]

    @pytest.fixture(autouse=True)
    def setup_data(self, db: Session):
        """Setup the data"""
        _setup_data(db)

    @pytest.mark.parametrize("test_case", test_cases)
    def test_target_retrieval(self, db: Session, test_case: TestCase):
        """Test the target retrieval"""

        feature_retrieval = RetrivalStore(db)

        result = feature_retrieval.get_target_values(
            project_name=test_case.project_name,
            entity_name=test_case.entity_name,
            join_keys=test_case.join_keys,
            feature_names=test_case.feature_names,
            start_time=test_case.start_time,
            end_time=test_case.end_time,
        )

        self.validate_result_structure(result, test_case)
        self.validate_expected_features(result, test_case)
        self.validate_row_count(result, test_case)
        self.validate_expected_entities(result, test_case)
        self.validate_date_range(result, test_case)
