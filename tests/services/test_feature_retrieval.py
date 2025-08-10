from datetime import datetime, timedelta

import pytest
from pytest import Session

from featurium.core.models import DataType
from featurium.services.feature_retrival import FeatureRetrieval
from tests.utils.factories import (
    create_entity,
    create_feature,
    create_feature_value,
    create_join_key,
    create_join_key_value,
    create_project,
)


@pytest.fixture()
def setup_dogs_project(db: Session):
    """Setup the dogs project"""
    project = create_project(db, "project:animals")

    # entity dog
    entity = create_entity(db, "entity:dog", project)
    join_key = create_join_key(db, "dog:name", entity)
    join_key_value_1 = create_join_key_value(db, join_key, "firulais")
    join_key_value_2 = create_join_key_value(db, join_key, "chiquito")

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
    join_key_value_1.feature_values.append(feature_value_1_1)
    join_key_value_1.feature_values.append(feature_value_1_2)

    feature_value_a_1 = create_feature_value(
        db, feature2, {"boolean": True}, datetime.now() - timedelta(days=28)
    )
    feature_value_a_2 = create_feature_value(
        db, feature2, {"boolean": False}, datetime.now() - timedelta(days=59)
    )
    join_key_value_1.feature_values.append(feature_value_a_1)
    join_key_value_1.feature_values.append(feature_value_a_2)

    # chiquito
    feature_value_2_1 = create_feature_value(
        db, feature, {"integer": 2}, datetime.now() - timedelta(days=10)
    )
    feature_value_2_2 = create_feature_value(
        db, feature, {"integer": 3}, datetime.now() - timedelta(days=20)
    )
    join_key_value_2.feature_values.append(feature_value_2_1)
    join_key_value_2.feature_values.append(feature_value_2_2)

    feature_value_b_1 = create_feature_value(
        db, feature2, {"boolean": True}, datetime.now() - timedelta(days=5)
    )
    feature_value_b_2 = create_feature_value(
        db, feature2, {"boolean": False}, datetime.now() - timedelta(days=15)
    )
    join_key_value_2.feature_values.append(feature_value_b_1)
    join_key_value_2.feature_values.append(feature_value_b_2)


@pytest.mark.usefixtures("setup_dogs_project")
class TestFeatureRetrieval:

    def test_feature_retrieval(self, db: Session):
        """Test the feature retrieval"""
        feature_retrieval = FeatureRetrieval(db)
        feature_retrieval.get_feature_values(
            "project:animals", "entity:dog", ["chiquito"], ["feature:dog:avg_legs"]
        )
