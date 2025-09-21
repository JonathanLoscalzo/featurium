# # import pytest

# # from featurium.feature_store import FeatureStore


# # def test_feature_store_initialization():
# #     """Test FeatureStore initialization."""
# #     store = FeatureStore()
# #     assert store.storage_backend is None
# #     assert isinstance(store._features, dict)
# #     assert len(store._features) == 0


# # def test_define_feature():
# #     """Test feature definition."""
# #     store = FeatureStore()
# #     store.define_feature(
# #         name="test_feature",
# #         description="A test feature",
# #         data_type="float",
# #     )

# #     assert "test_feature" in store._features
# #     assert store._features["test_feature"]["description"] == "A test feature"
# #     assert store._features["test_feature"]["data_type"] == "float"
# #     assert store._features["test_feature"]["version"] == "1.0.0"


# # def test_store_features_undefined():
# #     """Test storing features for undefined feature"""
# #     store = FeatureStore()
# #     with pytest.raises(ValueError):
# #         store.store_features("undefined_feature", {"test": 1})


# # def test_get_features():
# #     """Test feature retrieval"""
# #     store = FeatureStore()
# #     features = store.get_features(
# #         feature_names=["test_feature"],
# #         entity_ids=["entity1", "entity2"],
# #     )
# #     assert isinstance(features, dict)


# from datetime import datetime, timedelta

# import pytest
# from sqlalchemy.orm import Session

# from featurium.core.models import DataType
# from featurium.feature_store import FeatureStore
# from tests.utils.factories import (
#     create_entity,
#     create_feature,
#     create_feature_value,
#     create_join_key,
#     create_join_key_value,
#     create_project,
# )


# @pytest.mark.usefixtures("cleanup")
# class TestFeatureStore:
#     """Test the FeatureStore class."""

#     def test_feature_store_initialization(self, db: Session):
#         """Test FeatureStore initialization."""
#         store = FeatureStore(db)
#         assert store.db == db

#     def test_list_projects(self, db: Session):
#         """Test listing projects."""
#         store = FeatureStore(db)
#         projects = store.list_projects()
#         assert len(projects) == 0

#         create_project(db, "project:animals")
#         create_project(db, "project:plants")

#         projects = store.list_projects()
#         assert len(projects) == 2
#         assert "project:animals" in projects
#         assert "project:plants" in projects

#     def test_list_features(self, db: Session):
#         """Test listing features."""
#         store = FeatureStore(db)

#         project = create_project(db, "project:animals")
#         create_feature(db, "feature:avg_legs", project, DataType.FLOAT)

#         project = create_project(db, "project:plants")
#         create_feature(db, "feature:number_of_leaves", project, DataType.INTEGER)

#         db.commit()

#         # List features
#         features = store.list_features()
#         assert len(features) == 2
#         assert "feature:avg_legs" in features
#         assert "feature:number_of_leaves" in features

#         # List features for a animal project
#         features = store.list_features(project_name="project:animals")
#         assert len(features) == 1
#         assert "feature:avg_legs" in features

#         # List features for a plant project
#         features = store.list_features(project_name="project:plants")
#         assert len(features) == 1
#         assert "feature:number_of_leaves" in features

#     def test_list_entities(self, db: Session):
#         """Test listing entities."""
#         store = FeatureStore(db)

#         # Create a project
#         project = create_project(db, "project:animals")
#         create_entity(db, "entity:dog", project)
#         create_entity(db, "entity:cat", project)

#         project = create_project(db, "project:plants")
#         create_entity(db, "entity:tree", project)
#         create_entity(db, "entity:flower", project)

#         db.commit()

#         entities = store.list_entities()
#         assert len(entities) == 4
#         assert "entity:dog" in entities
#         assert "entity:cat" in entities
#         assert "entity:tree" in entities
#         assert "entity:flower" in entities

#         # List entities for a animal project
#         entities = store.list_entities(project_name="project:animals")
#         assert len(entities) == 2
#         assert "entity:dog" in entities
#         assert "entity:cat" in entities

#         # List entities for a plant project
#         entities = store.list_entities(project_name="project:plants")
#         assert len(entities) == 2
#         assert "entity:tree" in entities
#         assert "entity:flower" in entities

#     def test_list_of_features_by_entities(self, db: Session):
#         """Test listing features by entities."""
#         store = FeatureStore(db)

#         project = create_project(db, "project:animals")
#         entity = create_entity(db, "entity:dog", project)
#         join_key = create_join_key(db, "join_key:identity", entity, DataType.STRING)
#         join_key_value_1 = create_join_key_value(db, join_key, {"string": "firulais"})
#         join_key_value_2 = create_join_key_value(db, join_key, {"string": "chiquito"})

#         feature = create_feature(db, "feature:avg_legs", project, DataType.INTEGER)
#         feature.entities.append(entity)

#         feature_value_1_1 = create_feature_value(
#             db, feature, {"integer": 4}, datetime.now() - timedelta(days=30)
#         )
#         feature_value_1_2 = create_feature_value(
#             db, feature, {"integer": 3}, datetime.now() - timedelta(days=60)
#         )
#         join_key_value_1.feature_values.append(feature_value_1_1)
#         join_key_value_1.feature_values.append(feature_value_1_2)

#         feature_value_2_1 = create_feature_value(
#             db, feature, {"integer": 3}, datetime.now() - timedelta(days=10)
#         )
#         feature_value_2_2 = create_feature_value(
#             db, feature, {"integer": 2}, datetime.now() - timedelta(days=20)
#         )
#         join_key_value_2.feature_values.append(feature_value_2_1)
#         join_key_value_2.feature_values.append(feature_value_2_2)

#         db.commit()
#         timestamp = datetime.now() - timedelta(days=10)
#         result = store.get_historical_features(
#             # project_name="project:animals",
#             # entity_name="entity:dog",
#             # feature_names=["feature:avg_legs"],
#             # join_key_values=[{"string": "firulais"}],
#             timestamp=timestamp,
#         )

#         print(result)
