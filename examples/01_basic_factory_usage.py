"""
Basic Factory Usage Example

This example demonstrates different ways to create a FeatureStore instance
using the factory pattern.
"""

import tempfile
from pathlib import Path

from featurium import FeatureStoreFactory, FeaturiumConfig, create_feature_store
from featurium.core.models import DataType
from featurium.feature_store.schemas import (
    EntityInput,
    FeatureInput,
    FeatureRetrievalInput,
    FeatureValueInput,
    JoinKeyInput,
    JoinKeyValueInput,
    ProjectInput,
    TargetInput,
)


def example_1_simple_creation():
    """Example 1: Simple creation with direct parameters."""
    print("=" * 60)
    print("Example 1: Simple Creation")
    print("=" * 60)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name

    # Create feature store with direct parameters
    fs = create_feature_store(
        database_backend="sqlite",
        database_path=tmp_path,
    )

    print(f"✓ Created FeatureStore with database at: {tmp_path}")
    print(f"✓ Projects: {fs.list_projects()}")
    _ = fs  # Mark as used
    print()


def example_2_with_config_object():
    """Example 2: Using a configuration object."""
    print("=" * 60)
    print("Example 2: Using Configuration Object")
    print("=" * 60)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name

    # Create configuration
    config = FeaturiumConfig(
        database_backend="sqlite",
        database_path=tmp_path,
        echo_sql=False,
        create_tables=True,
    )

    # Create feature store from config
    fs = FeatureStoreFactory.create(config=config)

    print("✓ Created FeatureStore with config")
    print(f"  - Backend: {config.database_backend}")
    print(f"  - Path: {config.database_path}")
    print(f"  - Echo SQL: {config.echo_sql}")
    _ = fs  # Mark as used
    print()


def example_3_with_config_file():
    """Example 3: Using a configuration file."""
    print("=" * 60)
    print("Example 3: Using Configuration File")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create config file
        config_path = Path(tmpdir) / "featurium.toml"
        db_path = Path(tmpdir) / "featurium.db"

        config_path.write_text(
            f"""
[featurium]
database_backend = "sqlite"
database_path = "{db_path}"
create_tables = true
echo_sql = false
"""
        )

        # Create feature store from config file
        fs = create_feature_store(config_file=config_path)

        print(f"✓ Created FeatureStore from config file: {config_path}")
        _ = fs  # Mark as used
        print(f"✓ Database path: {db_path}")
        print()


def example_4_session_context():
    """Example 4: Using session context manager."""
    print("=" * 60)
    print("Example 4: Session Context Manager")
    print("=" * 60)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name

    config = FeaturiumConfig(
        database_backend="sqlite",
        database_path=tmp_path,
    )

    # Use context manager for automatic session management
    with FeatureStoreFactory.create_session_context(config) as (fs, session):
        print("✓ Feature store created with session context")
        _ = session  # Mark as used

        # Register a project
        projects = fs.register_projects([ProjectInput(name="demo_project", description="Demo project")])
        print(f"✓ Registered project: {projects[0].name}")

        # List projects
        project_names = fs.list_projects()
        print(f"✓ All projects: {project_names}")

    print("✓ Session automatically closed")
    print()


def example_5_complete_workflow():
    """Example 5: Complete workflow with factory - features, targets, and values."""
    print("=" * 60)
    print("Example 5: Complete Workflow")
    print("=" * 60)

    with tempfile.NamedTemporaryFile(dir="./", prefix="featurium_", suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name
        print(f"✓ Database path: {tmp_path}")

    # Create feature store
    fs = create_feature_store(
        database_backend="sqlite",
        database_path=tmp_path,
    )
    # fs = create_feature_store(
    #     database_backend="postgresql",
    #     database_host="127.0.0.1",
    #     database_port=5432,
    #     database_name="featurium",
    #     database_user="postgres",
    #     database_password="example",
    #     create_tables=True,
    #     echo_sql=False,
    # )

    # 1. Register project
    projects = fs.register_projects([ProjectInput(name="ecommerce", description="E-commerce features")])
    project = projects[0]
    print(f"✓ Registered project: {project.name}")

    # 2. Register entity
    entities = fs.register_entities(
        [EntityInput(name="user", project_id=project.id, description="User entity")]
    )
    entity = entities[0]
    print(f"✓ Registered entity: {entity.name}")

    # 3. Register join key
    join_keys = fs.register_join_keys(
        [JoinKeyInput(name="user_id", entity_id=entity.id, description="User identifier")]
    )
    join_key = join_keys[0]
    print(f"✓ Registered join key: {join_key.name}")

    # 4. Register features
    features = fs.register_features(
        [
            FeatureInput(
                name="age",
                project_id=project.id,
                data_type=DataType.INTEGER,
                description="User age",
                entity_ids=[entity.id],
            ),
            FeatureInput(
                name="country",
                project_id=project.id,
                data_type=DataType.STRING,
                description="User country",
                entity_ids=[entity.id],
            ),
        ]
    )
    print(f"✓ Registered {len(features)} features")

    # 5. Register targets
    targets = fs.register_targets(
        [
            TargetInput(
                name="purchased",
                project_id=project.id,
                data_type=DataType.BOOLEAN,
                description="Whether user made a purchase",
                entity_ids=[entity.id],
            ),
        ]
    )
    print(f"✓ Registered {len(targets)} targets")

    # 6. Register join key values (for 3 users)
    user_ids = [101, "102", "103"]
    jkv_list = fs.register_join_key_values(
        [JoinKeyValueInput(join_key_id=join_key.id, value=str(uid)) for uid in user_ids]
    )
    print(f"✓ Registered {len(jkv_list)} join key values")

    # 7. Register feature values
    feature_values = []
    # Age values for each user
    ages = [25, 34, 28]
    for jkv, age in zip(jkv_list, ages):
        feature_values.append(
            FeatureValueInput(
                attribute_id=features[0].id,  # age feature
                join_key_value_id=jkv.id,
                value=age,
            )
        )
    # Country values for each user
    countries = ["US", "UK", "CA"]
    for jkv, country in zip(jkv_list, countries):
        feature_values.append(
            FeatureValueInput(
                attribute_id=features[1].id,  # country feature
                join_key_value_id=jkv.id,
                value=country,
            )
        )
    fs.register_feature_values(feature_values)
    print(f"✓ Registered {len(feature_values)} feature values")

    # 8. Register target values
    target_values = []
    purchased_values = [True, False, True]
    for jkv, purchased in zip(jkv_list, purchased_values):
        target_values.append(
            FeatureValueInput(
                attribute_id=targets[0].id,  # purchased target
                join_key_value_id=jkv.id,
                value=purchased,
            )
        )
    fs.register_feature_values(target_values)
    print(f"✓ Registered {len(target_values)} target values")

    # 9. Retrieve feature values
    print("\n--- Retrieving Data ---")
    feature_df = fs.get_feature_values(
        FeatureRetrievalInput(
            project_name=project.name,
            entity_name=entity.name,
            join_keys=user_ids,
        )
    )
    print("✓ Retrieved features:")
    print(feature_df)

    # # 10. Retrieve target values
    # target_df = fs.get_target_values(
    #     FeatureRetrievalInput(
    #         project_name=project.name,
    #         entity_name=entity.name,
    #         join_keys=user_ids,
    #     )
    # )
    # print("\n✓ Retrieved targets:")
    # print(target_df)

    # # 11. Retrieve all values (features + targets)
    # all_df = fs.get_all_values(
    #     FeatureRetrievalInput(
    #         project_name=project.name,
    #         entity_name=entity.name,
    #         join_keys=user_ids,
    #     )
    # )
    # print("\n✓ Retrieved all values (features + targets):")
    # print(all_df)

    # print()


def example_6_with_overrides():
    """Example 6: Config file with parameter overrides."""
    print("=" * 60)
    print("Example 6: Config File with Overrides")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create config file
        config_path = Path(tmpdir) / "featurium.toml"
        db_path = Path(tmpdir) / "featurium.db"

        config_path.write_text(
            f"""
[featurium]
database_backend = "sqlite"
database_path = "{db_path}"
echo_sql = false
"""
        )

        # Load config from file but override echo_sql
        fs = create_feature_store(
            config_file=config_path,
            echo_sql=True,  # Override this setting
        )

        print("✓ Created FeatureStore from config file with overrides")
        print(f"  - Config file: {config_path}")
        print("  - Override: echo_sql = True")
        _ = fs  # Mark as used
        print()


def main():
    """Run all examples."""
    print("\n")
    print("🚀 Featurium Factory Examples")
    print("=" * 60)
    print()

    try:
        example_1_simple_creation()
        example_2_with_config_object()
        example_3_with_config_file()
        example_4_session_context()
        example_5_complete_workflow()
        example_6_with_overrides()

        print("=" * 60)
        print("✅ All examples completed successfully!")
        print("=" * 60)
    except Exception as e:
        print(f"❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
