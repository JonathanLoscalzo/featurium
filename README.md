# Featurium

Featurium is a lightweight, Python-based feature store designed to simplify the management and serving of machine learning features. It provides a simple yet powerful interface for feature engineering, storage, and retrieval, making it ideal for data scientists and ML engineers working on small to medium-sized projects.

## Features

- **Feature Management**: Easily define, version, and manage your ML features
- **Feature Storage**: Store features in various (SQL) backends (e.g., SQLite, PostgreSQL)
- **Feature Retrieval**: Quickly retrieve features for training and inference
<!-- - **Feature Serving**: Serve features in real-time for model inference -->
<!-- - **Feature Versioning**: Track different versions of your features -->
- **Metadata Management**: Keep track of feature definitions, data types, and descriptions
- **Simple API**: Intuitive Python interface for feature operations

## Use Cases

- Machine Learning Model Development
- Feature Engineering Pipelines
<!-- - Real-time Feature Serving -->
- Feature Version Control
- Feature Documentation and Discovery

## Getting Started

### Installation

```bash
pip install featurium
```

### Basic Usage

```python
from featurium import create_feature_store
from featurium.feature_store.schemas import (
    ProjectInput, EntityInput, FeatureInput,
    FeatureValueInput, JoinKeyInput, JoinKeyValueInput
)
from featurium.core.models import DataType

# Create the feature store (using DuckDB by default)
fs = create_feature_store(
    database_backend="duckdb",
    database_path="./featurium.ddb"
)

# Register a project
projects = fs.register_projects([
    ProjectInput(name="ecommerce", description="E-commerce features")
])

# Register an entity
entities = fs.register_entities([
    EntityInput(name="user", project_id=projects[0].id)
])

# Register features
features = fs.register_features([
    FeatureInput(
        name="age",
        project_id=projects[0].id,
        data_type=DataType.INTEGER,
        entity_ids=[entities[0].id]
    ),
    FeatureInput(
        name="country",
        project_id=projects[0].id,
        data_type=DataType.STRING,
        entity_ids=[entities[0].id]
    )
])

# Retrieve features
from featurium.feature_store.schemas import FeatureRetrievalInput

df = fs.get_feature_values(
    FeatureRetrievalInput(
        project_name="ecommerce",
        entity_name="user",
        join_keys=[123, 456],
        feature_names=["age", "country"]
    )
)
```

### Configuration

Featurium supports multiple ways to configure the feature store:

#### 1. Direct Parameters
```python
from featurium import create_feature_store

fs = create_feature_store(
    database_backend="duckdb",
    database_path="./featurium.ddb"
)
```

#### 2. Configuration File (TOML)
Create a `featurium.toml` file:
```toml
[featurium]
database_backend = "duckdb"
database_path = "./featurium.ddb"
create_tables = true
echo_sql = false
```

Then load it:
```python
from featurium import create_feature_store

fs = create_feature_store(config_file="featurium.toml")
```

#### 3. Environment Variables
```bash
export FEATURIUM_DATABASE_BACKEND=duckdb
export FEATURIUM_DATABASE_PATH=/path/to/featurium.ddb
```

```python
from featurium import create_feature_store

fs = create_feature_store()  # Automatically loads from env vars
```

For more details, see the [Configuration Guide](docs/CONFIGURATION.md).

## Project Structure

```
featurium/
├── core/           # Core feature store functionality
└── tests       # Unit tests for the feature Store
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License
TODO
<!-- This project is licensed under the MIT License - see the LICENSE file for details.  -->
