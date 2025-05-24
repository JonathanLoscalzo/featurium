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
<!--
### Basic Usage

```python
from featurium import FeatureStore

# Initialize the feature store
store = FeatureStore()

# Define a feature
store.define_feature(
    name="user_avg_purchase",
    description="Average purchase amount per user",
    data_type="float"
)

# Store features
store.store_features(
    feature_name="user_avg_purchase",
    data=user_purchase_data
)

# Retrieve features
features = store.get_features(
    feature_names=["user_avg_purchase"],
    entity_ids=["user1", "user2"]
)
``` -->

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
