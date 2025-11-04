# Featurium Configuration Guide

This guide explains how to configure and create Featurium FeatureStore instances.

## Table of Contents

- [Quick Start](#quick-start)
- [Configuration Methods](#configuration-methods)
- [Configuration Options](#configuration-options)
- [Examples](#examples)
- [Best Practices](#best-practices)

## Quick Start

The simplest way to create a FeatureStore:

```python
from featurium import create_feature_store

# Using environment variables or defaults
fs = create_feature_store(
    database_backend="duckdb",
    database_path="./featurium.ddb"
)

# Use the feature store
projects = fs.list_projects()
```

## Configuration Methods

Featurium supports multiple configuration methods, listed in order of precedence:

### 1. Direct Parameters (Highest Priority)

```python
from featurium import create_feature_store

fs = create_feature_store(
    database_backend="duckdb",
    database_path="/path/to/featurium.ddb",
    create_tables=True,
    echo_sql=False
)
```

### 2. Configuration Files

#### TOML Configuration

Create a `featurium.toml` file:

```toml
[featurium]
database_backend = "duckdb"
database_path = "./featurium.ddb"
create_tables = true
echo_sql = false
```

Load it:

```python
from featurium import create_feature_store

fs = create_feature_store(config_file="featurium.toml")
```

#### YAML Configuration

Create a `featurium.yaml` file:

```yaml
featurium:
  database_backend: duckdb
  database_path: ./featurium.ddb
  create_tables: true
  echo_sql: false
```

Load it:

```python
from featurium import create_feature_store

fs = create_feature_store(config_file="featurium.yaml")
```

### 3. Environment Variables (Lowest Priority)

Set environment variables with the `FEATURIUM_` prefix:

```bash
export FEATURIUM_DATABASE_BACKEND=duckdb
export FEATURIUM_DATABASE_PATH=/path/to/featurium.ddb
export FEATURIUM_CREATE_TABLES=true
export FEATURIUM_ECHO_SQL=false
```

Then create the feature store:

```python
from featurium import create_feature_store

# Automatically loads from environment variables
fs = create_feature_store()
```

### Combining Configuration Methods

You can combine configuration methods. Direct parameters override config files, which override environment variables:

```python
from featurium import create_feature_store

# Load base config from file, override specific settings
fs = create_feature_store(
    config_file="base_config.toml",
    echo_sql=True  # Override echo_sql from file
)
```

## Configuration Options

### Database Configuration

| Option | Type | Description | Default | Required |
|--------|------|-------------|---------|----------|
| `database_backend` | string | Database backend: `duckdb`, `sqlite`, `postgresql` | `duckdb` | No |
| `database_url` | string | Full database connection URL (overrides other settings) | `None` | No |
| `database_path` | string | Path to database file (DuckDB/SQLite) | `None` | Yes (for DuckDB/SQLite) |
| `database_host` | string | Database host (PostgreSQL) | `localhost` | Yes (for PostgreSQL) |
| `database_port` | int | Database port (PostgreSQL) | `5432` | Yes (for PostgreSQL) |
| `database_name` | string | Database name (PostgreSQL) | `featurium` | Yes (for PostgreSQL) |
| `database_user` | string | Database username (PostgreSQL) | `None` | Yes (for PostgreSQL) |
| `database_password` | string | Database password (PostgreSQL) | `None` | Yes (for PostgreSQL) |

### Feature Store Configuration

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `create_tables` | bool | Whether to create tables on startup | `true` |
| `echo_sql` | bool | Whether to echo SQL queries (for debugging) | `false` |

## Examples

### Example 1: DuckDB (File-based)

```python
from featurium import create_feature_store

fs = create_feature_store(
    database_backend="duckdb",
    database_path="./featurium.ddb"
)
```

### Example 2: SQLite (File-based)

```python
from featurium import create_feature_store

fs = create_feature_store(
    database_backend="sqlite",
    database_path="./featurium.db"
)
```

### Example 3: PostgreSQL (Server-based)

```python
from featurium import create_feature_store

fs = create_feature_store(
    database_backend="postgresql",
    database_host="localhost",
    database_port=5432,
    database_name="featurium",
    database_user="myuser",
    database_password="mypassword"
)
```

### Example 4: Custom Database URL

```python
from featurium import create_feature_store

fs = create_feature_store(
    database_url="postgresql://user:password@localhost:5432/featurium"
)
```

### Example 5: Using Configuration Object

```python
from featurium import FeaturiumConfig, FeatureStoreFactory

config = FeaturiumConfig(
    database_backend="duckdb",
    database_path="./featurium.ddb",
    echo_sql=True  # Enable SQL logging
)

fs = FeatureStoreFactory.create(config=config)
```

### Example 6: Session Context Manager

For automatic session management:

```python
from featurium import FeaturiumConfig, FeatureStoreFactory

config = FeaturiumConfig(
    database_backend="duckdb",
    database_path="./featurium.ddb"
)

with FeatureStoreFactory.create_session_context(config) as (fs, session):
    # Use feature store
    projects = fs.list_projects()

    # Perform operations
    # ...

    # Session is automatically closed after the block
```

### Example 7: Using Environment Variables

```bash
# Set environment variables
export FEATURIUM_DATABASE_BACKEND=duckdb
export FEATURIUM_DATABASE_PATH=~/data/featurium.ddb
export FEATURIUM_CREATE_TABLES=true
```

```python
from featurium import create_feature_store

# Automatically uses environment variables
fs = create_feature_store()
```

## Best Practices

### 1. Use Configuration Files for Production

For production deployments, use configuration files (TOML or YAML) to keep settings organized and version-controlled:

```python
# production.toml
fs = create_feature_store(config_file="production.toml")
```

### 2. Use Environment Variables for Secrets

Don't commit sensitive information (passwords) to configuration files. Use environment variables:

```toml
# featurium.toml
[featurium]
database_backend = "postgresql"
database_host = "db.example.com"
database_name = "featurium"
# Don't include password here
```

```bash
# Set password via environment variable
export FEATURIUM_DATABASE_PASSWORD=secret
```

```python
# Combine config file and environment variables
fs = create_feature_store(config_file="featurium.toml")
```

### 3. Use Session Context for Long-running Operations

When performing multiple operations, use the session context manager to ensure proper cleanup:

```python
from featurium import FeatureStoreFactory, FeaturiumConfig

config = FeaturiumConfig(database_path="./featurium.ddb")

with FeatureStoreFactory.create_session_context(config) as (fs, session):
    # Register entities
    fs.register_projects([...])

    # Register features
    fs.register_features([...])

    # Retrieve data
    df = fs.get_feature_values(...)
```

### 4. Path Expansion

Database paths support `~` expansion and environment variables:

```python
fs = create_feature_store(
    database_path="~/data/featurium.ddb"  # Expands to home directory
)

# Or with environment variables
fs = create_feature_store(
    database_path="$DATA_DIR/featurium.ddb"  # Expands $DATA_DIR
)
```

### 5. Different Configs for Different Environments

```python
import os

env = os.getenv("ENV", "development")
config_file = f"config.{env}.toml"

fs = create_feature_store(config_file=config_file)
```

### 6. Enable SQL Logging for Development

```python
fs = create_feature_store(
    database_path="./featurium.ddb",
    echo_sql=True  # See SQL queries for debugging
)
```

## Troubleshooting

### "database_path is required for DuckDB backend"

Make sure you provide a `database_path` when using DuckDB or SQLite:

```python
fs = create_feature_store(
    database_backend="duckdb",
    database_path="./featurium.ddb"  # Required!
)
```

### "database_user and database_password are required for PostgreSQL"

PostgreSQL requires credentials:

```python
fs = create_feature_store(
    database_backend="postgresql",
    database_user="myuser",
    database_password="mypassword",
    database_name="featurium"
)
```

### "Cannot find implementation or library stub for module named 'pydantic_settings'"

Install the required dependency:

```bash
pip install pydantic-settings
```

### "PyYAML is required to load YAML configuration files"

Install PyYAML if you want to use YAML configuration:

```bash
pip install pyyaml
```
