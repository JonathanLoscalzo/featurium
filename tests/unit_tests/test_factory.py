"""Tests for the FeatureStore factory."""

import os
import tempfile

import pytest

from featurium import FeatureStoreFactory, FeaturiumConfig, create_feature_store
from featurium.feature_store.feature_store import FeatureStore


class TestFeaturiumConfig:
    """Test configuration loading."""

    def test_default_config(self):
        """Test creating a config with defaults."""
        config = FeaturiumConfig(database_path="test.db")
        assert config.database_backend == "sqlite"
        assert config.database_path == "test.db"
        assert config.create_tables is True
        assert config.echo_sql is False

    @pytest.mark.skip("DuckDB backend is not supported yet.")
    def test_get_database_url_duckdb(self):
        """Test generating DuckDB URL."""
        pass

    def test_get_database_url_sqlite(self):
        """Test generating SQLite URL."""
        config = FeaturiumConfig(
            database_backend="sqlite",
            database_path="/tmp/test.db",
        )
        url = config.get_database_url()
        assert url == "sqlite:////tmp/test.db"

    def test_get_database_url_postgresql(self):
        """Test generating PostgreSQL URL."""
        config = FeaturiumConfig(
            database_backend="postgresql",
            database_host="localhost",
            database_port=5432,
            database_name="featurium",
            database_user="testuser",
            database_password="testpass",
        )
        url = config.get_database_url()
        assert url == "postgresql://testuser:testpass@localhost:5432/featurium"

    def test_explicit_database_url(self):
        """Test using explicit database URL."""
        config = FeaturiumConfig(
            database_url="sqlite:///custom/path.db",
        )
        url = config.get_database_url()
        assert url == "sqlite:///custom/path.db"

    def test_missing_database_path_raises_error(self):
        """Test that missing database path raises error for file-based backends."""
        config = FeaturiumConfig(database_backend="sqlite")
        with pytest.raises(ValueError, match="database_path is required"):
            config.get_database_url()

    def test_missing_postgresql_credentials_raises_error(self):
        """Test that missing PostgreSQL credentials raises error."""
        config = FeaturiumConfig(database_backend="postgresql")
        with pytest.raises(ValueError, match="database_user and database_password are required"):
            config.get_database_url()

    def test_expand_database_path_with_tilde(self):
        """Test that ~ is expanded in database paths."""
        config = FeaturiumConfig(
            database_backend="duckdb",
            database_path="~/test.ddb",
        )
        assert config.database_path.startswith(os.path.expanduser("~"))

    def test_from_toml(self, tmp_path):
        """Test loading config from TOML file."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            """
[featurium]
database_backend = "duckdb"
database_path = "/tmp/test.ddb"
create_tables = false
echo_sql = true
"""
        )

        config = FeaturiumConfig.from_toml(config_file)
        assert config.database_backend == "duckdb"
        assert config.database_path == "/tmp/test.ddb"
        assert config.create_tables is False
        assert config.echo_sql is True

    def test_from_toml_without_featurium_key(self, tmp_path):
        """Test loading config from TOML file without 'featurium' key."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            """
database_backend = "sqlite"
database_path = "/tmp/test.db"
"""
        )

        config = FeaturiumConfig.from_toml(config_file)
        assert config.database_backend == "sqlite"
        assert config.database_path == "/tmp/test.db"

    def test_from_file_auto_detect_toml(self, tmp_path):
        """Test auto-detecting TOML format."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            """
[featurium]
database_backend = "duckdb"
database_path = "/tmp/test.ddb"
"""
        )

        config = FeaturiumConfig.from_file(config_file)
        assert config.database_backend == "duckdb"

    def test_from_file_unsupported_format_raises_error(self, tmp_path):
        """Test that unsupported file format raises error."""
        config_file = tmp_path / "config.json"
        config_file.write_text("{}")

        with pytest.raises(ValueError, match="Unsupported configuration file format"):
            FeaturiumConfig.from_file(config_file)


class TestFeatureStoreFactory:
    """Test FeatureStore factory."""

    def test_create_with_config(self):
        """Test creating FeatureStore with explicit config."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            config = FeaturiumConfig(
                database_backend="sqlite",
                database_path=tmp_path,
                create_tables=True,
            )

            fs = FeatureStoreFactory.create(config=config)
            assert isinstance(fs, FeatureStore)
            assert fs.db is not None
            assert fs.registration_service is not None
            assert fs.retrieval_service is not None

        finally:
            # Cleanup
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_create_from_config_file(self, tmp_path):
        """Test creating FeatureStore from config file."""
        db_path = tmp_path / "test.db"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f"""
[featurium]
database_backend = "sqlite"
database_path = "{db_path}"
create_tables = true
"""
        )

        fs = FeatureStoreFactory.create_from_config_file(config_file)
        assert isinstance(fs, FeatureStore)

    def test_create_session_context(self):
        """Test creating FeatureStore with session context manager."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            config = FeaturiumConfig(
                database_backend="sqlite",
                database_path=tmp_path,
                create_tables=True,
            )

            with FeatureStoreFactory.create_session_context(config) as (fs, session):
                assert isinstance(fs, FeatureStore)
                assert session is not None

                # Test that we can use the feature store
                projects = fs.list_projects()
                assert isinstance(projects, list)

        finally:
            # Cleanup
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_create_feature_store_convenience_function(self):
        """Test convenience function for creating feature store."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            fs = create_feature_store(
                database_backend="sqlite",
                database_path=tmp_path,
            )
            assert isinstance(fs, FeatureStore)

        finally:
            # Cleanup
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_create_feature_store_with_config_file(self, tmp_path):
        """Test convenience function with config file."""
        db_path = tmp_path / "test.db"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f"""
[featurium]
database_backend = "sqlite"
database_path = "{db_path}"
"""
        )

        fs = create_feature_store(config_file=config_file)
        assert isinstance(fs, FeatureStore)

    def test_create_feature_store_with_overrides(self, tmp_path):
        """Test convenience function with config file and overrides."""
        db_path = tmp_path / "test.db"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f"""
[featurium]
database_backend = "sqlite"
database_path = "{db_path}"
echo_sql = false
"""
        )

        fs = create_feature_store(config_file=config_file, echo_sql=True)
        assert isinstance(fs, FeatureStore)
