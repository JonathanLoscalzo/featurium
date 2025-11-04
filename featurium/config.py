"""
Configuration module for Featurium.

This module provides configuration management for the feature store,
supporting both configuration files (TOML/YAML) and environment variables.
"""

import os
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseBackend(str, Enum):
    """Supported database backends."""

    DUCKDB = "duckdb"  # not used yet
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"


class FeaturiumConfig(BaseSettings):
    """
    Configuration for Featurium Feature Store.

    This class uses Pydantic Settings to load configuration from multiple sources:
    1. Environment variables (prefixed with FEATURIUM_)
    2. Configuration files (.toml or .yaml)
    3. Default values

    Attributes:
        database_backend: The database backend to use (duckdb, sqlite, postgresql).
        database_url: Full database connection URL. If provided, overrides other database settings.
        database_path: Path to the database file (for file-based databases like DuckDB/SQLite).
        database_host: Database host (for server-based databases).
        database_port: Database port (for server-based databases).
        database_name: Database name.
        database_user: Database username (for server-based databases).
        database_password: Database password (for server-based databases).
        create_tables: Whether to create tables on startup.
        echo_sql: Whether to echo SQL queries (for debugging).

    Example:
        >>> # From environment variables
        >>> os.environ["FEATURIUM_DATABASE_BACKEND"] = "duckdb"
        >>> os.environ["FEATURIUM_DATABASE_PATH"] = "/path/to/featurium.ddb"
        >>> config = FeaturiumConfig()

        >>> # From TOML file
        >>> config = FeaturiumConfig(_env_file="config.toml")

        >>> # Direct instantiation
        >>> config = FeaturiumConfig(
        ...     database_backend="duckdb",
        ...     database_path="/path/to/featurium.ddb"
        ... )
    """

    model_config = SettingsConfigDict(
        env_prefix="FEATURIUM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Database configuration
    database_backend: DatabaseBackend = Field(
        default=DatabaseBackend.SQLITE,
        description="Database backend to use",
    )

    database_url: Optional[str] = Field(
        default=None,
        description="Full database connection URL (overrides other settings)",
    )

    database_path: Optional[str] = Field(
        default=None,
        description="Path to database file (for DuckDB/SQLite)",
    )

    database_host: Optional[str] = Field(
        default="localhost",
        description="Database host (for PostgreSQL)",
    )

    database_port: Optional[int] = Field(
        default=5432,
        description="Database port (for PostgreSQL)",
    )

    database_name: Optional[str] = Field(
        default="featurium",
        description="Database name",
    )

    database_user: Optional[str] = Field(
        default=None,
        description="Database username (for PostgreSQL)",
    )

    database_password: Optional[str] = Field(
        default=None,
        description="Database password (for PostgreSQL)",
    )

    # Feature store configuration
    create_tables: bool = Field(
        default=True,
        description="Whether to create tables on startup",
    )

    echo_sql: bool = Field(
        default=False,
        description="Whether to echo SQL queries (for debugging)",
    )

    @field_validator("database_path", mode="before")
    @classmethod
    def expand_database_path(cls, v):
        """Expand ~ and environment variables in database path."""
        if v is None:
            return v
        return os.path.expanduser(os.path.expandvars(v))

    def get_database_url(self) -> str:
        """
        Get the database connection URL.

        Returns:
            Database connection URL string.

        Raises:
            ValueError: If configuration is invalid.
        """
        # If explicit URL is provided, use it
        if self.database_url:
            return self.database_url

        # Build URL based on backend
        if self.database_backend == DatabaseBackend.DUCKDB:
            raise ValueError("DuckDB backend is not supported yet.")
            # if not self.database_path:
            #     raise ValueError(
            #         "database_path is required for DuckDB backend. "
            #         "Set FEATURIUM_DATABASE_PATH or provide database_path in config."
            #     )
            # return f"duckdb:///{self.database_path}"

        elif self.database_backend == DatabaseBackend.SQLITE:
            if not self.database_path:
                raise ValueError(
                    "database_path is required for SQLite backend. "
                    "Set FEATURIUM_DATABASE_PATH or provide database_path in config."
                )
            return f"sqlite:///{self.database_path}"

        elif self.database_backend == DatabaseBackend.POSTGRESQL:
            if not self.database_user or not self.database_password:
                raise ValueError(
                    "database_user and database_password are required for PostgreSQL. "
                    "Set FEATURIUM_DATABASE_USER and FEATURIUM_DATABASE_PASSWORD."
                )

            return (
                f"postgresql://{self.database_user}:{self.database_password}"
                f"@{self.database_host}:{self.database_port}/{self.database_name}"
            )

        else:
            raise ValueError(f"Unsupported database backend: {self.database_backend}")

    @classmethod
    def from_toml(cls, path: str | Path) -> "FeaturiumConfig":
        """
        Load configuration from a TOML file.

        Args:
            path: Path to the TOML configuration file.

        Returns:
            FeaturiumConfig instance.

        Example:
            >>> config = FeaturiumConfig.from_toml("featurium.toml")
        """
        import tomllib

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with open(path, "rb") as f:
            data = tomllib.load(f)

        # Handle nested structure if config is under a "featurium" key
        featurium_data = data.get("featurium", data)

        return cls(**featurium_data)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "FeaturiumConfig":
        """
        Load configuration from a YAML file.

        Args:
            path: Path to the YAML configuration file.

        Returns:
            FeaturiumConfig instance.

        Example:
            >>> config = FeaturiumConfig.from_yaml("featurium.yaml")
        """
        try:
            import yaml  # type: ignore
        except ImportError:
            raise ImportError(
                "PyYAML is required to load YAML configuration files. " "Install it with: pip install pyyaml"
            )

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with open(path, "r") as f:
            data = yaml.safe_load(f)

        # Handle nested structure if config is under a "featurium" key
        featurium_data = data.get("featurium", data)

        return cls(**featurium_data)

    @classmethod
    def from_file(cls, path: str | Path) -> "FeaturiumConfig":
        """
        Load configuration from a file (auto-detect format).

        Supports .toml and .yaml/.yml files.

        Args:
            path: Path to the configuration file.

        Returns:
            FeaturiumConfig instance.

        Example:
            >>> config = FeaturiumConfig.from_file("config.toml")
            >>> config = FeaturiumConfig.from_file("config.yaml")
        """
        path = Path(path)
        suffix = path.suffix.lower()

        if suffix == ".toml":
            return cls.from_toml(path)
        elif suffix in [".yaml", ".yml"]:
            return cls.from_yaml(path)
        else:
            raise ValueError(
                f"Unsupported configuration file format: {suffix}. " "Supported formats: .toml, .yaml, .yml"
            )
