"""
Factory module for creating FeatureStore instances.

This module provides a factory function to create properly configured
FeatureStore instances with all required dependencies.
"""

from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from featurium.config import FeaturiumConfig
from featurium.core.models import Base
from featurium.feature_store.feature_store import FeatureStore
from featurium.services.registration.registration import RegistrationService
from featurium.services.retrieval.retrieval import RetrievalStore


class FeatureStoreFactory:
    """
    Factory for creating FeatureStore instances.

    This factory handles:
    - Loading configuration from various sources
    - Creating database engine and session
    - Creating tables if needed
    - Instantiating all required services
    - Building the FeatureStore instance

    Example:
        >>> # Using default configuration (from environment variables)
        >>> fs = FeatureStoreFactory.create()

        >>> # Using a configuration file
        >>> fs = FeatureStoreFactory.create_from_config_file("featurium.toml")

        >>> # Using explicit configuration
        >>> config = FeaturiumConfig(
        ...     database_backend="duckdb",
        ...     database_path="/path/to/featurium.ddb"
        ... )
        >>> fs = FeatureStoreFactory.create(config=config)

        >>> # Using a context manager (recommended)
        >>> with FeatureStoreFactory.create_session_context() as (fs, session):
        ...     projects = fs.list_projects()
    """

    @staticmethod
    def _build_feature_store(
        config: FeaturiumConfig,
        session: Optional[Session] = None,
    ) -> tuple[FeatureStore, Session]:
        """
        Private method to build FeatureStore with all dependencies.

        Args:
            config: Configuration object.
            session: Optional SQLAlchemy session. If None, creates a new session.

        Returns:
            Tuple of (FeatureStore, Session)
        """
        # Get database URL
        database_url = config.get_database_url()

        # Create engine
        engine = create_engine(
            database_url,
            echo=config.echo_sql,
        )

        # Create tables if needed
        if config.create_tables:
            Base.metadata.create_all(engine)

        # Create session if not provided
        if session is None:
            SessionLocal = sessionmaker(bind=engine)
            session = SessionLocal()

        # Create services with dependency injection
        registration_service = RegistrationService(db=session)
        retrieval_service = RetrievalStore(db=session)

        # Create and return FeatureStore
        feature_store = FeatureStore(
            registration_service=registration_service,
            retrieval_service=retrieval_service,
            db=session,
        )

        return feature_store, session

    @staticmethod
    def create(
        config: Optional[FeaturiumConfig] = None,
        session: Optional[Session] = None,
    ) -> FeatureStore:
        """
        Create a FeatureStore instance.

        Args:
            config: Configuration object. If None, loads from environment variables.
            session: SQLAlchemy session. If None, creates a new session.

        Returns:
            Configured FeatureStore instance.

        Note:
            If you provide your own session, you're responsible for managing
            its lifecycle (commit, rollback, close). Otherwise, consider using
            `create_session_context()` for automatic session management.

        Example:
            >>> fs = FeatureStoreFactory.create()
            >>> projects = fs.list_projects()
        """
        # Load config if not provided
        if config is None:
            config = FeaturiumConfig()

        # Build and return feature store
        feature_store, _ = FeatureStoreFactory._build_feature_store(config, session)
        return feature_store

    @staticmethod
    def create_from_config_file(config_path: str | Path) -> FeatureStore:
        """
        Create a FeatureStore from a configuration file.

        Args:
            config_path: Path to configuration file (.toml, .yaml, or .yml).

        Returns:
            Configured FeatureStore instance.

        Example:
            >>> fs = FeatureStoreFactory.create_from_config_file("featurium.toml")
            >>> fs = FeatureStoreFactory.create_from_config_file("config.yaml")
        """
        config = FeaturiumConfig.from_file(config_path)
        return FeatureStoreFactory.create(config=config)

    @staticmethod
    def create_from_env() -> FeatureStore:
        """
        Create a FeatureStore from environment variables.

        Environment variables should be prefixed with FEATURIUM_:
        - FEATURIUM_DATABASE_BACKEND
        - FEATURIUM_DATABASE_PATH
        - FEATURIUM_DATABASE_HOST
        - etc.

        Returns:
            Configured FeatureStore instance.

        Example:
            >>> import os
            >>> os.environ["FEATURIUM_DATABASE_BACKEND"] = "duckdb"
            >>> os.environ["FEATURIUM_DATABASE_PATH"] = "/path/to/featurium.ddb"
            >>> fs = FeatureStoreFactory.create_from_env()
        """
        config = FeaturiumConfig()
        return FeatureStoreFactory.create(config=config)

    @staticmethod
    @contextmanager
    def create_session_context(config: Optional[FeaturiumConfig] = None):
        """
        Create a FeatureStore with automatic session management.

        This is a context manager that handles session lifecycle automatically.

        Args:
            config: Configuration object. If None, loads from environment variables.

        Yields:
            Tuple of (FeatureStore, Session)

        Example:
            >>> with FeatureStoreFactory.create_session_context() as (fs, session):
            ...     # Use feature store
            ...     projects = fs.list_projects()
            ...
            ...     # Manual operations on session if needed
            ...     session.commit()
            ...
            ...     # Session is automatically closed after the block
        """
        # Load config if not provided
        if config is None:
            config = FeaturiumConfig()

        # Build feature store with all dependencies
        feature_store, session = FeatureStoreFactory._build_feature_store(config)

        try:
            yield feature_store, session
        finally:
            session.close()


# Convenience function for quick access
def create_feature_store(
    config_file: Optional[str | Path] = None,
    **config_kwargs,
) -> FeatureStore:
    """
    Convenience function to create a FeatureStore.

    This is the recommended way to create a FeatureStore for most use cases.

    Args:
        config_file: Optional path to configuration file.
        **config_kwargs: Configuration parameters passed directly to FeaturiumConfig.

    Returns:
        Configured FeatureStore instance.

    Examples:
        >>> # Using environment variables
        >>> fs = create_feature_store()

        >>> # Using a configuration file
        >>> fs = create_feature_store(config_file="featurium.toml")

        >>> # Using direct parameters
        >>> fs = create_feature_store(
        ...     database_backend="duckdb",
        ...     database_path="/path/to/featurium.ddb"
        ... )

        >>> # Mixing configuration file with overrides
        >>> fs = create_feature_store(
        ...     config_file="base_config.toml",
        ...     echo_sql=True  # Override echo_sql from file
        ... )
    """
    if config_file:
        # Load from file
        config = FeaturiumConfig.from_file(config_file)

        # Override with any direct parameters
        if config_kwargs:
            config_dict = config.model_dump()
            config_dict.update(config_kwargs)
            config = FeaturiumConfig(**config_dict)
    else:
        # Create from parameters (including env vars)
        config = FeaturiumConfig(**config_kwargs)

    return FeatureStoreFactory.create(config=config)
