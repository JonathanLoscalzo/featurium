"""
Feature Store Module

This module provides the main Feature Store functionality for managing
and serving machine learning features.
"""

from featurium.feature_store.feature_store import FeatureStore
from featurium.feature_store.protocols import RegistrationServiceProtocol, RetrievalServiceProtocol
from featurium.feature_store.schemas import (
    AssociationInput,
    AssociationOutput,
    EntityInput,
    EntityOutput,
    FeatureInput,
    FeatureOutput,
    FeatureRetrievalInput,
    FeatureValueInput,
    FeatureValueOutput,
    JoinKeyInput,
    JoinKeyOutput,
    JoinKeyValueInput,
    JoinKeyValueOutput,
    ProjectInput,
    ProjectOutput,
    TargetInput,
    TargetOutput,
)

__all__ = [
    # Main class
    "FeatureStore",
    # Protocols
    "RegistrationServiceProtocol",
    "RetrievalServiceProtocol",
    # Input schemas
    "ProjectInput",
    "EntityInput",
    "FeatureInput",
    "TargetInput",
    "JoinKeyInput",
    "JoinKeyValueInput",
    "FeatureValueInput",
    "AssociationInput",
    "FeatureRetrievalInput",
    # Output schemas
    "ProjectOutput",
    "EntityOutput",
    "FeatureOutput",
    "TargetOutput",
    "JoinKeyOutput",
    "JoinKeyValueOutput",
    "FeatureValueOutput",
    "AssociationOutput",
]
