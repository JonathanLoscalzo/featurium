"""
Feature Store Schemas

This module contains all Pydantic models used as parameter objects (inputs/outputs)
for the Feature Store operations.
"""

from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, field_validator

from featurium.core.models import AttributeType, DataType

# region Registration Input Schemas


class ProjectInput(BaseModel):
    """Input schema for creating a project."""

    name: str
    description: str = ""
    meta: Optional[dict] = None


class EntityInput(BaseModel):
    """Input schema for creating an entity."""

    name: str
    project_id: Optional[int] = None
    project_name: Optional[str] = None
    description: str = ""
    meta: Optional[dict] = None

    @field_validator("project_id", "project_name")
    @classmethod
    def validate_project_reference(cls, v, info):
        """Validate that at least one project reference is provided."""
        values = info.data
        if (
            "project_name" in values
            and "project_id" in values
            and values.get("project_id") is None
            and values.get("project_name") is None
        ):
            raise ValueError("Either project_id or project_name must be provided")
        return v


class FeatureInput(BaseModel):
    """Input schema for creating a feature."""

    name: str
    project_id: Optional[int] = None
    project_name: Optional[str] = None
    data_type: DataType
    description: str = ""
    meta: Optional[dict] = None
    entity_ids: Optional[List[int]] = None

    @field_validator("project_id", "project_name")
    @classmethod
    def validate_project_reference(cls, v, info):
        """Validate that at least one project reference is provided."""
        values = info.data
        if (
            "project_name" in values
            and "project_id" in values
            and values.get("project_id") is None
            and values.get("project_name") is None
        ):
            raise ValueError("Either project_id or project_name must be provided")
        return v


class TargetInput(BaseModel):
    """Input schema for creating a target."""

    name: str
    project_id: Optional[int] = None
    project_name: Optional[str] = None
    data_type: DataType
    description: str = ""
    meta: Optional[dict] = None
    entity_ids: Optional[List[int]] = None

    @field_validator("project_id", "project_name")
    @classmethod
    def validate_project_reference(cls, v, info):
        """Validate that at least one project reference is provided."""
        values = info.data
        if (
            "project_name" in values
            and "project_id" in values
            and values.get("project_id") is None
            and values.get("project_name") is None
        ):
            raise ValueError("Either project_id or project_name must be provided")
        return v


class JoinKeyInput(BaseModel):
    """Input schema for creating a join key."""

    name: str
    entity_id: Optional[int] = None
    entity_name: Optional[str] = None
    description: str = ""
    meta: Optional[dict] = None

    @field_validator("entity_id", "entity_name")
    @classmethod
    def validate_entity_reference(cls, v, info):
        """Validate that at least one entity reference is provided."""
        values = info.data
        if (
            "entity_name" in values
            and "entity_id" in values
            and values.get("entity_id") is None
            and values.get("entity_name") is None
        ):
            raise ValueError("Either entity_id or entity_name must be provided")
        return v


class JoinKeyValueInput(BaseModel):
    """Input schema for creating a join key value."""

    join_key_id: Optional[int] = None
    join_key_name: Optional[str] = None
    value: Any
    meta: Optional[dict] = None

    @field_validator("join_key_id", "join_key_name")
    @classmethod
    def validate_join_key_reference(cls, v, info):
        """Validate that at least one join key reference is provided."""
        values = info.data
        if (
            "join_key_name" in values
            and "join_key_id" in values
            and values.get("join_key_id") is None
            and values.get("join_key_name") is None
        ):
            raise ValueError("Either join_key_id or join_key_name must be provided")
        return v


class FeatureValueInput(BaseModel):
    """Input schema for creating a feature/attribute value."""

    attribute_id: Optional[int] = None
    attribute_name: Optional[str] = None
    join_key_value_id: int
    value: Any
    meta: Optional[dict] = None
    timestamp: Optional[datetime] = None

    @field_validator("attribute_id", "attribute_name")
    @classmethod
    def validate_attribute_reference(cls, v, info):
        """Validate that at least one attribute reference is provided."""
        values = info.data
        if (
            "attribute_name" in values
            and "attribute_id" in values
            and values.get("attribute_id") is None
            and values.get("attribute_name") is None
        ):
            raise ValueError("Either attribute_id or attribute_name must be provided")
        return v


class AssociationInput(BaseModel):
    """Input schema for associating attributes with entities."""

    attribute_id: int
    entity_id: int


# endregion


# region Registration Output Schemas


class ProjectOutput(BaseModel):
    """Output schema for a project."""

    id: int
    name: str
    description: str
    created_at: datetime
    updated_at: datetime
    meta: Optional[dict] = None

    model_config = {"from_attributes": True}


class EntityOutput(BaseModel):
    """Output schema for an entity."""

    id: int
    name: str
    description: str
    project_id: int
    created_at: datetime
    updated_at: datetime
    meta: Optional[dict] = None

    model_config = {"from_attributes": True}


class FeatureOutput(BaseModel):
    """Output schema for a feature."""

    id: int
    name: str
    description: str
    project_id: int
    data_type: DataType
    type: AttributeType
    is_label: bool
    created_at: datetime
    updated_at: datetime
    meta: Optional[dict] = None

    model_config = {"from_attributes": True}


class TargetOutput(BaseModel):
    """Output schema for a target."""

    id: int
    name: str
    description: str
    project_id: int
    data_type: DataType
    type: AttributeType
    is_label: bool
    created_at: datetime
    updated_at: datetime
    meta: Optional[dict] = None

    model_config = {"from_attributes": True}


class JoinKeyOutput(BaseModel):
    """Output schema for a join key."""

    id: int
    name: str
    description: str
    entity_id: int
    created_at: datetime
    updated_at: datetime
    meta: Optional[dict] = None

    model_config = {"from_attributes": True}


class JoinKeyValueOutput(BaseModel):
    """Output schema for a join key value."""

    id: int
    join_key_id: int
    value: dict
    created_at: datetime
    updated_at: datetime
    meta: Optional[dict] = None

    model_config = {"from_attributes": True}


class FeatureValueOutput(BaseModel):
    """Output schema for a feature/attribute value."""

    id: int
    attribute_id: int
    join_key_value_id: int
    value: dict
    timestamp: datetime
    created_at: datetime
    updated_at: datetime
    meta: Optional[dict] = None

    model_config = {"from_attributes": True}


class AssociationOutput(BaseModel):
    """Output schema for attribute-entity association."""

    attribute_id: int
    entity_id: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# endregion


# region Retrieval Schemas


class FeatureRetrievalInput(BaseModel):
    """Input schema for retrieving feature values."""

    project_name: str
    entity_name: str
    join_keys: Optional[List[Any]] = None
    feature_names: Optional[List[str]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    include_timestamp: bool = False

    @field_validator("end_time")
    @classmethod
    def validate_time_range(cls, v, info):
        """Validate that start_time is before end_time."""
        values = info.data
        start_time = values.get("start_time")
        if v and start_time and start_time > v:
            raise ValueError("start_time must be before end_time")
        return v


# endregion
