"""
Feature Store Data Models

This module defines the SQLAlchemy models for the Feature Store.

"""

from datetime import UTC, datetime
from enum import Enum
from typing import Any, List, Optional

from sqlalchemy import JSON, DateTime
from sqlalchemy import Enum as SQLEnum
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, relationship

Base = declarative_base()


class DataType(str, Enum):
    """Enum for feature data types"""

    INTEGER = "integer"
    FLOAT = "float"
    STRING = "string"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    JSON = "json"


class IdentifiableMixin:
    """Mixin for all models with common fields"""

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)


class ExtraDataMixin:
    """Base mixin for all models with common fields"""

    name: Mapped[str] = mapped_column(String(255), default="")
    description: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    meta: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)


class TimestampMixin:
    """Mixin for tracking creation and update timestamps"""

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )


class UserTrackingMixin:
    """Mixin for tracking user actions"""

    created_by: Mapped[str] = mapped_column(String(255), default="UNKNOWN")
    updated_by: Mapped[str] = mapped_column(String(255), default="UNKNOWN")


class TrackingMixin(TimestampMixin, UserTrackingMixin):
    """Mixin for tracking creation and update timestamps"""

    pass


class BaseModel(
    Base,
    IdentifiableMixin,
    ExtraDataMixin,
    TrackingMixin,
):
    """Base entity class that includes all common mixins"""

    __abstract__ = True


class Association(Base, TimestampMixin):
    """
    Association model for many-to-many relationship
    between FeatureValue and EntityValue
    """

    __abstract__ = True


class Project(BaseModel):
    """Project model that contains features and other related entities"""

    __tablename__ = "projects"

    # Relationships
    features: Mapped[List["Feature"]] = relationship(back_populates="project")
    entities: Mapped[List["Entity"]] = relationship(back_populates="project")
    targets: Mapped[List["Target"]] = relationship(back_populates="project")

    def __repr__(self) -> str:
        """String representation of the Project model"""
        return f"Project(id={self.id}, name='{self.name}')"


class Feature(BaseModel):
    """Feature model representing a measurable attribute"""

    __tablename__ = "features"

    # Columns
    data_type: Mapped[DataType] = mapped_column(SQLEnum(DataType))

    # Foreign keys
    project_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )

    # Relationships
    project: Mapped[Optional["Project"]] = relationship(back_populates="features")
    entities: Mapped[List["Entity"]] = relationship(
        back_populates="features",
        secondary="feature_entities",
    )
    feature_values: Mapped[List["FeatureValue"]] = relationship(
        back_populates="feature"
    )
    join_keys: Mapped[List["JoinKey"]] = relationship(
        secondary="feature_entities",
        primaryjoin="Feature.id == FeatureEntities.feature_id",
        # secondaryjoin="Entity.id == JoinKey.entity_id",
        secondaryjoin="FeatureEntities.entity_id == JoinKey.entity_id",
        viewonly=True,
    )

    def __repr__(self) -> str:
        """String representation of the Feature model"""
        return f"Feature(id={self.id}, name='{self.name}', type={self.data_type})"


class FeatureValue(BaseModel):
    """FeatureValue model storing actual values for features"""

    __tablename__ = "feature_values"

    # Columns
    value: Mapped[dict] = mapped_column(JSON)
    data_type: Mapped[DataType] = mapped_column(SQLEnum(DataType))
    timestamp: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(UTC)
    )

    # Foreign keys
    feature_id: Mapped[int] = mapped_column(ForeignKey("features.id"))

    # Relationships
    feature: Mapped["Feature"] = relationship(back_populates="feature_values")
    join_key_values: Mapped[List["JoinKeyValue"]] = relationship(
        back_populates="feature_values", secondary="feature_join_key_values"
    )

    @property
    def value_scalar(self) -> Any:
        """Get the scalar value from the JSON value based on data_type."""
        if not self.value or self.data_type.value not in self.value:
            return None
        # TODO: handle parse types
        return self.value[self.data_type.value]

    def __repr__(self) -> str:
        """String representation of the FeatureValue model"""
        return (
            f"FeatureValue(id={self.id}, feature='{self.feature.name}', "
            f"value={self.value_scalar})"
        )


class Entity(BaseModel):
    """Entity model representing a business object (e.g., trip)"""

    __tablename__ = "entities"

    # Foreign keys
    project_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )

    # Relationships
    project: Mapped[Optional["Project"]] = relationship(back_populates="entities")
    join_keys: Mapped[List["JoinKey"]] = relationship(back_populates="entity")
    features: Mapped[List["Feature"]] = relationship(
        back_populates="entities",
        secondary="feature_entities",
    )
    targets: Mapped[List["Target"]] = relationship(
        back_populates="entities",
        secondary="target_entities",
    )

    def __repr__(self) -> str:
        """String representation of the Entity model"""
        return f"Entity(id={self.id}, name='{self.name}')"


class JoinKey(BaseModel):
    """JoinKey model storing actual values for join keys"""

    __tablename__ = "join_keys"

    # Columns
    key: Mapped[str] = mapped_column(String(255))

    # Foreign keys
    entity_id: Mapped[int] = mapped_column(ForeignKey("entities.id"))

    # Relationships
    entity: Mapped["Entity"] = relationship(back_populates="join_keys")
    join_key_values: Mapped[List["JoinKeyValue"]] = relationship(
        back_populates="join_key",
    )

    def __repr__(self) -> str:
        """String representation of the JoinKey model"""
        return f"JoinKey(id={self.id}, key='{self.key}')"


class FeatureEntities(Association):
    """Association table between Feature and Entity."""

    __tablename__ = "feature_entities"

    # Foreign keys
    feature_id: Mapped[int] = mapped_column(ForeignKey("features.id"), primary_key=True)
    entity_id: Mapped[int] = mapped_column(ForeignKey("entities.id"), primary_key=True)

    def __repr__(self) -> str:
        """String representation of the FeatureEntities model"""
        return (
            f"FeatureEntities(feature_id={self.feature_id}, entity_id={self.entity_id})"
        )


class JoinKeyValue(BaseModel):
    """JoinKeyValue model storing actual values for join keys"""

    __tablename__ = "join_key_values"

    # Columns
    value: Mapped[dict] = mapped_column(JSON)
    data_type: Mapped[DataType] = mapped_column(SQLEnum(DataType))

    # Foreign keys
    join_key_id: Mapped[int] = mapped_column(ForeignKey("join_keys.id"))

    # Relationships
    join_key: Mapped["JoinKey"] = relationship(back_populates="join_key_values")
    feature_values: Mapped[List["FeatureValue"]] = relationship(
        back_populates="join_key_values", secondary="feature_join_key_values"
    )
    target_values: Mapped[List["TargetValue"]] = relationship(
        back_populates="join_key_values", secondary="target_join_key_values"
    )

    @property
    def value_scalar(self) -> Any:
        """Get the scalar value from the JSON value based on data_type."""
        if not self.value or self.data_type.value not in self.value:
            return None
        return self.value[self.data_type.value]

    def __repr__(self) -> str:
        """String representation of the JoinKeyValue model"""
        return (
            f"JoinKeyValue(id={self.id}, key='{self.join_key.key}', "
            f"value={self.value_scalar})"
        )


class FeatureJoinKeyValue(Association):
    """FeatureJoinKeyValue model storing actual values for join keys"""

    __tablename__ = "feature_join_key_values"

    # Foreign keys
    feature_value_id: Mapped[int] = mapped_column(
        ForeignKey("feature_values.id"), primary_key=True
    )
    join_key_value_id: Mapped[int] = mapped_column(
        ForeignKey("join_key_values.id"), primary_key=True
    )


class Target(BaseModel):
    """Target model for tracking target variables"""

    __tablename__ = "targets"

    # Foreign keys
    project_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )

    # Relationships
    project: Mapped[Optional["Project"]] = relationship(back_populates="targets")
    target_values: Mapped[List["TargetValue"]] = relationship(back_populates="target")
    entities: Mapped[List["Entity"]] = relationship(
        back_populates="targets",
        secondary="target_entities",
    )
    join_keys: Mapped[List["JoinKey"]] = relationship(
        secondary="target_entities",
        primaryjoin="Target.id == TargetEntities.target_id",
        secondaryjoin="Entity.id == JoinKey.entity_id",
        viewonly=True,
    )

    def __repr__(self) -> str:
        """String representation of the Target model"""
        return f"Target(id={self.id}, name='{self.name}')"


class TargetValue(BaseModel):
    """TargetValue model storing actual values for targets"""

    __tablename__ = "target_values"

    # Columns
    value: Mapped[dict] = mapped_column(JSON)
    data_type: Mapped[DataType] = mapped_column(SQLEnum(DataType))
    timestamp: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(UTC)
    )

    # Foreign keys
    target_id: Mapped[int] = mapped_column(ForeignKey("targets.id"))

    # Relationships
    target: Mapped["Target"] = relationship(back_populates="target_values")
    join_key_values: Mapped[List["JoinKeyValue"]] = relationship(
        back_populates="target_values",
        secondary="target_join_key_values",
    )

    @property
    def value_scalar(self) -> Any:
        """Get the scalar value from the JSON value based on data_type."""
        if not self.value or self.data_type.value not in self.value:
            return None
        return self.value[self.data_type.value]

    def __repr__(self) -> str:
        """String representation of the TargetValue model"""
        return (
            f"TargetValue(id={self.id}, target='{self.target.name}', "
            f"value={self.value_scalar})"
        )


class TargetEntities(Association):
    """Association table between Target and Entity."""

    __tablename__ = "target_entities"

    # Foreign keys
    target_id: Mapped[int] = mapped_column(ForeignKey("targets.id"), primary_key=True)
    entity_id: Mapped[int] = mapped_column(ForeignKey("entities.id"), primary_key=True)


class TargetJoinKeyValues(Association):
    """Association table between Target and JoinKeyValue."""

    __tablename__ = "target_join_key_values"

    # Foreign keys
    target_value_id: Mapped[int] = mapped_column(
        ForeignKey("target_values.id"), primary_key=True
    )
    join_key_value_id: Mapped[int] = mapped_column(
        ForeignKey("join_key_values.id"), primary_key=True
    )
