"""
Feature Store Data Models

This module defines the SQLAlchemy models for the Feature Store.

"""

from datetime import UTC, datetime
from enum import Enum
from textwrap import dedent
from typing import Any, List, Optional

from sqlalchemy import JSON, Boolean, DateTime
from sqlalchemy import Enum as SQLEnum
from sqlalchemy import ForeignKey, Sequence, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base model class"""

    __abstract__ = True
    pass


class AttributeType(str, Enum):
    """Enum for attribute types"""

    FEATURE = "feature"
    TARGET = "target"


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

    id: Mapped[int] = mapped_column(
        Sequence("Seq_id"),
        primary_key=True,
    )


class NamedMixin:
    """Mixin for all models with common fields"""

    name: Mapped[str] = mapped_column(String(255), default="")
    description: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)


class ExtraDataMixin:
    """Base mixin for all models with common fields"""

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


class BaseModel(
    Base,
    IdentifiableMixin,
    NamedMixin,
    TimestampMixin,
    UserTrackingMixin,
    ExtraDataMixin,
):
    """Base entity class that includes all common mixins"""

    __abstract__ = True


class BaseValueModel(
    Base,
    IdentifiableMixin,
    TimestampMixin,
    UserTrackingMixin,
    ExtraDataMixin,
):
    """Base value model that includes all common mixins"""

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
    __table_args__ = (UniqueConstraint("name", name="ux_projects_name"),)

    # Relationships
    entities: Mapped[List["Entity"]] = relationship(back_populates="project")
    attributes: Mapped[List["Attribute"]] = relationship(back_populates="project")

    # ViewOnly Relationships
    features: Mapped[List["Attribute"]] = relationship(
        "Attribute",
        primaryjoin="and_(Project.id==Attribute.project_id, Attribute.type=='FEATURE')",
        viewonly=True,
    )
    targets: Mapped[List["Attribute"]] = relationship(
        "Attribute",
        primaryjoin="and_(Project.id==Attribute.project_id, Attribute.type=='TARGET')",
        viewonly=True,
    )

    def __repr__(self) -> str:
        """String representation of the Project model"""
        return f"Project(id={self.id}, name='{self.name}')"


class Attribute(BaseModel):
    """Attribute model representing a measurable attribute"""

    __tablename__ = "attributes"

    type: Mapped[AttributeType] = mapped_column(
        SQLEnum(AttributeType), default=AttributeType.FEATURE
    )

    # Foreign keys
    project_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )

    # Relationships
    project: Mapped[Optional["Project"]] = relationship(back_populates="attributes")
    entities: Mapped[List["Entity"]] = relationship(
        back_populates="attributes",
        secondary="attribute_entities",
    )
    attribute_values: Mapped[List["AttributeValue"]] = relationship(
        back_populates="attribute",
    )

    # Columns
    data_type: Mapped[DataType] = mapped_column(SQLEnum(DataType))
    is_label: Mapped[bool] = mapped_column(Boolean, default=False)

    __table_args__ = (
        UniqueConstraint("project_id", "name", name="ux_attributes_project_id_name"),
    )

    def __repr__(self) -> str:
        """String representation of the Attribute model"""
        return f"Attribute(id={self.id}, name='{self.name}', type='{self.type}')"


class AttributeValue(BaseValueModel):
    """AttributeValue model storing actual values for attributes"""

    __tablename__ = "attribute_values"

    # Columns
    value: Mapped[dict] = mapped_column(JSON)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(UTC)
    )

    # Foreign keys
    attribute_id: Mapped[int] = mapped_column(ForeignKey("attributes.id"))
    join_key_value_id: Mapped[int] = mapped_column(
        ForeignKey("join_key_values.id"), nullable=True
    )

    # Relationships
    attribute: Mapped["Attribute"] = relationship(back_populates="attribute_values")
    join_key_value: Mapped["JoinKeyValue"] = relationship(
        back_populates="attribute_values",
    )

    __table_args__ = (
        UniqueConstraint(
            "join_key_value_id",
            "attribute_id",
            "timestamp",
            name="ux_attribute_values_join_key_value_id_attribute_id_timestamp",
        ),
    )

    def __repr__(self) -> str:
        """String representation of the AttributeValue model"""
        return (
            f"AttributeValue(id={self.id}, attribute='{self.attribute.name}', "
            f"value={self.value_scalar})"
        )

    @property
    def value_scalar(self) -> Any:
        """Get the scalar value from the JSON value based on data_type."""
        if not self.value or self.attribute.data_type.value not in self.value:
            return None

        return self.value[self.attribute.data_type.value]


class Entity(BaseModel):
    """Entity model representing a business object (e.g., trip)"""

    __tablename__ = "entities"
    __table_args__ = (
        UniqueConstraint("project_id", "name", name="ux_entities_project_id_name"),
    )
    # Foreign keys
    project_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )

    # Relationships
    project: Mapped[Optional["Project"]] = relationship(back_populates="entities")
    join_key: Mapped["JoinKey"] = relationship(back_populates="entity")
    attributes: Mapped[List["Attribute"]] = relationship(
        back_populates="entities",
        secondary="attribute_entities",
    )

    # ViewOnly Relationships
    features: Mapped[List["Attribute"]] = relationship(
        "Attribute",
        secondary="attribute_entities",
        primaryjoin="Entity.id==AttributeEntities.entity_id",
        secondaryjoin="and_(Attribute.id==AttributeEntities.attribute_id, Attribute.type=='FEATURE')",  # noqa: E501
        viewonly=True,
    )

    targets: Mapped[List["Attribute"]] = relationship(
        "Attribute",
        secondary="attribute_entities",
        primaryjoin="Entity.id==AttributeEntities.entity_id",
        secondaryjoin="and_(Attribute.id==AttributeEntities.attribute_id, Attribute.type=='TARGET')",  # noqa: E501
        viewonly=True,
    )

    def __repr__(self) -> str:
        """String representation of the Entity model"""
        return f"Entity(id={self.id}, name='{self.name}')"


class JoinKey(BaseModel):
    """JoinKey model storing actual values for join keys"""

    __tablename__ = "join_keys"
    __table_args__ = (
        UniqueConstraint("entity_id", "name", name="ux_join_keys_entity_id_name"),
    )

    # Foreign keys
    entity_id: Mapped[int] = mapped_column(ForeignKey("entities.id"))

    # Relationships
    entity: Mapped["Entity"] = relationship(back_populates="join_key")
    join_key_values: Mapped[List["JoinKeyValue"]] = relationship(
        back_populates="join_key",
    )

    def __repr__(self) -> str:
        """String representation of the JoinKey model"""
        return f"JoinKey(id={self.id}, entity_id={self.entity_id})"


class AttributeEntities(Association):
    """Association table between Attribute and Entity."""

    __tablename__ = "attribute_entities"

    # Foreign keys
    attribute_id: Mapped[int] = mapped_column(
        ForeignKey("attributes.id"), primary_key=True
    )
    entity_id: Mapped[int] = mapped_column(ForeignKey("entities.id"), primary_key=True)

    def __repr__(self) -> str:
        """String representation of the AttributeEntities model"""
        return (
            f"AttributeEntities(attribute_id={self.attribute_id}, "
            f"entity_id={self.entity_id})"
        )


class JoinKeyValue(BaseValueModel):
    """JoinKeyValue model storing actual values for join keys"""

    __tablename__ = "join_key_values"

    # Foreign keys
    join_key_id: Mapped[int] = mapped_column(ForeignKey("join_keys.id"))

    # Relationships
    join_key: Mapped["JoinKey"] = relationship(back_populates="join_key_values")

    # Columns
    value: Mapped[dict] = mapped_column(JSON)

    # Relación unificada
    attribute_values: Mapped[List["AttributeValue"]] = relationship(
        back_populates="join_key_value",
    )

    feature_values: Mapped[List["AttributeValue"]] = relationship(
        back_populates="join_key_value",
        primaryjoin=dedent(
            """and_(
            JoinKeyValue.id==AttributeValue.join_key_value_id,
            AttributeValue.attribute_id==Attribute.id,
            Attribute.type=='feature'
        )
        """
        ),
        viewonly=True,
    )

    target_values: Mapped[List["AttributeValue"]] = relationship(
        back_populates="join_key_value",
        primaryjoin=dedent(
            """and_(
                JoinKeyValue.id==AttributeValue.join_key_value_id,
                AttributeValue.attribute_id==Attribute.id,
                Attribute.type=='target'
            )
        """
        ),
        viewonly=True,
    )

    def __repr__(self) -> str:
        """String representation of the JoinKeyValue model"""
        return f"JoinKeyValue(id={self.id}, " f"value={self.value})"
