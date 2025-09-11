from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from featurium.core.models import (
    Attribute,
    AttributeType,
    AttributeValue,
    DataType,
    Entity,
    JoinKey,
    JoinKeyValue,
    Project,
)


def create_project(db: Session, name: str) -> Project:
    """Create a project and add it to the database."""
    project = Project(name=name)
    db.add(project)
    db.flush()
    return project


def create_feature(
    db: Session, name: str, project: Project, data_type: DataType
) -> Attribute:
    """Create a feature"""
    feature = Attribute(name=name, project=project, data_type=data_type, is_label=False)
    db.add(feature)
    db.flush()
    return feature


def create_entity(db: Session, name: str, project: Project) -> Entity:
    """Create a entity"""
    entity = Entity(name=name, project=project)
    db.add(entity)
    db.flush()
    return entity


def create_join_key(db: Session, name: str, entity: Entity) -> JoinKey:
    """Create a join key"""
    join_key = JoinKey(name=name, entity=entity)
    db.add(join_key)
    db.flush()
    return join_key


def create_join_key_value(db: Session, join_key: JoinKey, value: Any) -> JoinKeyValue:
    """Create a join key value"""
    join_key_value = JoinKeyValue(join_key=join_key, value=value)
    db.add(join_key_value)
    db.flush()
    return join_key_value


def create_feature_value(
    db: Session, attribute: Attribute, value: Any, timestamp: datetime
) -> AttributeValue:
    """Create a feature value"""
    attribute_value = AttributeValue(
        attribute=attribute, value=value, timestamp=timestamp
    )
    db.add(attribute_value)
    db.flush()
    return attribute_value


def create_target(
    db: Session, name: str, project: Project, data_type: DataType
) -> Attribute:
    """Create a target"""
    target = Attribute(
        name=name,
        project=project,
        data_type=data_type,
        is_label=True,
        type=AttributeType.TARGET,
    )
    db.add(target)
    db.flush()
    return target


def create_target_value(
    db: Session, target: Attribute, value: Any, timestamp: datetime
) -> AttributeValue:
    """Create a target value"""
    target_value = AttributeValue(attribute=target, value=value, timestamp=timestamp)
    db.add(target_value)
    db.flush()
    return target_value
