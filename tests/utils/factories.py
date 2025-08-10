from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from featurium.core.models import (
    DataType,
    Entity,
    Feature,
    FeatureValue,
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
) -> Feature:
    """Create a feature"""
    feature = Feature(name=name, project=project, data_type=data_type)
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
    db: Session, feature: Feature, value: Any, timestamp: datetime
) -> FeatureValue:
    """Create a feature value"""
    feature_value = FeatureValue(feature=feature, value=value, timestamp=timestamp)
    db.add(feature_value)
    db.flush()
    return feature_value
