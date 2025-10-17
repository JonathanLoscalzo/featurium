from typing import Any, List, Optional

from sqlalchemy.orm import Session

from featurium.core.models import (
    Attribute,
    AttributeEntities,
    AttributeType,
    AttributeValue,
    DataType,
    Entity,
    JoinKey,
    JoinKeyValue,
    Project,
)


class RegistrationService:
    """
    Registration service for the feature store.
    """

    def __init__(self, db: Session):
        self.db = db

    def register_project(self, name: str, description: str = "") -> Project:
        """Register a project."""
        project = Project(name=name, description=description)
        self.db.add(project)
        self.db.commit()
        return project

    def register_entity(
        self, name: str, project: Project, description: str = ""
    ) -> Entity:
        """Register an entity."""
        entity = Entity(name=name, project=project, description=description)
        self.db.add(entity)
        self.db.commit()
        return entity

    def register_feature(
        self, name: str, project: Project, data_type: DataType, description: str = ""
    ) -> Attribute:
        """Register a feature."""
        return self.__register_attribute(
            name=name,
            project=project,
            data_type=data_type,
            type=AttributeType.FEATURE,
            description=description,
        )

    def register_target(
        self, name: str, project: Project, data_type: DataType, description: str = ""
    ) -> Attribute:
        """Register a target."""
        target = Attribute(
            name=name,
            project=project,
            data_type=data_type,
            type=AttributeType.TARGET,
            description=description,
            is_label=True,
        )
        self.db.add(target)
        self.db.commit()
        return target

    def register_join_key(self, name: str, entity: Entity) -> JoinKey:
        """Register a join key."""
        join_key = JoinKey(name=name, entity=entity)
        self.db.add(join_key)
        self.db.commit()
        return join_key

    def register_join_key_value(self, join_key: JoinKey, value: Any) -> JoinKeyValue:
        """Register a join key value."""
        join_key_value = JoinKeyValue(join_key=join_key, value=value)
        self.db.add(join_key_value)
        self.db.commit()
        return join_key_value

    def associate_attribute_with_entity(
        self, attribute: Attribute, entity: Entity
    ) -> AttributeEntities:
        """Associate an attribute with an entity."""
        association = AttributeEntities(attribute_id=attribute.id, entity_id=entity.id)
        self.db.add(association)
        self.db.commit()
        return association

    def associate_attribute_value_with_join_key_value(
        self, attribute_value: AttributeValue, join_key_value: JoinKeyValue
    ) -> AttributeValue:
        """Associate an attribute value with a join key value."""
        attribute_value.join_key_value_id = join_key_value.id
        self.db.add(attribute_value)
        self.db.commit()
        return attribute_value

    def register_feature_value(
        self,
        feature: Attribute,
        join_key_value: JoinKeyValue,
        value: Any,
        metadata: Optional[dict] = None,
    ) -> AttributeValue:
        """Register a feature value."""
        feature_value = self.__register_attribute_value(
            attribute=feature,
            join_key_value=join_key_value,
            value=value,
            metadata=metadata,
        )
        return feature_value

    def register_target_value(
        self,
        target: Attribute,
        join_key_value: JoinKeyValue,
        value: Any,
        metadata: Optional[dict] = None,
    ) -> AttributeValue:
        """Register a target value."""
        target_value = self.__register_attribute_value(
            attribute=target,
            join_key_value=join_key_value,
            value=value,
            metadata=metadata,
        )
        return target_value

    # region Bulk operations
    def register_projects_bulk(self, projects: List[dict]) -> List[Project]:
        """Register multiple projects in bulk."""
        project_objects = []
        for project_data in projects:
            project = Project(
                name=project_data["name"],
                description=project_data.get("description", ""),
                meta=project_data.get("meta"),
            )

            self.db.add(project)
            project_objects.append(project)

        self.db.commit()
        return project_objects

    def register_entities_bulk(self, entities: List[dict]) -> List[Entity]:
        """Register multiple entities in bulk."""
        entity_objects = []
        for entity_data in entities:
            entity = Entity(
                name=entity_data["name"],
                project_id=entity_data["project_id"],
                description=entity_data.get("description", ""),
                meta=entity_data.get("meta"),
            )
            self.db.add(entity)
            entity_objects.append(entity)

        self.db.commit()
        return entity_objects

    def register_attributes_bulk(self, attributes: List[dict]) -> List[Attribute]:
        """Register multiple attributes in bulk."""
        attribute_objects = []
        for attr_data in attributes:
            attribute = Attribute(
                name=attr_data["name"],
                project_id=attr_data["project_id"],
                type=attr_data["type"],
                data_type=attr_data["data_type"],
                description=attr_data.get("description", ""),
                is_label=attr_data.get("is_label", False),
                meta=attr_data.get("meta"),
            )
            self.db.add(attribute)
            attribute_objects.append(attribute)

        self.db.commit()
        return attribute_objects

    def register_join_keys_bulk(self, join_keys: List[dict]) -> List[JoinKey]:
        """Register multiple join keys in bulk."""
        join_key_objects = []
        for jk_data in join_keys:
            join_key = JoinKey(
                name=jk_data["name"],
                entity_id=jk_data["entity_id"],
                description=jk_data.get("description", ""),
                meta=jk_data.get("meta"),
            )
            self.db.add(join_key)
            join_key_objects.append(join_key)

        self.db.commit()
        return join_key_objects

    def register_join_key_values_bulk(
        self, join_key_values: List[dict]
    ) -> List[JoinKeyValue]:
        """Register multiple join key values in bulk."""
        jkv_objects = []
        for jkv_data in join_key_values:
            join_key_value = JoinKeyValue(
                join_key_id=jkv_data["join_key_id"],
                value=jkv_data["value"],
                meta=jkv_data.get("meta"),
            )
            self.db.add(join_key_value)
            jkv_objects.append(join_key_value)

        self.db.commit()
        return jkv_objects

    def register_attribute_values_bulk(
        self, attribute_values: List[dict]
    ) -> List[AttributeValue]:
        """Register multiple attribute values in bulk."""
        av_objects = []
        for av_data in attribute_values:
            attribute_value = AttributeValue(
                attribute_id=av_data["attribute_id"],
                join_key_value_id=av_data["join_key_value_id"],
                value=av_data["value"],
                meta=av_data.get("meta"),
            )
            self.db.add(attribute_value)
            av_objects.append(attribute_value)

        self.db.commit()
        return av_objects

    def associate_attributes_with_entities_bulk(
        self, associations: List[dict]
    ) -> List[AttributeEntities]:
        """Associate multiple attributes with entities in bulk."""
        association_objects = []
        for assoc_data in associations:
            association = AttributeEntities(
                attribute_id=assoc_data["attribute_id"],
                entity_id=assoc_data["entity_id"],
            )
            self.db.add(association)
            association_objects.append(association)

        self.db.commit()
        return association_objects

    # endregion Bulk operations

    # region Query methods
    # def get_project(self, project_id: int) -> Optional[Project]:
    #     """Get a project by ID."""
    #     return self.db.query(Project).filter(Project.id == project_id).first()

    # def get_project_by_name(self, name: str) -> Optional[Project]:
    #     """Get a project by name."""
    #     return self.db.query(Project).filter(Project.name == name).first()

    # def get_entity(self, entity_id: int) -> Optional[Entity]:
    #     """Get an entity by ID."""
    #     return self.db.query(Entity).filter(Entity.id == entity_id).first()

    # def get_attribute(self, attribute_id: int) -> Optional[Attribute]:
    #     """Get an attribute by ID."""
    #     return self.db.query(Attribute).filter(Attribute.id == attribute_id).first()

    # def get_join_key(self, join_key_id: int) -> Optional[JoinKey]:
    #     """Get a join key by ID."""
    #     return self.db.query(JoinKey).filter(JoinKey.id == join_key_id).first()

    # def get_join_key_value(self, join_key_value_id: int) -> Optional[JoinKeyValue]:
    #     """Get a join key value by ID."""
    #     return (
    #         self.db.query(JoinKeyValue)
    #         .filter(JoinKeyValue.id == join_key_value_id)
    #         .first()
    #     )

    # def get_attribute_value(self, attribute_value_id: int) -> Optional[AttributeValue]: # noqa: E501
    #     """Get an attribute value by ID."""
    #     return (
    #         self.db.query(AttributeValue)
    #         .filter(AttributeValue.id == attribute_value_id)
    #         .first()
    #     )

    # endregion Query methods

    # region List methods
    # def list_projects(self) -> List[Project]:
    #     """List all projects."""
    #     return self.db.query(Project).all()

    # def list_entities(self, project_id: Optional[int] = None) -> List[Entity]:
    #     """List all entities, optionally filtered by project."""
    #     query = self.db.query(Entity)
    #     if project_id:
    #         query = query.filter(Entity.project_id == project_id)
    #     return query.all()

    # def list_attributes(
    #     self,
    #     project_id: Optional[int] = None,
    #     attribute_type: Optional[AttributeType] = None,
    # ) -> List[Attribute]:
    #     """List all attributes, optionally filtered by project and type."""
    #     query = self.db.query(Attribute)
    #     if project_id:
    #         query = query.filter(Attribute.project_id == project_id)
    #     if attribute_type:
    #         query = query.filter(Attribute.type == attribute_type)
    #     return query.all()

    # def list_join_keys(self, entity_id: Optional[int] = None) -> List[JoinKey]:
    #     """List all join keys, optionally filtered by entity."""
    #     query = self.db.query(JoinKey)
    #     if entity_id:
    #         query = query.filter(JoinKey.entity_id == entity_id)
    #     return query.all()

    # def list_join_key_values(
    #     self, join_key_id: Optional[int] = None
    # ) -> List[JoinKeyValue]:
    #     """List all join key values, optionally filtered by join key."""
    #     query = self.db.query(JoinKeyValue)
    #     if join_key_id:
    #         query = query.filter(JoinKeyValue.join_key_id == join_key_id)
    #     return query.all()

    # def list_attribute_values(
    #     self,
    #     attribute_id: Optional[int] = None,
    #     join_key_value_id: Optional[int] = None,
    # ) -> List[AttributeValue]:
    #     """List all attribute values, optionally filtered by attribute or join key value.""" # noqa: E501
    #     query = self.db.query(AttributeValue)
    #     if attribute_id:
    #         query = query.filter(AttributeValue.attribute_id == attribute_id)
    #     if join_key_value_id:
    #         query = query.filter(AttributeValue.join_key_value_id == join_key_value_id) # noqa: E501
    #     return query.all()

    # endregion List methods

    # region Private methods
    def __register_attribute(
        self,
        name: str,
        project: Project,
        data_type: DataType,
        type: AttributeType,
        description: str = "",
    ) -> Attribute:
        """Register an attribute."""
        attribute = Attribute(
            name=name,
            project=project,
            data_type=data_type,
            type=type,
            description=description,
        )
        self.db.add(attribute)
        self.db.commit()
        return attribute

    def __register_attribute_value(
        self,
        attribute: Attribute,
        join_key_value: JoinKeyValue,
        value: Any,
        metadata: Optional[dict] = None,
    ) -> AttributeValue:
        """Register an attribute value."""
        attribute_value = AttributeValue(
            attribute=attribute,
            join_key_value=join_key_value,
            value=value,
            meta=metadata or {},
        )
        self.db.add(attribute_value)
        self.db.commit()
        return attribute_value

    # endregion Private methods
