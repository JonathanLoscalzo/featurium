import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from featurium.core.models import (  # Target,; TargetValue,
    Attribute,
    AttributeValue,
    DataType,
    Entity,
    JoinKey,
    JoinKeyValue,
    Project,
)


@pytest.mark.usefixtures("cleanup")
class TestCreateModels:
    """Test the creation of models"""

    def test_project_model(self) -> None:
        """Test the creation and saving of a project"""
        project = Project(name="Test Project")
        assert project.name == "Test Project"

    def test_feature_model(self) -> None:
        """Test the creation and saving of a feature"""
        feature = Attribute(name="Test Feature")
        assert feature.name == "Test Feature"

    def test_feature_value_model(self) -> None:
        """Test the creation and saving of a feature value"""
        feature_value = AttributeValue(value={"string": "Test Feature Value"})
        assert feature_value.value == {"string": "Test Feature Value"}

    def test_entity_model(self) -> None:
        """Test the creation and saving of an entity"""
        entity = Entity(name="Test Entity")
        assert entity.name == "Test Entity"

    def test_join_key_model(self) -> None:
        """Test the creation and saving of a join key"""
        join_key = JoinKey(
            name="test:join",
            description="Test Join Key",
            meta={"tag": "value"},
        )
        assert join_key.name == "test:join"
        assert join_key.description == "Test Join Key"
        assert join_key.meta == {"tag": "value"}

    def test_join_key_value_model(self) -> None:
        """Test the creation and saving of a join key value"""
        join_key_value = JoinKeyValue(value={"string": "Test Join Key Value"})
        assert join_key_value.value == {"string": "Test Join Key Value"}

    def test_create_and_save(self, db: Session) -> None:
        """Test the creation and saving of a project"""
        project = Project(name="Test Project")
        db.add(project)
        db.commit()
        print(db.query(Project).all())
        assert project.id is not None
        assert project.name == "Test Project"


@pytest.mark.usefixtures("cleanup")
class TestModelConstraints:
    """Test the constraints of the models"""

    def test_project__unique_name(self, db: Session) -> None:
        """Test the creation of a project with a unique name"""
        project_name = f"Test Project {str(uuid.uuid4())}"
        project1 = Project(name=project_name)
        db.add(project1)
        db.commit()
        project2 = Project(name=project_name)
        with pytest.raises(IntegrityError):
            db.add(project2)
            db.commit()

    def test_feature__unique_name_per_project(self, db: Session) -> None:
        """Test the creation of a feature with a unique name"""
        project = Project(name=f"Test Project {str(uuid.uuid4())}")
        db.add(project)
        db.commit()
        feature1 = Attribute(
            name="Test Feature",
            project=project,
            data_type=DataType.FLOAT,
        )
        db.add(feature1)
        db.commit()
        feature2 = Attribute(
            name="Test Feature",
            project=project,
            data_type=DataType.FLOAT,
        )
        with pytest.raises(IntegrityError):
            db.add(feature2)
            db.commit()

    def test_feature__not_unique_name_between_projects(self, db: Session) -> None:
        """Test the creation of a feature with a unique name"""
        project1 = Project(name=f"Test Project {str(uuid.uuid4())}")
        project2 = Project(name=f"Test Project {str(uuid.uuid4())}")
        db.add_all([project1, project2])
        db.commit()
        feature1 = Attribute(
            name="Test Feature",
            project=project1,
            data_type=DataType.FLOAT,
        )
        db.add(feature1)
        db.commit()
        try:
            feature2 = Attribute(
                name="Test Feature",
                project=project2,
                data_type=DataType.FLOAT,
            )
            db.add(feature2)
            db.commit()
        except IntegrityError:
            pytest.fail("Feature name should be unique per project")

    def test_feature_value__unique_per_timestamp(self, db: Session) -> None:
        """Test the creation of a feature value with a unique timestamp"""
        feature = Attribute(
            name=f"Test Feature {str(uuid.uuid4())}",
            data_type=DataType.FLOAT,
        )
        join_key = JoinKey(name="Test Join Key", entity=Entity(name="Test Entity"))
        join_key_value = JoinKeyValue(
            value={"string": "Test Join Key Value"}, join_key=join_key
        )
        db.add(feature)
        db.add(join_key)
        db.add(join_key_value)
        db.flush()
        timestamp = datetime.now(UTC)
        feature_value1 = AttributeValue(
            value={"float": 1.0},
            attribute=feature,
            join_key_value_id=join_key_value.id,
            timestamp=timestamp,
        )
        db.add(feature_value1)
        db.flush()
        feature_value2 = AttributeValue(
            value={"float": 10.0},
            attribute=feature,
            join_key_value_id=join_key_value.id,
            timestamp=timestamp,
        )
        with pytest.raises(IntegrityError):
            db.add(feature_value2)
            db.commit()

    def test_feature_value__unique_per_timestamp__store_multiple_values(
        self, db: Session
    ) -> None:
        """Test the creation of multiples feature values for the same feature"""
        feature = Attribute(name="Test Feature", data_type=DataType.FLOAT)
        join_key = JoinKey(name="Test Join Key", entity=Entity(name="Test Entity"))
        join_key_value = JoinKeyValue(
            value={"string": "Test Join Key Value"}, join_key=join_key
        )
        db.add(feature)
        db.add(join_key)
        db.add(join_key_value)
        db.flush()
        feature_value1 = AttributeValue(
            value={"float": 1.0},
            attribute=feature,
            join_key_value_id=join_key_value.id,
            timestamp=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
        )
        db.add(feature_value1)
        db.flush()
        feature_value2 = AttributeValue(
            value={"float": 10.0},
            attribute=feature,
            join_key_value_id=join_key_value.id,
            timestamp=datetime(2025, 1, 1, 12, 0, 1, tzinfo=UTC),
        )
        db.add(feature_value2)

        try:
            db.commit()
        except IntegrityError:
            pytest.fail("Feature value should be unique per timestamp")

        assert len(db.query(AttributeValue).all()) == 2

    def test_entity__unique_name_per_project(self, db: Session) -> None:
        """Test the creation of an entity with a unique name"""
        project = Project(name=f"Test Project {str(uuid.uuid4())}")
        db.add(project)
        db.commit()
        entity_name = f"Test Entity {str(uuid.uuid4())}"
        entity1 = Entity(name=entity_name, project=project)
        db.add(entity1)
        db.commit()
        entity2 = Entity(name=entity_name, project=project)
        with pytest.raises(IntegrityError):
            db.add(entity2)
            db.commit()

    def test_entity__not_unique_name_between_projects(self, db: Session) -> None:
        """Test the creation of an entity with a unique name"""
        project1 = Project(name=f"Test Project {str(uuid.uuid4())}")
        project2 = Project(name=f"Test Project {str(uuid.uuid4())}")
        db.add_all([project1, project2])
        db.commit()
        entity_name = f"Test Entity {str(uuid.uuid4())}"
        entity1 = Entity(name=entity_name, project=project1)
        db.add(entity1)
        db.commit()
        try:
            entity2 = Entity(name=entity_name, project=project2)
            db.add(entity2)
            db.commit()
        except IntegrityError:
            pytest.fail("Entity name should be unique per project")

    def test_join_key__unique_name_per_entity(self, db: Session) -> None:
        """Test the creation of a join key with a unique key"""
        entity = Entity(name=f"Test Entity {str(uuid.uuid4())}")
        db.add(entity)
        db.commit()
        join_key_name = f"Test Join Key {str(uuid.uuid4())}"
        join_key1 = JoinKey(name=join_key_name, entity=entity)
        db.add(join_key1)
        db.commit()
        join_key2 = JoinKey(name=join_key_name, entity=entity)
        with pytest.raises(IntegrityError):
            db.add(join_key2)
            db.commit()

    def test_target__unique_name_per_project(self, db: Session) -> None:
        """Test the creation of a target with a unique name"""
        project = Project(name=f"Test Project {str(uuid.uuid4())}")
        db.add(project)
        db.commit()
        target1 = Attribute(
            name="Test Target",
            project=project,
            data_type=DataType.FLOAT,
            is_label=True,
        )
        db.add(target1)
        db.commit()
        target2 = Attribute(
            name="Test Target",
            project=project,
            data_type=DataType.FLOAT,
            is_label=True,
        )
        with pytest.raises(IntegrityError):
            db.add(target2)
            db.commit()

    def test_target__not_unique_name_between_projects(self, db: Session) -> None:
        """Test the creation of a target with a unique name"""
        project1 = Project(name=f"Test Project {str(uuid.uuid4())}")
        project2 = Project(name=f"Test Project {str(uuid.uuid4())}")
        db.add_all([project1, project2])
        db.commit()
        target_name = f"Test Target {str(uuid.uuid4())}"
        target1 = Attribute(
            name=target_name,
            project=project1,
            data_type=DataType.FLOAT,
            is_label=True,
        )
        db.add(target1)
        db.commit()
        target2 = Attribute(
            name=target_name,
            project=project2,
            data_type=DataType.FLOAT,
            is_label=True,
        )
        try:
            db.add(target2)
            db.commit()
        except IntegrityError:
            pytest.fail("Target name should be unique per project")

    def test_target_value__unique_per_timestamp(self, db: Session) -> None:
        """Test the creation of a target value with a unique timestamp"""
        target = Attribute(name="Test Target", data_type=DataType.FLOAT, is_label=True)
        join_key = JoinKey(name="Test Join Key", entity=Entity(name="Test Entity"))
        join_key_value = JoinKeyValue(
            value={"string": "Test Join Key Value"}, join_key=join_key
        )
        db.add(target)
        db.add(join_key)
        db.add(join_key_value)
        db.flush()
        target_value1 = AttributeValue(
            value={"float": 1.0},
            attribute=target,
            join_key_value_id=join_key_value.id,
            timestamp=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
        )
        db.add(target_value1)
        db.flush()
        target_value2 = AttributeValue(
            value={"float": 10.0},
            attribute=target,
            join_key_value_id=join_key_value.id,
            timestamp=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
        )
        with pytest.raises(IntegrityError):
            db.add(target_value2)
            db.commit()

    def test_target_value__unique_per_timestamp__store_multiple_values(
        self, db: Session
    ) -> None:
        """Test the creation of multiples target values for the same target"""
        target = Attribute(name="Test Target", data_type=DataType.FLOAT, is_label=True)
        db.add(target)
        db.flush()
        target_value1 = AttributeValue(
            value={"float": 1.0},
            attribute=target,
            timestamp=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
        )
        db.add(target_value1)
        db.flush()
        target_value2 = AttributeValue(
            value={"float": 10.0},
            attribute=target,
            timestamp=datetime(2025, 1, 1, 12, 0, 1, tzinfo=UTC),
        )
        db.add(target_value2)

        try:
            db.commit()
        except IntegrityError:
            pytest.fail("Target value should be unique per timestamp")

        assert len(db.query(AttributeValue).all()) == 2


@pytest.mark.usefixtures("cleanup")
@pytest.mark.skip(reason="Skipping taxi project test")
class TestTaxiProject:
    """Test the creation of models for a taxi project"""

    def _setup_instances(self, session: Session) -> None:
        try:
            # 1. Crear un proyecto
            self.project_name = "taxi_analytics " + str(uuid.uuid4())
            project = Project(
                name=self.project_name,
                description="Análisis de viajes en taxi",
                created_by="system",
                updated_by="system",
            )
            session.add(project)
            session.flush()  # Para obtener el ID del proyecto

            # 2. Crear la entidad Trip
            trip_entity = Entity(
                name="trip",
                description="Representa un viaje en taxi",
                project=project,
                created_by="system",
                updated_by="system",
            )
            session.add(trip_entity)
            session.flush()

            # 3. Crear la join key para la entidad Trip
            trip_id_key = JoinKey(
                name="trip:id",
                description="Identificador único del viaje",
                entity=trip_entity,
                created_by="system",
                updated_by="system",
            )
            session.add(trip_id_key)
            session.flush()

            # 4. Crear los features
            trip_distance = Attribute(
                name="trip_distance",
                description="Distancia del viaje en millas",
                data_type=DataType.FLOAT,
                project=project,
                created_by="system",
                updated_by="system",
            )

            trip_duration = Attribute(
                name="trip_duration",
                description="Duración del viaje en minutos",
                data_type=DataType.FLOAT,
                project=project,
                created_by="system",
                updated_by="system",
            )
            session.add_all([trip_distance, trip_duration])
            session.flush()

            # 5. Asociar los features con la entidad
            trip_entity.features.extend([trip_distance, trip_duration])
            session.flush()

            # 5.1 Crear los targets
            target = Attribute(
                name="rating",
                data_type=DataType.FLOAT,
                description="Rating del viaje",
                project=project,
                created_by="system",
                updated_by="system",
                is_label=True,
            )

            session.add(target)
            trip_entity.targets.append(target)
            session.flush()

            # 6. Crear los valores de ejemplo para tres viajes
            trips_data = [
                {
                    "trip_id": "trip_1",
                    "distance": 20.0,
                    "duration": 10.0,
                    "rating": 4.5,
                },
                {
                    "trip_id": "trip_2",
                    "distance": 18.0,
                    "duration": 15.0,
                    "rating": 4.0,
                },
                {
                    "trip_id": "trip_3",
                    "distance": 35.0,
                    "duration": 55.0,
                    "rating": 3.5,
                },
            ]

            for trip_data in trips_data:
                # Crear el valor de la join key (trip_id)
                trip_id_value = JoinKeyValue(
                    value={"string": trip_data["trip_id"]},
                    join_key=trip_id_key,
                    created_by="system",
                    updated_by="system",
                )
                session.add(trip_id_value)
                session.flush()

                # Crear los valores de los features para este viaje
                distance_value = AttributeValue(
                    value={"float": trip_data["distance"]},
                    timestamp=datetime.now(UTC),
                    feature=trip_distance,
                    created_by="system",
                    updated_by="system",
                )

                duration_value = AttributeValue(
                    value={"float": trip_data["duration"]},
                    timestamp=datetime.now(UTC),
                    feature=trip_duration,
                    created_by="system",
                    updated_by="system",
                )
                session.add_all([distance_value, duration_value])
                session.flush()

                # Asociar los valores de los features con el valor de la join key
                distance_value.join_key_value = trip_id_value
                duration_value.join_key_value = trip_id_value
                session.flush()

                # Crear target value
                target_value = AttributeValue(
                    value={"float": trip_data["rating"]},
                    timestamp=datetime.now(UTC),
                    created_by="system",
                    updated_by="system",
                    attribute=target,
                )

                session.add(target_value)
                session.flush()

                target_value.join_key_value_id = trip_id_value.id
                session.flush()

            # Commit todos los cambios
            session.commit()

            # Verificar que todo se guardó correctamente
            print("Datos guardados exitosamente!")
        except Exception as e:
            print(f"Error al guardar los datos: {e}")
            raise
        finally:
            session.rollback()
            session.close()

    def test_creation(self, db: Session) -> None:
        """Test the creation of models for a taxi project"""
        session = db
        try:
            self._setup_instances(db)
        except Exception:
            raise AssertionError("Error during setup instances")

        query = (
            select(Entity)
            .join(Project, Entity.project_id == Project.id)
            .where(Entity.name == "trip")
            .where(Project.name == self.project_name)
        )
        trip: Entity = session.execute(query).scalar_one()
        assert trip is not None
        assert trip.name == "trip"
        assert trip.description == "Representa un viaje en taxi"
        assert len(trip.features) == 2
        assert trip.join_key is not None
        assert trip.join_key.name == "trip:id"
