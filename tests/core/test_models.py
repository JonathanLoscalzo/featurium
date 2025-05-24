from datetime import UTC, datetime
from typing import Generator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from featurium.core.models import (  # Target,; TargetValue,
    Base,
    Entity,
    Feature,
    FeatureValue,
    JoinKey,
    JoinKeyValue,
    Project,
    Target,
    TargetValue,
)


@pytest.fixture()
def get_db_session() -> Generator[Session, None, None]:
    """Create the session"""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        yield session


class TestCreateModels:
    """Test the creation of models"""

    def test_project_model(self) -> None:
        project = Project(name="Test Project")
        assert project.name == "Test Project"

    def test_feature_model(self) -> None:
        feature = Feature(name="Test Feature")
        assert feature.name == "Test Feature"

    def test_feature_value_model(self) -> None:
        feature_value = FeatureValue(value={"string": "Test Feature Value"})
        assert feature_value.value == {"string": "Test Feature Value"}

    def test_entity_model(self) -> None:
        entity = Entity(name="Test Entity")
        assert entity.name == "Test Entity"

    def test_join_key_model(self) -> None:
        join_key = JoinKey(key="Test Join Key")
        assert join_key.key == "Test Join Key"

    def test_join_key_value_model(self) -> None:
        join_key_value = JoinKeyValue(value={"string": "Test Join Key Value"})
        assert join_key_value.value == {"string": "Test Join Key Value"}

    def test_create_and_save(self, get_db_session: Session) -> None:
        project = Project(name="Test Project")
        get_db_session.add(project)
        get_db_session.commit()
        print(get_db_session.query(Project).all())
        assert project.id is not None
        assert project.name == "Test Project"


class TestTaxiProjectEasy:
    def _setup_instances(self, session: Session) -> None:
        try:
            # 1. Crear un proyecto
            project = Project(
                name="taxi_analytics",
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
                name="trip_id",
                key="trip_id",
                entity=trip_entity,
                created_by="system",
                updated_by="system",
            )
            session.add(trip_id_key)
            session.flush()

            # 4. Crear los features
            trip_distance = Feature(
                name="trip_distance",
                description="Distancia del viaje en millas",
                data_type="FLOAT",
                project=project,
                created_by="system",
                updated_by="system",
            )

            trip_duration = Feature(
                name="trip_duration",
                description="Duración del viaje en minutos",
                data_type="FLOAT",
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
            target = Target(
                name="rating",
                description="Rating del viaje",
                project=project,
                created_by="system",
                updated_by="system",
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
                    data_type="STRING",
                    join_key=trip_id_key,
                    created_by="system",
                    updated_by="system",
                )
                session.add(trip_id_value)
                session.flush()

                # Crear los valores de los features para este viaje
                distance_value = FeatureValue(
                    value={"float": trip_data["distance"]},
                    data_type="FLOAT",
                    timestamp=datetime.now(UTC),
                    feature=trip_distance,
                    created_by="system",
                    updated_by="system",
                )

                duration_value = FeatureValue(
                    value={"float": trip_data["duration"]},
                    data_type="FLOAT",
                    timestamp=datetime.now(UTC),
                    feature=trip_duration,
                    created_by="system",
                    updated_by="system",
                )
                session.add_all([distance_value, duration_value])
                session.flush()

                # Asociar los valores de los features con el valor de la join key
                distance_value.join_key_values.append(trip_id_value)
                duration_value.join_key_values.append(trip_id_value)
                session.flush()

                # Crear target value
                target_value = TargetValue(
                    value={"float": trip_data["rating"]},
                    data_type="FLOAT",
                    timestamp=datetime.now(UTC),
                    created_by="system",
                    updated_by="system",
                    target_id=target.id,
                )

                session.add(target_value)
                session.flush()

                target_value.join_key_values.append(trip_id_value)
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

    def test_creation(self, get_db_session: Session) -> None:
        session = get_db_session
        try:
            self._setup_instances(get_db_session)
        except Exception:
            raise AssertionError("Error during setup instances")

        trip: Entity = session.query(Entity).filter_by(name="trip").one()
        assert trip is not None
        assert trip.name == "trip"
        assert trip.description == "Representa un viaje en taxi"
        assert len(trip.features) == 2
        assert len(trip.join_keys) == 1
        assert trip.join_keys[0].key == "trip_id"
