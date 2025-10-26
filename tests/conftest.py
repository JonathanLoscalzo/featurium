from typing import Any, Generator

import pytest
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import Session

from featurium.core.models import Base


@pytest.fixture()
def db() -> Generator[Session, None, None]:
    """Create the session"""
    # engine = create_engine("sqlite:///featurium.db")
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(bind=engine) as session:
        yield session


@pytest.fixture()
def cleanup(db: Session) -> Generator[None, Any, None]:
    """Clean up the database after the test"""
    metadata = MetaData()
    metadata.reflect(bind=db.get_bind())  # Carga todas las tablas
    with db.begin():
        for table in reversed(metadata.sorted_tables):  # Reverso por dependencias FK
            db.execute(table.delete())  # DELETE FROM table
    yield
    # Base.metadata.drop_all(db.get_bind())


@pytest.fixture()
def db_validator(db: Session):
    """Create a DBValidator instance for database validation in tests."""
    from tests.integration_tests.db_validator import DBValidator

    return DBValidator(db)
