from typing import Generator

import pytest
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import Session

from featurium.core.models import Base


@pytest.fixture()
def db() -> Generator[pytest.Session, None, None]:
    """Create the session"""
    engine = create_engine("sqlite:///featurium.db")
    # engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(bind=engine) as session:
        yield session


@pytest.fixture()
def cleanup(db: Session) -> None:
    """Clean up the database after the test"""
    metadata = MetaData()
    metadata.reflect(bind=db.get_bind())  # Carga todas las tablas
    with db.begin():
        for table in reversed(metadata.sorted_tables):  # Reverso por dependencias FK
            db.execute(table.delete())  # DELETE FROM table
    yield
    # Base.metadata.drop_all(db.get_bind())
