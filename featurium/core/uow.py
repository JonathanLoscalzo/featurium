from contextlib import contextmanager
from types import TracebackType
from typing import Optional, Type

from sqlalchemy.orm import Session


class UnitOfWork:
    def __init__(self, session: Session):
        self.session = session or Session()

    def __enter__(self) -> "UnitOfWork":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if exc_val:
            self.session.rollback()
        else:
            self.session.commit()
        self.session.close()

    @contextmanager
    def get_session(self):
        """Helper function to manage sessions"""
        session = Session()
        try:
            yield session
        except Exception as e:
            session.rollback()
            raise UnitOfWorkException(e)
        finally:
            session.close()


class UnitOfWorkException(Exception):
    """Exception raised by the UnitOfWork"""

    pass
