from dataclasses import dataclass
from datetime import datetime
from typing import Any, List

import pandas as pd


@dataclass
class BaseTestCase:
    test_case_id: str
    description: str
    project_name: str
    entity_name: str
    join_keys: List[Any] | None = None
    feature_names: List[str] | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    include_timestamp: bool = False


@dataclass
class TestCase(BaseTestCase):
    expected_features: List[str] | None = None
    expected_entities: List[str] | None = None
    expected_row_count: int | None = None


@dataclass
class ErrorTestCase(BaseTestCase):
    expected_error: type = None


class BaseTestCase:
    def validate_result_structure(self, result, test_case):
        """Valida la estructura básica del resultado"""
        assert (
            not result.empty
        ), f"Result should not be empty for {test_case.test_case_id}"

    def validate_expected_features(self, result, test_case):
        """Valida que se retornan las features esperadas"""
        if test_case.expected_features:
            actual_features = result.columns.unique().tolist()
            assert set(actual_features) == set(
                test_case.expected_features
            ), f"Expected {test_case.expected_features}, got {actual_features}"

    def validate_expected_entities(self, result, test_case):
        """Valida que se retornan las entidades esperadas"""
        if test_case.expected_entities:
            # Asumiendo que hay una columna que identifica la entidad
            actual_entities = result.index.unique().tolist()
            assert set(actual_entities) == set(
                test_case.expected_entities
            ), f"Expected {test_case.expected_entities}, got {actual_entities}"

    def validate_row_count(self, result, test_case):
        """Valida el número de filas esperadas"""
        if test_case.expected_row_count:
            assert (
                result.shape[0] == test_case.expected_row_count
            ), f"Expected {test_case.expected_row_count} rows, got {result.shape[0]}"

    def validate_date_range(self, result, test_case):
        """Valida que las fechas están en el rango correcto"""
        if test_case.start_time and test_case.end_time:
            timestamps = pd.to_datetime(result["timestamp"])
            assert all(timestamps >= test_case.start_time)
            assert all(timestamps <= test_case.end_time)
            assert all(timestamps <= test_case.end_time)
