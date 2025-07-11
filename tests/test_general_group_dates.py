import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.append("../bps_to_omop/")
from utils.common import group_dates


# == TESTS =============================================================================
def test_simple_grouping():
    """Test when there is basic grouping"""
    nombre_columnas = ["person_id", "start_date", "end_date", "type_concept"]
    n_days = 365
    df_in = [
        # Estas fechas deberían juntarse porque están a menos de 365 dias
        # type_concept debería ser 2
        (1, "2020-01-01", "2020-02-01", 1),
        (1, "2020-03-01", "2020-04-01", 2),
        (1, "2020-05-01", "2020-12-01", 2),
        # Esta última de la misma persona no
        (1, "2022-01-01", "2022-01-01", 2),
    ]
    df_in = pd.DataFrame.from_records(df_in, columns=nombre_columnas).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    df_out = [
        # Estas fechas deberían juntarse porque están a menos de 365 dias
        # type_concept debería ser 2
        (1, "2020-01-01", "2020-12-01", 2),
        # Esta última de la misma persona no
        (1, "2022-01-01", "2022-01-01", 2),
    ]
    df_out = pd.DataFrame.from_records(df_out, columns=nombre_columnas).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )
    result = group_dates(df_in, n_days).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, df_out)


def test_concatenated_dates():
    """Test handling of concatenated periods"""
    nombre_columnas = ["person_id", "start_date", "end_date", "type_concept"]
    n_days = 365
    df_in = [
        (2, "2020-01-01", "2020-06-01", 1),
        (2, "2020-03-01", "2020-09-01", 1),
        (2, "2020-06-01", "2020-12-01", 2),
    ]
    df_in = pd.DataFrame.from_records(df_in, columns=nombre_columnas).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    df_out = [
        (2, "2020-01-01", "2020-12-01", 1),
    ]
    df_out = pd.DataFrame.from_records(df_out, columns=nombre_columnas).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )
    result = group_dates(df_in, n_days).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, df_out)


def test_dates_not_close():
    """Test behavior when dates are not close"""
    nombre_columnas = ["person_id", "start_date", "end_date", "type_concept"]
    n_days = 365
    df_in = [
        (3, "2021-01-01", "2021-01-01", 1),
        (3, "2023-02-01", "2023-02-01", 2),
        (3, "2024-03-01", "2024-04-01", 3),
    ]
    df_in = pd.DataFrame.from_records(df_in, columns=nombre_columnas).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    df_out = [
        (3, "2021-01-01", "2021-01-01", 1),
        (3, "2023-02-01", "2023-02-01", 2),
        (3, "2024-03-01", "2024-04-01", 3),
    ]
    df_out = pd.DataFrame.from_records(df_out, columns=nombre_columnas).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )
    result = group_dates(df_in, n_days).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, df_out)


def test_dates_close_but_different_person():
    """Test behavior when dates are close but person is different"""
    nombre_columnas = ["person_id", "start_date", "end_date", "type_concept"]
    n_days = 365
    df_in = [
        (4, "2024-01-01", "2024-02-01", 1),
        (5, "2025-01-01", "2025-02-01", 2),
    ]
    df_in = pd.DataFrame.from_records(df_in, columns=nombre_columnas).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    df_out = [
        (4, "2024-01-01", "2024-02-01", 1),
        (5, "2025-01-01", "2025-02-01", 2),
    ]
    df_out = pd.DataFrame.from_records(df_out, columns=nombre_columnas).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )
    result = group_dates(df_in, n_days).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, df_out)


def test_dates_close_enough_but_sparse():
    """Should be close because they are close, but if you remove one
    the others are too far apart and will not group up."""
    nombre_columnas = ["person_id", "start_date", "end_date", "type_concept"]
    n_days = 365
    df_in = [
        (6, "2020-01-01", "2020-12-01", 1),
        (6, "2021-01-01", "2021-12-01", 2),
        (6, "2022-01-01", "2022-12-01", 2),
        (6, "2023-01-01", "2023-12-01", 2),
    ]
    df_in = pd.DataFrame.from_records(df_in, columns=nombre_columnas).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )

    df_out = [
        (6, "2020-01-01", "2023-12-01", 2),
    ]
    df_out = pd.DataFrame.from_records(df_out, columns=nombre_columnas).assign(
        start_date=lambda x: pd.to_datetime(x["start_date"]),
        end_date=lambda x: pd.to_datetime(x["end_date"]),
    )
    result = group_dates(df_in, n_days).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, df_out)
