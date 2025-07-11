"""
Utility functions to manage pyarrow tablese along the OMOP-CDM ETL process
"""

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc


def create_uniform_int_array(length: int, value: int = 0) -> pa.array:
    """Create an uniform int array with a specific length

    By default is an array of zeroes, can be modified
    defining a integer value.

    Parameters
    ----------
    length : int
        length of the array.
    value : int, optional, default 0
        Value that fills the array

    Returns
    -------
    pa.array
        pyarrow array with int64 datatype.
    """
    # creamos un array de zeros con numpy y
    # lo pasamos a pyarrow forzando int64
    zeros = pa.array(np.zeros(shape=length), pa.int64())
    if value == 0:
        return zeros
    else:
        # Sumamos la cantidad que sea
        return pc.add(zeros, value)  # pylint: disable=E1101


def create_uniform_str_array(length: int, string: str) -> pa.array:
    """Create an uniform string array with a specific length


    Parameters
    ----------
    length : int
        length of the array.
    string : str
        string to be repeated.

    Returns
    -------
    pa.array
        pyarrow array filled with null and string datatype.
    """
    return pa.array([string] * length, pa.string())


def create_null_int_array(length: int) -> pa.array:
    """Create an uniform null array with a specific length


    Parameters
    ----------
    length : int
        length of the array.

    Returns
    -------
    pa.array
        pyarrow array filled with null and int64 datatype.
    """
    return pa.nulls(length, pa.int64())


def create_null_str_array(length: int) -> pa.array:
    """Create an uniform null array with a specific length


    Parameters
    ----------
    length : int
        length of the array.

    Returns
    -------
    pa.array
        pyarrow array filled with null and string datatype.
    """
    return pa.nulls(length, pa.string())


def create_null_double_array(length: int) -> pa.array:
    """Create an uniform null array with a specific length


    Parameters
    ----------
    length : int
        length of the array.

    Returns
    -------
    pa.array
        pyarrow array filled with null and double datatype.
    """
    return pa.array([None] * length, type=pa.float64())


def create_uniform_double_array(length: int, value: int = 0) -> pa.array:
    """Create an uniform double array with a specific length

    By default is an array of zeroes, can be modified
    defining a integer value.

    Parameters
    ----------
    length : int
        length of the array.
    value : int, optional, default 0
        Value that fills the array

    Returns
    -------
    pa.array
        pyarrow array with int64 datatype.
    """
    return pa.array([value] * length, type=pa.float64())
