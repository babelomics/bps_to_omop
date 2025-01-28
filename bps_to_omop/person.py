# 05/07/2024
#
# Este archivo agrupa las transformaciones necesarias para generar
# la tabla PERSON en una instancia OMOP-CDM.
#
# https://ohdsi.github.io/CommonDataModel/cdm54.html#person
#
# http://omop-erd.surge.sh/omop_cdm/tables/PERSON.html
#

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

# == person_id ==
# Esta columna se extrae del campo COD_NUHSA
# En principio vamos a dejarlo igual, para que sea fácil de transponer en el resto de tablas


def transform_person_id(
    input_table: pa.Table, input_fieldname: str
) -> tuple[pa.array, pa.array]:
    """Transform field_name from input_table to fit into
    person_id et al fields in an OMOP-CDM instance.

    person_id tiene must be integer, we removed letters and
    changed to int32.

    Parameters
    ----------
    input_table : pa.Table
        Input table to extract the field from.
    input_fieldname : str
        field name of the input table

    Returns
    -------
    tuple[pa.array,pa.Array]
        tuple with two pyarrow arrays for the OMOP fields:
        ('person_id','person_source_value')
    """
    person_source_value_tmp = input_table[input_fieldname]
    # Remove first letters and switch to int32
    person_id_tmp = pc.utf8_slice_codeunits(
        person_source_value_tmp, 2
    )  # pylint: disable=E1101
    person_id_tmp = person_id_tmp.cast(pa.int64())

    return (person_id_tmp, person_source_value_tmp)


# == year_of_birth et al ==
# Esta columna se extrae del campo FECNAC
# De la columna BPS FECNAC con formato YYYYMMDD se extraen
# 3 grupos de enteros, year_of_birth, month_of_birth y day_of_birth
def transform_person_dates(
    input_table: pa.Table, input_fieldname: str
) -> tuple[pa.array, pa.array, pa.array]:
    """Transforms from FECNAC column in a BPS instance
    to year_of_birth, month_of_birth and day_of_birth
    fields compatible with an OMOP-CDM instance.

    Parameters
    ----------
    input_table : pa.Table
        Input table to extract the field from.
    input_fieldname : str
        field name of the input table

    Returns
    -------
    tuple[pa.array, pa.array, pa.array]
        tuple with two pyarrow arrays for the OMOP fields:
        ('year_of_birth','month_of_birth','day_of_birth)
    """

    # Sacamos el array que transformaremos
    date = input_table[input_fieldname]
    # Año: Nos quedamos los 4 primeros numeros y cambiamos a int32
    try:
        year_of_birth = pc.year(date).cast(pa.int32())  # pylint: disable=E1101
        month_of_birth = pc.month(date).cast(pa.int32())  # pylint: disable=E1101
        day_of_birth = pc.day(date).cast(pa.int32())  # pylint: disable=E1101
    except Exception as exc:
        raise ValueError(
            "Could not infer dates. Check column type is correct."
        ) from exc

    # Acabar con person_id
    return (year_of_birth, month_of_birth, day_of_birth)


def build_date_columns(input_table: pa.table):
    """
    Extracts year, month, and day components from a start_date column and appends them
    as new columns to the input table.

    Parameters
    ----------
    input_table : pyarrow.Table
        Input table containing a 'start_date' column.

    Returns
    -------
    pyarrow.Table
        A new table with three additional columns:
        - 'year_of_birth': Extracted year from start_date
        - 'month_of_birth': Extracted month from start_date
        - 'day_of_birth': Extracted day from start_date

    Examples
    --------
    >>> table = pa.table({'start_date': ['2000-01-01', '1995-12-31']})
    >>> result = build_date_columns(table)
    >>> print(result.column_names)
    ['start_date', 'year_of_birth', 'month_of_birth', 'day_of_birth']
    """

    year, month, day = transform_person_dates(input_table, "start_date")
    output_table = input_table.append_column("year_of_birth", year)
    output_table = output_table.append_column("month_of_birth", month)
    output_table = output_table.append_column("day_of_birth", day)

    return output_table


def transform_gender(
    input_table: pa.Table, input_fieldname: tuple[str, str], mapping: dict
) -> tuple[pa.array, pa.array, pa.array]:
    """
    Generates gender relate field for an OMOP-CDM instance.

    Parameters
    ----------
    input_table : pa.Table
        Table from wich to retrieve gender data
    input_fieldname : tuple[str, str]
        Field names where gender code and natural language values are
        stored. First item is the code, second item is natural language.
    mapping : dict
        mapping to transform from source to OMOP

    Returns
    -------
    tuple[pa.array, pa.array, pa.array]
        _description_
    """
    # TODO: Simplify this. Sometimes you only have one field. Just do the mapping.
    # -- Sacamos el array que transformaremos
    # Por convenio, el primer campo, por defecto CODSEXO,
    # representa el código del género, que definirá el
    # gender_concept_id y al gender_source_concept_id.
    # El segundo campo representa el valor en lenguage natural,
    # que irá al gender_source_value
    inp_field_code = input_table[input_fieldname[0]]
    inp_field_lang = input_table[input_fieldname[1]]

    # Guardamos ya los sources values
    gender_source_value_tmp = inp_field_lang
    gender_source_concept_id_tmp = inp_field_code
    # Pasamos a numpy
    tmp = inp_field_code.to_numpy()
    try:
        gender_concept_id_tmp = np.vectorize(mapping.get)(tmp)
    except TypeError as exc:
        raise TypeError("Mapping was unsuccesful. Check mapping completeness.") from exc
    return (
        gender_concept_id_tmp,
        gender_source_concept_id_tmp,
        gender_source_value_tmp,
    )


if __name__ == "__main__":
    print("Ahoy!. Making some tests...")

    # Definimos test table
    table = pa.Table.from_arrays(
        [
            pa.array(
                [
                    "AN0000049915",
                    "AN0000660611",
                    "AN0001236042",
                    "AN0001262415",
                    "AN0001613231",
                ],
                pa.string(),
            ),
            pa.array([19491229, 19310514, 19300207, 19391002, 19360922], pa.int32()),
            pa.array([1, 0, 1, 0, 3], pa.int32()),
            pa.array(
                ["Hombre", "Mujer", "Hombre", "Mujer", "Desconocido"], pa.string()
            ),
        ],
        schema=pa.schema(
            [
                pa.field("ID", pa.string()),
                pa.field("date", pa.int32()),
                pa.field("sexcode", pa.int32()),
                pa.field("sex", pa.string()),
            ]
        ),
    )
    # Testeamos transform_person_id
    person_id, person_source_value = transform_person_id(
        input_table=table, input_fieldname="ID"
    )
    assert person_id == pc.utf8_slice_codeunits(table["ID"], 2).cast(pa.int32())
    assert person_source_value == table["ID"]

    # Testeamos transform_year_of_birth
    year, month, day = transform_person_dates(input_table=table, input_fieldname="date")
    assert year == pc.utf8_slice_codeunits(table["date"].cast(pa.string()), 0, 4).cast(
        pa.int32()
    )
    assert month == pc.utf8_slice_codeunits(table["date"].cast(pa.string()), 4, 6).cast(
        pa.int32()
    )
    assert day == pc.utf8_slice_codeunits(table["date"].cast(pa.string()), 6, 8).cast(
        pa.int32()
    )

    # Testeamos transform_gender
    mapping_dict = {1: 8532, 0: 8507, 3: 0}
    result = transform_gender(
        input_table=table, input_fieldname=["sexcode", "sex"], mapping=mapping_dict
    )
    gender_concept_id, gender_source_concept_id, gender_source_value = result
    assert gender_concept_id.tolist() == [8532, 8507, 8532, 8507, 0]
    assert gender_source_value.to_pylist() == [
        "Hombre",
        "Mujer",
        "Hombre",
        "Mujer",
        "Desconocido",
    ]
    assert gender_source_concept_id.to_pylist() == [1, 0, 1, 0, 3]
