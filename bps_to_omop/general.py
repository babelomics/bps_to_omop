# General functions to aid in the OMOP-CDM ETL

import warnings

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import scipy.stats as st


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


def find_overlap_index(df: pd.DataFrame) -> pd.Series:
    """Finds all rows that:
       - belong to the same person_id
       - are contained with the previous row.
       - are not single day visits
    and removes them.

    Parameters
    ----------
    df : pd.DataFrame
        pandas Dataframe with at least four columns.
        Assumes first column is person_id, second column is
        start_date and third column is end_date.

    Returns
    -------
    pd.Series
        pandas Series with bools. True if row is contained
        with the previous row, False otherwise.
    """
    # 1. Check that current and previous patient are the same
    idx_person = df.iloc[:, 0] == df.iloc[:, 0].shift(1)
    # 2. Check that current start_date is later that previous start_date
    idx_start = df.iloc[:, 1] >= df.iloc[:, 1].shift(1)
    # 3. Check that current end_date is sooner that previous end_date
    idx_end = df.iloc[:, 2] <= df.iloc[:, 2].shift(1)
    # 4. Check that current interval and previos interval are not both single_day
    interval = df.iloc[:, 2] - df.iloc[:, 1]
    idx_int_curr = interval <= pd.Timedelta(1, unit="D")
    idx_int_prev = interval.shift(1) <= pd.Timedelta(1, unit="D")
    idx_interval = ~(idx_int_curr & idx_int_prev)
    # 5. If everything past is true, I can drop the row
    return idx_start & idx_end & idx_person & idx_interval


def remove_overlap(
    df: pd.DataFrame,
    sorting_columns: tuple,
    ascending_order: tuple,
    verbose: int = 0,
    _counter: int = 0,
    _counter_lim: int = 1000,
) -> pd.DataFrame:
    """Removes all rows that are completely contained within
    another row. It will not remove rows that are only partially
    contained within the previous one.

    The function works by sorting the rows by columns. If two or
    more rows are overlapping, only the top one will be kept.


    Parameters
    ----------
    df : pd.DataFrame
        pandas dataframe with overlapping rows to be removed.
        Selection of columns is done by selecting ncols in order.
        This allows its use for different tables with columns
        that have the same purpose but different names.
    sorting_columns : tuple
        Columns to use for sorting.
        Usually, expects 4 columns: 'person_id', 'start_date', 'end_date'
        and some '*_concept_id', like 'visit_concept_id'.
    ascending_order : tuple
        List of bools indicating if each row should have ascending or descending
        order.
        Important! Usually all are true except end_date column. See Notes.
    verbose : int, optional, default 0
        Information output
        - 0 No info
        - 2 Show number of iterations
        - 3 Show an example of the first row being removed and
            the row that contains it.
    _counter : int
        Iteration control param. Number of iterations.
        0 will be used to begin and function will take over.
    _counter_lim : int, optional, default 1000
        Iteration control param. Limit of iterations

    Returns
    -------
    pd.DataFrame
        Copy of input dataframe with contained rows removed.

    Notes
    -------
    The usual behavior is to have 'person_id', 'start_date' and 'end_date'
    as first columns, in ascending, ascending and descending order, respectively.
    This ensures that:
    - All records for the same person are together (sorting by person_id first)
    - Earlier records are placed at the top (sorting by ascending start_date)
    - Longer duration visits are placed at the top (sorting by descending end_date)

    Bear in mind that missing values will be placed at the bottom by default. Any extra
    columns provided will leave any missing values out in case of overlapping records.
    """
    # == Preparation =================================================
    # Sanity checks
    if len(sorting_columns) != len(ascending_order):
        raise ValueError(
            "'sorting_columns' and 'ascending_order' lengths must be equal."
        )

    cond_sort = sorting_columns[:3] != ["person_id", "start_date", "end_date"]
    cond_asce = ascending_order[:3] != [True, True, False]
    if cond_sort or cond_asce:
        warnings.warn(
            "Sorting and ascending initial columns are not the expected order. \
                 Make sure data output is correct."
        )

    # Sort the dataframe if first iteration
    if _counter == 0:
        if verbose > 0:
            print("Removing overlapping rows...")
        if verbose > 1:
            print(f" Iter 0 => {df.shape[0]} initial rows.")
        df = df.sort_values(sorting_columns, ascending=ascending_order)

    # == Find indexes ================================================
    # Get the rows
    idx_to_remove = find_overlap_index(df)

    # == Main "loop" =================================================
    # Prepare next loop
    idx_to_remove_sum = idx_to_remove.sum()
    _counter += 1
    # If there's still room to go, go
    if (idx_to_remove_sum != 0) and (_counter < _counter_lim):
        if verbose > 1:
            # Show iteration and number of rows removed
            print(f" Iter {_counter} => {idx_to_remove_sum} rows removed.")
        if verbose > 2:
            # Get first removed row and show container and contained row
            idx_max = df.index.get_loc(idx_to_remove.idxmax())
            print(f"{df.iloc[(idx_max-1):idx_max+1, :4]}")
        return remove_overlap(
            df.loc[~idx_to_remove], sorting_columns, ascending_order, verbose, _counter
        )
    else:
        return df


def find_person_index(df: pd.DataFrame) -> tuple[pd.Series]:
    """Finds all rows that are contained with the previous
    row, making sure they belong to the same person_id.

    Parameters
    ----------
    df : pd.DataFrame
        pandas Dataframe with at least three columns.
        Assumes first column is person_id, second column is
        start_date and third column is end_date

    Returns
    -------
    tuple[pd.Series]
        Tuple with three pandas Series with bools:
        - idx_person_first, True if first row of the person
        - idx_person_last, True if last row of the person
        - idx_person_only, True if only row of the person
        False otherwise.
    """

    # Create index for first, last or only person in dataset
    idx_person_first = (df.iloc[:, 0] == df.iloc[:, 0].shift(-1)) & (
        df.iloc[:, 0] != df.iloc[:, 0].shift(1)
    )
    idx_person_last = (df.iloc[:, 0] != df.iloc[:, 0].shift(-1)) & (
        df.iloc[:, 0] == df.iloc[:, 0].shift(1)
    )
    idx_person_only = (df.iloc[:, 0] != df.iloc[:, 0].shift(-1)) & (
        df.iloc[:, 0] != df.iloc[:, 0].shift(1)
    )
    return (idx_person_first, idx_person_last, idx_person_only)


def group_dates(df: pd.DataFrame, n_days: int, verbose: int = 0) -> pd.DataFrame:
    """Groups rows of dates from the same person that are less
    than n_days apart, keeping only the first start_date and
    the last end_date, respectively.

    It will remove rows that are partially contained within
    the previous one.

    Parameters
    ----------
    df : pd.DataFrame
        pandas dataframe with at least four columns:
        ['person_id', 'start_date', 'end_date', 'type_concept'].
        Column names do not need to be the same but, the order
        must be the same as here.
        This allows its use for different tables with columns
        that have the same purpose but different names.
    n_days : int
        Minimum number of days between consecutive rows to consider
        them as separate periods. Any interval under n_days will be
        removed. Any interval above n_days will be kept.
    verbose : int, optional
        Information output, default 0
        - 0 No info
        - 1 Show stage of processing

    Returns
    -------
    pd.DataFrame
        Copy of input dataframe with grouped rows.
    """

    # == Preparation ==============================================
    if verbose > 0:
        print("Grouping dates:")
        print("- Sorting and preparing data...")
    # Sort so we know for sure the order is right
    df_rare = df.copy().sort_values(
        [df.columns[0], df.columns[1], df.columns[2]], ascending=[True, True, False]
    )
    # It is VERY important to reset the index to make sure we can
    # retrieve them realiably after sorting them.
    df_rare = df_rare.reset_index(drop=True)

    # == Index look-up ============================================
    if verbose > 0:
        print("- Looking up indexes...")
    (idx_person_first, idx_person_last, idx_person_only) = find_person_index(df_rare)
    # Create index if the break is too big and needs to be kept
    next_interval = df_rare.iloc[:, 1].shift(-1) - df_rare.iloc[:, 2]
    idx_interval = next_interval >= pd.Timedelta(n_days, unit="D")

    # == Retrieve relevant rows ===================================
    if verbose > 0:
        print("- Retrieving rows...")
    # -- start_date and person_id ---------------------------------
    # To retrieve the start_date we need the indexes of:
    # - single day periods (idx_person_only == True)
    # - first dates (idx_person_first == True)
    # - Rows just after period breaks, (idx_interval.index + 1)

    # Get the person condition indexes
    idx_start = df_rare.index[
        idx_person_only | idx_person_first | idx_interval.shift(1)
    ]

    # -- end_date -------------------------------------------------
    # Get the indexes
    idx_end = df_rare.index[idx_person_only | idx_person_last | idx_interval]

    # == Compute type_concept =====================================
    if verbose > 0:
        print("- Computing type_concept...")
    # Iterate over idx_start and idx_end to get the periods
    mode_values = []
    for i in np.arange(len(idx_start)):
        df_tmp = df_rare.loc[idx_start[i] : idx_end[i]]
        mode_values.append(st.mode(df_tmp.iloc[:, 3].values)[0])

        if (verbose > 1) and ((i) % int(len(idx_start) / 4) == 0):
            print(f"  - ({(i+1)/len(idx_start)*100:.1f} %) {(i+1)}/{len(idx_start)}")
    if verbose > 1:
        print(f"  - (100.0 %) {len(idx_start)}/{len(idx_start)}")

    # == Build final dataframe ====================================
    if verbose > 0:
        print("- Closing up...")
    # Create a copy (.loc) with the first two columns
    df_done = df_rare.loc[idx_start, [df.columns[0], df.columns[1]]]
    # Append values found to final dataframe
    df_done[df.columns[2]] = df_rare.loc[idx_end, [df.columns[2]]].values
    # Add to dataframe
    df_done[df.columns[3]] = mode_values

    if verbose > 0:
        print("- Done!")
    return df_done


def find_visit_occurence_id(
    events_df: pd.DataFrame,
    event_columns: list,
    visits_df: pd.DataFrame,
    verbose: int = 0,
) -> pd.DataFrame:
    """
    Find valid date ranges by merging condition and visit occurrence data.

    This function merges input_df and visit occurrence dataframes,
    then filters for input_df start dates that fall within visit date ranges.

    Parameters
    ----------
    events_df : pandas.DataFrame
        Input dataframe for which to assign visit_occurrence_id's
    event_columns : list
        Column names that contains, in this order:
        - 'person_id'   Identifier for each person in the dataframe
        - 'start_date'  Date to fit between 'visit_start_date' and 'visit_end_date'.
        - 'events_id'   Unique identifier for each registry of events_df.
    visits_df : pandas.DataFrame
        DataFrame containing visit occurrence data.
        Must include columns: 'person_id', 'visit_start_date',
        'visit_end_date', 'visit_occurrence_id'.
        Column names need to be the same. This is to ensure
        the correct table (VISIT_OCCURRENCE) is being used.
    verbose : int, optional, default 0
        Verbosity level for function output.
        0: No output
        1: Additionally, print state of the processing
        2+: Additionally, print all debug information

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the original table event_df plus the value for
        the visit_occurence_id, visit_start_date and visit_end_date, if found.

    Raises
    ------
    ValueError
        If required columns are missing in input DataFrames.
    """
    pd.options.mode.copy_on_write = True
    # == Initial message ==============================================
    if verbose > 0:
        print("Looking for visit_occurrence_id matches:")

    # == Initial Checks ===============================================
    # Check for required columns in events_df
    if verbose > 0:
        print(" Checking input...")
    required_input_columns = event_columns
    missing_input_columns = set(required_input_columns) - set(events_df.columns)
    if missing_input_columns:
        raise ValueError(
            f"Missing required columns in events_df: {missing_input_columns}"
        )
    # Check for required columns in visits_df
    required_visit_columns = [
        "person_id",
        "visit_start_datetime",
        "visit_end_datetime",
        "visit_occurrence_id",
    ]
    missing_visit_columns = set(required_visit_columns) - set(visits_df.columns)
    if missing_visit_columns:
        raise ValueError(
            f"Missing required columns in visits_df: {missing_visit_columns}"
        )
    visits_df = visits_df[required_visit_columns]

    # == Force dtypes and sort ========================================
    # Ensure start_date and visit dates are datetime
    if verbose > 0:
        print(" Sorting dataframes...")
    events_df[event_columns[1]] = events_df[event_columns[1]].astype("datetime64[ms]")
    visits_df["visit_start_datetime"] = visits_df["visit_start_datetime"].astype(
        "datetime64[ms]"
    )
    visits_df["visit_end_datetime"] = visits_df["visit_end_datetime"].astype(
        "datetime64[ms]"
    )

    # Drop all duplicates, if visits are not unique we cannot assign them
    visits_df = visits_df.drop_duplicates(
        subset=["person_id", "visit_start_datetime", "visit_end_datetime"], keep=False
    )

    # Sort the neccesary columns of dataframes
    events_df = events_df.sort_values([event_columns[0], event_columns[1]])
    visits_df = visits_df.sort_values(
        [event_columns[0], "visit_start_datetime", "visit_end_datetime"]
    )

    # == Merging ======================================================
    if verbose > 0:
        print(" Combining results...")
    merged_df = pd.merge(
        events_df.reset_index(drop=True),
        visits_df.reset_index(drop=True),
        on=event_columns[0],
        how="left",
    )

    # Check if merge resulted in any matches
    if merged_df["visit_occurrence_id"].isna().all():
        raise ValueError(
            (
                "No matching records found after merging."
                + "Check if person_id values align between dataframes."
            )
        )

    # == Filter for valid ranges ======================================
    if verbose > 0:
        print(" Filtering valid ranges...")
    # Create mask for dates within range
    date_range_mask = (
        merged_df[event_columns[1]] >= merged_df["visit_start_datetime"]
    ) & (merged_df[event_columns[1]] <= merged_df["visit_end_datetime"])
    # Filter only valid ranges
    valid_ranges = merged_df[date_range_mask]

    # Merge with original to retrieve events without visit_occurrence_id
    final_df = pd.merge(
        events_df,
        valid_ranges[
            [
                event_columns[0],
                event_columns[2],
                "visit_occurrence_id",
                "visit_start_datetime",
                "visit_end_datetime",
            ]
        ],
        on=[event_columns[0], event_columns[2]],
        how="left",
    )
    # Sometimes, there might be events that land in visits that share a day.
    # Those would be duplicated on the event_id. Let's drop those duplicates
    # Since they're ordered, we will only lose the second visit, the one
    # that starts with the event
    final_df = final_df.drop_duplicates([event_columns[0], event_columns[2]])

    if verbose > 1:
        if valid_ranges.empty:
            print(
                (
                    " Warning: No valid date ranges found."
                    + "All condition start dates are outside visit date ranges."
                )
            )
        print(f"  Shape of events_df: {events_df.shape}")
        print(f"  Shape of visits_df: {visits_df.shape}")
        print(f"  Shape of merged_df: {merged_df.shape}")
        print(f"  Shape of valid_ranges: {valid_ranges.shape}")
        print(f"  Shape of final_df: {final_df.shape}")

    if verbose > 0:
        print(" Done.")

    return final_df


def fill_omop_table(
    table: pa.Table, omop_schema: pa.Schema, verbose: int = 0
) -> pa.Table:
    """
    Fill missing columns in a PyArrow table to match the OMOP Common Data Model schema.

    This function adds missing columns to the input table based on the provided OMOP schema.
    It handles both nullable and non-nullable fields, creating appropriate default values.

    Parameters
    ----------
    table : pa.Table
        The input PyArrow table to be filled.
    omop_schema : pa.Schema
        The target OMOP schema to conform to.
    verbose : int, optional, default 0
        Verbosity level for function output.
        0: No output
        1+: Prints information about added columns.

    Returns
    -------
    pa.Table
        A PyArrow table with all required columns as per the OMOP schema.

    Notes
    -----
    - For nullable fields, null values are used.
    - For non-nullable fields, default values are used (0 for int64, '' for string).
    - Warnings are issued for field types not explicitly handled (other than int64 and string).
    """
    if verbose > 0:
        print("Adding missing columns...")

    table_size = len(table)
    missing_fields = [
        field for field in omop_schema if field.name not in table.column_names
    ]

    for field in missing_fields:
        if verbose > 0:
            print(
                f"  Adding: {field.name}, Type: {field.type}, Nullable: {field.nullable}"
            )

        if field.type not in [pa.int64(), pa.string(), pa.float64()]:
            print(
                f"Unhandled field type {field.type} for field {field.name}. "
                f"Defaulting to string type."
            )
            field = field.with_type(pa.string())

        default_value = (
            None
            if field.nullable
            else (
                0
                if field.type == pa.int64()
                else 0.0 if field.type == pa.float64() else ""
            )
        )

        if field.nullable:
            array = (
                create_null_int_array(table_size)
                if field.type == pa.int64()
                else (
                    create_null_double_array(table_size)
                    if field.type == pa.float64()
                    else create_null_str_array(table_size)
                )
            )
        else:
            array = (
                create_uniform_int_array(table_size, default_value)
                if field.type == pa.int64()
                else (
                    create_uniform_double_array(table_size, default_value)
                    if field.type == pa.float64()
                    else create_uniform_str_array(table_size, default_value)
                )
            )

        table = table.append_column(field.name, array)

    return table


def reorder_omop_table(table: pa.Table, omop_schema: pa.Schema) -> pa.Table:
    """
    Reorder columns of a PyArrow table to match the OMOP Common Data Model schema.

    Parameters
    ----------
    table : pa.Table
        The input PyArrow table to be reordered.
    omop_schema : pa.Schema
        The target OMOP schema that defines the desired column order.

    Returns
    -------
    pa.Table
        A new PyArrow table with columns reordered to match the OMOP schema.

    Notes
    -----
    - This function assumes that all columns in the OMOP schema are present in the input table.
    - Columns in the input table that are not in the OMOP schema will be excluded from the output.
    """
    column_order = [field.name for field in omop_schema]
    return table.select(column_order)


def format_table(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Formats table to provided schema, adding, removing and renaming
    columns as necessary.

    Parameters
    ----------
    df : pa.Table
        Input table to be formatted
    schema : dict
        Schema information

    Returns
    -------
    pa.Table
        Formatted table
    """
    # -- Finishing up
    # Fill other fields
    table = fill_omop_table(table, schema)
    table = reorder_omop_table(table, schema)
    # Cast to schema
    table = table.cast(schema)

    return table


def rename_table_columns(table: pa.Table, col_map: dict) -> pa.Table:
    """
    Rename columns in a pyarrow Table based on a mapping dictionary.

    Columns not included in the mapping dictionary will be left as is.

    Parameters
    ----------
    table : pa.Table
        The input pyarrow Table whose columns need to be renamed.
    col_map : dict
        Dictionary mapping old column names to new column names.

    Returns
    -------
    pa.Table
        A new pyarrow Table with renamed columns.

    Raises
    ------
    ValueError
        If col_map contains columns that don't exist in the table.

    Examples
    --------
    >>> import pyarrow as pa
    >>> data = pa.table({'a': [1, 2], 'b': [3, 4]})
    >>> col_map = {'a': 'x', 'b': 'y'}
    >>> renamed_table = rename_table_columns(data, col_map)
    >>> renamed_table.column_names
    ['x', 'y']
    """
    # Validate that all columns in col_map exist in the table
    invalid_cols = set(col_map.keys()) - set(table.column_names)
    if invalid_cols:
        raise ValueError(f"Column(s) {invalid_cols} not found in table")

    # Create a mapping for all columns, using original names for unmapped columns
    renamed_cols = {col: col_map.get(col, col) for col in table.column_names}

    # Return the table with renamed columns
    return table.rename_columns([renamed_cols[col] for col in table.column_names])


def create_wide_relationship_table(
    concept_df: pd.DataFrame,
    concept_relationship_df: pd.DataFrame,
    concept_df_right: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Create a wide relationship table by joining CONCEPT and CONCEPT_RELATIONSHIP tables
    from the OMOP common data model.

    This function combines information from the CONCEPT and CONCEPT_RELATIONSHIP tables to
    create a single wide table that allows for easy consultation of relationships between
    medical concepts. It preserves all columns from the CONCEPT table, placing the unique
    columns from CONCEPT_RELATIONSHIP in the center to make consultations easier.
    Uses *_1 and *_2 suffixes to differentiate between left and right CONCEPT tables.

    Parameters
    ----------
    concept_df : pd.DataFrame
        The CONCEPT table from the OMOP CDM.
    concept_relationship_df : pd.DataFrame
        The CONCEPT_RELATIONSHIP table from the OMOP CDM.

    Returns
    -------
    pd.DataFrame
        A wide table containing concept relationships with all attributes from both
        related concepts.

    Notes
    -----
    This function focuses on concepts in the 'Condition' domain.

    The resulting DataFrame will have the following structure:
    - All columns from concept_df suffixed with '_1' for the left concept
    - 'concept_id_1', 'relationship_id', and 'concept_id_2' as 'ci_1','relationshio' and 'ci_2'
        from concept_relationship_df
    - All columns from concept_df suffixed with '_2' for the right concept
    """
    # Convert specified columns to category dtype for memory efficiency
    category_columns = [
        "domain_id",
        "vocabulary_id",
        "concept_class_id",
        "standard_concept",
    ]
    concept_df.loc[:, category_columns] = concept_df[category_columns].astype(
        "category"
    )
    concept_relationship_df["relationship_id"] = concept_relationship_df[
        "relationship_id"
    ].astype("category")

    # Prepare left table (concept 1)
    left_df = concept_df.add_suffix("_1")

    # Prepare central table (relationships)
    central_df = concept_relationship_df[
        ["concept_id_1", "relationship_id", "concept_id_2"]
    ].rename(
        columns={
            "concept_id_1": "ci_1",
            "relationship_id": "relationship",
            "concept_id_2": "ci_2",
        }
    )

    # Prepare right table (concept 2)
    try:
        right_df = concept_df_right.add_suffix("_2")
    except:
        right_df = concept_df.add_suffix("_2")

    # Join left and central tables
    merged_df = pd.merge(
        left_df, central_df, left_on="concept_id_1", right_on="ci_1", how="left"
    )

    # Join merged result with right table
    final_df = pd.merge(
        merged_df, right_df, left_on="ci_2", right_on="concept_id_2", how="left"
    )

    return final_df


def get_icd_codes(code_bps: str, bps_df: pd.DataFrame) -> list[tuple[str, str]]:
    """
    Retrieve ICD OMOP-compatible diagnosis codes for a given BPS (Base Poblacional de Salud) code.

    This function takes a BPS code and a DataFrame containing BPS to CIE (ICD) mappings,
    and returns a list of tuples with the corresponding CIE codes.

    Parameters
    ----------
    code_bps : str
        The BPS code to look up.
    bps_df : pd.DataFrame
        A DataFrame containing the mapping between BPS codes and CIE (ICD) codes.
        Expected columns: 'CODIGO_PATOLOGIA', 'COD_CIE_NORMALIZADO', 'TIPO_CIE'

    Returns
    -------
    list[tuple[str, str]]
        A list of tuples, each containing:
        - The normalized CIE (ICD) code
        - The corresponding OMOP vocabulary ID ('ICD10CM' or 'ICD9CM')

    Notes
    -----
    The function performs the following steps:
    1. Filters the input DataFrame for rows matching the given BPS code.
    2. Extracts the normalized CIE codes.
    3. Maps the Spanish CIE types to OMOP vocabulary IDs.
    4. Pairs each CIE code with its corresponding vocabulary ID.

    The mapping from Spanish to English vocabularies is as follows:
    - 'CIE10ES' -> 'ICD10CM'
    - 'CIE9MC' -> 'ICD9CM'
    """
    # Filter DataFrame for the given BPS code
    matched_rows = bps_df[bps_df["CODIGO_PATOLOGIA"] == code_bps]

    # Extract normalized CIE codes
    cie_codes = matched_rows["COD_CIE_NORMALIZADO"].tolist()

    # Map Spanish CIE types to OMOP vocabulary IDs
    vocabulary_mapping = {"CIE10ES": "ICD10CM", "CIE9MC": "ICD9CM"}
    omop_vocab_ids = matched_rows["TIPO_CIE"].map(vocabulary_mapping).tolist()

    # Pair CIE codes with their corresponding OMOP vocabulary IDs
    return list(zip(cie_codes, omop_vocab_ids))


def map_source_value(
    df: pd.DataFrame,
    target_vocab: dict,
    concept_df: pd.DataFrame,
    source_column: str = "source_value",
    vocabulary_column: str = "vocabulary_id",
    concept_id_column: str = "source_concept_id",
) -> pd.DataFrame:
    """Map source_value concepts to concept IDs across multiple vocabularies.

    This function takes two dataframes - one containing source concepts and another
    containing the OMOP CONCEPT table (concept_df) - and maps the source concepts to their
    corresponding concept IDs. Supports multiple vocabularies in the input data.

    It links either the 'concept_name' or 'concept_code' to the corresponding 'concept_id'.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing source concepts to be mapped.
        Must contain columns:
            - vocabulary_id : str
            - source_value : str
    target_vocab : dict
        Dictionary with target vocabularies as keys and their corresponding
        target column in CONCEPT table as values. Values can be:
            - 'concept_name' to map to concept names in concept_df table.
            - 'concept_code' to map to concept codes in concept_df table.
    concept_df : pd.DataFrame
        Reference DataFrame containing the concept mappings.
        Must contain columns:
            - vocabulary_id : str
            - concept_code : str
            - concept_name : str
            - concept_id : int
    source_column : str, optional, default "source_value"
        Name of the column that has the source values.
    vocabulary_column : str, optional, default "vocabulary_id"
        Name of the column that has the vocabulary_id values.
    concept_id_column : str, optional, default "source_concept_id"
        Name of the column that will be returned with the corresponding concept_id.


    Returns
    -------
    pd.DataFrame
        Input DataFrame with an additional column:
            - condition_source_concept_id : int
                Mapped concept IDs corresponding to source values

    Raises
    ------
    ValueError
        If no vocabulary is found
    KeyError
        If any source concepts are not found in the mapping

    Notes
    -----
    - The original DataFrame is not modified; a copy is returned
    """

    # Create a copy of the input DataFrame to store results
    result_df = df.copy()
    result_df[concept_id_column] = np.nan

    # Process each vocabulary
    for vocab, target in target_vocab.items():
        # Create masks for current vocabulary
        df_mask = df[vocabulary_column] == vocab
        concept_mask = concept_df["vocabulary_id"] == vocab

        # Get subset of data for current vocabulary
        df_subset = df[df_mask]
        concept_subset = concept_df[concept_mask]

        # Get unique concepts for current vocabulary
        unique_concepts = df_subset[source_column].unique()
        mapping_df = concept_subset[concept_subset[target].isin(unique_concepts)]

        # Create mapping for current vocabulary
        concept_map = dict(zip(mapping_df[target], mapping_df["concept_id"]))

        # Update only the rows for current vocabulary
        result_df.loc[df_mask, concept_id_column] = df_subset[source_column].map(
            concept_map
        )

    # Force correct datatypes
    result_df[concept_id_column] = result_df[concept_id_column].astype(pd.Int64Dtype())
    return result_df


def map_source_concept_id(
    df: pd.DataFrame,
    concept_rel_df: pd.DataFrame,
    source_column: str = "source_concept_id",
    concept_id_column: str = "concept_id",
) -> pd.DataFrame:
    """
    Maps source concepts to standard concepts using an OMOP CONCEPT_RELATIONSHIP DataFrame.

    This function identifies source concept IDs in the input DataFrame and maps them
    to their corresponding standard concepts using a 'Maps to' relationship from the
    provided CONCEPT_RELATIONSHIP table.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame containing source concepts.
    concept_rel_df : pandas.DataFrame
        CONCEPT_RELATIONSHIP table DataFrame containing the mapping information.
        Must have columns: 'relationship_id', 'concept_id_1', 'concept_id_2'.
    source_column : str, optional, default "source_concept_id"
        Name of the column in df containing the source concept IDs to be mapped.
    target_column : str, optional, default is "concept_id"
        Name of the output column that will contain the mapped standard concept IDs.

    Returns
    -------
    pandas.DataFrame
        A copy of the input DataFrame with an additional column (specified by target_column)
        containing the mapped standard concepts. Unmapped concepts will be set to 0.

    Notes
    -----
    - The function only considers 'Maps to' relationships from the concept_rel_df
    - Unmapped concepts will be set to 0 in the output
    - The original DataFrame is not modified; a copy is returned
    - All concept IDs are converted to Int64 type

    Examples
    --------
    >>> df = pd.DataFrame({'source_concept_id': [1, 2, 3]})
    >>> concept_rel_df = pd.DataFrame({
    ...     'relationship_id': ['Maps to', 'Maps to'],
    ...     'concept_id_1': [1, 2],
    ...     'concept_id_2': [100, 200]
    ... })
    >>> result = map_source_concept_id(df, concept_rel_df)
    >>> result['source_concept_id']
    0    100
    1    200
    2      0
    Name: source_concept_id, dtype: Int64
    """

    # Filter for 'Maps to' relationships
    mapping_relationships = concept_rel_df[
        concept_rel_df["relationship_id"] == "Maps to"
    ]

    # Create a copy of the input DataFrame to store results
    result_df = df.copy()

    # Get unique concepts
    unique_concepts = df[source_column].unique()
    mapping_df = mapping_relationships[
        mapping_relationships["concept_id_1"].isin(unique_concepts)
    ]

    # Create and apply mapping for current vocabulary
    concept_map = dict(zip(mapping_df["concept_id_1"], mapping_df["concept_id_2"]))

    # Update only the rows for current vocabulary
    result_df[concept_id_column] = result_df[source_column].map(concept_map)

    # Fill unmapped values (NaN) with 0
    result_df[concept_id_column] = result_df[concept_id_column].fillna(0)

    # Force correct datatypes
    result_df[source_column] = result_df[source_column].astype(pd.Int64Dtype())
    result_df[concept_id_column] = result_df[concept_id_column].astype(pd.Int64Dtype())

    return result_df


def normalize_text(text):
    """Normalize a string by converting to lowercase and removing accents.

    Parameters
    ----------
    text : str
        The text to be normalized.

    Returns
    -------
    str
        The normalized text.

    Raises
    ------
    TypeError
        If the input `text` is not a string.
    """
    if not isinstance(text, str):
        raise TypeError("Input must be a string")

    # Dictionary mapping accented characters to their non-accented equivalents
    accent_mappings = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ü": "u",
        "ñ": "n",
        "Á": "a",
        "É": "e",
        "Í": "i",
        "Ó": "o",
        "Ú": "u",
        "Ü": "u",
        "Ñ": "n",
    }

    # Convert to lowercase first
    text = text.lower()

    # Replace accented characters
    for accented, normal in accent_mappings.items():
        text = text.replace(accented, normal)

    return text


def create_vocabulary_mapping(
    df: pd.DataFrame,
    vocabulary_df: pd.DataFrame,
    source_column: str,
    vocab_code_column: str,
    vocab_value_column: str,
) -> dict:
    """
    Create a mapping dictionary between source codes and their corresponding vocabulary values.

    Parameters
    ----------
    df : pandas.DataFrame
        The source dataframe containing the codes to be mapped.
    vocabulary_df : pandas.DataFrame
        The vocabulary dataframe containing the mapping information.
    source_column : str
        The column name in source_df containing the codes to be mapped.
    vocab_code_column : str
        The column name in vocabulary_df containing the codes that match source_column.
    vocab_value_column : str
        The column name in vocabulary_df containing the values to map to.

    Returns
    -------
    dict
        A dictionary mapping codes from source_column to their corresponding values.
    """
    unique_codes = df[source_column].unique()

    filtered_vocab = vocabulary_df.loc[
        vocabulary_df[vocab_code_column].isin(unique_codes),
        [vocab_code_column, vocab_value_column],
    ].drop_duplicates()

    mapping_dict = filtered_vocab.set_index(vocab_code_column)[
        vocab_value_column
    ].to_dict()

    return mapping_dict


def apply_source_mapping(
    table: pa.Table, value_mappings: dict, output_columns: dict = None
) -> pa.Table:
    """
    Apply value mappings to specified columns in a PyArrow table and create new mapped columns.

    This function takes source value columns and creates corresponding concept_id columns
    based on provided mappings. For each source column, it creates a new column with '_concept_id'
    suffix containing the mapped values.

    Parameters
    ----------
    table : pa.Table
        Input PyArrow table containing the source value columns to be mapped
    value_mappings : dict
        Dictionary where keys are column names and values are mapping dictionaries.
        Each mapping dictionary maps source values to their corresponding concept IDs.
    output_columns : Optional, dict, default None
        Optional dictionary mapping source column names to desired output column names.
        If not provided or if a column is not in the dictionary, defaults to replacing
        '_source_value' with '_concept_id'.

    Returns
    -------
    pa.Table
        PyArrow table with additional columns containing the mapped values.
        New columns are named by replacing '_source_value' with '_concept_id' in
        the original column names.

    Examples
    --------
    >>> mappings = {
    ...     "gender_source_value": {"M": 8507, "F": 8532}
    ... }
    >>> table = pa.table({
    ...     "gender_source_value": ["M", "F", "M"]
    ... })
    >>> result = apply_source_mapping(table, mappings)
    >>> print(result.column_names)
    ['gender_source_value', 'gender_concept_id']
    """
    result_table = table

    for source_column, mapping in value_mappings.items():
        # Vectorize the mapping function with a default value of None
        vectorized_map = np.vectorize(lambda x: mapping.get(x, None))

        # Apply mapping and convert to PyArrow array
        mapped_values = pa.array(vectorized_map(table[source_column].to_numpy()))

        # Get output column name from custom mapping or use default
        if output_columns:
            concept_column = output_columns.get(
                source_column, source_column.replace("_source_value", "_concept_id")
            )
        else:
            concept_column = source_column.replace("_source_value", "_concept_id")

        # Append new column with mapped values
        result_table = result_table.append_column(concept_column, mapped_values)

    return result_table


def find_unmapped_values(
    df: pd.DataFrame, source_value_column: str, source_concept_id_column: str
) -> list:
    """
    Identify source values that have no concept ID mapping (NaN values).

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame containing source values and concept IDs
    source_value_column : str
        Name of column containing original source values/codes
    source_concept_id_column : str
        Name of column containing existing concept ID mappings

    Returns
    -------
    list
        List of source values that have no concept ID mapping

    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'source_code': ['A1', 'B2', 'C3'],
    ...     'source_concept_id': [123, np.nan, 789]
    ... })
    >>> unmapped = find_unmapped_values(df, 'source_code', 'source_concept_id')
    >>> print(unmapped)  # ['B2']
    """
    existing_mappings = (
        df.set_index(source_value_column)[source_concept_id_column]
        .drop_duplicates()
        .to_dict()
    )

    return [
        value
        for value, concept_id in existing_mappings.items()
        if (pd.isna(concept_id) | (concept_id == 0))
    ]


def update_concept_mappings(
    df: pd.DataFrame,
    source_value_column: str,
    source_concept_id_column: str,
    target_concept_id_column: str,
    new_concept_mappings: dict,
) -> pd.DataFrame:
    """
    Update concept mappings in a DataFrame using provided new mappings.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame containing source values and concept IDs
    source_value_column : str
        Name of column containing original source values/codes
    source_concept_id_column : str
        Name of column containing existing concept ID mappings
    target_concept_id_column : str
        Name of column where updated concept IDs will be stored
    new_concept_mappings : dict
        Dictionary of {source_value: concept_id} pairs to update existing mappings

    Returns
    -------
    pandas.DataFrame
        Copy of input DataFrame with updated concept ID mappings if unmapped values
        exist, otherwise returns original DataFrame unchanged

    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'source_code': ['A1', 'B2', 'C3'],
    ...     'source_concept_id': [123, np.nan, 789],
    ...     'concept_id': [None, None, None]
    ... })
    >>> new_mappings = {'B2': 456}
    >>> result = update_concept_mappings(
    ...     df, 'source_code', 'source_concept_id', 'concept_id', new_mappings
    ... )
    """
    result_df = df.copy()
    existing_mappings = (
        result_df.set_index(source_value_column)[source_concept_id_column]
        .drop_duplicates()
        .to_dict()
    )

    try:
        # Update existing mappings with new ones
        existing_mappings.update(new_concept_mappings)

        # Apply updated mappings to create new concept ID column
        result_df[target_concept_id_column] = result_df[source_value_column].map(
            existing_mappings
        )
    except KeyError as e:
        print(f"Error: No concept ID found for source value '{e.args[0]}'")

    return result_df
