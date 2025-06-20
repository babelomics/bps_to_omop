# General functions to aid in the OMOP-CDM ETL

import warnings

import numpy as np
import pandas as pd
import polars as pl
import scipy.stats as st


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


def find_visit_occurrence_id(
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
    # Transform to polars dataframes
    events_df = pl.from_pandas(events_df)
    visits_df = pl.from_pandas(visits_df)

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
    visits_df = visits_df.select(required_visit_columns)

    # == Force dtypes and sort ========================================
    # Ensure start_date and visit dates are datetime
    if verbose > 0:
        print(" Sorting dataframes...")
    events_df = events_df.with_columns(pl.col(event_columns[1]).cast(pl.Datetime("ms")))
    visits_df = visits_df.with_columns(
        pl.col("visit_start_datetime").cast(pl.Datetime("ms")),
        pl.col("visit_end_datetime").cast(pl.Datetime("ms")),
    )

    # Drop all duplicates, if visits are not unique we cannot assign them
    visits_df = visits_df.unique(
        subset=["person_id", "visit_start_datetime", "visit_end_datetime"], keep="none"
    )

    # Sort the neccesary columns of dataframes
    events_df = events_df.sort([event_columns[0], event_columns[1]])
    visits_df = visits_df.sort(
        [event_columns[0], "visit_start_datetime", "visit_end_datetime"]
    )

    # == Merging ======================================================
    if verbose > 0:
        print(" Combining results...")
    merged_df = visits_df.join(events_df, on=event_columns[0], how="left")

    # Check if merge resulted in any matches
    if merged_df["visit_occurrence_id"].is_null().all():
        raise ValueError(
            (
                "No matching records found after merging."
                + "Check if person_id values align between dataframes."
            )
        )

    # == Filter for valid ranges ======================================
    if verbose > 0:
        print(" Filtering valid ranges...")
    # Filter only valid ranges within range
    valid_ranges = merged_df.filter(
        pl.col(event_columns[1]).is_between(
            pl.col("visit_start_datetime"), pl.col("visit_end_datetime"), closed="both"
        )
    )
    valid_ranges = valid_ranges[
        [
            event_columns[0],
            event_columns[2],
            "visit_occurrence_id",
            "visit_start_datetime",
            "visit_end_datetime",
        ]
    ]

    # Merge with original to retrieve events without visit_occurrence_id
    final_df = events_df.join(
        valid_ranges,
        on=[event_columns[0], event_columns[2]],
        how="left",
    )

    # Sometimes, there might be events that land in visits that share a day.
    # Those would be duplicated on the event_id. Let's drop those duplicates
    # Since they're ordered, we will only lose the second visit, the one
    # that starts with the event
    final_df = final_df.unique(subset=[event_columns[0], event_columns[2]])

    if verbose > 1:
        if valid_ranges.is_empty():
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

    return final_df.to_pandas()


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
