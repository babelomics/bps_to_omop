"""
Common functions to aid in the OMOP-CDM ETL process
"""

import pandas as pd
import polars as pl


def find_visit_occurrence_id(
    events_df: pd.DataFrame,
    event_columns: list,
    visits_df: pd.DataFrame,
    verbose: int = 0,
) -> pd.DataFrame:
    """
    Find valid date ranges by merging an input table with visit occurrence data.

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


def retrieve_visit_in_batches(
    events_df: pd.DataFrame,
    event_columns: list,
    visit_df: pd.DataFrame,
    batch_size: int = 10000,
) -> pd.DataFrame:
    """Serially retrieves a table to match the visit's dates with the dates in the input
    dataframe.

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
    batch_size : int, optional
        _description_, by default 10000

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the original table event_df plus the value for
        the visit_occurence_id, visit_start_date and visit_end_date, if found.
    """
    # -- Iterate over unique ppl
    # Get list of unique ppl
    list_ppl = events_df["person_id"].unique()

    # Process serially in batches
    df_out = []
    for i_init in list(range(0, len(list_ppl), batch_size)):
        # Retrieve only ppl_batch number of ppl
        try:
            list_ppl_tmp = list_ppl[i_init : i_init + batch_size]
        except IndexError:
            list_ppl_tmp = list_ppl[i_init:]
        # Restrict dataframes to those ppl
        df_tmp = events_df[events_df["person_id"].isin(list_ppl_tmp)]
        visit_tmp = visit_df[visit_df["person_id"].isin(list_ppl_tmp)]
        # Find the visit_occurrence_id for this batch
        out_tmp = find_visit_occurrence_id(df_tmp, event_columns, visit_tmp, verbose=0)
        df_out.append(out_tmp)

    # Concatenate and return
    return pd.concat(df_out)


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
