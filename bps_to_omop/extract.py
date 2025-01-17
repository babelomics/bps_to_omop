# 14/08/2024
#
# General functions to aid in the OMOP-CDM ETL extraction stage

import os
import re
from itertools import product
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import yaml


def get_file_paths_on_cond(
    dir_path: str, end_str: str = None, start_str: str = None
) -> list[str]:
    """Returns a list of files present on a giving directory.

    Will filter looking for specific start or end strings in filename.

    Parameters
    ----------
    dir_path : str
        path where to look for files
    end_str : str, optional
        String that must appear at the end of filename, by default None
    start_str : str, optional
        String that must appear at the start of filename, by default None

    Returns
    -------
    list[str]
        list of filenames present in 'dir_path' after filtering.
        Will include the 'dir_path' path as well.
    """
    file_list = os.listdir(dir_path)
    if end_str:
        file_list = [f"{dir_path}/{f}" for f in file_list if f.endswith(end_str)]
    if start_str:
        file_list = [f"{dir_path}/{f}" for f in file_list if f.startswith(start_str)]
    # Normalize
    file_list = [os.path.normpath(f) for f in file_list]
    return file_list


def try_read(
    filename: str,
    candidate_params: dict,
    default_params: dict,
    funcs_to_check: list,
    nrows: int = 100,
) -> list:
    """try to read a csv file with the provided parameters

    Parameters
    ----------
    filename : str
        path and filename of the csv file to read.
    candidate_params : dict
        candidate params we are trying.
    default_params : dict
        other parameters needed for reading.
    funcs_to_check : list
        List of functions to apply
        dataframe is valid.
    nrows : int, by default 100
        number of rows to read. More rows allow for more precise
        data inferring, but requires more time.

    Returns
    -------
    list
        [Dataframe and None] if success,
        [None and error message] if failure.
    """
    # Try to read the file and make some checks
    try:
        # == First test: file reading
        df = pd.read_csv(filename, **candidate_params, **default_params, nrows=nrows)
        # == Second test: If only one column, not realistic
        if len(df.columns) == 1:
            raise AssertionError("Only one column read.")
        # == Apply external tests
        for func in funcs_to_check:
            func(df)
        return (df, None)
    # If anything fails, return None and error
    except (AssertionError, ValueError) as inst:
        return (None, inst)
    except TypeError as inst:
        raise TypeError("Error reading. Check if funcs_to_check is a list.") from inst


def is_first_col_NUHSA_like(df: pd.DataFrame):
    """Check if first column of a dataframe is compatible
    with NUSHA formatting.

    Firstly checks if first column contains NUHSA in the name.

    Secondly checks if values follow the format 'AN0123456789'.
    ie. AN followed by 10 numbers.

    Parameters
    ----------
    df : pd.DataFrame
        dataframe to check
    """
    # 1 - Check if first column is the NUHSA code
    str_to_check = ["NUHSA", "NUSA"]
    if not any(x in df.columns[0].upper() for x in str_to_check):
        raise AssertionError("First column name does not contain 'NUHSA'")
    # 2 - Check if NUHSA number fits the pattern
    if not (
        df.iloc[:, 0].str.startswith("AN")
        & df.iloc[:, 0].str[2:].str.isnumeric()
        & (df.iloc[:, 0].str.len() == 12)
    ).all():
        raise AssertionError("First column values do not match the NUHSA format")


def generate_param_combinations(candidate_params: dict) -> list[dict]:
    """Generates all combinatios for key value
    pairs for the values provided in candidate_params
    for each key.

    Parameters
    ----------
    candidate_params : dict
        dictionary of possible options for each parameter

    Returns
    -------
    list[dict]
        List with all possible combinations
    """
    # Get the keys and values from the candidate_params dictionary
    keys = list(candidate_params.keys())
    values = list(candidate_params.values())

    # Generate all combinations of parameter values
    combinations = list(product(*values))

    # Create a list of dictionaries for each combination
    result = []
    for combo in combinations:
        result.append(dict(zip(keys, combo)))

    return result


def get_reading_params(
    data_dir: Path,
    file_list: list,
    default_params: dict,
    candidate_params: dict,
    funcs_to_check: list,
    verbose: int = 0,
) -> dict:
    """Get the successful reading parameters from
    a candidates_params dict.

    Parameters
    ----------
    data_dir : PosixPath
        Common directory for all files.
    file_list : list
        Path of files from data_dir to try to read.
    default_params : dict
        Parameters that should work on every file.
    candidate_params : dict
        Parameters to test from.
    funcs_to_check : list
        Checking functions to verify that resulting
        dataframe is valid.
    verbose : int, optional
        Information output, by default 0
        - 0 No info
        - 1 Show file being processed and succesful params

    Returns
    -------
    dict
        Dictionary with filenames/paths as keys and
        dictionary of sucessful parameters as values.
    """
    # Transform to path
    data_dir_local = Path(data_dir)

    # Iteramos sobre los archivos
    readoptions_dict = {}
    for f in file_list[:]:
        # -- Read the file
        # Get all possible combinations
        candidate_params_list = generate_param_combinations(candidate_params)
        # Initialize the lists for the file
        for params in candidate_params_list:
            _, error = try_read(
                data_dir_local / f, params, default_params, funcs_to_check
            )
            if error is None:
                readoptions_dict[f] = params
                readoptions_dict[f].update(default_params)
        if len(readoptions_dict) == 0:
            raise AssertionError(
                f"{f} was not read succesfully."
                + f"\nParams tried{candidate_params_list}"
            )
        elif verbose > 0:
            print(f" {f} => {readoptions_dict[f]}")

    # Return
    return readoptions_dict


def find_matching_keys_on_dict(
    dictionary: dict[str : list[str]], search_words: list[str]
) -> dict[set]:
    """Given a dictionary with filenames as keys and a list
    of column names as values, will look for strings contained in those
    lists and return a dictionary with the column names that matched.

    It looks for substrings, not whole words.

    Parameters
    ----------
    dictionary : dict[str:list[str]]
        dictionary where keys are filenames, values are a list
        with the column names
    search_words : list[str]
        list of search words to use

    Returns
    -------
    dict[set]
        dictionary where keys are filenames, values are a list
        with the column names that matched.
    """
    # Initialiaze returning dict
    matching_keys = {}
    # Compile regex patterns for each search word
    # re.IGNORECASE me vale para ignorar las caps
    patterns = [
        re.compile(rf"{re.escape(word)}", re.IGNORECASE) for word in search_words
    ]
    # Iterate over dict of list of column names
    for key, string_list in dictionary.items():
        # Find strings in each list
        matching_keys[key] = find_matching_keys(string_list, search_words, patterns)
    return matching_keys


def find_matching_keys(
    string_list: list[str], search_words: list[str], patterns=None
) -> list:
    """Given a list of search words, returns any string in
    'string_list' that is contained in the list 'search_words'.

    Parameters
    ----------
    string_list : list[str]
        list of strings to parse
    search_words : list[str]
        list of search words to look for
    patterns : _type_, optional
        regex pattern to be used, by default will
        look for substrings, not whole words, without
        case sensitivity.

    Returns
    -------
    list
        list of strings in string_list that matched
        with any words in search_words
    """

    if patterns is None:
        patterns = [
            re.compile(rf"{re.escape(word)}", re.IGNORECASE) for word in search_words
        ]
    matches = set()
    for string in string_list:
        # iterate over the regex patterns
        for pattern in patterns:
            # If pattern matches
            if pattern.search(string):
                matches.add(string)
                break
    return list(matches)


def find_matching_keys_on_files(
    data_dir: Path,
    input_files: list,
    readoptions_dict: dict,
    search_words: list[str] = ("fecha", "fec", "inicio", "fin", "f_"),
    verbose: int = 0,
) -> dict[str : list[str]]:
    """Given a list of files and a readptions_dict (see get_reading_params),
    finds columns in each file that contains words contained in search_words

    Parameters
    ----------
    data_dir : PosixPath
        Common directory for all files.
    file_list : list
        Path of files from data_dir to try to read.
    readoptions_dict : dict
        dictionary where the key is the file path and the value is a
        dict with the list of options to pass to pd.read_csv to
        read the file correctly.
    search_words : list[str], optional
        search words to look in the columns,
        by default ['fecha', 'fec', 'inicio', 'fin', 'f_']
    verbose : int, optional
        Information output, by default 0
        - 0 No info
        - 1 Show file being processed and matched columns

    Returns
    -------
    dict[str:list[str]]
        Dictionary with keys as file names and values as a list of
        columns in each file that contained any of the search_words.
    """
    date_columns = {}
    for f in input_files:
        d = pd.read_csv(data_dir / f, nrows=2, **readoptions_dict[f])
        column_names = d.columns.to_list()
        date_columns[f] = find_matching_keys(column_names, search_words)
        if verbose > 0:
            print(" ", f, " => ", date_columns[f])
    return date_columns


def get_date_parser_options(
    data_dir: list,
    file_list: list,
    date_columns: dict,
    candidate_formats: dict,
    csv_readoptions_dict: dict,
    nrows: int = 1000,
    verbose: int = 0,
) -> list[dict, dict]:
    """Get the succesful parsing parameters for the
    date columns specified in 'date_columns' for all
    files in 'file_list'.

    It will first to try to nicely transform to
    datetime. If no combination works nicely, it will
    try to coerce the transformation, reporting (if
    verbose >= 1) the number of values that were transformed
    to nans/nulls in the process.

    Parameters
    ----------
    data_dir : PosixPath
        Common directory for all files.
    file_list : list
        Path of files from data_dir to try to read.
    date_columns : dict
        Dict with date columns (values) for each file (keys)
    candidate_formats : dict
        Dict with the possible formats (values) for each
        option for the function pd.to_datetime (keys)
    csv_readoptions_dict : dict
        Dict with reading parameters options (values) for each
        file (keys). See get_reading_params().
    nrows : int, optional
        rows to use to identify date parsing, by default 1000.
        More rows are more reliable but require more time.
    verbose : int, optional
        Information output, by default 0
        - 0 No info
        - 1 Show file being processed
        - 2 Also show in-out columns

    Returns
    -------
    list[dict, dict]
        First dict contains the date parsing options (values)
        for each file (keys).
        Second dict contains the same information but only
        for those file were coercing was neccesary.
        Useful for refining date parsing or verifying.
    """

    # Prepare params to try
    error_params = {"errors": ["raise", "coerce"]}
    error_params.update(candidate_formats)
    params_dict = generate_param_combinations(error_params)
    # Iterate over files
    df_date_formats = {}
    df_date_formats_coercions = {}
    for f in file_list[:]:

        # -- Verbose! --
        if verbose > 0:
            print(" ", f)
        # --------------

        # Read first nrows as string with readoptions
        df = pd.read_csv(
            data_dir / f,
            nrows=nrows,
            dtype="str",
            usecols=date_columns[f],
            **csv_readoptions_dict[f],
        )
        # Iterate over dte columns
        tmp_dict = {}
        for col in date_columns[f]:
            # Iterate over possible options and try parsing
            tmp_dict[col] = None
            for options in params_dict:
                try:
                    date = pd.to_datetime(df[col], **options)
                    # If pd.to_datetime has not failed, we save.
                    tmp_dict[col] = options

                    # -- Verbose! --
                    if verbose > 0:
                        print("  ", col, " => ", options)
                    if verbose > 1:
                        print(pd.concat([df[col], date], axis=1).head(5))
                    # --------------

                    # == Manage coercing ==
                    # if option was to coerce, save it and give a warning.
                    if options["errors"] == "coerce":
                        df_date_formats_coercions[f] = {col: options}
                        # Give the warning
                        n = date.isna().sum()
                        n_p = n / nrows * 100
                        if verbose > 0:
                            print(
                                f'  Coercing dates at field "{col}"\n '
                                + f"{n} nan values retrieved from {nrows} rows read (~{n_p:.2f} %)."
                                + "\n => Check file and ensure that parsing is correct."
                            )

                    # If we're here reading wass succesful, get out of params_dict loop
                    break
                except ValueError:
                    pass
            else:
                # If we got here, no options were found, report it.
                if verbose > 0:
                    print(col, " => ", None, "-- (!!!) --")

        # == Save result ==
        df_date_formats[f] = tmp_dict

    # == Return ==
    return (df_date_formats, df_date_formats_coercions)


def find_start_end_dates(
    df_pq: pa.Table, col_names: list[str], verbose: int = 0
) -> tuple:
    """Given a pyarrow table and 1 or 2 columns with datetimes,
    find the column that goes first (start) and the one that goes
    after (end).

    Parameters
    ----------
    df_pq : pa.Table
        pyarrow table
    col_names : list[str]
        column labels of the datetime columns to compare
    verbose : int, optional
        Information output, by default 0
        - 0 No info
        - 1 Show date order

    Returns
    -------
    tuple
        tuple with start array and end array

    Raises
    ------
    ValueError
        If dates are not always in order,
        if there are more than 2 names in col_names
    TypeError
        If no names were provided in col_names
    """

    # Retrieve the inidivual date columns
    col_values = [df_pq[col] for col in col_names]

    # == Good cases ==
    # If 1 column, assign the same
    if len(col_names) == 1:
        start_tmp, end_tmp = col_values[0], col_values[0]
        if verbose > 0:
            print(f" {col_names[0]} is the only date column")

    # If 2 columns, find out which one is first
    elif len(col_names) == 2:

        # -- all equal --
        # if all dates are equal the order doesnt matter
        if pc.all(
            pc.equal(col_values[0], col_values[1])
        ).as_py():  # pylint: disable=E1101
            start_tmp, end_tmp = col_values[0], col_values[1]
            if verbose > 0:
                print(f" {col_names[0]} is the same as {col_names[1]}")

        # -- First column is start date --
        # If the first col is always less or equal than the second
        # The first col is the start and the second is the end.
        elif pc.all(
            pc.less_equal(col_values[0], col_values[1])
        ).as_py():  # pylint: disable=E1101
            start_tmp, end_tmp = col_values[0], col_values[1]
            if verbose > 0:
                print(f" {col_names[0]} happens before {col_names[1]}")

        # -- Second column is start date --
        # If the first col is always greater or equal than the second
        # The second col is the start and the first is the end
        elif pc.all(
            pc.greater_equal(col_values[0], col_values[1])
        ).as_py():  # pylint: disable=E1101
            start_tmp, end_tmp = col_values[1], col_values[0]
            if verbose > 0:
                print(f" {col_names[1]} happens before {col_names[0]}")

        # In any other case, raise
        else:
            raise ValueError("Dates are not always in order between columns.")

    # == Bad cases ==
    elif len(col_names) == 0:
        raise TypeError("No columns provided in col_names")
    else:
        raise ValueError("More than 2 columns have to be especially handled")

    return (start_tmp, end_tmp)


def read_yaml_config(config_file_path: str) -> dict:
    """
    Reads a YAML configuration file and returns its contents as a dictionary.

    This function safely loads a YAML file, which can contain configuration
    options, settings, or any structured data in YAML format.

    Parameters
    ----------
    config_file_path : str
        The file path to the YAML configuration file to be read.

    Returns
    -------
    dict
        A dictionary containing all the configuration options and data
        structures defined in the YAML file.

    Raises
    ------
    FileNotFoundError
        If the specified configuration file does not exist.
    yaml.YAMLError
        If there's an error parsing the YAML content.

    Examples
    --------
    >>> config = read_yaml_config('settings.yaml')
    >>> print(config['database']['host'])
    'localhost'
    """
    try:
        with open(config_file_path, "r", encoding="utf-8") as config_file:
            # Use safe_load to prevent arbitrary code execution
            config_data = yaml.safe_load(config_file)
        return config_data
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Configuration file not found: {config_file_path}"
        ) from e
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML file: {e}") from e


def update_yaml_config(
    config_file_path: str,
    new_entry_key: str,
    new_entry_data: dict,
) -> None:
    """
    Reads a YAML configuration file, updates or adds information in a specified section,
    and writes the changes back to the file.

    This function allows for adding or updating information in any top-level section of
    the YAML file.
    If the specified section doesn't exist, it will be created. The function also
    updates the 'Last_modified' field in the 'Metadata' section with the current timestamp.

    Parameters
    ----------
    config_file_path : str
        Path to the YAML configuration file to be read and updated.
    new_entry_key : str
        The key or label for the new information being added or updated within the
        specified section.
    new_entry_data : dict
        A dictionary containing the information to be added or updated in the YAML file.

    Returns
    -------
    None
        This function doesn't return any value, but it modifies the specified YAML file in-place.

    Examples
    --------
    >>> update_yaml_config('config.yaml', 'new_entry', {'key': 'value'})
    >>> update_yaml_config('config.yaml', 'visit_1', {'date': '2023-09-26', 'doctor': 'Dr. Smith'})
    """
    # Read existing configuration and append new one
    try:
        config_data = read_yaml_config(config_file_path)
        config_data[new_entry_key] = new_entry_data
    except (FileNotFoundError, TypeError):
        config_data = new_entry_data

    # Write updated configuration back to file
    with open(config_file_path, "w+", encoding="utf-8") as config_file:
        yaml.dump(
            config_data,
            config_file,
            sort_keys=False,
            width=float("inf"),
            default_flow_style=False,
        )


def apply_modifications(data_dir: Path, yaml_file: str, verbose: int = 0) -> None:
    """
    Apply modifications described in yaml file and save the modified files.

    This function reads a YAML configuration file, applies the specified
    modifications to the input files, and saves the modified files to
    the designated output locations.

    Parameters
    ----------
    data_dir : pathlib.Path
        Common folder where input and output files are stored. Usually provided by
        environmental variables.
    yaml_file : str
        Path to the YAML configuration file.
        Expected to contain the following keys:
            - input_dir: Path to the directory containing the input files.
            - input_files: List of input filenames.
            - output_dir: Path to the directory where the modified files will be saved.
            - output_files: List of output filenames for each input file.
                          e.g., {'file1.csv': 'output1.parquet', 'file2.csv': 'output2.parquet'}.
            - read_options: Dictionary of read options for each input file,
                          e.g., {'file1.csv': {'sep': ';'}, 'file2.csv': {'header': None}}.
            - date_formats: Dictionary of date formatting options for each file and column,
                           e.g.,
                           {'file1.csv': {'date_column': {'format': '%Y-%m-%d'}},
                            'file2.csv': {'timestamp_col': {'format': '%Y-%m-%d %H:%M:%S'}}}

    verbose : int, optional
        Verbosity level.
        - 0: No information output. (default)
        - 1: Show the stage in modification.
        - 2: Also show parameters used in reading and formatting files.

    Examples
    --------
    >>> apply_modifications("modifications.yaml")
    >>> apply_modifications("modifications.yaml", verbose=2)

    """

    # Get the extraction configuration parameters
    if verbose > 0:
        print("Reading configuration file...")

    yaml_dict = read_yaml_config(yaml_file)

    input_dir = data_dir / yaml_dict["input_dir"]
    output_dir = data_dir / yaml_dict["output_dir"]
    input_files = yaml_dict["input_files"]
    output_files = yaml_dict["output_files"]
    read_options = yaml_dict["read_options"]
    date_formats = yaml_dict["date_formats"]

    # Make sure we can write!
    os.makedirs(output_dir, exist_ok=True)

    # Iterate over filse and apply the changes
    if verbose > 0:
        print("Applying transformations...")
    for i, f in enumerate(input_files[:]):
        if verbose > 0:
            print(f" ({i/len(input_files)*100:<4.2f} %) Reading {f}")
        if verbose > 1:
            print(f" > Read Options: \n{read_options[f]}")
        # Read file
        df = pd.read_csv(input_dir / f, **read_options[f], dtype="str")
        # Read dates and resave
        for col, options in date_formats[f].items():
            try:
                df[col] = pd.to_datetime(df[col], **options)
                if verbose > 1:
                    print(f" > {col} date format: \n{options}")
            # Sometimes, if not enough rows were checked with the
            # date parser, you'll find a row that cannot be parsed
            # deep in the file. This forces the conversion
            # => If you got here, you should be sure that the format
            #    is correct.
            except ValueError:
                if verbose > 0:
                    print("  > (!) Date formatting failed! Coercing...")
                options["errors"] = "coerce"
                df[col] = pd.to_datetime(df[col], **options)

        # Before saving, try to enforce objects types
        df = df.infer_objects()
        if verbose > 1:
            print(" > Resulting datatypes:")
            print(df.info())
        # Save to parquet
        new_name = output_dir / output_files[f]
        os.makedirs(new_name.parent, exist_ok=True)
        output_files[f] = f"{f.replace('txt', 'parquet')}"
        df.to_parquet(new_name)

    if verbose > 0:
        print("Done!")
