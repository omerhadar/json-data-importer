import json
import pandas as pd
from glob import glob
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
from functools import partial

class JsonImporter:
    """
    Parallel JSON file importer and transformer.

    The Importer reads multiple input files from a directory, parses them in
    parallel using multiple processes, normalizes nested JSON structures into
    pandas DataFrames according to a configuration file, and returns one
    DataFrame per logical table.

    Parsing behavior, column selection, and renaming are fully driven by the
    provided table configuration.

    Notes
    -----
    - Parsing is performed using ``ProcessPoolExecutor`` to improve performance.
    - Each input file is parsed independently and results are merged per table.

    Parameters
    ----------
    table_config_file_path : str
        Path to a JSON configuration file defining:
        - the root record path
        - output table names
        - column selection and renaming
        - optional nested tables
    files_dir : str
        Directory containing input files to be parsed.
    num_of_workers : int
        Number of worker processes to use for parallel parsing.

    Attributes
    ----------
    table_config : dict
        Parsed table configuration loaded from ``table_config_file_path``.
    files : list[str]
        List of file paths discovered in ``files_dir``.
    num_of_workers : int
        Number of worker processes used during parsing.

    Methods
    -------
    parse()
        Parse all input files in parallel and return normalized tables.

    Returns
    -------
    dict[str, pandas.DataFrame]
        A dictionary mapping table names to merged pandas DataFrames.
        Each DataFrame contains data aggregated from all input files
        and includes only the configured columns, renamed as specified.

    Examples
    --------
    >>> importer = JsonImporter(
    ...     table_config_file_path="table_config.json",
    ...     files_dir="input_files",
    ...     num_of_workers=4
    ... )
    >>> tables = importer.parse_files()
    >>> users_df = tables["user_data"]
    """

    def __init__(self, table_config_file_path, files_dir, num_of_workers):
        with open(table_config_file_path, 'r') as config_file:
            self.table_config = json.load(config_file)

        self.files = glob(f'{files_dir}/*')
        self.num_of_workers = num_of_workers

    @staticmethod
    def parse_single_file(file_path, config):
        with open(file_path, 'r') as input_file:
            data = json.load(input_file)

        result_tables = {config['table_name']: pd.json_normalize(data, record_path=config['record_path'])}
        for nested_table in config['nested_tables']:
            result_tables[nested_table['table_name']] = pd.json_normalize(
                data,
                record_path=[config['record_path'], nested_table['record_path']],
                meta=nested_table['parent_id']
            )
        return result_tables

    def parse_files(self):

        result = defaultdict(list)
        with ProcessPoolExecutor(max_workers=self.num_of_workers) as executor:
            for file, res in zip(self.files,
                                 executor.map(partial(self.parse_single_file, config=self.table_config), self.files)):
                for table in res.keys():
                    result[table].append(res[table])
                    print(f'finished parsing {file}')
            for table in result.keys():
                result[table] = pd.concat(result[table], ignore_index=True)
            print('finished merging')

        for table in result.keys():
            result[table] = (
                result[table].reindex(columns=list(self.table_config['columns'].keys()))
                .rename(columns=self.table_config['columns'])
                .reset_index(drop=True)
            )

        return result
