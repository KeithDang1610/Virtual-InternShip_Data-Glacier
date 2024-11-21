import logging
import os
import subprocess
import yaml
import pandas as pd
import datetime
import gc
import re

##############
# Utilities  #
##############

def read_config_file(filepath):
  # Reads a YAML configuration file and returns the parsed content.
    if not os.path.exists(filepath):
        logging.error(f"Config file not found: {filepath}")
        raise FileNotFoundError(f"Config file not found: {filepath}")
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(f"Error reading YAML file: {exc}")
            raise

def replacer(string, char):
  # Replaces consecutive occurrences of a character in a string with a single instance.
    pattern = char + '{2,}'
    string = re.sub(pattern, char, string)
    return string

##################
# Data Validation #
##################

def col_header_val(df, table_config):
    logging.info("Starting column header validation.")
  
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(r'[^\w]', '_', regex=True)
    df.columns = list(map(lambda x: x.strip('_'), list(df.columns)))
    df.columns = list(map(lambda x: replacer(x, '_'), list(df.columns)))

    expected_col = list(map(lambda x: x.lower(), table_config['columns']))
    expected_col.sort()
    
    df.columns = list(map(lambda x: x.lower(), list(df.columns)))
    df = df.reindex(sorted(df.columns), axis=1)

    if len(df.columns) == len(expected_col) and list(expected_col) == list(df.columns):
        logging.info("Column name and length validation passed.")
        print("Column name and column length validation passed.")
        return 1
    else:
        logging.warning("Column name and column length validation failed.")
        print("Column name and column length validation failed.")

        mismatched_columns_file = list(set(df.columns).difference(expected_col))
        missing_YAML_file = list(set(expected_col).difference(df.columns))

        print("Following File columns are not in the YAML file:", mismatched_columns_file)
        print("Following YAML columns are not in the uploaded file:", missing_YAML_file)

        logging.error(f"File columns not in YAML: {mismatched_columns_file}")
        logging.error(f"YAML columns missing in file: {missing_YAML_file}")

        return 0

####################
# File Processing  #
####################

def read_large_file(filepath, sep=',', chunksize=10**6):
    logging.info(f"Reading file: {filepath}")
    try:
        chunk_list = []
        for chunk in pd.read_csv(filepath, sep=sep, chunksize=chunksize):
            chunk_list.append(chunk)
        logging.info(f"File read successfully: {filepath}")
        return pd.concat(chunk_list, axis=0)
    except Exception as e:
        logging.error(f"Error reading file {filepath}: {e}")
        raise

def write_file(df, filepath, sep='|', compression='gzip'):
    logging.info(f"Writing file to {filepath}")
    try:
        df.to_csv(filepath, sep=sep, index=False, compression=compression)
        logging.info(f"File written successfully: {filepath}")
    except Exception as e:
        logging.error(f"Error writing file {filepath}: {e}")
        raise

####################
# Summary Function #
####################

def generate_file_summary(df, filepath):
    total_rows, total_columns = df.shape
    file_size = os.path.getsize(filepath)

    summary = {
        "Total Rows": total_rows,
        "Total Columns": total_columns,
        "File Size (MB)": file_size / (1024 ** 2)
    }

    print(f"Summary: {summary}")
    logging.info(f"File Summary: {summary}")
    return summary