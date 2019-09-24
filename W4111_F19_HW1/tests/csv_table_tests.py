from src.CSVDataTable import CSVDataTable
import logging
import os
import time
import json


# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.ERROR)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.ERROR)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = '../Data/baseballdatabank-2019.2/core/'


def load_data():

    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv",
        "delimiter": ";"
    }
    print("Table is Appearances and key_columns are ['yearID','teamID','lgID','playerID']")
    csv_tbl = CSVDataTable("Appearances", connect_info, key_columns=['yearID','teamID','lgID','playerID'])
    print("Loaded table = \n", csv_tbl)
    return True

load_data()
