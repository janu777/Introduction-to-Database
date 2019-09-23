import csv

from src.CSVDataTable import CSVDataTable


connect_info = {
    "directory": '../Data/baseballdatabank-2019.2/core/',
    "file_name": "Appearances.csv"
}

csv_tbl = CSVDataTable("people", connect_info, None)
result = csv_tbl.find_by_primary_key(['BS1','barnero01','31'], ['yearID','teamID','lgID'])
result1 = csv_tbl.find_by_template(result)
print(result,result1)
# csv_data = load_csv(dir_path, file_name)
# row = dict(csv_data[0])
# m1 = matches_template(row, {"birthDay": "25", "birthCountry": "USA"})
# print("Match = ", m1)
# cols = get_columns(row, ['playerID', 'nameLast'])
# print("The requested columns are = ", cols)
