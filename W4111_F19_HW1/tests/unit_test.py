import csv

from src.CSVDataTable import CSVDataTable


# from src.BaseDataTable import BaseDataTable


# def load_csv(dir_path, file_name):
#     path = dir_path + file_name
#     result = []
#     with open(path, "r") as in_file:
#         csv_file = csv.DictReader(in_file)
#         for r in csv_file:
#             result.append(r)
#     return result
#
#
# def matches_template(row, template):
#     result = True
#     if template is not None:
#         for k, v in template.items():
#             if v != row.get(k, None):
#                 result = False
#                 break
#     return result
#
#
# def get_columns(row, col_list):
#     result = {}
#     for c in col_list:
#         result[c] = row[c]
#     return result


connect_info = {
    "directory": '../Data/baseballdatabank-2019.2/core/',
    "file_name": "Appearances.csv"
}

csv_tbl = CSVDataTable("people", connect_info, None)
result = csv_tbl.find_by_primary_key(['25','25'], None)
print(result)
# csv_data = load_csv(dir_path, file_name)
# row = dict(csv_data[0])
# m1 = matches_template(row, {"birthDay": "25", "birthCountry": "USA"})
# print("Match = ", m1)
# cols = get_columns(row, ['playerID', 'nameLast'])
# print("The requested columns are = ", cols)
