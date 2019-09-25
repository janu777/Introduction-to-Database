import csv

from src.CSVDataTable import CSVDataTable


connect_info = {
    "directory": '../Data/baseballdatabank-2019.2/core/',
    "file_name": "Appearances.csv"
}

csv_tbl = CSVDataTable("Appearances", connect_info, ['yearID','teamID','playerID',''])
result = csv_tbl.find_by_primary_key(['1871','CL1','bassjo01'])
result1 = csv_tbl.find_by_template({'teamID':'TRO','playerID':'abercda01'})
result2 = csv_tbl.delete_by_key(['1871','CL1','bassjo01'])
result3 = csv_tbl.delete_by_template({'teamID':'TRO','playerID':'abercda01'})
result4 = csv_tbl.update_by_key(['1871','RC1','barkeal01'],['1996','JGS','janane1'])
result5 = csv_tbl.update_by_template({'teamID':'JGS','playerID':'janane1'},{'teamID':'JJJ','playerID':'jjjjjjjjjj'})
result6 = csv_tbl.insert({'G_ss': '15', 'teamID': 'BS1', 'G_lf': '0',
                          'lgID': 'NA', 'yearID': '1871', 'G_3b': '0',
                          'G_defense': '31', 'G_rf': '0', 'G_all': '31',
                          'G_pr': '0', 'GS': '31', 'G_batting': '31', 'G_1b': '0',
                          'G_p': '0', 'playerID': 'barnero01', 'G_c': '0', 'G_cf': '0',
                          'G_2b': '16', 'G_ph': '0', 'G_of': '0', 'G_dh': '0'})
print('find_by_primary_key',result)
print('find_by_template',result1)
print('delete_by_key',result2)
print('delete_by_template',result3)
print('update_by_key',result4)
print('update_by_template',result5)
print('insert',result6)
# csv_data = load_csv(dir_path, file_name)
# row = dict(csv_data[0])
# m1 = matches_template(row, {"birthDay": "25", "birthCountry": "USA"})
# print("Match = ", m1)
# cols = get_columns(row, ['playerID', 'nameLast'])
# print("The requested columns are = ", cols)
