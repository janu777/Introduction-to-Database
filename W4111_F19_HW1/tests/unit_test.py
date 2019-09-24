import csv

from src.CSVDataTable import CSVDataTable


connect_info = {
    "directory": '../Data/baseballdatabank-2019.2/core/',
    "file_name": "Appearances.csv"
}

csv_tbl = CSVDataTable("Appearances", connect_info, ['yearID','teamID','lgID',''])
result = csv_tbl.find_by_primary_key(['BS1','barnero01','31'], ['yearID','teamID','lgID'])
result1 = csv_tbl.find_by_template(result[0])
result2 = csv_tbl.delete_by_key(['BS1','barnero01','31'])
result3 = csv_tbl.delete_by_template(result[0])
result4 = csv_tbl.update_by_key(['TRO','NA','abercda01'],['BS1','barnero01','31'])
result5 = csv_tbl.update_by_template({'playerID': 'bealsto01'},{'playerID':'janane123'})
result6 = csv_tbl.insert([{'G_ss': '15', 'teamID': 'BS1', 'G_lf': '0', 'lgID': 'NA', 'yearID': '1871', 'G_3b': '0', 'G_defense': '31', 'G_rf': '0', 'G_all': '31', 'G_pr': '0', 'GS': '31', 'G_batting': '31', 'G_1b': '0', 'G_p': '0', 'playerID': 'barnero01', 'G_c': '0', 'G_cf': '0', 'G_2b': '16', 'G_ph': '0', 'G_of': '0', 'G_dh': '0'}, {'G_ss': '0', 'teamID': 'BS1', 'G_lf': '13', 'lgID': 'NA', 'yearID': '1871', 'G_3b': '0', 'G_defense': '18', 'G_rf': '4', 'G_all': '18', 'G_pr': '0', 'GS': '17', 'G_batting': '18', 'G_1b': '0', 'G_p': '0', 'playerID': 'barrofr01', 'G_c': '0', 'G_cf': '0', 'G_2b': '1', 'G_ph': '0', 'G_of': '17', 'G_dh': '0'}, {'G_ss': '0', 'teamID': 'BS1', 'G_lf': '0', 'lgID': 'NA', 'yearID': '1871', 'G_3b': '0', 'G_defense': '29', 'G_rf': '27', 'G_all': '29', 'G_pr': '0', 'GS': '29', 'G_batting': '29', 'G_1b': '0', 'G_p': '0', 'playerID': 'birdsda01', 'G_c': '7', 'G_cf': '0', 'G_2b': '0', 'G_ph': '0', 'G_of': '27', 'G_dh': '0'}, {'G_ss': '0', 'teamID': 'BS1', 'G_lf': '18', 'lgID': 'NA', 'yearID': '1871', 'G_3b': '0', 'G_defense': '19', 'G_rf': '1', 'G_all': '19', 'G_pr': '0', 'GS': '18', 'G_batting': '19', 'G_1b': '0', 'G_p': '0', 'playerID': 'conefr01', 'G_c': '0', 'G_cf': '0', 'G_2b': '0', 'G_ph': '0', 'G_of': '19', 'G_dh': '0'}, {'G_ss': '0', 'teamID': 'BS1', 'G_lf': '0', 'lgID': 'NA', 'yearID': '1871', 'G_3b': '0', 'G_defense': '31', 'G_rf': '1', 'G_all': '31', 'G_pr': '0', 'GS': '31', 'G_batting': '31', 'G_1b': '30', 'G_p': '0', 'playerID': 'gouldch01', 'G_c': '0', 'G_cf': '0', 'G_2b': '0', 'G_ph': '0', 'G_of': '1', 'G_dh': '0'}, {'G_ss': '1', 'teamID': 'BS1', 'G_lf': '0', 'lgID': 'NA', 'yearID': '1871', 'G_3b': '0', 'G_defense': '16', 'G_rf': '0', 'G_all': '16', 'G_pr': '0', 'GS': '15', 'G_batting': '16', 'G_1b': '0', 'G_p': '0', 'playerID': 'jackssa01', 'G_c': '0', 'G_cf': '1', 'G_2b': '14', 'G_ph': '0', 'G_of': '1', 'G_dh': '0'}, {'G_ss': '0', 'teamID': 'BS1', 'G_lf': '0', 'lgID': 'NA', 'yearID': '1871', 'G_3b': '1', 'G_defense': '29', 'G_rf': '5', 'G_all': '29', 'G_pr': '0', 'GS': '29', 'G_batting': '29', 'G_1b': '0', 'G_p': '0', 'playerID': 'mcveyca01', 'G_c': '29', 'G_cf': '0', 'G_2b': '0', 'G_ph': '0', 'G_of': '5', 'G_dh': '0'}, {'G_ss': '0', 'teamID': 'BS1', 'G_lf': '0', 'lgID': 'NA', 'yearID': '1871', 'G_3b': '31', 'G_defense': '31', 'G_rf': '0', 'G_all': '31', 'G_pr': '0', 'GS': '31', 'G_batting': '31', 'G_1b': '0', 'G_p': '0', 'playerID': 'schafha01', 'G_c': '0', 'G_cf': '0', 'G_2b': '0', 'G_ph': '0', 'G_of': '0', 'G_dh': '0'}, {'G_ss': '0', 'teamID': 'BS1', 'G_lf': '0', 'lgID': 'NA', 'yearID': '1871', 'G_3b': '0', 'G_defense': '31', 'G_rf': '0', 'G_all': '31', 'G_pr': '0', 'GS': '31', 'G_batting': '31', 'G_1b': '0', 'G_p': '31', 'playerID': 'spaldal01', 'G_c': '0', 'G_cf': '9', 'G_2b': '0', 'G_ph': '0', 'G_of': '9', 'G_dh': '0'}, {'G_ss': '15', 'teamID': 'BS1', 'G_lf': '0', 'lgID': 'NA', 'yearID': '1871', 'G_3b': '0', 'G_defense': '16', 'G_rf': '0', 'G_all': '16', 'G_pr': '0', 'GS': '16', 'G_batting': '16', 'G_1b': '1', 'G_p': '0', 'playerID': 'wrighge01', 'G_c': '0', 'G_cf': '0', 'G_2b': '0', 'G_ph': '0', 'G_of': '0', 'G_dh': '0'}, {'G_ss': '1', 'teamID': 'BS1', 'G_lf': '0', 'lgID': 'NA', 'yearID': '1871', 'G_3b': '0', 'G_defense': '31', 'G_rf': '0', 'G_all': '31', 'G_pr': '0', 'GS': '31', 'G_batting': '31', 'G_1b': '0', 'G_p': '9', 'playerID': 'wrighha01', 'G_c': '0', 'G_cf': '30', 'G_2b': '0', 'G_ph': '0', 'G_of': '30', 'G_dh': '0'}])

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
