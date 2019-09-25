import json

import pymysql

from src.RDBDataTable import RDBDataTable
from src.dbutils import run_q,create_select
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def t2():
    default_cnx = pymysql.connect(host='127.0.0.1',
                                  user='dbuser',
                                  password='dbuserdbuser',
                                  db='lahman2019raw',
                                  charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)
    table_name = "lahman2019raw.People"
    fields = '*'
    template = { "playerID": "aardsda01"}
    sql, args = create_select(table_name, template, fields)
    print("SQL = ", sql, ", args = ", args)

    result = run_q(sql, args,conn=default_cnx)
    print("Return code = ", result[0])
    print("Data = ")
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")

def load_data():
    c_info = dict(host="127.0.0.1", port=3306, user="dbuser", password="dbuserdbuser", db="lahman2019raw")
    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    print("RDB table = ", r_dbt)
    return r_dbt
r_dbt = load_data()
result1 = r_dbt.find_by_primary_key(['try1'])
result2 = r_dbt.find_by_template({'birthYear':1954, 'birthState':'CA'})
result3 = r_dbt.delete_by_key(['aaronha01'])
result4 = r_dbt.delete_by_template({'birthYear':1954, 'birthState':'CA'})
result5 = r_dbt.update_by_key(['abadan01'],['janane'])
result6 = r_dbt.update_by_template({'birthYear':1954},[1996])
result7 = r_dbt.insert({'playerID':'try1','birthYear':'1996','birthMonth':'','birthDay':'','birthCountry':'','birthState':'','birthCity':'',
                        'deathYear':'','deathMonth':'','deathDay':'','deathCountry':'','deathState':'','deathCity':'','nameFirst':'',
                        'nameLast':'','nameGiven':'','weight':'','height':'','bats':'',
                        'throws':'','debut':'','finalGame':'','retroID':'','bbrefID':''})

print(result1)