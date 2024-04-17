import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# -------------------------------------------------------
# global variables
# -------------------------------------------------------
mysql_user = 'niv2'
mysql_password = '7124175'
mysql_host_name = 'localhost'
mysql_db = 'DWH'

# sql queries:
query_1 = text('''DROP TABLE IF EXISTS test;''')

query_2 = text('''CREATE TABLE IF NOT EXISTS test (customer_name nvarchar(50));''')

query_3 = text('''INSERT INTO test(customer_name) VALUE("abc");''')

query_4 = text('''SELECT * FROM test''')

# -------------------------------------------------------
# wrapers
# -------------------------------------------------------


# -------------------------------------------------------
# functions
# -------------------------------------------------------

def my_sql_engine(mysql_user:str, mysql_password:str, mysql_host_name:str, mysql_db:str):
    mysql_engine= create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host_name}/{mysql_db}",
                                pool_recycle=3600, echo='debug')  

    return mysql_engine

def main():
    # create mysql engine:
    DWH_engine = my_sql_engine(mysql_user, mysql_password, mysql_host_name, mysql_db)
    
    # create tables
    try:
        with DWH_engine.connect() as connection:
            transaction = connection.begin()
            connection.execute(query_1)
            connection.execute(query_2)
            connection.execute(query_3)
            res = connection.execute(query_4)

            for row in res:
                print(row)

            transaction.commit()
    except Exception as e:
        print(f'--> error_1: {e}')
        transaction.rollback()

    # get all db table:
    # inspactor = alc.inspect(mysql_engine)
    # db_tbl_lst = inspactor.get_table_names()
    # print(db_tbl_lst)




    # pd_df = pd.read_sql_query()

# -------------------------------------------------------
# main - run script
# -------------------------------------------------------

main()