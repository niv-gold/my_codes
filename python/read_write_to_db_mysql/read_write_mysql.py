import pandas as pd
import numpy as np
from time import sleep
from sqlalchemy import create_engine, text

# -------------------------------------------------------
# global variables
# -------------------------------------------------------
mysql_user = 'niv2'
mysql_password = '7124175'
mysql_host_name = 'localhost'
mysql_db = 'DWH'

# sql queries:
query_1 = text('''DROP TABLE IF EXISTS customers;''')

query_2 = text('''    CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP   
);''')

query_3 = text('''  INSERT INTO customers(customer_id,first_name,last_name,email) 
                    VALUES  (604671487,"Niv","Goldberg","nivgodlber1@gmail.com"),
                            (200069078,"Liann","Goldberg","liann.lever@gmail.com"),
                            (3387999978,"Amit","Goldberg","Amit.goldberg@gmail.com");''')

query_4 = text(''' UPDATE customers
               SET first_name = 'Shir'
               WHERE customer_id =  604671487''')
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
    
    with DWH_engine.connect() as connection:
        trans = connection.begin()
        try:
            connection.execute(query_1)
            connection.execute(query_2)
            connection.execute(query_3)            
            print('--> App spleeps for 5 seconds...')
            sleep(5)
            connection.execute(query_4)
            trans.commit()
        except Exception as e:
            print(f'--> error_1 - transaction was roledback due to: {e}')
            trans.rollback()

    # get all db table:
    # inspactor = alc.inspect(mysql_engine)
    # db_tbl_lst = inspactor.get_table_names()
    # print(db_tbl_lst)




    # pd_df = pd.read_sql_query()

# -------------------------------------------------------
# main - run script
# -------------------------------------------------------

main()