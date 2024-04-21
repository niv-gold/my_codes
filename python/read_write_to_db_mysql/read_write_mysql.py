import pandas as pd
import numpy as np
from time import sleep
from sqlalchemy import create_engine, text, Engine, inspect

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
                    VALUES  (060467487,"Niv","Goldberg","nivgodlber1@gmail.com"),
                            (200069078,"Liann","Goldberg","liann.lever@gmail.com"),
                            (3387999978,"Amit","Goldberg","Amit.goldberg@gmail.com");''')

query_4 = text(''' UPDATE customers
               SET first_name = 'Shir'
               WHERE customer_id =  060467487''')

# kwargs
kwg = {'table':'customers', 'customer_id':'060467487', 'first_name':'Daniel', 'last_name':'Lev-Er', 'email':'Daniel.lev-Er@gmail.com'}

# -------------------------------------------------------
# wrapers
# -------------------------------------------------------


# -------------------------------------------------------
# functions
# -------------------------------------------------------

def my_sql_engine(mysql_user:str, mysql_password:str, mysql_host_name:str, mysql_db:str):
    mysql_engine= create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host_name}/{mysql_db}",
                                pool_recycle=3600, echo=False)  
    return mysql_engine

def create_customer_tbl(DWH_engine:Engine):

    # "with" inable transaction and connection to mysql management.
    with DWH_engine.connect() as connection:
        trans = connection.begin()
        try:
            print('--> drop table if exist')
            connection.execute(query_1)
            print('--> create table')
            connection.execute(query_2)
            print('--> insetrs data to table')
            connection.execute(query_3)            
            print('--> App spleeps for 5 seconds...')
            sleep(5)
            print('--> update row with id 060467487')
            connection.execute(query_4)
            print('--> commit changes')
            trans.commit()
        except Exception as e:
            print(f'--> error_1 - transaction was roledback due to: {e}')
            trans.rollback()


def upsert_tbl(DWH_engine:Engine, **kwg):
    if kwg['customer_id'] is None: 
        print('--> kwarg is empty!')
        return None
    # update set syntax: key=value  
    lst_set_key_value = [f'{k}="{v}"' for k,v in kwg.items() if k != 'table']
    str_set_key_value = ','.join(lst_set_key_value)
    
    # insert syntax: (col_n...) value(val_n..)/

    q1=text(f'''  SELECT 1
                    FROM {kwg['table']} 
                    WHERE customer_id={kwg['customer_id']} limit 1;''')    
    q2=text(f'''    UPDATE customers
                    SET {str_set_key_value}
                    WHERE customer_id={kwg['customer_id']}''')
    q3=text('''INSERT INTO customers()''')

    with DWH_engine.connect() as connection:
        trans = connection.begin()
        try:
            res1 = connection.execute(q1).fetchone()
            if res1 is not None: 
                connection.execute(q2)
            else: print('--> Insert ....')
            connection.commit()
        except Exception as e:
           print(f'--> error_2 - transaction was roledback due to: {e}')
           trans.rollback()  

def main():
    DWH_engine = my_sql_engine(mysql_user, mysql_password, mysql_host_name, mysql_db)
    upsert_tbl(DWH_engine, **kwg)

# -------------------------------------------------------
# main - run script
# -------------------------------------------------------

main()