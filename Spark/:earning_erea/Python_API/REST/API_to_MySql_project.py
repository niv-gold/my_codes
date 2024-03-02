# ================================================================================================
# the chalange:
# ================================================================================================
# 1. connect to rest_api that holds the data
# 2. check the return information type: text, json..
# 3. isolatated 5 attributes and save them in pandas dataframe
# 5.1 convert the request into JSON
# 5.2 exact 5 elements from JSON that are columns with data
# 4. clean the data: nulls, trim acces white spaces and more
# 5. creat a connection to mysql DB
# 6. store the data in the DB 
# 7. delete data from DB.
# 8. update data in the DB.
# 9 extract data from DB to be present in looker AWS report system 

# ================================================================================================
# libraries
# ================================================================================================
import requests as req
import json
import pandas as pd
from sqlalchemy import create_engine, text
from flatten_json import flatten

# ================================================================================================
# variables
# ================================================================================================


# MySql
username = 'niv2'
password = '7124175'
dbname = 'DWH'

# ================================================================================================
# functions
# ================================================================================================
def get_respons(url:str, pai_key='', search=''):
    api_reques = url + pai_key + search
    respons = req.get(api_reques)   
    return respons

def extract_key_value(movie_data:dict, keys_list:list):
    dict_movie = {k:v for k,v in movie_data.items() if k in keys_list }    
    return dict_movie

def dict_value_lower(dict_movie:dict):
    dict_out = {k:v.lower() for k,v in dict_movie.items() if isinstance(v,str)}
    return dict_out

def create_sqlAlchemy_engine(username:str, password:str, dbname:str):
    connection_string = f"mysql+pymysql://{username}:{password}@localhost/{dbname}"
    engine = create_engine(connection_string)
    return engine

def flating_json_normalize(json_dict:dict):
    col_lst = [k for k,v in json_dict.items()]
    col_lst.remove('Ratings')
    df = pd.json_normalize(json_dict, record_path='Ratings', meta=col_lst)    
    return df

def run():
    #==============================================
    # connect to REST API
    #==============================================
    # --set api variables --
    api_url = 'http://www.omdbapi.com/?'
    api_key = 'apikey=fc4b3c2c'
    api_movie_title = '&t=pretty woman'

    # -- respons handling --
    respons = get_respons(api_url,api_key, api_movie_title)
    contant_type =  respons.headers['Content-Type']
    request_dict = respons.json() # return json as python object (dict)
    respons_text = respons.text # return json as a string
    respons_code = respons.status_code

    print(respons.text)
    print(request_dict['Response'])
    # -- respons check --
    print( f'Head: {contant_type}\n')
    if respons_code == 200 and request_dict['Response']=='True':
        print(f'respons status code is {respons_code}, A OK!\n')
    elif request_dict['Response']=='False':
        request_dict['Error']
        return None
    else: 
        print(f'respons got an error {respons_code}\n')
    
    #==============================================
    # Transforming data
    #============================================== 
    # -- extract a list of keys from arequest.text --
    key_list = ['Title', 'Year', 'Runtime', 'Plot','Ratings'] # Rating contains a dict data type
    request_dict = json.loads(respons_text) # convert json from str into python obj (Dict)
    dict_movie = extract_key_value(request_dict, key_list)
    
    # -- lower dict value --
    dict_movie_lower = dict_value_lower(dict_movie)

    # -- normelaize list of dicts in json using panda --
    flat_json = flatten(dict_movie_lower)

    # -- convert to pandas object --
    lst_dict_movie = [flat_json]
    pd_df = pd.DataFrame(lst_dict_movie) 

    #==============================================
    # Writing dataa to Mysql
    #==============================================
    # -- create an engine to mysql --
    engine = create_sqlAlchemy_engine(username, password, dbname)
    
    # write dataframe into MySql
    pd_df.to_sql(name='movies_api', con=engine, if_exists='append', index=False )


    # # -- query mysql data--    # with sql_engine.connect() as connection:
    # with sql_engine.connect() as connection:
    #     # Use the text() construct to create an executable SQL expression
    #     result = connection.execute(text("SELECT * from mysql.user"))

    #     # Fetch the result (for demonstration purposes)
    #     for row in result:
    #         print(row)

run()