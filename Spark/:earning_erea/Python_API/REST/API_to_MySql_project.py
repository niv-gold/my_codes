import requests as req
import json
import pandas as pd
from sqlalchemy import create_engine, text

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

def create_sqlAlchemy_engine():
    username = 'niv2'
    password = '7124175'
    dbname = 'mysql'

    # -- create the connection string --
    connection_string = f"mysql+pymysql://{username}:{password}@localhost/{dbname}"

    # -- create the database engine --
    engine = create_engine(connection_string)
    return engine

def run():
    # --set api variables --
    api_url = 'http://www.omdbapi.com/?'
    api_key = 'apikey=fc4b3c2c'
    api_movie_title = '&t=terminator'

    respons = get_respons(api_url,api_key, api_movie_title)
    contant_type =  respons.headers['Content-Type']
    respons_json = respons.json() # extract request body as a json 
    respons_text = respons.text
    respons_code = respons.status_code

    # -- respons check --
    print( f'Head: {contant_type}\n')
    if respons_code == 200:
        print(f'respons status code is {respons_code}, A OK!\n')
    else: 
        print(f'respons got an error {respons_code}\n')
    
    # -- converting json text to python object Dictionary --
    # print(respons_text, end='\n') # json as a string
    # print(respons_json, end='\n') #json as python object Dict

    # txt_to_json = json.loads(respons_text)
    # print(txt_to_json["Genre"])
    
    # -- extract a list of  keys from request.json --
    key_list = ['Title', 'Year', 'Runtime', 'Plot'] # Ratings.Value nested dict in a list ?? chalange!!
    dict_movie = extract_key_value(respons_json, key_list)
    # print(dict_movie)

    # -- extract a list of keys from arequest.text --
    request_txt_json = json.loads(respons_text)
    dict_movie = extract_key_value(request_txt_json, key_list)
    # print(dict_movie)

    # -- lower casing all values --
    dict_monie_lower = dict_value_lower(dict_movie)
    # print(dict_monie_lower)

    # -- convert to pandas object --

    lst_dict_movie = [dict_monie_lower]
    pd_df = pd.DataFrame(lst_dict_movie)
    print(pd_df.loc[0]['Title'])

    # -- create an engine to mysql --
    sql_engine = create_sqlAlchemy_engine()
    
    # -- write pandas df to mysql --
    with sql_engine.connect() as connection:
        # Use the text() construct to create an executable SQL expression
        result = connection.execute(text("SELECT * from mysql.user"))

        # Fetch the result (for demonstration purposes)
        for row in result:
            print(row)

run()