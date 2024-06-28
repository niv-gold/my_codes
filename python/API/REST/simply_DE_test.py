# =============================================
# libraries
# =============================================
import requests
import pandas as pd
from sqlalchemy import create_engine
import schedule
import time

# =============================================
# global var`s
# =============================================
# API
api_key= 'k_f3mxqolc'
url = 'https://imdb-api.com/api/'

# MySql
username = 'niv2'
password = '7124175'
dbname = 'DWH'

# =============================================
# main
# =============================================

def get_response(url:str, headers:str, query_str:str)-> dict:
    try:
        respons = requests.get(url=url, headers=headers, params=query_str)
        if respons.status_code != 200:
            raise ValueError(f"API respnse status code is: {respons.status_code}")        
        respons_dict = respons.json()
        return respons_dict
    except Exception as e:
        print( f'Response content is not valid JSON - error massage: {e}\n')

def transform_pd_df(res_dict:dict)->pd.DataFrame:
    try:
        req_to_pd_df = pd.DataFrame(res_dict)
        pd_df_col = req_to_pd_df.contents
        pd_df_col_into_rows_list = [pd.DataFrame.from_dict(row, orient='index').transpose() for row in pd_df_col]
        union_rows_into_pd_df = pd.concat(pd_df_col_into_rows_list, axis=0)
        return union_rows_into_pd_df
    except Exception as e:
        print(f'Error msg: {e}')

def pd_df_filter_data(pd_df_row_data:pd.DataFrame)-> pd.DataFrame:
    df = pd_df_row_data
    col_selction = ['_id', 'release_date', 'title', 'vote_average', 'vote_count','sources', 'contentType']
    pd_clean_1 = df.loc[(df['contentType']=='movie') & (df['vote_average']>=6) & (df['vote_count']>=30) ,col_selction]
    return pd_clean_1

def check_if_movieId_exists(engine, pd_df_api:pd.DataFrame)->pd.DataFrame:
    api_movies_id_list = [row for row in pd_df_api['_id']]
    api_movies_id_str = str(api_movies_id_list)[1:-1]
    where_cond = f'_id IN ({api_movies_id_str})'    
    tbl_name = 'dim_movies'
    query = f'SELECT _id FROM DWH.{tbl_name} WHERE {where_cond}'
    df_api_movies_exists_in_trg = pd.read_sql_query(query, engine)
    api_movies_exists_in_trg_lst = [str(movie_id) for movie_id in df_api_movies_exists_in_trg['_id']]
    api_movies_not_in_trg_lst = [movie_id for movie_id in api_movies_id_list if movie_id not in api_movies_exists_in_trg_lst]
    pd_df_new_movies = pd_df_api.loc[(pd_df_api['_id'].isin(api_movies_not_in_trg_lst))]
    return pd_df_new_movies

def df_convert_cols_to_str(pd_df:pd.DataFrame)->pd.DataFrame:
    df_movies_cols_conversion = { col:'str' for col in pd_df.columns}
    df = pd_df.astype(df_movies_cols_conversion)
    return df

def create_mysql_engine(username:str, password:str, dbname:str):
    connection_string = f"mysql+pymysql://{username}:{password}@localhost/{dbname}"
    engine = create_engine(connection_string)
    return engine

def main():
    # variables
    url = "https://movies-api14.p.rapidapi.com/search"
    headers = { "X-RapidAPI-Key": "2ad05ca92cmshc7100dfdd7e411cp10b600jsnfa918e7ff684",
                "X-RapidAPI-Host": "movies-api14.p.rapidapi.com"}
    querystring = {"query":"breaking bad"}

    # Pipeline
    response_dict = get_response(url, headers, querystring)     
    pd_df_data = transform_pd_df(response_dict)
    pd_clean_data = pd_df_filter_data(pd_df_data)    
    df_to_str = df_convert_cols_to_str(pd_clean_data)
    # print(df_to_str)

    mysql_engine = create_mysql_engine(username,password,dbname)    
    df_new_movies = check_if_movieId_exists(mysql_engine,df_to_str)
    df_new_movies.to_sql(name='dim_movies',con=mysql_engine, if_exists='append', index=False)
    print(f'New movies:\n {df_new_movies}')

#-------------------------------------------------------------------------------------

# main run the script functions
# main()
schedule.every().minutes(1).do(main)