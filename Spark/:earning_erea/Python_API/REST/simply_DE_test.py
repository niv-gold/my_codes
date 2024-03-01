# =============================================
# libraries
# =============================================
import requests
import pandas as pd
from datetime import datetime as dt

# =============================================
# global var`s
# =============================================
api_key= 'k_f3mxqolc'
url = 'https://imdb-api.com/api/'

# =============================================
# main
# =============================================

def get_response(url:str, headers:str, query_str:str)-> dict:
    try:
        respons = requests.get(url=url, headers=headers, params=query_str)
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

def api_clean_data():
    pass

def my_sql_egine():
    pass

def extract_exist_movies():
    pass

def check_if_movie_exists():
    pass

def insert_movies():
    pass

def schedual_run():
    pass

def main():
    # local variables
    url = "https://movies-api14.p.rapidapi.com/search"
    headers = { "X-RapidAPI-Key": "2ad05ca92cmshc7100dfdd7e411cp10b600jsnfa918e7ff684",
                "X-RapidAPI-Host": "movies-api14.p.rapidapi.com"}
    querystring = {"query":"breaking bad"}

    # functions
    response_dict = get_response(url, headers, querystring)    
    pd_df_data = transform_pd_df(response_dict)
    print(pd_df_data)

#-------------------------------------------------------------------------------------

# main run the script functions
main()