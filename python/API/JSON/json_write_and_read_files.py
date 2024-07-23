import requests
import pandas as pd
import json

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

def write_response_to_file(pd_df_data:pd.DataFrame)-> None:
    with open ('pd_df_api.json', 'w') as out_file:
        json.dump(pd_df_data.to_dict(orient='records'),out_file, indent=4)

def read_response_from_file(json_file_name:str)-> dict:
    with open (json_file_name, 'r') as file_in:
        json_read_file = json.load(file_in)
        return json_read_file

def main():
    # local variables
    url = "https://movies-api14.p.rapidapi.com/search"
    headers = { "X-RapidAPI-Key": "2ad05ca92cmshc7100dfdd7e411cp10b600jsnfa918e7ff684",
                "X-RapidAPI-Host": "movies-api14.p.rapidapi.com"}
    querystring = {"query":"breaking bad"}

    # functions
    response_dict = get_response(url, headers, querystring)    
    pd_df_data = transform_pd_df(response_dict)
    write_response_to_file(pd_df_data)
    json_read = read_response_from_file('pd_df_api.json')
    print(json_read)


#-------------------------------------------------------------------------------------

# main run the script functions
main()