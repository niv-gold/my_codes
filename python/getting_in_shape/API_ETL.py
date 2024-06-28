#======================================================
# import modles
#======================================================
import requests as rq
import pandas as pd
import json
import time


#======================================================
# variables
#======================================================
url_1 = "https://covid-19-data.p.rapidapi.com/country/code"
querystring = {"format":"json","code":"il"}
headers = {
	"x-rapidapi-key": "2ad05ca92cmshc7100dfdd7e411cp10b600jsnfa918e7ff684",
	"x-rapidapi-host": "covid-19-data.p.rapidapi.com"
}


#======================================================
# codding
#======================================================
def get_state_query_list(states_lst:list)->list:
    state_query_lst = [f'{{"format":"json","code":"{state}"}}' for state in states_lst]
    return state_query_lst


def get_API_respons(url, headers:str, querystring:str)-> rq.Response:
    '''connect API'''
    dict_q = json.loads(querystring)
    print(dict_q)
    respons = rq.get(url=url, headers=headers, params=dict_q)

    if respons.status_code != 200:
        raise ValueError(f'Request Error code: {respons.status_code}')    
    rsp_dict = respons.json()[0]
    time.sleep(2)
    
    return rsp_dict


def get_multi_API_response(url:list, headers:str, state_lst:str):
    '''list all states respons from REST_API into list'''    
    state_query_lst = get_state_query_list(state_lst)
    rsp_lst = [get_API_respons(url, headers, stase_q) for stase_q in state_query_lst]
    return rsp_lst


def increase_death_by_1(df:pd.DataFrame)->pd.DataFrame:
    'manipulate row value in adefined culomn, in this case adding 1 to death colum in eatch row'
    df['deaths'] = df['deaths'].map(lambda x: x+1)
    return df

def map_as_case_when(df:pd.DataFrame)->pd.DataFrame:
    df['case_when'] = df['critical'].map(lambda x: x*10 if x>100 else x/10)
    return df


def save_data_to_db():
    pass

def main():
    lst_of_states = ['it','il','ru','us','ca']
    rsp_lst= get_multi_API_response(url=url_1, headers=headers, state_lst=lst_of_states)
    pd_df = pd.DataFrame(rsp_lst)
    print(pd_df,'\n')
    df_inc_1 = increase_death_by_1(pd_df)
    print(df_inc_1)
    df_case_when = map_as_case_when(df_inc_1)
    print(df_case_when)
    df_case_when.drop('case_when', axis=1, inplace=True)
    print(df_case_when)

# run main
main()
