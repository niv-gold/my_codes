#======================================================
# import modles
#======================================================
import requests as rq
import pandas as pd
import json
import time
from  datetime import timedelta


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
    time.sleep(1.5)
    
    return rsp_dict


def get_multi_API_response(url:list, headers:str, state_lst:str):
    '''list all states respons from REST_API into list'''    
    state_query_lst = get_state_query_list(state_lst)
    rsp_lst = [get_API_respons(url, headers, stase_q) for stase_q in state_query_lst]
    return rsp_lst


def increase_death_by_1(df:pd.DataFrame)->pd.DataFrame:
    'manipulate row value in adefined culomn, in this case adding 1 to death colum in each row'
    df['deaths'] = df['deaths'].map(lambda x: x+1)
    return df


def map_as_case_when(df:pd.DataFrame)->pd.DataFrame:
    'using case when to populate new column "case_when"'
    df['case_when'] = df['critical'].map(lambda x: x*10 if x>100 else x/10)
    return df


def apply_survivers(df:pd.DataFrame)->pd.DataFrame:
    ''
    df['survivers'] = df.apply(lambda row: row['recovered']/row['confirmed'] if row['confirmed'] > 0 else 0, axis=1)
    return df


def self_join(df:pd.DataFrame)->pd.DataFrame:
    'self join df'
    join_cond_list = ['country','code']
    df_self_joind = pd.merge(df, df, on=join_cond_list ,how='inner', suffixes=['_l','_r'])
    #df_self_joind = pd.merge(df, df, left_on='country', right_on='country' ,how='inner', suffixes=['_l','_r'])
    df_select_columns = df_self_joind.loc[:,['country', 'code',  'confirmed_l',  'recovered_l',  'critical_l',  'deaths_l', 'survivers_r']]
    #slice by auto index numeric value, 1 to 4 will be returned:
    df_slice_1 = df_select_columns.iloc[1:5]
    #slice by row position, 0 to 2 will be returned::
    df_slice_2 = df_slice_1[0:3]
    #conditional slicing by column values:
    df_slice_3 = df_slice_2[df_slice_1['code'].isin(['IT','IL','RU','US'])].reset_index()
    #set DF index using an existing column:
    df_slice_3.set_index('country',inplace=True)
    #slicing by the new DF index
    df_slice_4 = df_slice_3.loc[:'Russia']
    return df_slice_4


def clean_df_data(df:pd.DataFrame)->pd.DataFrame:
    df_stg_1 = df.fillna(0)
    df_stg_1['country'] = df_stg_1['country'].str.replace('a', '#')
    df_stg_1['lastChange'] = df_stg_1['lastChange'].map(lambda x: pd.to_datetime(x) + timedelta(days=1))
    return df_stg_1


def LAG_LEAD_func(df:pd.DataFrame)->pd.DataFrame:
    df['deaths_LAG_1'] = df['deaths'].shift(-1).fillna(0)
    df['deaths_LEAD_2'] = df['deaths'].shift(2).fillna(0)
    return(df)

def WF_sum_3_rows(df:pd.DataFrame)->pd.DataFrame:
    df['sum_last_3'] = df['critical'].rolling(window=3).sum().fillna(0)
    df['critical_running_total'] = df['critical'].cumsum()
    return df

def WF_runT_by_state(df:pd.DataFrame)->pd.DataFrame:
    df['state_cumsum'] = df.groupby(['code']).apply(lambda x: x.sort_values(by='critical')['deaths'].cumsum()).reset_index(level=0, drop=True)
    df = df.sort_values(by='code', ascending=False)
    return df

def group_by_col(df:pd.DataFrame)->pd.DataFrame:
    df_groupBy = df.groupby('code').agg(deaths_sum=('deaths','sum'),critical_sum=('critical','sum'))
    return df_groupBy

def save_data_to_db():
    pass

def main():
    lst_of_states = ['it','il','ru','us','ca','it','it']
    rsp_lst= get_multi_API_response(url=url_1, headers=headers, state_lst=lst_of_states)
    pd_df = pd.DataFrame(rsp_lst)
    print(pd_df,'\n')
    df_inc_1 = increase_death_by_1(pd_df)
    print(df_inc_1)
    df_case_when = map_as_case_when(df_inc_1)
    print(df_case_when)
    df_case_when.drop('case_when', axis=1, inplace=True)
    print(df_case_when)
    df_apply = apply_survivers(df_case_when)
    print(df_apply)
    df_join = self_join(df_apply)
    print(df_join)
    df_clean = clean_df_data(df_apply)
    print(df_clean)
    df_lag_lead = LAG_LEAD_func(df_apply)
    print(df_lag_lead)
    df_runningtotal = WF_sum_3_rows(df_lag_lead)
    print(df_runningtotal)
    df_wf = WF_runT_by_state(df_runningtotal)
    print(df_wf)
    df_groupby = group_by_col(df_wf)
    print(df_groupby)


# run main
main()
