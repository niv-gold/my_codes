# --------------------------------------------------------------------
# Models
# --------------------------------------------------------------------
import pandas as pd
import numpy as np
import datetime as dt

# --------------------------------------------------------------------
# Variables
# --------------------------------------------------------------------
csv_path = 'python/map/large_sales_data.csv'

# --------------------------------------------------------------------
# functions
# --------------------------------------------------------------------

def import_row_data(file_path:str)-> pd.DataFrame:
    pd_row_df = pd.read_csv(file_path)
    return pd_row_df


def convert_col_to_float(pd_df:pd.DataFrame)-> pd.DataFrame:
    pd_df['Quantity'] = np.int32(pd_df['Quantity'])
    pd_df['PricePerUnit'] = np.float32(pd_df['PricePerUnit'])
    return pd_df


def validate_datetime(df: pd.DataFrame)->pd.DataFrame:
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
    return df


def set_category(df:pd.DataFrame)-> pd.DataFrame:
    # using lambda as case when
    df['Category'] =  df['Product'].map(lambda x: 'Category_A' if x.startswith(('A','B')) else 'Category_B')
    return df


def set_totalSales(df:pd.DataFrame)-> pd.DataFrame:
    df['total_sales'] = df.apply(lambda row: row['Quantity'] * row['PricePerUnit'], axis=1)
    return df


def total_sales_per_ategory(df:pd.DataFrame)->pd.DataFrame:
    df_total_sales = df.groupby('Category').agg(category_otal_sales=('total_sales','sum'))
    return df_total_sales

def top_n_avg_price_per_unit(df:pd.DataFrame, top_n:int)->pd.DataFrame:
    # top n products by quantity sold
    df_product_quantity = df.groupby('Product').agg(total_prod_quantity=('Quantity','sum'))
    df_quant_top_n = df_product_quantity['total_prod_quantity'].nlargest(top_n)
    col_list = ['OrderID','Product','Quantity','PricePerUnit','OrderDate','Category','total_sales']
    df_3_largest = pd.merge(df,df_quant_top_n, on='Product', how='inner').filter(items=col_list)
    df_3_largest = df_3_largest.groupby('Product').agg(quant=('Quantity','sum'),total_sales=('total_sales','sum'))
    df_3_largest['avg_priceUnit'] = df_3_largest.apply(lambda x: x['total_sales']/x['quant'], axis=1)
    return df_3_largest

def product_highst_sales_per_month(df:pd.DataFrame)->pd.DataFrame:
    df['sale_yyyyMM'] = df['OrderDate'].map(lambda x: x.strftime('%Y%m'))  
    df_grp_by_prod_quant = df.groupby(['sale_yyyyMM','Product'],as_index=False).agg(prod_M_quant=('Quantity','sum'), total_sales_per_M=('total_sales','sum')).\
            sort_values(by=['sale_yyyyMM','prod_M_quant'], ascending=[False,False])
    df_grp_by_prod_quant['rank_per_M'] = df_grp_by_prod_quant.groupby(['sale_yyyyMM'],as_index=False)['prod_M_quant'].rank(method='first',ascending=False)    
    df_rank_1 = df_grp_by_prod_quant[df_grp_by_prod_quant['rank_per_M']==1]
    df_output = df_rank_1[['sale_yyyyMM','Product','total_sales_per_M']]
    return(df_output)

def main():
    pd_df_row = import_row_data(csv_path)
    pd_float = convert_col_to_float(pd_df_row)    
    pd_datetime = validate_datetime(pd_float)
    df_category = set_category(pd_datetime)
    df_totalSales = set_totalSales(df_category)
    print('-- main table --')
    print(df_totalSales)
    
    df_category_total_sales = total_sales_per_ategory(df_totalSales)
    print('-- total sales per category --')
    print(df_category_total_sales)
    
    df_quant_top_3 = top_n_avg_price_per_unit(df_totalSales,3)
    print('-- top n product by quantity --')
    print(df_quant_top_3)

    df_sales_per_month = product_highst_sales_per_month(df_totalSales)
    print('-- top sold product per month --')
    print(df_sales_per_month)

# --------------------------------------------------------------------
# run script
# --------------------------------------------------------------------

main()