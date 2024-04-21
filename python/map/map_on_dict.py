# ------------------------------------------------------
# Import modules
# ------------------------------------------------------
import pandas as pd
import numpy as np
import csv
# ------------------------------------------------------
# variables
# ------------------------------------------------------
csv_path = 'python/map/large_sales_data.csv'
lst_dict = []
total_sales_by_product_list = []
# ------------------------------------------------------
# functions
# ------------------------------------------------------

# load row as a dictionary into a list.
def load_csv_into_list_of_dict(csv_path:str):
    with open(csv_path, 'r') as file:
        reader = csv.DictReader(file)

        for row in reader:
           lst_dict.append(row)
    return lst_dict

# convert value data type using map
def convert_keys_to_numeric(dict_in:dict):
    dict_in['Quantity']=np.float32(dict_in['Quantity'])
    dict_in['PricePerUnit']=np.float32(dict_in['PricePerUnit'])
    return dict_in
    

# create new column using map
def calc_TotalSale(dict_in:dict):
    dict_in['TotalSale'] = dict_in['Quantity']*dict_in['PricePerUnit']
    return dict_in


def main():
    raw_data = load_csv_into_list_of_dict(csv_path)
    print(raw_data[0])

    raw_data_numeric = list(map(convert_keys_to_numeric,raw_data))
    print(raw_data_numeric[0])

    raw_data_totalSale = list(map(calc_TotalSale,raw_data_numeric))
    print(raw_data_totalSale[0])

    Total_sum = sum([sale_transaction['TotalSale'] for sale_transaction in raw_data_totalSale])
    print(f'Total sales: {Total_sum}')

    Total_count = sum( [sale_transaction['Quantity'] for sale_transaction in raw_data_totalSale] )

    avg_price_unit = Total_sum/Total_count
    print(f'Average unit price: {avg_price_unit}')

    pd_tbl = pd.DataFrame(raw_data_totalSale)
    bast_sold_product = pd_tbl.groupby('Product').agg(Total_quantity=('Quantity','sum'))
    print(bast_sold_product)
    
    max_raw_index_name = bast_sold_product['Total_quantity'].idxmax()
    max_quantity_raw = bast_sold_product.loc[max_raw_index_name].iloc[0]
    print(f'bast seler is: {max_raw_index_name}, with {max_quantity_raw} units sold')
# ------------------------------------------------------
# run script
# ------------------------------------------------------

main()