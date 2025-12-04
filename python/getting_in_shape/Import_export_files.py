# Import csv and json files into pandas data frame.
# scrolling trough files line by line
# save pandas data frame into a json file, mysql table.

import pandas as pd
import csv

# Import csv file from local computer

csv_path = '/home/niv/home/GitHubeRepos/my_codes/python/getting_in_shape/large_sales_data.csv'

df_row = pd.read_csv(csv_path).head(10)
print(df_row)
df_row = pd.read_csv(csv_path).tail(10)
print(df_row)

# read csv row by row as dictionary
row_list = []
with open(csv_path, 'r') as file:
    read_row_csv = csv.DictReader(file)

    for row in read_row_csv:
        row_list.append(row)

print(row_list[0])
