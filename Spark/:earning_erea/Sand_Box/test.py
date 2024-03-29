import pandas as pd

# Sample DataFrame
data = {
    'Name': ['John Doe', 'Jane Smith'],
    'Age': [28, 34],
    'Occupation': ['Data Scientist', 'Engineer']
}
df = pd.DataFrame(data)

# Convert DataFrame to JSON
json_str = df.to_json()

print(json_str)

# Convert DataFrame to JSON with different orientations
json_records = df.to_json(orient='records')
json_columns = df.to_json(orient='columns')
json_index = df.to_json(orient='index')
json_values = df.to_json(orient='values')

print("JSON Records:")
print(json_records)
print("\nJSON Columns:")
print(json_columns)
print("\nJSON Index:")
print(json_index)
print("\nJSON Values:")
print(json_values)

