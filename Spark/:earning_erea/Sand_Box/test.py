import pandas as pd
from pandas import json_normalize

# Sample JSON object with a nested list
nested_json_with_list = {
    "school_name": "XYZ High School",
    "location": "City Z",
    "students": [
        {"name": "John", "age": 17, "grade": "A"},
        {"name": "Daisy", "age": 16, "grade": "B"},
        {"name": "Luke", "age": 17, "grade": "C"}
    ]
}

# Flatten the JSON object focusing on the 'students' list
flat_with_list = json_normalize(nested_json_with_list, record_path='students', meta=['school_name', 'location'])

print(flat_with_list)
