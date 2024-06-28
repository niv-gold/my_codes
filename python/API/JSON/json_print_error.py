import json

# Example of an invalid JSON string
invalid_json_string = '{"name": "John", "age": 30, @"city": :1}'

try:
    # Attempt to parse the JSON
    parsed_data = json.loads(invalid_json_string)
except Exception as e:
    # Print error details if JSON is invalid
    print(f"Invalid JSON: {e}")
    # print(f"Error at line: {e.lineno}, column: {e.colno}")