

# json dump - Serializes obj as a JSON formatted stream to a Python file-like object (fp).
import json
data = {'name': 'John', 'age': 30, 'city': 'New York'}
with open('data.json', 'w') as outfile:
    json.dump(data, outfile)

# json load - Deserializes JSON data from a file-like object (fp) into a Python object
import json
with open('data.json', 'r') as infile:
    data = json.load(infile)
    print(data)  # `data` is now a Python dictionary.
    print(data['name'])
    print(data['age'])

# json dumps - Serializes obj to a JSON formatted str (string). It returns the JSON data as a string.
import json
data = {'name': 'John', 'age': 30, 'city': 'New York'}
json_string = json.dumps(data)
print(json_string)

# json loads - Deserializes (a str, bytes or bytearray instance containing a JSON document) into a Python object.
import json
json_string = '{"name": "John", "age": 30, "city": "New York"}'
data = json.loads(json_string)
print(data)  # `data` is now a Python dictionary.
data