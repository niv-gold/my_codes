#%% Get US currency exchange into other currencys
import requests

key = "2667b94d2d14f8120b93fb42ad9e1658ac10"
base = "USD"
output = "json"

url = f"https://currencyapi.net/api/v1/rates?key={key}&base={base}&output={output}"
headers = {
    'Accept': 'application/json'
}

response = requests.get(url, headers=headers)
print(response.text)

#%% All currencys details data
import requests

key = "2667b94d2d14f8120b93fb42ad9e1658ac10"
output = "json"

url = f"https://currencyapi.net/api/v1/currencies?key={key}&output={output}"
headers = {
    'Accept': 'application/json'
}

response = requests.get(url, headers=headers)
print(response.text)