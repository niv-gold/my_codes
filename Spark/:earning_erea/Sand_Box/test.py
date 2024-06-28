import requests

url = "https://covid-19-data.p.rapidapi.com/country/code"

querystring = {"format":"json","code":"it"}

headers = {
	"x-rapidapi-key": "2ad05ca92cmshc7100dfdd7e411cp10b600jsnfa918e7ff684",
	"x-rapidapi-host": "covid-19-data.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())