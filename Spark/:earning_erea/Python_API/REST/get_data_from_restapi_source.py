# missions:
# 1. connect to rest_api that holds the data
# 2. check the return information type: text, json..
# 3. isolatated 5 attributes and save them in pandas dataframe
# 5.1 convert the request into JSON
# 5.2 exact 5 elements from JSON that are columns with data
# 4. clean the data: nulls, trim acces white spaces and more
# 5. creat a connection to mysql DB
# 6. store the data in the DB 
# 7. delete data from DB.
# 8. update data in the DB.
# 9 extract data from DB to be present in looker AWS report system 

#----------------------------------------------
# API details
#----------------------------------------------

# Here is your key: fc4b3c2c
# Please append it to all of your API requests,
# OMDb API: http://www.omdbapi.com/?i=tt3896198&apikey=fc4b3c2c

import requests as req
import pandas as pd

# connect to rest_api that holds the data

def get_api_data(url:str):    
    request = req.get(url)
    
    #A method that will raise an HTTPError exception if the response status code was an error (4xx or 5xx). 
    request.raise_for_status()

    req_code = request.status_code
    req_reason = request.reason
    if req_code == 200:
        print(f'request return code: {req_code} ({req_reason})\n')
    else:         
        print(f'request return code: {req_code} ({req_reason})\n')
        pass

    req_text = request.text
    print(f'respons: {req_text}\n')

    #request Header
    req_head = request.headers
    print(f'request header: {req_head}\n')

    req_json = request.json()
    print(f'request as a json: {req_json}')

    print(f'row type: {str(type(req_text))}')
    print(f'json type: {str(type(req_json))}')


# run the code
api_url = 'http://www.omdbapi.com/?'
api_key = 'apikey=fc4b3c2c'
api_movie_title = '&t=terminator' 
api_request = api_url + api_key + api_movie_title
get_api_data(api_request)