import pandas as pd
import os, requests

def load(endpoint):
    results = {}

    response = requests.get(endpoint, verify= False)
    headers = response.headers
    print(response.headers['Link'])
    last_page = int(headers['Link'].split('&page')[-1][:3].strip("=>"))
    print(last_page)

    for page in range(1, last_page+1):
        l1 = []
        l2 = []
        response = requests.get(endpoint,verify=False, params={'page': page}).json()
        for item in response:
            l1.append(item['dcterms:identifier'][0]['@value'])
            l2.append(item['@id'])
    
    results.update({'id':l1,'omeka_url':l2})

    omeka_df = pd.DataFrame(results)

    return omeka_df