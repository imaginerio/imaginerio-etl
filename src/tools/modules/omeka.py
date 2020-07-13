import os, requests

def list_items(endpoint):
    results = []

    response = requests.get(endpoint, verify= False)
    headers = response.headers
    print(response.headers['Link'])
    last_page = int(headers['Link'].split('&page')[-1][:3].strip("=>"))
    print(last_page)

    for page in range(1, last_page+1):
        response = requests.get(endpoint,verify=False, params={'page': page}).json()
        for item in response:
            code = item['dcterms:identifier'][0]['@value']
            results.append(code)

    return results