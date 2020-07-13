import requests

def request_omeka ():
    results = []

    response = requests.get(os.environ['OMEKA_API_URL'], params={'page': page, 'per_page': 200, 'sort_by': 'id', 'sort_order': 'asc'} )
    headers = response.headers
    last_page = int(headers['Link'].split(',')[2].split(";")[0].split('=')[-1].strip('>'))


    for page in range(2, last_page+1):
        response = requests.get(os.environ['OMEKA_API_URL'], params={'page': page, 'per_page': 200, 'sort_by': 'id', 'sort_order': 'asc'}).json()
        for item in response:
            code = item['dcterms:identifier'][0]['@value']
            results.append(code)

    return results