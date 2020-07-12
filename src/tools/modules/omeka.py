import requests

results = []

response = requests.get("situatedviews.axismaps.io/api/items")
headers = response.headers
last_page = int(headers['Link'].split(',')[2].split(";")[0].split('=')[-1].strip('>'))

for page in range(2, last_page+1):
    response = requests.get("http://www.slaveryimages.org/api/items", params={'page': page, 'per_page': 10, 'sort_by': 'id', 'sort_order': 'asc'}).json()
    for item in response:
        code = item['dcterms:identifier'][0]['@value']
        results.append(code)

print(results)