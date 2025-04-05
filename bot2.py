import requests

key = 'af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir'

headers = {
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Mobile Safari/537.36',
    'Accept': 'application/json',
}

params = {
    'categoryId': 24,
    'locationId': 653240,
    'withImagesOnly': 'true',
    'sort': 'date',
    'limit': 3,
    'query': '1-к. квартира Санкт-Петербург',
    'key': key,
}

response = requests.get("https://m.avito.ru/api/9/items", headers=headers, params=params)
data = response.json()

print(data)

for item in data.get("result", {}).get("items", []):
    if item["type"] == "item":
        print(item["value"]["title"])
        print(item["value"]["price"])
        print("https://www.avito.ru/items/" + str(item["value"]["id"]))
        print("-" * 50)
