import http.client
import json
import ssl

CONFIG_FILE_PATH = "airflow/config/football_players_data_config.json"

with open(CONFIG_FILE_PATH, "r") as f:
    config = json.load(f)

context = ssl._create_unverified_context()

conn = http.client.HTTPSConnection("v3.football.api-sports.io", context=context)

headers = {
    'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': config["API_FOOTBALL_KEY"]
    }

conn.request("GET", "/teams?league=39&season=2021", headers=headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))
