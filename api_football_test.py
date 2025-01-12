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

conn.request("GET", "/players?league=39&season=2023&page=2", headers=headers)

res = conn.getresponse()
data = res.read()

json_data = json.loads(data.decode("utf-8"))

# Specify the file path where you want to save the data
output_file_path = "football_players_data.json"

# Write the JSON data into a file
with open(output_file_path, "w") as json_file:
    json.dump(json_data, json_file, indent=4)

print(f"Data saved to {output_file_path}")
