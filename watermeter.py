import requests
import json
import prefect
from dotenv import dotenv_values

from database import write_to_database

@prefect.task
def fetch_data(endpoint):
    response = requests.get(endpoint)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        raise Exception(f"Failed to retrieve data from {endpoint}, status code: {response.status_code}")

@prefect.task
def process_data(data):
    keys = ['total_liter_m3']
    return {key: data[key] for key in keys if key in data}

@prefect.task
def store_data(database_connection, table_name, data):
    write_to_database(database_connection, table_name, data)

@prefect.flow(name="Water Meter ETL")
def water_meter_etl():
    config = dotenv_values('.env')
    data = fetch_data(config['HW_WATER_ENDPOINT'])
    processed_data = process_data(data)
    store_data(config['DATABASE_CONNECTION'], config['HW_WATER_TABLE'], processed_data)


def process():
    config = dotenv_values('.env')

    # Read the current data
    response = requests.get(config['HW_WATER_ENDPOINT'])
    data = json.loads(response.text)

    # Filter the keys that we want
    keys = ['total_liter_m3']
    data = { key: data[key] for key in keys }

    # Write
    write_to_database(config['DATABASE_CONNECTION'], config['HW_WATER_TABLE'], data)

if __name__ == "__main__":
    process()
