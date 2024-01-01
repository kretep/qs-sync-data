import requests
import json
from dotenv import dotenv_values

from database import write_to_database

def process():
    config = dotenv_values('.env')

    # Read the current data
    response = requests.get(config['HOMEWIZARD_ENDPOINT'])
    data = json.loads(response.text)

    # Filter the keys that we want
    keys = ['total_power_export_t1_kwh', 'total_power_export_t2_kwh', 'total_power_import_t1_kwh', 'total_power_import_t2_kwh', 'active_power_w', 'total_gas_m3', 'gas_timestamp']
    data = { key: data[key] for key in keys }

    # Write
    write_to_database(config['DATABASE_CONNECTION'], config['HOMEWIZARD_TABLE'], data)

if __name__ == "__main__":
    process()
