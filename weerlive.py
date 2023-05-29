import sys
import requests
from datetime import datetime
from dotenv import dotenv_values
from sqlalchemy import (MetaData, create_engine, insert)

from database import write_to_database

def process():
    config = dotenv_values('.env')

    # Get the data
    data = get_data(config)

    # Filter the keys that we want
    keys = ['temp', 'gtemp', 'samenv', 'lv', 'windr', 'winds', 'luchtd', 'dauwp', 'zicht', 'image']
    data = { key: data[key] for key in keys }

    write_to_database(config['DATABASE_CONNECTION'], config['WEERLIVE_TABLE'], data)

def get_data(config):
    try:
        wl_key = config['WEERLIVE_KEY']
        wl_location = config['WEERLIVE_LOCATION']
        resp = requests.get(f'http://weerlive.nl/api/json-data-10min.php?key={wl_key}&locatie={wl_location}', verify=False, timeout=5)
    except requests.exceptions.Timeout as e:
        print("Timed out: %s" % repr(e))
        raise Exception(repr(e))
    except requests.exceptions.RequestException as e:
        return {}
    if resp.status_code != 200:
        return {}

    try:
        data = resp.json()
        if type(data) == dict:
            return data['liveweer'][0]
        else:
            return {}
    except simplejson.scanner.JSONDecodeError:
        return {}

if __name__ == "__main__":
    process()
