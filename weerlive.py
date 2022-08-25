import sys
import requests
from datetime import datetime
from dotenv import dotenv_values
from sqlalchemy import (MetaData, create_engine, insert)

def process():
    config = dotenv_values('.env')

    # Get the data
    data = get_data(config)

    # Filter the keys that we want
    keys = ['temp', 'gtemp', 'samenv', 'lv', 'windr', 'winds', 'luchtd', 'dauwp', 'zicht', 'image']
    data = { key: data[key] for key in keys }

    # Write to database
    engine = create_engine(config['DATABASE_CONNECTION'])
    with engine.connect() as conn:
        # Get table from database
        metadata = MetaData(bind=conn)
        metadata.reflect(only=[config['WEERLIVE_TABLE']])
        table = metadata.tables[config['WEERLIVE_TABLE']]

        # Create & execute query
        query = insert(table).values(**data)
        try:
            conn.execute(query)
            print('Data written to database')
        except Exception as e:
            sys.exit(f'{datetime.now()} {e}')

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
