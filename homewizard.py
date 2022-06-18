import sys
import requests
import json
from datetime import datetime
from dotenv import dotenv_values
from sqlalchemy import (MetaData, create_engine, insert)

def process():
    config = dotenv_values('.env')

    # Read the current data
    response = requests.get(config['HOMEWIZARD_ENDPOINT'])
    data = json.loads(response.text)

    # Filter the keys that we want
    keys = ['total_power_export_t1_kwh', 'total_power_export_t2_kwh', 'total_power_import_t1_kwh', 'total_power_import_t2_kwh', 'active_power_w', 'total_gas_m3', 'gas_timestamp']
    data = { key: data[key] for key in keys }
    print(data)

    # Write to database
    engine = create_engine(config['DATABASE_CONNECTION'])
    with engine.connect() as conn:
        # Get table from database
        metadata = MetaData(bind=conn)
        metadata.reflect(only=[config['TABLE_NAME']])
        table = metadata.tables[config['TABLE_NAME']]

        # Create & execute query
        query = insert(table).values(**data)
        try:
            conn.execute(query)
            print('Data written to database')
        except Exception as e:
            sys.exit(f'{datetime.now()} {e}')

if __name__ == "__main__":
    process()
