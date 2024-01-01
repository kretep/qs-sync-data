from dotenv import dotenv_values
import pandas as pd
from database import write_dataframe_to_database

def process():
    config = dotenv_values('.env')

    # Read the current data
    df = pd.read_csv(config['HARDLOOPTIJDEN_FILE'], sep=',')

    # Extract the columns that we want
    columns = ["date", "start_time", "distance", "run_time", "cadence", "heart_rate", "temperature", "comments"]
    df = df[columns]

    # Write the data to the database
    write_dataframe_to_database(config['DATABASE_CONNECTION'], config['HARDLOOPTIJDEN_TABLE'], df)

if __name__ == "__main__":
    process()
