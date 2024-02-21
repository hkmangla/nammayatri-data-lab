import pandas as pd
from sqlalchemy import create_engine
from clickhouse_driver import Client

# Database connection parameters
db_config = {
    'host': 'localhost',
    'port': 5434,  # Update with your port number
    'dbname': 'atlas_dev',
    'user': 'atlas_driver_offer_bpp_user'
}

clickhouse_config = {
    'host': 'localhost',
    'port': 9000,  # Update with your port number
    'dbname': 'atlas_dev',
    'user': 'atlas_driver_offer_bpp_user'
}

def connect_db (db_config):
  return create_engine(f"postgresql+psycopg2://{db_config['user']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")

def connect_clickhouse (clickhouse_config):
  return Client(host=clickhouse_config['host'], port=clickhouse_config['port'], user=clickhouse_config['user'], database=clickhouse_config['dbname'])   

def load_db_data(engine, schema, table_name):
    """
    Load data from a PostgreSQL table into a pandas DataFrame.

    Args:
    schema (str): Schema of the PostgreSQL table.
    table_name (str): Name of the PostgreSQL table to load data from.

    Returns:
    pd.DataFrame: DataFrame containing data from the specified table.
    """
    full_table_name = f"{schema}.{table_name}"
    query = f"SELECT * FROM {full_table_name}"
    df = pd.read_sql(query, engine)
    return df
