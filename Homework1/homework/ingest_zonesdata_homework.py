#!/usr/bin/env python
# coding: utf-8
# # Ingesting Data into Postgres Database
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import os


dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}


#### Command line interface
@click.command()
@click.option('--pg_user', default = 'root', help = 'PostgreSQL user')
@click.option('--pg_password', default = 'root', help = 'PostgreSQL password')
@click.option('--pg_host', default = 'localhost', help = 'PostgreSQL host')
@click.option('--pg_port', default = 5432, type = int, help = 'PostgreSQL port')
@click.option('--pg_db', default = 'ny_taxi', help = 'PostgreSQL database name')
@click.option('--target_table', default = 'zones_data', help = 'Target table name')
@click.option('--chunksize', default = 10000, type = int, help = 'Chunk size for reading CSV')

def run(pg_user, pg_password, pg_host, pg_port, pg_db, chunksize, target_table):
    
    # Read a sample of the data
    url = os.path.join("taxi_zone_lookup.csv")

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')


    # ##### Importing Chunk-sized data


    #creation of chunk-sized iterator
    
    df_iter = pd.read_csv(
        url,
        dtype = dtype,
        iterator = True,
        chunksize = chunksize
    )

    # inserting the data chunk by chunk into the database table with a progress bar
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(n=0).to_sql(
                name = target_table,
                con = engine, 
                if_exists = 'replace'
            )
            first = False
        df_chunk.to_sql(
            name = target_table, 
            con = engine, 
            if_exists = 'append'
        )

if __name__ == '__main__':
    run()
