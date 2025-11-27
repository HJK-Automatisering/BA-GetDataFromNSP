import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

#######################################################################

load_dotenv()

def get_engine():
    odbc_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={os.getenv('DB_SERVER')};'
        f'DATABASE={os.getenv('DB_NAME')};'
        f'UID={os.getenv('DB_USERNAME')};'
        f'PWD={os.getenv('DB_PASSWORD')};'
        'Trust_Connection=yes;'     
        'TrustServerCertificate=yes;')

    from urllib.parse import quote_plus
    conn_str = f'mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}'
    engine = create_engine(conn_str, fast_executemany=True)
    logging.info('SQL-connection established')
    return engine