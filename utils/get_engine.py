import logging
from urllib.parse import quote_plus

import sqlalchemy
from sqlalchemy.engine import Engine

from config import DB_DRIVER, DB_SERVER, DB_NAME, DB_USERNAME, DB_PASSWORD

#######################################################################

logger = logging.getLogger(__name__)

def get_engine() -> Engine:
    '''
    Beskrivelse:
        Opretter og returnerer en SQLAlchemy Engine-instans baseret på
        konfigurationsvariabler fra config-modulet. Forbindelsen
        etableres via pyodbc.

    Flow:
        1. Logger at engine-oprettelse er initieret.
        2. Samler en ODBC-connection string til SQL Server.
        3. URL-enkoder connection string til brug i SQLAlchemy.
        4. Opretter engine med fast_executemany aktiveret.
        5. Logger succesfuld oprettelse eller fejl.

    Args:
        Ingen. Engine konfigureres udelukkende ud fra
        globale konfigurationsvariabler.

    Returns:
        Engine:
            En initialiseret SQLAlchemy Engine med
            fast_executemany=True.

    Raises:
        sqlalchemy.exc.SQLAlchemyError:
            Hvis engine-oprettelsen mislykkes, fx pga. ugyldig
            connection string eller netværksfejl.
    '''
    logger.info('Henter engine')
    odbc_str = (
        f'DRIVER={DB_DRIVER};'
        f'SERVER={DB_SERVER};'
        f'DATABASE={DB_NAME};'
        f'UID={DB_USERNAME};'
        f'PWD={DB_PASSWORD};'
        'Trusted_Connection=no;'     
        'TrustServerCertificate=yes;')

    conn_str = f'mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}'
    try:
        engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)
        logger.info('Engine hentet')
    except sqlalchemy.exc.SQLAlchemyError as exc:
        logger.error('Fejl ved oprettelse af database-engine: %s', exc, exc_info=True)
        raise
    return engine