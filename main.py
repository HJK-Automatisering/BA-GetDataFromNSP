####################################################################### 

__maintainer__      = 'Anders H. Vestergaard'
__author__          = 'Anders H. Vestergaard'
__contributors__    = []
__email__           = 'anders.vestergaard@hjoerring.dk'
__version__         = '1.0.1'
__status__          = 'Production'

#######################################################################

'''
Hovedscriptet for ETL-processen, der periodisk henter nye eller opdaterede
tickets fra NSP-API'et, formatterer rå data og upserter dem i databasen.
Processen kører i en uendelig løkke med en pause mellem hver cyklus.
'''

#######################################################################

import logging
import sys
import time

import pandas as pd
from sqlalchemy.engine import Engine

from config import SCRIPT_RUNTIME
from utils.api_fetch import api_fetch, ApiError
from utils.format_df import format_df
from utils.get_engine import get_engine
from utils.get_last_updated import get_last_updated
from utils.update_tickets import update_tickets
from utils.write_to_sql import write_to_sql

#######################################################################

logger = logging.getLogger(__name__)

def setup_logging() -> None:
    '''
    Beskrivelse:
        Initialiserer logging-konfiguration for applikationen.

    Flow:
        1. Konfigurerer root-logger til INFO-niveau.             
        2. Logger til stdout med timestamps, niveau og modulnavn.

    Args:
        Ingen.                                                

    Returns:
        None.                                           

    Raises:
        None.                                                    
    '''
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout)])

def main(engine: Engine) -> None:
    '''
    Beskrivelse:
        Én ETL-cyklus, der henter nye tickets, formatterer dem
        og skriver dem til SQL.                                  

    Flow:
        1. Finder seneste last_updated fra databasen.            
        2. Henter tickets opdateret siden timestamp via NSP-API'et.
        3. Konverterer API-responsen til DataFrame.
        4. Formatterer DataFrame til standardiseret format.
        5. Upserter dimensionstabeller og tickets.
        6. Opdaterer afledte ticketfelter i databasen.

    Args:
        engine:
            SQLAlchemy Engine-instans med forbindelse
            til mål-databasen.                                  

    Returns:
        None. Resultatet ligger i database-sideeffekter
        og logging.                                              

    Raises:
        ApiError:
            Hvis API-kaldet fejler og api_fetch hæver
            en ApiError.                                         
    '''
    timestamp = get_last_updated(engine)
    response = api_fetch(timestamp)
    try:
        data = response.json()
    except ValueError as exc:
        logger.error('API-respons kunne ikke parses som JSON: %s', exc, exc_info=True)
        return
    
    df = pd.DataFrame(data['Data'])
    if df.empty:
        logger.info('Ingen nye tickets i API-responsen')
        return
    
    df = format_df(df)
    write_to_sql(engine, df)
    update_tickets(engine)
 
if __name__ == '__main__':
    setup_logging() 
    engine = get_engine()

    while True:
        logger.info('Loop initieret')
        try:
            main(engine)
        except ApiError as exc:
            logger.error('API-fejl i loop: %s', exc, exc_info=True)
        except Exception as exc:
            logger.critical('Uventet fejl i loop: %s', exc, exc_info=True)
            
        logger.info('Loop gennemført')
        time.sleep(int(SCRIPT_RUNTIME))