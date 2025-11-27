####################################################################### 

__maintainer__      = 'Anders H. Vestergaard'
__author__          = 'Anders H. Vestergaard'
__contributors__    = []
__email__           = 'anders.vestergaard@hjoerring.dk'
__version__         = '1.0.0'
__status__          = 'Production'

#######################################################################

# TODO Error handling
# TODO Docker

#######################################################################

import logging
import os
import pandas as pd
import time
from dotenv import load_dotenv
from utils.api_fetch import api_fetch
from utils.format_df import format_df
from utils.update_env import update_env
from utils.write_to_sql import write_to_sql
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%m-%Y %H:%M:%S', level=logging.INFO)
logging.info('Libraries imported')

#######################################################################

load_dotenv()

def main():
    response = api_fetch()
    update_env()
    data = response.json()
    df = pd.DataFrame(data['Data'])
    if df.empty:
        logging.info('Ingen data i df')
        return
    df = format_df(df)
    write_to_sql(df)
    return
 
if __name__ == '__main__':
    while True:
        logging.info('Loop initiated')
        main()
        logging.info('Loop completed')
        time.sleep(int(os.getenv('SCRIPT_RUNTIME')))