import logging
import sys

#######################################################################

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