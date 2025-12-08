import datetime
import logging

from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import TIMESTAMP_FALLBACK

#######################################################################

logger = logging.getLogger(__name__)

def get_last_updated(engine: Engine) -> str:
    '''
    Beskrivelse:
        Henter seneste last_updated-timestamp fra tickets-tabellen
        og returnerer det som en ISO8601 UTC-streng. Hvis der ikke
        findes nogen værdier, anvendes et fast fallback-timestamp.

    Flow:
        1. Udfører en SQL-forespørgsel:
           SELECT MAX(last_updated) AS max_last_updated FROM tickets;
        2. Læser første række fra resultatet.
        3. Hvis værdien er None, logges info og fallback-timestamp
           returneres.
        4. Ellers sættes tidszonen til UTC og værdien konverteres til
           ISO8601-streng med 'Z'-suffix.

    Args:
        engine:
            En SQLAlchemy Engine-instans med forbindelse til
            databasen, der indeholder tickets-tabellen.

    Returns:
        str:
            ISO8601 UTC-timestamp som streng (fx '2025-09-01T00:00:00Z'),
            enten fra databasen eller fra fallback-værdien.

    Raises:
        sqlalchemy.exc.SQLAlchemyError:
            Hvis databaseforbindelsen fejler, eller hvis
            forespørgslen ikke kan udføres.
    '''
    logger.info('Henter last_updated')
    query = '''
        SELECT MAX(last_updated) AS max_last_updated
        FROM tickets;
        '''
    with engine.connect() as conn:
        result = conn.execute(text(query))
        row = result.fetchone()

    max_dt = row.max_last_updated
    if max_dt is None:
        logger.info('Ingen last_updated fundet, bruger fallback-timestamp')
        return TIMESTAMP_FALLBACK
    max_dt = max_dt.replace(tzinfo=datetime.timezone.utc)
    logger.info('last_updated hentet')
    return max_dt.isoformat().replace('+00:00', 'Z')