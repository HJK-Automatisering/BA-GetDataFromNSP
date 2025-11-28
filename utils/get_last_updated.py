import datetime
import logging
from sqlalchemy import text

#######################################################################

def get_last_updated(engine):
    query = '''
        SELECT MAX(last_updated) AS max_last_updated
        FROM tickets;
        '''
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        row = result.fetchone()

    max_dt = row.max_last_updated
    max_dt = max_dt.replace(tzinfo=datetime.timezone.utc)
    logging.info('last_updated fetched')
    return max_dt.isoformat().replace('+00:00', 'Z')