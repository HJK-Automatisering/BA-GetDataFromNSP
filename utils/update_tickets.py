import logging

import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy import text

#######################################################################

logger = logging.getLogger(__name__)

def update_tickets(engine: Engine) -> None:
    '''
    Beskrivelse:
        Opdaterer afledte felter på åbne tickets i databasen ved at
        beregne days_till_start og offset_duration for alle rækker
        i tickets-tabellen, hvor closed_date er NULL.

    Flow:
        1. Sætter en variabel @today til dagens dato (GETDATE()).
        2. Opdaterer kolonnen days_till_start for alle åbne tickets:
           - antal dage fra i dag til start_date
           - sættes til 0 hvis start_date er NULL eller er i dag/fortid.
        3. Opdaterer kolonnen offset_duration for alle åbne tickets:
           - sættes til 0 hvis end_date er NULL, før i dag, eller
             hvis start_date er NULL.
           - hvis start_date er efter i dag: offset_duration = duration.
           - ellers: offset_duration = DATEDIFF(day, @today, end_date) + 1.

    Args:
        engine:
            En SQLAlchemy Engine-instans med forbindelse til
            databasen, hvor tickets-tabellen findes.

    Returns:
        None. Effekten er udelukkende opdateringer i databasen
        via en SQL UPDATE-sætning.

    Raises:
        sqlalchemy.exc.SQLAlchemyError:
            Hvis udførslen af SQL-forespørgslen fejler, fx pga.
            forbindelsesproblemer eller låsefejl i databasen.
    '''
    logger.info('Opdaterer tickets')
    update_query = '''
    DECLARE @today date = CAST(GETDATE() AS date);
    UPDATE t
    SET
        days_till_start = CASE
            WHEN t.start_date IS NULL THEN 0
            WHEN t.start_date <= @today THEN 0
            ELSE DATEDIFF(day, @today, t.start_date)
        END,
        offset_duration = CASE
            WHEN t.end_date IS NULL THEN 0
            WHEN t.end_date < @today THEN 0
            WHEN t.start_date IS NULL THEN 0
            WHEN t.start_date > @today THEN t.duration
            ELSE DATEDIFF(day, @today, t.end_date) + 1
        END
    FROM tickets t
    WHERE t.closed_date IS NULL;
    '''
    try:
        with engine.begin() as conn:
            conn.execute(text(update_query))
            logger.info('tickets opdateret')
    except sqlalchemy.exc.SQLAlchemyError as exc:
        logger.error('Fejl ved opdatering af tickets: %s', exc, exc_info=True)
        raise