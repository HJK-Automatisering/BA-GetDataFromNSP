import logging
from sqlalchemy import text

#######################################################################

def update_tickets(engine):
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
    with engine.begin() as conn:
        conn.execute(text(update_query))
    logging.info('tickets updated')