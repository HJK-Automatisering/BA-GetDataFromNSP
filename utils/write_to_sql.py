import logging
from typing import Tuple, List

import pandas as pd
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.create_dim_df import create_dim_df
from utils.create_ticket_df import create_ticket_df

#######################################################################

logger = logging.getLogger(__name__)

def write_to_sql(engine: Engine, df: pd.DataFrame) -> None:
    '''
    Beskrivelse:
        Skriver dimensionstabeller og tickets til SQL-databasen via upserts
        baseret på et allerede formatteret DataFrame.

    Flow:
        1. Danner fem dimensionstabeller ud fra det rå DataFrame:
           - agent_groups
           - task_types
           - task_areas
           - task_status
           - reasons_for_rejection
        2. Danner et normaliseret ticket_df (facts) via create_ticket_df.
        3. Upserter hver række i dimensionstabellerne med MERGE:
           - Matcher på id
           - Opdaterer label-kolonnen ved match
           - Indsætter ny række ved ikke-match
        4. Upserter hver række i ticket_df til tickets-tabellen med MERGE:
           - Matcher på id
           - Opdaterer alle øvrige kolonner ved match
           - Indsætter ny række ved ikke-match

    Args:
        engine:
            SQLAlchemy Engine-instans med forbindelse til databasen.
        df:
            Rå DataFrame med ticket-data (samme som efter format_df),
            der indeholder alle nødvendige kolonner til både dimensioner
            og tickets.

    Returns:
        None. Effekten er udelukkende opdateringer/indsættelser
        i databasen.

    Raises:
        sqlalchemy.exc.SQLAlchemyError:
            Hvis en databasefejl opstår under transaktionen, fx
            ved låseproblemer eller forbindelsesfejl.
    '''
    logger.info('Skriver til SQL')
    agent_groups_df = create_dim_df(df, 'AgentGroup.Id', 'AgentGroup', 'id', 'group')
    task_types_df = create_dim_df(df, 'u_Opgavetype.Id', 'u_Opgavetype', 'id', 'type')
    task_areas_df = create_dim_df(df, 'u_Omrder.Id', 'u_Omrder', 'id', 'area')
    task_status_df = create_dim_df(df, 'BaseEntityStatus.Id', 'BaseEntityStatus', 'id', 'status')
    reasons_for_rejection_df = create_dim_df(df, 'u_Afvisningsrsag.Id', 'u_Afvisningsrsag', 'id', 'reason')
    ticket_df = create_ticket_df(df)

    dim_tables: List[Tuple[str, pd.DataFrame, str]] = [
        ('agent_groups', agent_groups_df, 'group'),
        ('task_types', task_types_df, 'type'),
        ('task_areas', task_areas_df, 'area'),
        ('task_status', task_status_df, 'status'),
        ('reasons_for_rejection', reasons_for_rejection_df, 'reason')]

    try:
        with engine.begin() as conn:
            for table_name, dim_df, label_col in dim_tables:
                for _, row in dim_df.iterrows():
                    conn.execute(
                        text(
                            f'''
                            MERGE {table_name} AS target
                            USING (SELECT :id AS id, :label AS label) AS source
                            ON target.id = source.id
                            WHEN MATCHED THEN
                                UPDATE SET [{label_col}] = source.label
                            WHEN NOT MATCHED THEN
                                INSERT (id, [{label_col}])
                                VALUES (source.id, source.label);
                            '''),
                        {'id': row['id'], 'label': row[label_col]})
                logger.info('%s upserted (%s rows)', table_name, len(dim_df))

            all_cols = list(ticket_df.columns)
            non_id_cols = [c for c in all_cols if c != 'id']

            def col(column_name: str) -> str:
                return f'[{column_name}]'

            source_select = ', '.join([f':{c} AS {col(c)}' for c in all_cols])
            update_set = ', '.join([f'target.{col(c)} = source.{col(c)}' for c in non_id_cols])
            insert_cols = ', '.join([col(c) for c in all_cols])
            insert_vals = ', '.join([f'source.{col(c)}' for c in all_cols])

            merge_sql = text(
                'MERGE tickets AS target '
                f'USING (SELECT {source_select}) AS source '
                'ON target.id = source.id '
                'WHEN MATCHED THEN '
                f'UPDATE SET {update_set} '
                'WHEN NOT MATCHED THEN '
                f'INSERT ({insert_cols}) '
                f'VALUES ({insert_vals});')

            for _, row in ticket_df.iterrows():
                params = row.to_dict()
                conn.execute(merge_sql, params)

            logger.info('tickets upserted (%s rows)', len(ticket_df))
    except sqlalchemy.exc.SQLAlchemyError as exc:
        logger.error('Fejl ved skrivning til SQL: %s', exc, exc_info=True)
        raise