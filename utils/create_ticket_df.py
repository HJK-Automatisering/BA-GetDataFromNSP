import logging
from typing import List

import pandas as pd

#######################################################################

# Flere hjælpekolonner. (is_planned = 0,1), ... ?

logger = logging.getLogger(__name__)

def create_ticket_df(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Beskrivelse:
        Danner en normaliseret ticket-DataFrame til indlæsning i databasen
        baseret på rå NSP-ticketdata.

    Flow:
        1. Konverterer rå datokolonner til datetime (med coercion ved fejl).
        2. Beregner afledte felter:
           - open_days: antal dage fra oprettelse til lukning (inkl. begge dage)
           - duration: antal dage fra opstart til afslutning (inkl. begge dage)
           - queue_days: antal dage fra oprettelse til opstart
        3. Omdøber kolonner til konsistente snake_case-navne.
        4. Vælger et fast sæt kolonner til output.
        5. Konverterer udvalgte id- og datokolonner til object og sætter
           0 og NaN til None, så de håndteres korrekt som NULL i SQL.

    Args:
        df:
            Rå DataFrame med NSP-ticketdata. Forventede kolonner inkluderer bl.a.:
            'ReferenceNo', 'AgentGroup.Id', 'BaseEntityStatus.Id',
            'Priority', 'BaseAgent', 'BaseEndUser', 'BaseHeader',
            'u_Opstart', 'u_Afslutning', 'u_Opgavetype.Id',
            'u_Omrder.Id', 'u_Afvisningsrsag.Id',
            'CreatedDate', 'CloseDateTime', 'UpdatedDate'.

    Returns:
        pd.DataFrame:
            En DataFrame med normaliseret ticket-struktur og kolonnerne:
            ['id', 'agent_group_id', 'task_status_id', 'created_date',
             'closed_date', 'open_days', 'queue_days', 'priority',
             'agent', 'user', 'ticket_title', 'start_date', 'end_date',
             'duration', 'task_type_id', 'task_area_id',
             'reason_for_rejection_id', 'last_updated'].

    Raises:
        KeyError:
            Hvis en eller flere af de forventede kolonnenavne
            ikke findes i df (fx ved fejl i tidligere pipeline-step).
    '''
    logger.info('Danner ticket_df')
    date_cols: List[str] = [
        'CreatedDate',
        'CloseDateTime',
        'u_Opstart',
        'u_Afslutning']
    
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    df['open_days'] = (
    df['CloseDateTime'].dt.normalize() - df['CreatedDate'].dt.normalize()).dt.days + 1
    df['duration'] = (df['u_Afslutning'].dt.normalize() - df['u_Opstart'].dt.normalize()).dt.days + 1
    df['queue_days'] = (df['u_Opstart'].dt.normalize() - df['CreatedDate'].dt.normalize()).dt.days
    ticket_df = df.rename(columns={
        'ReferenceNo': 'id',
        'AgentGroup.Id': 'agent_group_id',
        'BaseEntityStatus.Id': 'task_status_id',
        'Priority': 'priority',
        'BaseAgent': 'agent',
        'BaseEndUser': 'user',
        'BaseHeader': 'ticket_title',
        'u_Opstart': 'start_date',
        'u_Afslutning': 'end_date',
        'u_Opgavetype.Id': 'task_type_id',
        'u_Omrder.Id': 'task_area_id',
        'u_Afvisningsrsag.Id': 'reason_for_rejection_id',
        'CreatedDate': 'created_date',
        'CloseDateTime': 'closed_date',
        'UpdatedDate': 'last_updated'})

    ticket_df = ticket_df[[
        'id',
        'agent_group_id',
        'task_status_id',
        'created_date',
        'closed_date',
        'open_days',
        'queue_days',
        'priority',
        'agent',
        'user',
        'ticket_title',
        'start_date',
        'end_date',
        'duration',
        'task_type_id',
        'task_area_id',
        'reason_for_rejection_id',
        'last_updated']]

    int_cols: List[str] = [
        'open_days',
        'queue_days',
        'duration',
        'agent_group_id',
        'task_status_id',
        'task_type_id',
        'task_area_id',
        'reason_for_rejection_id']
    
    for col in int_cols:
        ticket_df[col] = ticket_df[col].astype('object')
        ticket_df.loc[ticket_df[col] == 0, col] = None
        ticket_df.loc[ticket_df[col].isna(), col] = None
    
    normalized_date_cols: List[str] = [
        'created_date',
        'closed_date',
        'start_date',
        'end_date']
    
    for col in normalized_date_cols:
        ticket_df[col] = ticket_df[col].astype('object')
        ticket_df.loc[ticket_df[col].isna(), col] = None

    ticket_df = ticket_df.where(ticket_df.notna(), None)
    logger.info('ticket_df dannet')
    return ticket_df