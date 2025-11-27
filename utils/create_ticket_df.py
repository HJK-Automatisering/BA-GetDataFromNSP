import logging
import pandas as pd

#######################################################################

def create_ticket_df(df):
    date_cols = ['CreatedDate', 'CloseDateTime', 'u_Opstart', 'u_Afslutning']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    df['open_days'] = (df['CloseDateTime'] - df['CreatedDate']).dt.days + 1
    df['processing_days'] = (df['u_Afslutning'] - df['u_Opstart']).dt.days + 1

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
        'CloseDateTime': 'closed_date'})

    ticket_df = ticket_df[[
        'id',
        'agent_group_id',
        'task_status_id',
        'created_date',
        'closed_date',
        'open_days',
        'priority',
        'agent',
        'user',
        'ticket_title',
        'start_date',
        'end_date',
        'processing_days',
        'task_type_id',
        'task_area_id',
        'reason_for_rejection_id']]
    
    id_cols = [
        'open_days',
        'processing_days',
        'agent_group_id',
        'task_status_id',
        'task_type_id',
        'task_area_id',
        'reason_for_rejection_id']
    for col in id_cols:
        ticket_df[col] = ticket_df[col].astype('object')
        ticket_df.loc[ticket_df[col] == 0, col] = None
        ticket_df.loc[ticket_df[col].isna(), col] = None
    
    date_cols = [
        'created_date',
        'closed_date',
        'start_date',
        'end_date']
    for col in date_cols:
        ticket_df[col] = ticket_df[col].astype('object')
        ticket_df.loc[ticket_df[col].isna(), col] = None

    ticket_df = ticket_df.where(ticket_df.notna(), None)
    logging.info('ticket_df created')
    return ticket_df