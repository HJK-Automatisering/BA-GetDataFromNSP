import logging
import pandas as pd
import time

#######################################################################

def create_ticket_df(df):
    today = pd.to_datetime(pd.Timestamp.today().normalize())
    date_cols = ['CreatedDate', 'CloseDateTime', 'u_Opstart', 'u_Afslutning']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    df['open_days'] = (df['CloseDateTime'] - df['CreatedDate']).dt.days + 1
    df['duration'] = (df['u_Afslutning'] - df['u_Opstart']).dt.days + 1
    df['queue_days'] = (df['u_Opstart'] - df['CreatedDate']).dt.days
    days_till_start = (df['u_Opstart'] - today).dt.days
    days_till_start = days_till_start.where(df['u_Opstart'] >= today, 0)
    days_till_start = days_till_start.fillna(0).clip(lower=0).astype(int)
    df['days_till_start'] = days_till_start
    offset_duration = (df['u_Afslutning'] - today).dt.days + 1
    offset_duration = offset_duration.where(df['u_Opstart'] <= today, df['duration'])
    offset_duration = offset_duration.fillna(0).clip(lower=0).astype(int)
    df['offset_duration'] = offset_duration
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
        'days_till_start',
        'offset_duration',
        'task_type_id',
        'task_area_id',
        'reason_for_rejection_id',
        'last_updated']]

    int_cols = [
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