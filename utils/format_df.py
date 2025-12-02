import logging
import pandas as pd

#######################################################################

import pandas as pd

def format_df(df):
    drop_cols = [
        'Id',
        'EntityType',
        'Priority.Id',
        'BaseAgent.Id',
        'BaseEndUser.Id']
    
    df = df.drop(columns=drop_cols, errors='ignore')
    date_cols = [
        'CreatedDate',
        'CloseDateTime',
        'u_Opstart',
        'u_Afslutning']
    
    for col in date_cols:
        dt = pd.to_datetime(df[col], errors='coerce', utc=True)
        dt = dt.dt.tz_convert('Europe/Copenhagen')
        df[col] = dt.dt.strftime('%Y-%m-%d')

    df['u_Opgavetype'] = (df['u_Opgavetype'].str.replace('Ticket.u_Opgavetype.', '', regex=False).str.replace('.DisplayNameId.label-en', '', regex=False))
    df['u_Omrder'] = (df['u_Omrder'].str.replace('Ticket.u_Omrder.', '', regex=False).str.replace('.DisplayNameId.label-en', '', regex=False))
    df['u_Afvisningsrsag'] = (df['u_Afvisningsrsag'].str.replace('Ticket.u_Afvisningsrsag.', '', regex=False).str.replace('.DisplayNameId.label-en', '', regex=False))
    logging.info('Dataframe formatted')
    return df
