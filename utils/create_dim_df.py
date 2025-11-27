import logging

#######################################################################

DICT_GROUP = {
    }

DICT_TYPE = {
    181930: 'Øvrige'}

DICT_AREA = {
    179117: 'Udvikling og Administration',
    181932: 'Børne- og Familieområdet'}

DICT_STATUS = {
    1:'Registreret',
    3:'Tildelt',
    6:'Påbegyndt',
    10:'Løst',
    11:'Lukket',
    25:'Svar modtaget',
    26:'Genåbnet',
    27:'Afventer',
    28:'Afventer bruger',
    29:'Afventer leverandør',
    35:'Løst uden mail'}

DICT_REASON = {
    183001: 'Ikke afvist'}

def create_dim_df(df, id_col, label_col, id_name='id', label_name=None):
    dim_df = (df[[id_col, label_col]].dropna(subset=[id_col]) .drop_duplicates(subset=[id_col], keep='first').rename(columns={id_col: id_name, label_col: label_name}))
    if label_name == 'group':
        dim_df[label_name] = dim_df[id_name].map(lambda x: DICT_GROUP.get(x, dim_df.loc[dim_df[id_name] == x, label_name].iloc[0]))
    if label_name == 'type':
        dim_df[label_name] = dim_df[id_name].map(lambda x: DICT_TYPE.get(x, dim_df.loc[dim_df[id_name] == x, label_name].iloc[0]))
    if label_name == 'area':
        dim_df[label_name] = dim_df[id_name].map(lambda x: DICT_AREA.get(x, dim_df.loc[dim_df[id_name] == x, label_name].iloc[0]))
    if label_name == 'status':
        dim_df[label_name] = dim_df[id_name].map(lambda x: DICT_STATUS.get(x, dim_df.loc[dim_df[id_name] == x, label_name].iloc[0]))
    if label_name == 'reason':
        dim_df[label_name] = dim_df[id_name].map(lambda x: DICT_REASON.get(x, dim_df.loc[dim_df[id_name] == x, label_name].iloc[0]))
    logging.info('%s_df created', label_name)
    return dim_df