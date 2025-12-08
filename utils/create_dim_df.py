import logging
from typing import Dict

import pandas as pd

#######################################################################

logger = logging.getLogger(__name__)

DICT_GROUP: Dict[int, str] = {
    }

DICT_TYPE: Dict[int, str] = {
    181930: 'Øvrige'}

DICT_AREA: Dict[int, str] = {
    179117: 'Udvikling og Administration',
    181932: 'Børne- og Familieområdet'}

DICT_STATUS: Dict[int, str] = {
    1: 'Registreret',
    3: 'Tildelt',
    6: 'Påbegyndt',
    10: 'Løst',
    11: 'Lukket',
    25: 'Svar modtaget',
    26: 'Genåbnet',
    27: 'Afventer',
    28: 'Afventer bruger',
    29: 'Afventer leverandør',
    35: 'Løst uden mail'}

DICT_REASON: Dict[int, str] = {
    183001: 'Ikke afvist'}

DICT_LOOKUP: Dict[str, Dict[int, str]] = {
    'group': DICT_GROUP,
    'type': DICT_TYPE,
    'area': DICT_AREA,
    'status': DICT_STATUS,
    'reason': DICT_REASON}

def create_dim_df(
    df: pd.DataFrame,
    id_column_name: str,
    label_column_name: str,
    id_name: str = 'id',
    label_name: str = 'label') -> pd.DataFrame:
    '''
    Beskrivelse:
        Opretter en dimensionstabel ud fra et råt DataFrame ved at udtrække
        id- og label-kolonner, rense data og mappe labels via DICT_LOOKUP.
        Funktionen understøtter dimensionstyperne 'group', 'type', 'area',
        'status' og 'reason'.

    Flow:
        1. Udvælger id- og label-kolonner fra df.
        2. Dropper rækker uden id (NaN i id-kolonnen).
        3. Fjerner dubletter baseret på id-kolonnen.
        4. Omdøber kolonner til id_name og label_name.
        5. Finder det relevante opslagsdict i DICT_LOOKUP baseret på label_name.
        6. Mapper id'er til nye labels via opslagsdict, og anvender
           den oprindelige label fra df som fallback, hvis der ikke findes et match.
        7. Returnerer en ren dimensionstabel med kolonnerne [id_name, label_name].

    Args:
        df:
            Rå DataFrame indeholdende kolonnerne id_column_name og
            label_column_name.
        id_column_name:
            Navnet på kolonnen i df der indeholder id-værdierne.
        label_column_name:
            Navnet på kolonnen der indeholder den oprindelige tekst/label.
        id_name:
            Nyt navn for id-kolonnen i output-DataFrame (default 'id').
        label_name:
            Nyt navn for label-kolonnen i output-DataFrame. Bruges også som
            nøgle til at slå op i DICT_LOOKUP.

    Returns:
        pd.DataFrame:
            En dimensionstabel med to kolonner: [id_name, label_name].
            Labels oversættes via DICT_LOOKUP[label_name] hvis tilgængelig,
            ellers anvendes den originale label.

    Raises:
        KeyError:
            Hvis id_column_name eller label_column_name ikke findes i df.
        IndexError:
            Hvis fallback-labelen for en id-værdi ikke kan findes i df
            (teoretisk kanttilfælde).
    '''
    logger.info('Danner %s_df', label_name)
    dim_df = (
        df[[id_column_name, label_column_name]]
        .dropna(subset=[id_column_name])
        .drop_duplicates(subset=[id_column_name])
        .rename(columns={id_column_name: id_name, label_column_name: label_name}))

    lookup_dict = DICT_LOOKUP.get(label_name)

    if lookup_dict is not None:
        dim_df[label_name] = dim_df[id_name].map(
            lambda dim_id: lookup_dict.get(
                dim_id,
                dim_df.loc[dim_df[id_name] == dim_id, label_name].iloc[0]))

    logger.info('%s_df dannet', label_name)
    return dim_df