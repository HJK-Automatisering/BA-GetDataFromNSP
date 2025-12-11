import logging
from typing import List

import pandas as pd

from config import TIMEZONE, DATE_FORMAT

#######################################################################

logger = logging.getLogger(__name__)

DROP_COLS: List[str] = [
    'Id',
    'EntityType',
    'Priority.Id',
    'BaseAgent.Id',
    'BaseEndUser.Id']

DATE_COLS: List[str] = [
    'CreatedDate',
    'CloseDateTime',
    'u_Opstart',
    'u_Afslutning']

def format_df(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Beskrivelse:
        Formaterer rå ticket-data fra API'et til et konsistent DataFrame-format
        ved at droppe tekniske kolonner, standardisere datoer og rydde
        op i udvalgte tekstfelter.

    Flow:
        1. Dropper tekniske/overflødige kolonner defineret i DROP_COLS.
        2. Konverterer datokolonner i DATE_COLS til datetime (UTC),
           konverterer dem til lokal tidszone (TIMEZONE) og formatterer
           dem som tekst baseret på DATE_FORMAT.
        3. Rydder op i tekstfelterne 'u_Opgavetype', 'u_Omrder' og
           'u_Afvisningsrsag' ved at fjerne faste prefix/suffix-strenge.

    Args:
        df:
            Rå DataFrame som returneres fra API-kaldet, forventes at
            indeholde mindst kolonnerne i DATE_COLS samt
            'u_Opgavetype', 'u_Omrder' og 'u_Afvisningsrsag'.

    Returns:
        pd.DataFrame:
            Samme DataFrame-objekt (modificeret in-place) med:
            - færre kolonner,
            - formaterede dato-strenge,
            - rensede tekstfelter for opgavetype, område
              og afvisningsårsag.

    Raises:
        KeyError:
            Hvis en eller flere af de forventede kolonner i DATE_COLS
            eller tekstkolonnerne til oprydning ikke findes i df.
    '''
    logger.info('Formaterer df')
    df = df.drop(columns=DROP_COLS, errors='ignore')
    for col in DATE_COLS:
        if col not in df.columns:
            logger.warning('Forventet datokolonne mangler: %s', col)
            continue

        raw = df[col].astype('string').str.strip()
        parsed_values = []
        for value in raw:
            # value er enten en str eller pd.NA
            if value is pd.NA or value == '':
                parsed_values.append(pd.NaT)
            else:
                parsed_values.append(
                    pd.to_datetime(value, errors='coerce', utc=True))

        dt = pd.Series(parsed_values, index=df.index)
        dt = dt.dt.tz_convert(TIMEZONE)
        df[col] = dt.dt.strftime(DATE_FORMAT)

    df['u_Opgavetype'] = (
        df['u_Opgavetype']
        .str.replace('Ticket.u_Opgavetype.', '', regex=False)
        .str.replace('.DisplayNameId.label-en', '', regex=False))
    df['u_Omrder'] = (
        df['u_Omrder']
        .str.replace('Ticket.u_Omrder.', '', regex=False)
        .str.replace('.DisplayNameId.label-en', '', regex=False))
    df['u_Afvisningsrsag'] = (
        df['u_Afvisningsrsag']
        .str.replace('Ticket.u_Afvisningsrsag.', '', regex=False)
        .str.replace('.DisplayNameId.label-en', '', regex=False))
    logger.info('df formateret')
    return df
