import json
import logging

import requests

from config import API_URL, API_KEY

#######################################################################

logger = logging.getLogger(__name__)

class ApiError(Exception):
    '''
    Beskrivelse:
        Specialiseret exception-type til API-relaterede fejl,
        fx manglende forbindelse eller ugyldigt svar fra NSP-API'et.

    Flow:
        Bruges til at indkapsle alle forventede fejl ved API-kald,
        så kaldere kan håndtere dem samlet (fx i main-loopet).       

    Args:
        Ingen.

    Returns:
        Ingen. Klassen repræsenterer kun en fejlsituation.

    Raises:
        Ingen direkte. Bruges som type for hævede fejl.               
    '''
    pass

def api_fetch(timestamp: str) -> requests.Response:
    '''
    Beskrivelse:
        Henter tickets fra NSP-API'et, filtreret på UpdatedDate >= timestamp.
        Resultatet returneres som et `requests.Response`-objekt.       

    Flow:
        1. Logger at et API-kald initieres.
        2. Bygger JSON-payload med ønskede kolonner og filtre.
        3. Sender POST-request til NSP-API'et med API-nøgle.
        4. Logger succes ved 2xx-svar og hæver ApiError ved fejl.

    Args:
        timestamp:
            ISO8601-dato/tid i UTC, typisk hentet fra databasen
            via last_updated-feltet.

    Returns:
        requests.Response:
            HTTP-responsen fra NSP-API'et, forudsat at statuskoden
            er i 2xx-intervallet.

    Raises:
        ApiError:
            Hvis API'et returnerer en statuskode uden for 2xx-området
            eller hvis selve HTTP-kaldet fejler (fx netværksfejl).
    '''
    logger.info('Henter data fra API')
    payload = json.dumps({
        'entityType': 'Ticket',
        'page': 1,
        'pageSize': 1000,
        'columns': [
            'ReferenceNo',
            'BaseEntityStatus',
            'AgentGroup',
            'CreatedDate',
            'CloseDateTime',
            'Priority',
            'BaseAgent',
            'BaseEndUser',
            'BaseHeader',
            'u_Opstart',
            'u_Afslutning',
            'u_Opgavetype',
            'u_Omrder',
            'u_Afvisningsrsag',
            'UpdatedDate'],
        'filters': {
            'logic': 'and',
            'filters': [{
                'field': 'AgentGroup.GroupName',
                'operator': 'eq',
                'value': 'Digitalisering og Data'},
                {
                'field': 'UpdatedDate',
                'operator': 'gte',
                'value': timestamp}]
            }
        })
    headers = {
        'x-api-key': API_KEY,
        'Content-Type': 'application/json'}
    try:
        response = requests.post(
            API_URL, 
            headers=headers, 
            data=payload,
            timeout=30)
    except requests.exceptions.RequestException as exc:
        logger.error('Forbindelsesfejl ved API-kald: %s', exc, exc_info=True)
        raise ApiError('API request failed due to a connection error') from exc
    if response.ok:
        logger.info('Data hentet fra API')
        logger.debug('API-kald statuskode: %s', response.status_code)
    else:
        logger.error('API-kald fejlede (%s): %s', response.status_code, response.text)
        raise ApiError(f'API request failed with status {response.status_code}')
    return response