import json
import logging
import os
import requests
from dotenv import load_dotenv

#######################################################################

load_dotenv()

def api_fetch():
    url = os.getenv('API_URL')
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
        'u_Afvisningsrsag'],
    'filters': {
        'logic': 'and',
        'filters': [{
            'field': 'AgentGroup.GroupName',
            'operator': 'eq',
            'value': 'Digitalisering og Data'},
            {
            'field': 'UpdatedDate',
            'operator': 'gte',
            'value': os.getenv('LAST_READ')}
            ]
        }
    })
    headers = {
        'x-api-key': os.getenv('API_KEY'),
        'Content-Type': 'application/json'}
    response = requests.request('POST', url, headers=headers, data=payload)
    logging.info('Data fetched')
    return response
