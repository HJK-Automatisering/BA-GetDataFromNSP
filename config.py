import os

from dotenv import load_dotenv

#######################################################################

load_dotenv()

# SQL-settings
DB_DRIVER = os.getenv('DB_DRIVER')
DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# API-settings
API_URL = os.getenv('API_URL')
API_KEY = os.getenv('API_KEY')

# Script settings
SCRIPT_RUNTIME = int(os.getenv('SCRIPT_RUNTIME', '3600'))
TIMEZONE = 'Europe/Copenhagen'
DATE_FORMAT = '%Y-%m-%d'
TIMESTAMP_FALLBACK = '2025-09-01T00:00:00Z'