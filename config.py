# config.py
import os
from pytz import timezone

# FTP server details
FTP_SERVER = 'ftp3.interparts.pl'
FTP_USER = '426'
FTP_PASSWORD = 'zEXZLN7xl*&^'
CSV_FILE_PATH = '426_ce.csv'
LOCAL_FILE_PATH = '426_ce.csv'

# Allegro.pl API details
ALLEGRO_API_URL = 'https://api.allegro.pl'
ALLEGRO_API_KEY = '09dac701707948b8b9385d06a0e2'
ALLEGRO_CLIENT_ID = '09dac70170794b385d06a0e2'
ALLEGRO_CLIENT_SECRET = 'cBDuSM144bWn8iex9vGLyX0CGRbTbccXwE2'
ACCESS_TOKEN_FILE = 'access_token.json'

# Placeholder image URL
PLACEHOLDER_IMAGE_URL = 'https://a.allegroimg.com/original/118b23/ee762f364313a256352b897db343'

# Polish time zone
POLISH_TZ = timezone('Europe/Warsaw')

# PostgreSQL database details
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'apauctions'
DB_USER = 'postgres'
DB_PASSWORD = 'test12345'

# Logging configuration
LOG_DIR = 'logs'
DB_PROCESS_LOG_FILE = 'dbProcessLogs.log'
PARAMETERS_LOG_FILE = 'parametersLogs.log'
PRODUCT_LOG_FILE = 'productLogs.log'
