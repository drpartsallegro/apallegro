# config.py
# PostgreSQL database configuration
DATABASE_CONFIG = {
    'dbname': 'apauctions',         # Replace with your database name
    'user': 'your_db_user',       # Replace with your PostgreSQL username
    'password': 'your_db_password',  # Replace with your PostgreSQL password
    'host': 'localhost',
    'port': '5432'
}

# FTP server details
FTP_SERVER = 'ff'
FTP_USER = '426'
FTP_PASSWORD = 'zEXZLN7xl*&^'
CSV_FILE_PATH = '426_ce.csv'
LOCAL_FILE_PATH = '426_ce.csv'

# Allegro.pl API details
ALLEGRO_API_URL = 'https://api.allegro.pl'
ALLEGRO_API_KEY = '09dacff'
ALLEGRO_CLIENT_ID = '09dacff4bfab8b8b9385d06a0e2'
ALLEGRO_CLIENT_SECRET = 'cBDuSM144bWn8ifffek1zXV2qn5ex9vGLyX0CGRbTbccXwE2'
ACCESS_TOKEN_FILE = 'access_token.json'

# Placeholder image URL
PLACEHOLDER_IMAGE_URL = 'https://a.allegroimg.com/original/118b23/ee762f364313a256352b897db343'

# Time zone (if needed)
TIMEZONE = 'Europe/Warsaw'
