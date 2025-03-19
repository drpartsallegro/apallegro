# db.py
import psycopg2
from psycopg2.extras import RealDictCursor
from functools import wraps
import time
from config import DATABASE_CONFIG

RETRY_COUNT = 5
RETRY_DELAY = 1

def get_db_connection():
    conn = psycopg2.connect(
        dbname=DATABASE_CONFIG['dbname'],
        user=DATABASE_CONFIG['user'],
        password=DATABASE_CONFIG['password'],
        host=DATABASE_CONFIG['host'],
        port=DATABASE_CONFIG['port']
    )
    return conn

def retry_on_lock(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        for attempt in range(RETRY_COUNT):
            try:
                return func(*args, **kwargs)
            except psycopg2.OperationalError as e:
                if 'lock' in str(e).lower():
                    time.sleep(RETRY_DELAY)
                else:
                    raise
        raise psycopg2.OperationalError("Database is locked after multiple attempts")
    return wrapper

def execute_db_query(query, params=None):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def fetchall_db(query, params=None):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(query, params)
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()
        conn.close()

@retry_on_lock
def read_combined_data_from_db():
    query = """
        SELECT tecdoc_id, manufacturer, amount, price, final_price, details, package_qty, offer_id, status, 
               length, height, width, weight, is_big, ean, ilcode
        FROM auctions;
    """
    rows = fetchall_db(query)
    # Return a list of dictionaries as in the original code.
    return [dict(row) for row in rows]

@retry_on_lock
def update_combined_data_in_db(updated_item, status='3'):
    query = """
        UPDATE auctions
        SET offer_id = %s, status = %s, length = %s, height = %s, width = %s, weight = %s, is_big = %s
        WHERE tecdoc_id = %s AND ean = %s;
    """
    params = (
        updated_item.get('offer_id'),
        status,
        updated_item.get('length'),
        updated_item.get('height'),
        updated_item.get('width'),
        updated_item.get('weight'),
        updated_item.get('is_big'),
        updated_item.get('tecdoc_id'),
        updated_item.get('ean')
    )
    execute_db_query(query, params)

@retry_on_lock
def save_offer_id_to_db(tecdoc_id, offer_id, ean):
    if offer_id is None:
        offer_id = ""
    query = """
        UPDATE auctions
        SET offer_id = %s
        WHERE tecdoc_id = %s AND ean = %s;
    """
    params = (offer_id, tecdoc_id, ean)
    execute_db_query(query, params)

@retry_on_lock
def remove_offer_id_from_db(offer_id):
    query = """
        UPDATE auctions
        SET offer_id = NULL, status = '1'
        WHERE offer_id = %s;
    """
    params = (offer_id,)
    execute_db_query(query, params)

def ensure_column_exists(cursor, table_name, column_name, column_type):
    # Check if column exists using information_schema
    query = """
        SELECT column_name FROM information_schema.columns 
        WHERE table_name=%s AND column_name=%s;
    """
    cursor.execute(query, (table_name, column_name))
    exists = cursor.fetchone()
    if not exists:
        alter_query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};"
        cursor.execute(alter_query)
