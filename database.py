# database.py
import os
import glob
import shutil
import math
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, FTP_SERVER, FTP_USER, FTP_PASSWORD, CSV_FILE_PATH

os.environ["PGCLIENTENCODING"] = "UTF8"

def get_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        options="-c client_encoding=UTF8"
    )
    conn.set_client_encoding('UTF8')
    return conn

def execute_db_query(query, params=()):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
    finally:
        conn.close()

def fetch_all(query, params=()):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchall()
    finally:
        conn.close()

def create_auctions_table():
    query = """
    CREATE TABLE IF NOT EXISTS auctions (
        id SERIAL PRIMARY KEY,
        tecdoc_id TEXT,
        manufacturer TEXT,
        amount INTEGER,
        price REAL,
        final_price REAL,
        details TEXT,
        package_qty TEXT,
        offer_id TEXT,
        status TEXT,
        extra_cost REAL,
        length REAL DEFAULT 0,
        height REAL DEFAULT 0,
        width REAL DEFAULT 0,
        weight REAL DEFAULT 0,
        is_big INTEGER DEFAULT 0,
        ean TEXT,
        ilcode TEXT DEFAULT ''
    );
    """
    execute_db_query(query)

def update_combined_data_in_db(updated_item, status='3'):
    query = """
        UPDATE auctions
        SET offer_id = %s, status = %s, length = %s, height = %s, width = %s, weight = %s, is_big = %s
        WHERE tecdoc_id = %s AND ean = %s;
    """
    params = (
        updated_item['offer_id'], status, updated_item['length'], updated_item['height'],
        updated_item['width'], updated_item['weight'], updated_item['is_big'],
        updated_item['tecdoc_id'], updated_item['ean']
    )
    execute_db_query(query, params)

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

def remove_offer_id_from_db(offer_id):
    query = """
        UPDATE auctions
        SET offer_id = NULL, status = '1'
        WHERE offer_id = %s;
    """
    execute_db_query(query, (offer_id,))

def read_combined_data_from_db():
    query = """
    SELECT tecdoc_id, manufacturer, amount, price, final_price, details, package_qty, offer_id, status, length, height, width, weight, is_big, ean
    FROM auctions;
    """
    rows = fetch_all(query)
    return rows

def setup_database():
    create_auctions_table()
    # If you need to add missing columns (like in your ensure_column_exists),
    # you can query information_schema.columns and ALTER TABLE accordingly.
    # (For brevity, this example assumes the table is fully defined above.)

#############################
# TEMPORARY TABLE FOR AMOUNTS
#############################

def create_temp_amounts_table():
    # We create a persistent table that will serve as our temp table.
    execute_db_query("DROP TABLE IF EXISTS temp_amounts;")
    query = """
    CREATE TABLE temp_amounts (
        ilcode TEXT PRIMARY KEY,
        amount INTEGER
    );
    """
    execute_db_query(query)

def insert_into_temp_amounts(data):
    # data is a list of (ilcode, amount)
    query = """
    INSERT INTO temp_amounts (ilcode, amount)
    VALUES (%s, %s)
    ON CONFLICT (ilcode) DO UPDATE SET amount = EXCLUDED.amount;
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(query, data)
    finally:
        conn.close()

def perform_chunked_updates(updates, chunk_size=500):
    query = """
        UPDATE auctions
        SET amount = %s, status = %s
        WHERE ilcode = %s;
    """
    total = len(updates)
    for i in range(0, total, chunk_size):
        chunk = updates[i:i+chunk_size]
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.executemany(query, chunk)
        finally:
            conn.close()

def merge_temp_into_main(app, temp_table="temp_amounts"):
    # 1. Read temp_amounts into a dict
    temp_rows = fetch_all(f"SELECT ilcode, amount FROM {temp_table};")
    temp_dict = {row['ilcode']: row['amount'] for row in temp_rows}
    app.log_message(f"Loaded {len(temp_dict)} ilcodes from temporary table.")
    # 2. Read auctions data
    main_rows = fetch_all("SELECT ilcode, amount, offer_id FROM auctions;")
    updates = []
    new_updates = []
    zero_out_missing = []
    for row in main_rows:
        ilcode = row['ilcode']
        old_amount = row['amount'] if row['amount'] is not None else 0
        offer_id = row['offer_id'] if row['offer_id'] is not None else ""
        if ilcode in temp_dict:
            new_amount = temp_dict[ilcode]
            if old_amount == new_amount:
                status = '3'
            elif old_amount > 0 and new_amount == 0:
                status = '0'
            elif old_amount == 0 and new_amount > 0:
                status = '1'
            else:
                status = '2'
            if status in ('0', '2') and offer_id.strip() != "":
                updates.append((new_amount, status, ilcode))
            elif status == '1' and offer_id.strip() == "":
                new_updates.append((new_amount, status, ilcode))
            del temp_dict[ilcode]
        else:
            if old_amount > 0 and offer_id != "":
                zero_out_missing.append((0, '0', ilcode))
    if updates:
        perform_chunked_updates(updates)
        app.log_message(f"Updated {len(updates)} records in auctions.")
    if new_updates:
        perform_chunked_updates(new_updates)
        app.log_message(f"Updated {len(new_updates)} new records in auctions (status 1).")
    if zero_out_missing:
        query = "UPDATE auctions SET amount=%s, status=%s WHERE ilcode=%s;"
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.executemany(query, zero_out_missing)
        finally:
            conn.close()
        app.log_message(f"Zeroed out {len(zero_out_missing)} records not found in temp table.")
    execute_db_query(f"DROP TABLE IF EXISTS {temp_table};")
    app.log_message("merge_temp_into_main completed (no new insert).")

def get_auction_status_counts():
    data = fetch_all("SELECT status, COUNT(*) AS cnt FROM auctions GROUP BY status;")
    results = {row['status']: row['cnt'] for row in data}
    total = sum(results.values())
    for i in range(4):
        results[str(i)] = results.get(str(i), 0)
    return results

def create_backup():
    import os, glob
    from datetime import datetime
    # Delete any existing backup files
    for backup in glob.glob("auctions_backup_*.sql"):
        os.remove(backup)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"auctions_backup_{timestamp}.sql"
    # Specify the full path to pg_dump.exe (make sure the path is correct)
    pg_dump_path = r'"C:\Program Files\PostgreSQL\17\bin\pg_dump.exe"'
    os.environ["PGPASSWORD"] = os.getenv("DB_PASSWORD", "test12345")
    cmd = f"{pg_dump_path} -h {os.getenv('DB_HOST', 'localhost')} -p {os.getenv('DB_PORT', '5432')} -U {os.getenv('DB_USER', 'postgres')} -d {os.getenv('DB_NAME', 'apauctions')} -f {backup_file}"
    ret = os.system(cmd)
    if ret != 0:
        print("Error: pg_dump command failed.")
    else:
        print(f"Database backup created: {backup_file}")

def download_csv(local_file_path):
    import ftplib, socket
    try:
        with ftplib.FTP(FTP_SERVER) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASSWORD)
            with open(local_file_path, 'wb') as local_file:
                ftp.retrbinary(f'RETR {CSV_FILE_PATH}', local_file.write)
    except ftplib.all_errors as e:
        print(f"FTP error: {e}")
    except socket.gaierror as e:
        print(f"Network error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def get_all_ilcodes_from_db():
    data = fetch_all("SELECT DISTINCT ilcode FROM auctions;")
    return {row['ilcode'] for row in data}

#############################
# TEMP TABLE FOR CSV PROCESSING
#############################

def create_temp_table():
    execute_db_query("DROP TABLE IF EXISTS temp_auctions;")
    query = """
    CREATE TABLE temp_auctions (
        tecdoc_id TEXT,
        manufacturer TEXT,
        amount INTEGER,
        price REAL,
        final_price REAL,
        details TEXT,
        package_qty TEXT,
        extra_cost REAL,
        length REAL DEFAULT 0,
        height REAL DEFAULT 0,
        width REAL DEFAULT 0,
        weight REAL DEFAULT 0,
        is_big INTEGER DEFAULT 0,
        ean TEXT,
        ilcode TEXT DEFAULT ''
    );
    """
    execute_db_query(query)

def insert_into_temp_auctions(data):
    # data is list of tuples with 15 columns
    query = """
    INSERT INTO temp_auctions (tecdoc_id, manufacturer, amount, price, final_price, details, package_qty, extra_cost, length, height, width, weight, is_big, ean, ilcode)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(query, data)
    finally:
        conn.close()

def read_temp_data():
    query = """
    SELECT tecdoc_id, manufacturer, amount, price, final_price, details, package_qty, extra_cost, 
           length, height, width, weight, is_big, ean, ilcode
    FROM temp_auctions;
    """
    return fetch_all(query)

def cleanup_temp_database():
    execute_db_query("DROP TABLE IF EXISTS temp_auctions;")
