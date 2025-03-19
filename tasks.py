# tasks.py
import os
import shutil
import glob
import math
import time
from datetime import datetime
from config import LOCAL_FILE_PATH, FTP_SERVER, FTP_USER, FTP_PASSWORD, CSV_FILE_PATH
from db import execute_db_query, get_db_connection
import ftplib
import csv
import psycopg2

def create_backup():
    # Delete any existing backup files
    for backup in glob.glob("auctions_backup_*.sql"):
        os.remove(backup)
    # Create new backup file with timestamp using pg_dump (ensure pg_dump is in your PATH)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"auctions_backup_{timestamp}.sql"
    cmd = f"pg_dump -U {os.environ.get('PGUSER', '')} -h {os.environ.get('PGHOST', 'localhost')} {os.environ.get('PGDATABASE', '')} > {backup_file}"
    os.system(cmd)
    print(f"Database backup created: {backup_file}")

def download_data_csv(local_file_path=LOCAL_FILE_PATH):
    try:
        with ftplib.FTP(FTP_SERVER) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASSWORD)
            with open(local_file_path, 'wb') as local_file:
                ftp.retrbinary(f'RETR {CSV_FILE_PATH}', local_file.write)
    except ftplib.all_errors as e:
        print(f"FTP error: {e}")
        return False
    return True

# --- Functions for CSV parsing into a temporary table ---
def create_temp_table():
    query = '''
    CREATE TABLE IF NOT EXISTS temp_auctions (
        id SERIAL PRIMARY KEY,
        tecdoc_id TEXT,
        manufacturer TEXT,
        amount INTEGER,
        price NUMERIC,
        final_price NUMERIC,
        details TEXT,
        package_qty TEXT,
        extra_cost NUMERIC,
        length NUMERIC DEFAULT 0,
        height NUMERIC DEFAULT 0,
        width NUMERIC DEFAULT 0,
        weight NUMERIC DEFAULT 0,
        is_big INTEGER DEFAULT 0,
        ean TEXT,
        ilcode TEXT DEFAULT ''
    );
    '''
    execute_db_query(query)

def parse_csv_into_temp_db(csv_path, chunk_size=500):
    create_temp_table()
    conn = get_db_connection()
    cur = conn.cursor()
    total_rows = 0
    chunk = []
    print(f"Opening {csv_path} and loading into temp_auctions...")
    with open(csv_path, newline='', encoding='latin-1') as f:
        reader = csv.reader(f, delimiter=';')
        # next(reader, None)  # Skip header if needed
        for row in reader:
            total_rows += 1
            chunk.append(row)
            if len(chunk) == chunk_size:
                _insert_chunk(cur, chunk)
                conn.commit()
                chunk = []
        if chunk:
            _insert_chunk(cur, chunk)
            conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted total {total_rows} rows into temp_auctions.")

def _insert_chunk(cursor, chunk):
    data = []
    for row in chunk:
        if len(row) < 2:
            continue
        # Adjust indices according to your CSV (example: ilcode in index 3, amount in index 5)
        ilcode = row[3].strip() if len(row) > 3 else ''
        try:
            amount = int(float(row[5].strip())) if len(row) > 5 else 0
        except ValueError:
            amount = 0
        # Insert minimal data – extend with additional fields as needed
        data.append((None, None, amount, None, None, None, None, None, None, None, None, None, None, None, ilcode))
    if data:
        query = '''
        INSERT INTO temp_auctions (tecdoc_id, manufacturer, amount, price, final_price, details, 
                                     package_qty, extra_cost, length, height, width, weight, is_big, ean, ilcode)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        cursor.executemany(query, data)

def merge_temp_into_main(app, temp_table_name="temp_auctions", main_table="auctions"):
    conn = get_db_connection()
    cur = conn.cursor()
    # Load data from temp_auctions
    cur.execute(f"SELECT ilcode, amount FROM {temp_table_name}")
    temp_rows = cur.fetchall()
    temp_dict = {row[0]: row[1] for row in temp_rows}
    app.log_message(f"Loaded {len(temp_dict)} ilcodes from temporary table.")
    # Load main table data
    cur.execute(f"SELECT ilcode, amount, offer_id FROM {main_table}")
    main_rows = cur.fetchall()
    updates = []
    new_updates = []
    zero_out_missing = []
    processed_rows = 0
    total_rows = len(main_rows)
    status2_counter = 0
    for (ilcode, old_amount, offer_id) in main_rows:
        processed_rows += 1
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
            if status in ('0', '2') and offer_id and offer_id.strip() != "":
                updates.append((new_amount, status, ilcode))
            elif status == '1' and (not offer_id or offer_id.strip() == ""):
                new_updates.append((new_amount, status, ilcode))
            del temp_dict[ilcode]
        else:
            if old_amount > 0 and offer_id != "":
                zero_out_missing.append((0, '0', ilcode))
    if updates:
        query = """
            UPDATE auctions
            SET amount = %s, status = %s
            WHERE ilcode = %s;
        """
        cur.executemany(query, updates)
        conn.commit()
        app.log_message(f"Updated {len(updates)} records in auctions.")
    if new_updates:
        cur.executemany(query, new_updates)
        conn.commit()
        app.log_message(f"Updated {len(new_updates)} new records in auctions (status 1).")
    if zero_out_missing:
        query = "UPDATE auctions SET amount=0, status='0' WHERE ilcode=%s;"
        cur.executemany(query, [(t[2],) for t in zero_out_missing])
        conn.commit()
        app.log_message(f"Zeroed out {len(zero_out_missing)} records not found in temp table.")
    cur.close()
    conn.close()
    if os.path.exists("temp_auctions"):
        try:
            # For PostgreSQL, you might simply drop the table.
            drop_temp_table()
            app.log_message("Deleted temporary table.")
        except Exception as e:
            app.log_message(f"WARNING: Could not remove temporary table: {e}")
    app.log_message("✅ merge_temp_into_main completed (no new insert).")

def drop_temp_table():
    query = "DROP TABLE IF EXISTS temp_auctions;"
    execute_db_query(query)

def display_processing_summary(app):
    query_total = "SELECT COUNT(*) AS total FROM auctions;"
    query_status = "SELECT status, COUNT(*) AS count FROM auctions GROUP BY status;"
    total = fetchall_db(query_total)[0]['total']
    results = fetchall_db(query_status)
    counts = {str(row['status']): row['count'] for row in results}
    summary = [
        f"📄 Total Rows Processed: {total}",
        f"❌ Status 0 (Needs Deletion): {counts.get('0', 0)}",
        f"✅ Status 1 (Needs Creation): {counts.get('1', 0)}",
        f"🔄 Status 2 (Needs Update): {counts.get('2', 0)}",
        f"📦 Status 3 (No Change): {counts.get('3', 0)}"
    ]
    for line in summary:
        app.log_message(line)

# Additional functions (bulk_update_auctions, zero_missing_ilcodes, worker_insert_temp_db, etc.)
# should be refactored similarly using psycopg2 in place of sqlite3.
