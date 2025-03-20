#!/usr/bin/env python3
# main.py
import csv
import sys
import ftplib
import requests
import schedule
import time
import json
import os
import socket
import webbrowser
import threading
import random
import math
import shutil
import glob
import re
import ctypes
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Queue, cpu_count, Value, Manager
from queue import Empty
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from datetime import datetime
from pytz import timezone
import logging

from config import (FTP_SERVER, FTP_USER, FTP_PASSWORD, CSV_FILE_PATH, LOCAL_FILE_PATH,
                    ALLEGRO_API_URL, ALLEGRO_API_KEY, ALLEGRO_CLIENT_ID, ALLEGRO_CLIENT_SECRET, ACCESS_TOKEN_FILE,
                    PLACEHOLDER_IMAGE_URL, POLISH_TZ, LOG_DIR)
import database
import utils
import allegro
from utils import calculate_margin, normalize_unicode, replace_special_characters

# Global Allegro access token is handled in allegro.py
# Setup logging
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
db_process_logger = logging.getLogger('dbProcessLogger')
parameters_logger = logging.getLogger('parametersLogger')
product_logger = logging.getLogger('productLogger')

db_process_handler = logging.FileHandler(os.path.join(LOG_DIR, 'dbProcessLogs.log'))
db_process_handler.setLevel(logging.INFO)
db_process_formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
db_process_handler.setFormatter(db_process_formatter)
db_process_logger.addHandler(db_process_handler)

parameters_handler = logging.FileHandler(os.path.join(LOG_DIR, 'parametersLogs.log'))
parameters_handler.setLevel(logging.INFO)
parameters_formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
parameters_handler.setFormatter(parameters_formatter)
parameters_logger.addHandler(parameters_handler)

product_handler = logging.FileHandler(os.path.join(LOG_DIR, 'productLogs.log'))
product_handler.setLevel(logging.ERROR)
product_formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
product_handler.setFormatter(product_formatter)
product_logger.addHandler(product_handler)

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s %(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Application class with Tkinter UI
class Application(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Allegro Auction Manager")
        self.geometry("1024x800")
        self.configure(bg="#f0f0f0")
        style = ttk.Style()
        style.configure("TFrame", background="#f0f0f0")
        style.configure("TLabel", background="#f0f0f0", font=("Helvetica", 10))
        style.configure("TButton", font=("Helvetica", 10))
        style.configure("TNotebook", background="#f0f0f0")
        style.configure("TNotebook.Tab", font=("Helvetica", 10))
        style.configure("TProgressbar", thickness=20)
        self.tab_control = ttk.Notebook(self)
        self.log_tab = ttk.Frame(self.tab_control)
        self.tab_control.add(self.log_tab, text='Log')
        self.tab_control.pack(expand=1, fill="both")
        self.log_text = scrolledtext.ScrolledText(self.log_tab, height=30, width=150, wrap=tk.WORD, font=("Helvetica", 10))
        self.log_text.pack()
        self.progress_bar = ttk.Progressbar(self, mode='determinate')
        self.progress_bar.pack(fill=tk.X, pady=5)
        self.start_button = ttk.Button(self, text="Start", command=self.start_process, width=20)
        self.start_button.pack(pady=10)
        self.update_amounts_button = ttk.Button(self, text="Update Product Amounts", command=self.update_amounts_button_action, width=20)
        self.update_amounts_button.pack(pady=10)
        self.add_edit_button = ttk.Button(self, text="Add/Edit Auctions", command=self.add_edit_auctions, width=20)
        self.add_edit_button.pack(pady=10)
        self.stop_button = ttk.Button(self, text="Stop Auctions", command=self.stop_all_active_auctions, width=20)
        self.stop_button.pack_forget()
        self.remove_2szt_button = ttk.Button(self, text="Remove 2 szt", command=self.open_remove_2szt_popup, width=20)
        self.remove_2szt_button.pack_forget()
        self.find_duplicates_button = ttk.Button(self, text="Find Duplicates", command=self.find_duplicates, width=20)
        self.find_duplicates_button.pack_forget()
        self.delete_inactive_button = ttk.Button(self, text="Delete Inactive", command=self.confirm_delete_inactive, width=20)
        self.delete_inactive_button.pack(pady=10)
        self.creation_errors = []
        self.update_errors = []
        self.deletion_errors = []
        self.multiple_products_count = 0
        database.setup_database()
        self.log_queue = Queue()
        self.log_thread = threading.Thread(target=self.log_listener, daemon=True)
        self.log_thread.start()
    
    def confirm_delete_inactive(self):
        if messagebox.askokcancel("Delete Inactive Offers", "Are you sure you want to delete all inactive offers?"):
            self.delete_inactive_offers()

    def delete_inactive_offers(self):
        self.toggle_buttons('disabled')
        self.log_message("Deleting all inactive offers... (dummy implementation)")
        # Here you can add the actual logic (or a Thread that calls run_delete_inactive_offers)
        # For now, we simply re-enable the buttons.
        self.toggle_buttons('normal')

    def log_message(self, message):
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)

    def toggle_buttons(self, state):
        self.start_button.config(state=state)
        self.stop_button.config(state=state)
        self.add_edit_button.config(state=state)
        self.update_amounts_button.config(state=state)

    def start_process(self):
        self.toggle_buttons('disabled')
        self.progress_bar.start()
        threading.Thread(target=self.run_job).start()

    def run_job(self):
        self.log_message("Starting the job...")
        self.progress_bar.start()
        try:
            database.download_csv(LOCAL_FILE_PATH)
            self.log_message("Downloaded files")
            new_data = process_csv(self, LOCAL_FILE_PATH)
            self.log_message("Job finished successfully.")
        except Exception as e:
            error_message = f"Error in run_job: {str(e)}"
            self.log_message(error_message)
            logging.error(error_message)
        finally:
            self.progress_bar.stop()
            self.toggle_buttons('normal')

    def update_amounts_button_action(self):
        threading.Thread(target=self.run_update_amounts).start()

    def run_update_amounts(self):
        self.toggle_buttons('disabled')
        self.log_message("Creating database backup...")
        database.create_backup()
        self.log_message("Downloading product data file...")
        if not database.download_csv(LOCAL_FILE_PATH):
            self.log_message("Failed to download data file.")
            self.toggle_buttons('normal')
            return
        self.log_message("Creating temporary amounts table and loading CSV...")
        database.create_temp_amounts_table()
        data = parse_amounts_csv(LOCAL_FILE_PATH)
        database.insert_into_temp_amounts(data)
        self.log_message("Merging temporary amounts into auctions...")
        database.merge_temp_into_main(self)
        self.log_message("Generating processing summary...")
        display_processing_summary(self)
        self.log_message("Finished updating amounts.")
        self.toggle_buttons('normal')

    def add_edit_auctions(self):
        self.toggle_buttons('disabled')
        self.log_message("Adding/Editing auctions...")
        threading.Thread(target=self.run_add_edit_auctions).start()

    def run_add_edit_auctions(self):
        from allegro import check_and_get_access_token, fetch_active_auctions, create_or_update_auction, delete_auction, update_auction
        if not check_and_get_access_token(self):
            self.log_message("Device authentication failed. Exiting...")
            self.toggle_buttons('normal')
            return
        combined_data = read_combined_data_from_db()
        if not combined_data:
            self.log_message("No parsed data available to process auctions.")
            self.toggle_buttons('normal')
            return
        headers = {
            'Authorization': f'Bearer {allegro.ACCESS_TOKEN}',
            'Accept': 'application/vnd.allegro.public.v1+json'
        }
        active_auctions = fetch_active_auctions(self, headers)
        total_tasks = sum(1 for item in combined_data if item['status'] in {'0', '1', '2'})
        self.progress_bar['maximum'] = total_tasks
        self.progress_bar['value'] = 0
        def process_auctions_by_status(status, process_function):
            utils.prevent_sleep()
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for item in [i for i in combined_data if i['status'] == status]:
                    self.log_message(f"Processing auction for TecDoc ID {item['tecdoc_id']} with status {status}...")
                    futures.append(executor.submit(process_function, item))
                for future in futures:
                    future.result()
                    self.progress_bar['value'] += 1
                    self.progress_bar.update()
        process_auctions_by_status('0', self.process_deletion)
        process_auctions_by_status('2', self.process_update)
        process_auctions_by_status('1', self.process_creation)
        utils.allow_sleep()
        self.log_message("Finished adding/editing auctions.")
        self.toggle_buttons('normal')

    def stop_all_active_auctions(self):
        self.toggle_buttons('disabled')
        self.log_message("Stopping all active auctions...")
        threading.Thread(target=self.run_stop_all_active_auctions).start()

    def run_stop_all_active_auctions(self):
        from allegro import stop_all_active_auctions
        stop_all_active_auctions(self)
        self.log_message("Finished stopping auctions.")
        self.toggle_buttons('normal')

    def open_remove_2szt_popup(self):
        popup = tk.Toplevel(self)
        popup.title("Enter Product Name, Category ID, and Predefined Message")
        popup.geometry("800x800")
        label_product_name = ttk.Label(popup, text="Product Name:")
        label_product_name.pack(padx=10, pady=5)
        product_name_entry = ttk.Entry(popup, width=50)
        product_name_entry.pack(padx=10, pady=5)
        label_category_id = ttk.Label(popup, text="Category ID:")
        label_category_id.pack(padx=10, pady=5)
        category_id_entry = ttk.Entry(popup, width=50)
        category_id_entry.pack(padx=10, pady=5)
        label_predefined_message = ttk.Label(popup, text="Predefined Message:")
        label_predefined_message.pack(padx=10, pady=5)
        predefined_message_entry = scrolledtext.ScrolledText(popup, height=10, width=50)
        predefined_message_entry.pack(padx=10, pady=5)
        label_keywords = ttk.Label(popup, text="Keywords (separated by ';'):")
        label_keywords.pack(padx=10, pady=5)
        keywords_entry = ttk.Entry(popup, width=50)
        keywords_entry.pack(padx=10, pady=5)
        def on_confirm():
            product_name = product_name_entry.get().strip()
            category_id = category_id_entry.get().strip()
            predefined_message = predefined_message_entry.get("1.0", tk.END).strip()
            keywords = keywords_entry.get().strip()
            if product_name and category_id and predefined_message and keywords:
                self.predefined_message = f"<p><b>{predefined_message}</b></p>"
                self.keywords = keywords.split(";")
                self.remove_2szt_process(product_name, category_id)
                popup.destroy()
            else:
                messagebox.showwarning("Input Error", "Please fill in all fields.")
        confirm_button = ttk.Button(popup, text="Confirm", command=on_confirm)
        confirm_button.pack(pady=10)
        popup.transient(self)
        popup.grab_set()
        self.wait_window(popup)

    def remove_2szt_process(self, product_name, category_id):
        if not hasattr(self, 'predefined_message') or not self.predefined_message:
            messagebox.showwarning("Missing Predefined Message", "Please enter a predefined message first.")
            return
        threading.Thread(target=self.run_remove_2szt, args=(product_name, category_id)).start()

    def run_remove_2szt(self, product_name, category_id):
        from allegro import main_sequence
        self.toggle_buttons('disabled')
        main_sequence(self, product_name, category_id)
        self.toggle_buttons('normal')

    def find_duplicates(self):
        self.toggle_buttons('disabled')
        self.log_message("Finding and deactivating duplicate auctions...")
        threading.Thread(target=self.run_find_duplicates).start()

    def run_find_duplicates(self):
        from allegro import fetch_active_auctions, deactivate_auction
        if not allegro.check_and_get_access_token(self):
            self.log_message("Device authentication failed. Exiting...")
            self.toggle_buttons('normal')
            return
        headers = {
            'Authorization': f'Bearer {allegro.ACCESS_TOKEN}',
            'Accept': 'application/vnd.allegro.public.v1+json'
        }
        active_auctions = fetch_active_auctions(self, headers)
        name_counts = {}
        for auction in active_auctions:
            name = auction.get('name')
            name_counts.setdefault(name, []).append(auction)
        potential_duplicates = {name: auctions for name, auctions in name_counts.items() if len(auctions) > 1}
        duplicates = []
        def get_product_id(offer_id):
            try:
                response = requests.get(f'{ALLEGRO_API_URL}/sale/product-offers/{offer_id}', headers=headers)
                response.raise_for_status()
                product_set = response.json().get('productSet', [])
                if product_set:
                    return product_set[0]['product']['id']
                else:
                    return None
            except requests.exceptions.HTTPError as e:
                self.log_message(f"Failed to fetch product ID for offer ID {offer_id}: {str(e)}")
                return None
        for name, auctions in potential_duplicates.items():
            self.log_message(f"Processing auctions with name {name}")
            product_ids = {}
            for auction in auctions:
                offer_id = auction['id']
                pid = get_product_id(offer_id)
                if pid:
                    if pid in product_ids:
                        duplicates.append(auction)
                        if product_ids[pid] not in duplicates:
                            duplicates.append(product_ids[pid])
                    else:
                        product_ids[pid] = auction
                else:
                    self.log_message(f"Product ID not found for offer ID {offer_id}")
        if not duplicates:
            self.log_message("No duplicates found.")
        else:
            with open('duplicates', 'w') as file:
                for auction in duplicates:
                    offer_id = auction['id']
                    product_name = auction.get('name', '')
                    price = auction.get('sellingMode', {}).get('price', {}).get('amount', '')
                    amount = auction.get('stock', {}).get('available', '')
                    file.write(f"{offer_id} ; {product_name} ; {price} ; {amount}\n")
                    if deactivate_auction(self, offer_id):
                        self.log_message(f"Deactivated duplicate auction with ID {offer_id}")
                    else:
                        self.deletion_errors.append((offer_id, "Error deactivating auction"))
        self.log_message("Finished finding and deactivating duplicate auctions.")
        self.toggle_buttons('normal')

    def process_creation(self, item):
        from allegro import create_or_update_auction
        if not item['ean']:
            self.log_message(f"Skipping auction creation for TecDoc ID {item['tecdoc_id']} due to missing EAN.")
            return {'type': 'error'}
        if not re.match(r'^\d+$', item['ean']):
            self.log_message(f"Skipping auction creation for TecDoc ID {item['tecdoc_id']} due to invalid EAN.")
            return {'type': 'error'}
        product_id, product_image, multiple_products_found = fetch_product_id(self, item['tecdoc_id'], item['ean'], item['manufacturer'], item['details'])
        if product_id is None:
            self.log_message(f"Skipping auction creation for EAN {item['ean']} due to missing product ID.")
            update_combined_data_in_db(item, status='3')
            return {'type': 'error'}
        offer_id = create_or_update_auction(self, product_id, item, draft=multiple_products_found)
        if offer_id:
            item['offer_id'] = offer_id
            update_combined_data_in_db(item, status='3')
            if multiple_products_found:
                self.multiple_products_count += 1
            return {'type': 'created'}
        else:
            self.creation_errors.append((item['tecdoc_id'], "Error creating auction"))
            return {'type': 'error'}

    def process_deletion(self, item):
        from allegro import delete_auction
        offer_id = item['offer_id']
        if offer_id:
            if delete_auction(self, offer_id):
                item['offer_id'] = ''
                item['status'] = '3'
                update_combined_data_in_db(item, status='3')
                return {'type': 'removed'}
            else:
                self.deletion_errors.append((item['tecdoc_id'], "Error deleting auction"))
                return {'type': 'error'}
        else:
            self.log_message(f"No offer ID found for TecDoc ID {item['tecdoc_id']}. Skipping deletion.")
            return {'type': 'error'}

    def process_update(self, item):
        from allegro import update_auction
        offer_id = item['offer_id']
        if offer_id:
            if update_auction(self, offer_id, item):
                item['status'] = '3'
                update_combined_data_in_db(item, status='3')
                return {'type': 'updated'}
            else:
                self.update_errors.append((item['tecdoc_id'], "Error updating auction"))
                return {'type': 'error'}
        else:
            self.log_message(f"No offer ID found for TecDoc ID {item['tecdoc_id']}. Skipping update.")
            return {'type': 'error'}

    def log_listener(self):
        while True:
            try:
                log_msg = self.log_queue.get()
                if log_msg is None:
                    break
                self.after(0, self.log_message, log_msg)
            except Empty:
                continue

# Dummy implementations for CSV processing and comparing/updating data
def parse_amounts_csv(file_path):
    data = []
    with open(file_path, newline='', encoding='latin-1') as f:
        reader = csv.reader(f, delimiter=';')
        for row in reader:
            if len(row) < 6:
                continue
            ilcode = row[3].strip()
            try:
                amount = int(float(row[5].strip()))
            except ValueError:
                amount = 0
            data.append((ilcode, amount))
    return data

def display_processing_summary(app):
    counts = database.get_auction_status_counts()
    total = sum(counts.values())
    app.log_message("Processing Summary:")
    app.log_message(f"Total Rows Processed: {total}")
    for status in range(4):
        app.log_message(f"Status {status} Auctions: {counts.get(str(status), 0)}")

def read_combined_data_from_db():
    return database.fetch_all("SELECT tecdoc_id, manufacturer, amount, price, final_price, details, package_qty, offer_id, status, length, height, width, weight, is_big, ean FROM auctions;")

def process_csv(app, file_path):
    # Instead of reading a local SQLite file, call the PostgreSQL backup routine.
    try:
        database.create_backup()
        app.log_message("Database backup created successfully.")
    except Exception as e:
        app.log_message(f"Backup creation failed: {str(e)}")
    
    app.log_message("Parsing CSV file...")
    # [Rest of your CSV processing code follows...]
    from multiprocessing import Queue, Value, cpu_count, Process
    queue = Queue()
    log_queue = Queue()
    num_workers = cpu_count() - 1
    total_products = Value('i', 0)
    processes = []
    max_products = 1370000  # adjust if needed
    for _ in range(num_workers):
        p = Process(target=worker, args=(queue, total_products, max_products, log_queue))
        p.start()
        processes.append(p)
    database.create_temp_table()
    log_thread = threading.Thread(target=log_listener, args=(app, log_queue))
    log_thread.start()
    start_time = datetime.now()
    chunk_size = 500
    chunk = []
    chunk_index = 0
    with open(file_path, newline='', encoding='latin-1') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=';')
        next(csvreader)  # Skip header
        for row in csvreader:
            if total_products.value >= max_products:
                break
            chunk.append(row)
            if len(chunk) == chunk_size:
                queue.put((chunk, chunk_index))
                chunk = []
                chunk_index += 1
        if chunk and total_products.value < max_products:
            queue.put((chunk, chunk_index))
    for _ in range(num_workers):
        queue.put(None)
    app.progress_bar['maximum'] = max_products
    app.progress_bar['value'] = 0
    while any(p.is_alive() for p in processes):
        app.progress_bar['value'] = total_products.value
        progress_percentage = (total_products.value / max_products) * 100
        app.log_message(f"Progress: {progress_percentage:.2f}%")
        app.update_idletasks()
        time.sleep(0.1)
    for p in processes:
        p.join()
    end_time = datetime.now()
    duration = end_time - start_time
    app.log_message(f"CSV processing completed in {duration.total_seconds()} seconds.")
    app.log_message(f"Total products parsed: {total_products.value}")
    data = database.read_temp_data()
    database.cleanup_temp_database()
    log_queue.put(None)
    log_thread.join()
    existing_data = read_combined_data_from_db()
    app.log_message("Starting compare_and_update_data process...")
    try:
        compare_and_update_data(app, data, existing_data)
    except Exception as e:
        app.log_message(f"Error in compare_and_update_data: {str(e)}")
    return data

def worker(queue, total_products, max_products, log_queue):
    """
    Worker process that retrieves a chunk from the queue and processes it.
    """
    while True:
        try:
            chunk_data = queue.get(timeout=30)
            if chunk_data is None:
                break
            chunk, chunk_index = chunk_data
            parse_csv_chunk(chunk, chunk_index, total_products, max_products, log_queue)
        except Empty:
            continue

def parse_csv_chunk(chunk, chunk_index, total_products, max_products, log_queue):
    """
    Parses a single chunk of CSV rows.
    """
    BATCH_SIZE = 500
    data = []
    parsed_items = 0
    log_queue.put(f"Processing chunk {chunk_index} with {len(chunk)} rows.")
    for row in chunk:
        if total_products.value >= max_products:
            break
        if len(row) >= 20:
            ean = row[18].strip() if len(row) > 18 else ""
            ilcode = row[19].strip() if len(row) > 19 else ""
            from utils import is_valid_ean, calculate_margin, normalize_unicode, replace_special_characters
            if not is_valid_ean(ean):
                continue
            tecdoc_id = row[9].strip()
            if not tecdoc_id:
                continue
            try:
                amount = int(float(row[2].strip()))
                price = round(float(row[11].strip().replace(',', '.')), 2)
                margin = calculate_margin(price)
                final_price = round(price * 1.23 * margin, 2)
                extra_cost = round(float(row[16].strip().replace(',', '.')) if row[16].strip() else 0.0, 2)
                final_price += extra_cost
                length = float(row[3].strip().replace(',', '.')) if row[3].strip() else 0
                height = float(row[4].strip().replace(',', '.')) if row[4].strip() else 0
                width = float(row[5].strip().replace(',', '.')) if row[5].strip() else 0
                weight = float(row[17].strip().replace(',', '.')) if row[17].strip() else 0
                is_big = 2 if weight > 31 or length > 150 or width > 150 or height > 150 else 1
                details = replace_special_characters(normalize_unicode(row[10].strip()))
                package_qty = row[14].strip()
                data.append((tecdoc_id, row[1].strip(), amount, price, final_price, details, package_qty, extra_cost,
                             length, height, width, weight, is_big, ean, ilcode))
                parsed_items += 1
                if parsed_items % BATCH_SIZE == 0:
                    database.insert_into_temp_auctions(data)
                    data = []
            except Exception as e:
                logging.error(f"Error parsing row: {e}")
    if data:
        database.insert_into_temp_auctions(data)
    with total_products.get_lock():
        total_products.value += parsed_items
    log_queue.put(f"Finished processing chunk {chunk_index}. Total products processed: {total_products.value}")
    logging.info(f"Finished processing chunk {chunk_index}. Total products processed: {total_products.value}")

def log_listener(app, log_queue):
    """
    Listens for log messages from worker processes and passes them to the app's log.
    """
    while True:
        msg = log_queue.get()
        if msg is None:
            break
        app.log_message(msg)

def compare_and_update_data(app, new_data, existing_data):
    try:
        if new_data is None:
            return []

        if not hasattr(app, 'log_message'):
            raise AttributeError("'app' object has no attribute 'log_message'")

        # Use combination of tecdoc_id and ean as the key for data comparison
        new_dict = {(item['tecdoc_id'], item['ean']): item for item in new_data}
        existing_dict = {(item['tecdoc_id'], item['ean']): item for item in existing_data}

        total_items = len(existing_dict) + len(set(new_dict) - set(existing_dict))
        app.progress_bar['maximum'] = total_items

        processed_items = 0
        updated_data = []

        for key, new_item in new_dict.items():
            tecdoc_id, new_ean = key
            new_ean = new_ean or ''
            new_length = float(new_item.get('length') or 0)
            new_height = float(new_item.get('height') or 0)
            new_width = float(new_item.get('width') or 0)
            new_weight = float(new_item.get('weight') or 0)
            new_amount = int(new_item.get('amount') or 0)
            new_extra_cost = round(float(new_item.get('extra_cost') or 0), 2) * 1.12 * 1.23
            new_price = round(float(new_item.get('price') or 0), 2)
            margin = calculate_margin(new_price)
            new_final_price = round((new_price * 1.23 * margin) + new_extra_cost, 2)  # Ensure final_price includes extra_cost
            new_ilcode = new_item.get('ilcode', '')

            if new_weight > 31 or new_length > 150 or new_width > 150 or new_height > 150:
                new_is_big = 2
            else:
                new_is_big = 1 if ((new_length > 41 and new_width > 70) or
                                   (new_height > 38 and new_width > 70) or
                                   (new_length > 41 and new_height > 38) or
                                   (new_length > 70) or
                                   (new_width > 70) or
                                   (new_height > 38) or
                                   (new_length + new_height + new_width > 150) or
                                   new_weight > 25 or
                                   (new_height == 0 and new_length == 0 and new_width == 0) or
                                   new_weight == 0) else 0

            new_item['is_big'] = new_is_big

            # Fetch the old item using (tecdoc_id, ean) combination
            old_item = existing_dict.get((tecdoc_id, new_ean))

            if old_item is None:
                # This is a new item, not found in existing_dict
                if new_amount > 0:
                    new_item['status'] = '1'  # Mark as a new item with stock
                else:
                    new_item['status'] = '3'  # Mark as a new item without stock
                updated_data.append(new_item)
                db_process_logger.info(f"New item found: Tecdoc: {tecdoc_id}, EAN: {new_ean} New amount {new_amount}, New price {new_final_price}, Status {new_item['status']}")
                app.log_message(f"New item found: Tecdoc: {tecdoc_id}, EAN: {new_ean} New amount {new_amount}, New price {new_final_price}, Status {new_item['status']}")
            else:
                # Existing item, update logic
                old_length = float(old_item.get('length') or 0)
                old_height = float(old_item.get('height') or 0)
                old_width = float(old_item.get('width') or 0)
                old_weight = float(old_item.get('weight') or 0)
                old_amount = int(old_item.get('amount') or 0)
                old_price = round(float(old_item.get('price') or 0), 2)
                margin = calculate_margin(old_price)
                old_final_price = round(float(old_price * 1.23 * margin), 2)
                old_is_big = int(old_item.get('is_big') or 0)
                old_extra_cost = round(float(old_item.get('extra_cost') or 0), 2)

                old_details = replace_special_characters(normalize_unicode(old_item.get('details', '')))
                old_package_qty = replace_special_characters(normalize_unicode(old_item.get('package_qty', '')))
                new_details = replace_special_characters(normalize_unicode(new_item.get('details', '')))
                new_package_qty = replace_special_characters(normalize_unicode(new_item.get('package_qty', '')))

                if new_amount == 0 and old_amount > 0 and old_item.get('offer_id'):
                    old_item['status'] = '0'
                    db_process_logger.info(f"Status 0: Tecdoc: {tecdoc_id}, EAN: {new_ean} New amount {new_amount}, Old amount: {old_amount}, New price {new_final_price}, Old price {old_final_price}, Offer ID: {old_item.get('offer_id')}")
                    app.log_message(f"Status 0: Tecdoc: {tecdoc_id}, EAN: {new_ean} New amount {new_amount}, Old amount: {old_amount}, New price {new_final_price}, Old price {old_final_price}, Offer ID: {old_item.get('offer_id')}")
                elif old_amount == 0 and new_amount > 0 and not old_item.get('offer_id'):
                    old_item['status'] = '1'
                    db_process_logger.info(f"Status 1: Tecdoc: {tecdoc_id}, EAN: {new_ean} New amount {new_amount}, Old amount: {old_amount}, New price {new_final_price}, Old price {old_final_price}, Offer ID: {old_item.get('offer_id')}")
                    app.log_message(f"Status 1: Tecdoc: {tecdoc_id}, EAN: {new_ean} New amount {new_amount}, Old amount: {old_amount}, New price {new_final_price}, Old price {old_final_price}, Offer ID: {old_item.get('offer_id')}")
                elif (old_amount > 0 and new_amount > 0 and old_item.get('offer_id') and
                      (new_price != old_price or new_amount != old_amount or new_details != old_details or
                       new_package_qty != old_package_qty or new_length != old_length or new_height != old_height or
                       new_width != old_width or new_weight != old_weight or new_is_big != old_is_big)):
                    old_item['status'] = '2'
                    db_process_logger.info(f"Status 2: Tecdoc: {tecdoc_id}, EAN: {new_ean} New amount {new_amount}, Old amount: {old_amount}, New price {new_final_price}, Old price {old_final_price}, Offer ID: {old_item.get('offer_id')}")
                    app.log_message(f"Status 2: Tecdoc: {tecdoc_id}, EAN: {new_ean} New amount {new_amount}, Old amount: {old_amount}, New price {new_final_price}, Old price {old_final_price}, Offer ID: {old_item.get('offer_id')}")
                else:
                    old_item['status'] = '3'

                if 0 < new_final_price < 1 and new_amount > 0 and not old_item.get('offer_id'):
                    if old_final_price == 1:
                        old_item['status'] = '3'
                    elif new_final_price == 0:
                        old_item['status'] = '3'
                    else:
                        new_final_price = 1
                        old_item['status'] = '1'
                        app.log_message(f"Setting final price to 1 for TecDoc ID {tecdoc_id}, EAN: {new_ean}")

                if new_final_price is not None and (new_final_price < (new_price * 1.23 * 1.13) or new_final_price < (old_price * 1.23 * 1.13)) and new_amount > 0:
                    old_item['status'] = '6'
                    app.log_message(f"PRICE DANGER: TECDOCID {tecdoc_id}, EAN: {new_ean} New final price {new_final_price}, New price {new_price}, Old price {old_price}")
                    db_process_logger.info(f"PRICE DANGER: TECDOCID: {tecdoc_id}, EAN: {new_ean} New amount {new_amount}, Old amount: {old_amount}, New price {new_final_price}, Old price {old_final_price}, Offer ID: {old_item.get('offer_id')}")

                if old_final_price > 9999 or new_final_price > 9999:
                    old_item['status'] = '3'

                # Update the old_item values
                old_item['price'] = new_price
                old_item['final_price'] = new_final_price
                old_item['amount'] = new_amount
                old_item['details'] = new_details
                old_item['package_qty'] = new_package_qty
                old_item['length'] = new_length
                old_item['height'] = new_height
                old_item['width'] = new_width
                old_item['weight'] = new_weight
                old_item['is_big'] = new_is_big
                old_item['extra_cost'] = new_extra_cost
                old_item['ean'] = new_ean
                old_item['ilcode'] = new_ilcode

                updated_data.append(old_item)

            processed_items += 1
            app.progress_bar['value'] = processed_items
            app.progress_bar.update()

        for (tecdoc_id, ean) in set(existing_dict) - set(new_dict):
            old_item = existing_dict[(tecdoc_id, ean)]
            old_amount = int(old_item.get('amount') or 0)
            if old_amount > 0 and old_item.get('offer_id'):
                old_item['amount'] = 0
                old_item['status'] = '0'
            elif old_item['status'] == '3':
                old_item['status'] = '3'
            else:
                old_item['status'] = '7'

            updated_data.append(old_item)

            processed_items += 1
            app.progress_bar['value'] = processed_items
            app.progress_bar.update()

        conn = database.get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM auctions;")
                    for item in updated_data:
                        cur.execute('''
                            INSERT INTO auctions (tecdoc_id, manufacturer, amount, price, final_price, details, package_qty, offer_id, status, length, height, width, weight, is_big, extra_cost, ean, ilcode)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ''', (
                            item.get('tecdoc_id'),
                            item.get('manufacturer'),
                            item.get('amount', 0),
                            item.get('price'),
                            item.get('final_price'),
                            item.get('details', ''),
                            item.get('package_qty', ''),
                            item.get('offer_id', ''),
                            item.get('status', ''),
                            item.get('length', 0),
                            item.get('height', 0),
                            item.get('width', 0),
                            item.get('weight', 0),
                            item.get('is_big', 0),
                            item.get('extra_cost', 0),
                            item.get('ean', ''),
                            item.get('ilcode', '')
                        ))
            conn.commit()
        finally:
            conn.close()

        app.progress_bar['value'] = app.progress_bar['maximum']
        app.progress_bar.update()
        app.log_message("compare_and_update_data process completed successfully.")

    except Exception as e:
        error_message = f"Error in compare_and_update_data method: {str(e)}"
        if hasattr(app, 'log_message'):
            app.log_message(error_message)
        logging.error(error_message)
        raise
    app.log_message(f"Total items to update: {len(updated_data)}")
    return updated_data

if __name__ == "__main__":
    app = Application()
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        app.toggle_buttons = lambda state: print(f"[UI] toggle_buttons({state})")
        app.log_message = lambda msg: print(f"[LOG] {msg}")
        app.progress_bar.start = lambda: print("[UI] progress_bar.start()")
        app.progress_bar.stop = lambda: print("[UI] progress_bar.stop()")
        if mode == "morning":
            print("[AUTO] Running morning sequence...")
            app.start_process()
            import time
            time.sleep(2)
            app.run_add_edit_auctions()
            print("[AUTO] Morning sequence completed. Exiting.")
            app.destroy()
        elif mode == "afternoon":
            print("[AUTO] Running afternoon sequence...")
            app.run_update_amounts()
            import time
            time.sleep(2)
            app.run_add_edit_auctions()
            print("[AUTO] Afternoon sequence completed. Exiting.")
            app.destroy()
        else:
            print(f"[AUTO] Unknown mode '{mode}'. Running in normal GUI mode.")
            app.mainloop()
    else:
        app.mainloop()
                                                  