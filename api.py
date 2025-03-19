# api.py
import base64
import json
import time
import random
import requests
import webbrowser
from datetime import datetime
from config import ALLEGRO_API_URL, ALLEGRO_CLIENT_ID, ALLEGRO_CLIENT_SECRET, ACCESS_TOKEN_FILE, PLACEHOLDER_IMAGE_URL
from db import save_offer_id_to_db, remove_offer_id_from_db
from db import retry_on_lock

ACCESS_TOKEN = None

def retry(max_retries=5, delay=1, backoff=2, exceptions=(requests.exceptions.RequestException,)):
    from functools import wraps
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries == max_retries:
                        raise
                    sleep_time = delay * (backoff ** (retries - 1))
                    sleep_time += random.uniform(0, 1)
                    print(f"Retry {retries}/{max_retries} for {func.__name__} in {sleep_time:.2f} seconds due to {e}")
                    time.sleep(sleep_time)
        return wrapper
    return decorator

def check_and_get_access_token(app):
    global ACCESS_TOKEN
    try:
        with open(ACCESS_TOKEN_FILE, 'r') as file:
            tokens = json.load(file)
            ACCESS_TOKEN = tokens['access_token']
            refresh_token = tokens.get('refresh_token')
            expires_at = tokens.get('expires_at', 0)
            if datetime.now().timestamp() < expires_at:
                app.log_message("Using stored access token.")
                return True
            elif refresh_token:
                app.log_message("Access token expired. Refreshing token...")
                return refresh_access_token(app, refresh_token)
    except Exception:
        pass
    return get_device_code(app)

def get_device_code(app):
    global ACCESS_TOKEN
    credentials = f"{ALLEGRO_CLIENT_ID}:{ALLEGRO_CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    headers = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {'client_id': ALLEGRO_CLIENT_ID}
    app.log_message("Requesting device code...")
    try:
        response = requests.post('https://allegro.pl/auth/oauth/device', headers=headers, data=data)
        response.raise_for_status()
        device_code = response.json()['device_code']
        user_code = response.json()['user_code']
        verification_uri = response.json()['verification_uri']
        app.log_message(f"Please go to {verification_uri} and enter the code {user_code} to authorize.")
        webbrowser.open(verification_uri)
        return poll_for_access_token(app, device_code)
    except requests.exceptions.HTTPError as e:
        app.log_message(f"Failed to get device code: {str(e)} - Response: {e.response.text}")
        return False

def poll_for_access_token(app, device_code):
    global ACCESS_TOKEN
    credentials = f"{ALLEGRO_CLIENT_ID}:{ALLEGRO_CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    headers = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:device_code',
        'device_code': device_code,
        'client_id': ALLEGRO_CLIENT_ID,
        'client_secret': ALLEGRO_CLIENT_SECRET
    }
    app.log_message("Polling for access token...")
    try:
        while True:
            response = requests.post('https://allegro.pl/auth/oauth/token', headers=headers, data=data)
            if response.status_code == 200:
                ACCESS_TOKEN = response.json()['access_token']
                expires_in = response.json()['expires_in']
                refresh_token = response.json().get('refresh_token')
                tokens = {
                    'access_token': ACCESS_TOKEN,
                    'expires_at': datetime.now().timestamp() + expires_in,
                    'refresh_token': refresh_token
                }
                with open(ACCESS_TOKEN_FILE, 'w') as file:
                    json.dump(tokens, file)
                app.log_message("Access token obtained successfully.")
                return True
            elif response.status_code == 400:
                error = response.json().get('error')
                if error == 'authorization_pending':
                    app.log_message("Authorization pending. Waiting...")
                    time.sleep(5)
                    continue
                else:
                    app.log_message(f"Failed to get access token: {response.json()}")
                    return False
            else:
                response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        app.log_message(f"Failed to poll for access token: {str(e)} - Response: {e.response.text}")
        return False

def refresh_access_token(app, refresh_token):
    global ACCESS_TOKEN
    credentials = f"{ALLEGRO_CLIENT_ID}:{ALLEGRO_CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    headers = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': ALLEGRO_CLIENT_ID,
        'client_secret': ALLEGRO_CLIENT_SECRET
    }
    app.log_message("Refreshing access token...")
    try:
        response = requests.post('https://allegro.pl/auth/oauth/token', headers=headers, data=data)
        response.raise_for_status()
        token_response = response.json()
        ACCESS_TOKEN = token_response['access_token']
        expires_in = token_response['expires_in']
        refresh_token = token_response.get('refresh_token')
        tokens = {
            'access_token': ACCESS_TOKEN,
            'expires_at': datetime.now().timestamp() + expires_in,
            'refresh_token': refresh_token
        }
        with open(ACCESS_TOKEN_FILE, 'w') as file:
            json.dump(tokens, file)
        app.log_message("Access token refreshed successfully.")
        return True
    except requests.exceptions.HTTPError as e:
        app.log_message(f"Failed to refresh access token: {str(e)} - Response: {e.response.text}")
        return False

@retry(max_retries=5, delay=1, backoff=2)
def delete_auction(app, offer_id):
    global ACCESS_TOKEN
    session = requests.Session()
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Accept': 'application/vnd.allegro.public.v1+json',
        'Content-Type': 'application/vnd.allegro.public.v1+json'
    }
    app.log_message(f"Deleting auction {offer_id}...")
    try:
        patch_data = {'publication': {'status': 'ENDED'}}
        response = session.patch(f'{ALLEGRO_API_URL}/sale/product-offers/{offer_id}', headers=headers, json=patch_data)
        response.raise_for_status()
        app.log_message(f"Successfully deleted auction {offer_id}.")
        remove_offer_id_from_db(offer_id)
        return True
    except requests.exceptions.HTTPError as e:
        error_message = e.response.json().get('errors', [{}])[0].get('userMessage', str(e))
        app.log_message(f"Failed to delete auction {offer_id}: {error_message}")
        return False

@retry(max_retries=5, delay=1, backoff=2)
def update_auction(app, offer_id, item):
    global ACCESS_TOKEN
    session = requests.Session()
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Accept': 'application/vnd.allegro.public.v1+json',
        'Content-Type': 'application/vnd.allegro.public.v1+json'
    }
    final_price = item['final_price']
    patch_data = {
        "stock": {"available": item['amount']},
        "sellingMode": {
            "price": {
                "amount": str(final_price),
                "currency": "PLN"
            }
        }
    }
    app.log_message(f"Updating auction {offer_id}...")
    def send_request():
        try:
            response = session.patch(f'{ALLEGRO_API_URL}/sale/product-offers/{offer_id}', headers=headers, json=patch_data)
            response.raise_for_status()
            app.log_message(f"Successfully updated auction {offer_id}.")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                app.log_message("Access token expired. Refreshing token...")
                if check_and_get_access_token(app):
                    headers['Authorization'] = f'Bearer {ACCESS_TOKEN}'
                    return send_request()
            error_message = e.response.json().get('errors', [{}])[0].get('userMessage', str(e))
            app.log_message(f"Failed to update auction {offer_id}: {error_message}")
            return False
    return send_request()

@retry(max_retries=5, delay=1, backoff=2)
def create_or_update_auction(app, product_id, item, draft=False):
    global ACCESS_TOKEN
    # (Assume prevent_sleep() is defined elsewhere if needed)
    session = requests.Session()
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Accept': 'application/vnd.allegro.public.v1+json',
        'Content-Type': 'application/vnd.allegro.public.v1+json'
    }
    # Fetch product data via another function (not fully shown here)
    from api import fetch_product_data  # assume defined similarly
    product_details = fetch_product_data(product_id)
    if not product_details:
        app.log_message(f"Failed to fetch product data for product ID {product_id}")
        return None

    final_price = item['final_price']
    category_id = product_details['category_id']
    if '4142' in category_id:
        final_price += 33
    if final_price > 10000:
        draft = True

    # Determine shipping rate based on is_big (logic preserved)
    SHIPPING_RATE_IDS = {
        0: "ec30a81a-b787-4251-9988-a8fe161ec265",
        1: "0b2b5667-504a-4887-b8fe-f9c8ad3539ae",
        2: "c4902b58-9128-49cb-8fb6-95ef42f1400d",
        3: "4ee0e89c-58ce-418a-b716-42734e3806f5",
    }
    shipping_rate_id = SHIPPING_RATE_IDS.get(item.get('is_big', 0), SHIPPING_RATE_IDS[0])
    
    # Create auction description via a helper (assume defined below)
    from api import create_auction_description  # assume defined similarly
    description_sections = create_auction_description(app, product_id, item, product_details)
    # Combine images (logic preserved)
    product_images = [img['url'] for img in product_details.get('images', [])[:2]]
    description_images = []
    for section in description_sections:
        for section_item in section.get('items', []):
            if section_item.get('type') == 'IMAGE':
                description_images.append(section_item.get('url'))
    combined_images = list(dict.fromkeys(product_images + description_images))[:2]
    if not combined_images:
        combined_images = [PLACEHOLDER_IMAGE_URL]
    # Adjust images in description sections
    image_index = 0
    for section in description_sections:
        for section_item in section.get('items', []):
            if section_item.get('type') == 'IMAGE':
                if image_index >= len(combined_images):
                    section_item['url'] = PLACEHOLDER_IMAGE_URL
                else:
                    section_item['url'] = combined_images[image_index]
                    image_index += 1

    offer_data = {
        "productSet": [{
            "product": {"id": product_id},
            "safetyInformation": {"type": "NO_SAFETY_INFORMATION"}
        }],
        "sellingMode": {
            "price": {"amount": str(final_price), "currency": "PLN"}
        },
        "stock": {"available": item['amount']},
        "publication": {"status": "INACTIVE" if final_price == 1 else ("INACTIVE" if draft else "ACTIVE")},
        "images": combined_images,
        "delivery": {
            "handlingTime": "P2D",
            "shippingRates": {"id": shipping_rate_id}
        },
        "description": {"sections": description_sections}
    }
    def send_request():
        try:
            response = session.post(f'{ALLEGRO_API_URL}/sale/product-offers', headers=headers, json=offer_data)
            if response.status_code == 422:
                errors = response.json().get('errors', [])
                for error in errors:
                    if error.get('code') == "ConstraintViolationException.MissingRequiredParameters":
                        message = error.get('message', '')
                        missing_param_ids = list(map(int, __import__('re').findall(r'\d+', message)))
                        app.log_message(f"⚠️ Missing parameters for product ID {product_id}: {missing_param_ids}")
                        # (Fetch missing parameters and update offer_data accordingly)
                        # For brevity, assume we retry here.
                        response = session.post(f'{ALLEGRO_API_URL}/sale/product-offers', headers=headers, json=offer_data)
                        response.raise_for_status()
                        offer_id = response.json()['id']
                        app.log_message(f"✅ Auction created/updated for product ID {product_id}. Offer ID: {offer_id}")
                        save_offer_id_to_db(item['tecdoc_id'], offer_id, item['ean'])
                        return offer_id
            response.raise_for_status()
            offer_id = response.json()['id']
            app.log_message(f"✅ Auction created/updated for product ID {product_id}. Offer ID: {offer_id}")
            save_offer_id_to_db(item['tecdoc_id'], offer_id, item['ean'])
            return offer_id
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                app.log_message("⚠️ Access token expired. Refreshing token...")
                if check_and_get_access_token(app):
                    headers['Authorization'] = f'Bearer {ACCESS_TOKEN}'
                    return send_request()
            error_message = e.response.json().get('errors', [{}])[0].get('userMessage', str(e))
            app.log_message(f"❌ Failed to create/update auction for product ID {product_id}: {error_message} - Response: {e.response.text}")
            return None
    return send_request()
