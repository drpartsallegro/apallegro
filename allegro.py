# allegro.py
import base64
import json
import time
import webbrowser
import re
import requests
import random
from datetime import datetime
import logging
from utils import retry
from config import ALLEGRO_API_URL, ALLEGRO_CLIENT_ID, ALLEGRO_CLIENT_SECRET, PLACEHOLDER_IMAGE_URL, ACCESS_TOKEN_FILE

ACCESS_TOKEN = None  # global variable

@retry(max_retries=5, delay=1, backoff=2, exceptions=(requests.exceptions.RequestException,))
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
    response = requests.post(f'{ALLEGRO_API_URL}/auth/oauth/token', headers=headers, data=data)
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
    while True:
        response = requests.post(f'{ALLEGRO_API_URL}/auth/oauth/token', headers=headers, data=data)
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

def get_device_code(app):
    global ACCESS_TOKEN
    credentials = f"{ALLEGRO_CLIENT_ID}:{ALLEGRO_CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    headers = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'client_id': ALLEGRO_CLIENT_ID
    }
    app.log_message("Requesting device code...")
    response = requests.post(f'{ALLEGRO_API_URL}/auth/oauth/device', headers=headers, data=data)
    response.raise_for_status()
    device_code = response.json()['device_code']
    user_code = response.json()['user_code']
    verification_uri = response.json()['verification_uri']
    app.log_message(f"Please go to {verification_uri} and enter the code {user_code} to authorize.")
    webbrowser.open(verification_uri)
    return poll_for_access_token(app, device_code)

def check_and_get_access_token(app):
    global ACCESS_TOKEN
    if os.path.exists(ACCESS_TOKEN_FILE):
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
    return get_device_code(app)

@retry(max_retries=5, delay=1, backoff=2)
def create_or_update_auction(app, product_id, item, draft=False):
    from utils import prevent_sleep, allow_sleep
    prevent_sleep()
    session = requests.Session()
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Accept': 'application/vnd.allegro.public.v1+json',
        'Content-Type': 'application/vnd.allegro.public.v1+json'
    }
    # Fetch product data (assuming fetch_product_data is defined below)
    product_details = fetch_product_data(product_id)
    if not product_details:
        app.log_message(f"Failed to fetch product data for product ID {product_id}")
        return None
    final_price = item['final_price']
    category_id = product_details['category']['id']
    if '4142' in category_id:
        final_price += 33
    if final_price > 10000:
        draft = True
    is_big = item.get('is_big', 0)
    item['is_big'] = is_big
    shipping_rate_ids = {
        0: "ec30a81a-b787-4251-9988-a8fe161ec265",
        1: "0b2b5667-504a-4887-b8fe-f9c8ad3539ae",
        2: "c4902b58-9128-49cb-8fb6-95ef42f1400d",
        3: "4ee0e89c-58ce-418a-b716-42734e3806f5",
    }
    shipping_rate_id = shipping_rate_ids.get(is_big, shipping_rate_ids[0])
    description_sections = create_auction_description(app, product_id, item, product_details)
    product_images = [img['url'] for img in product_details.get('images', [])[:2]]
    description_images = []
    for section in description_sections:
        for section_item in section.get('items', []):
            if section_item.get('type') == 'IMAGE':
                description_images.append(section_item.get('url'))
    combined_images = list(dict.fromkeys(product_images + description_images))[:2]
    if not combined_images:
        combined_images = [PLACEHOLDER_IMAGE_URL]
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
            "product": {
                "id": product_id
            },
            "safetyInformation": {
                "type": "NO_SAFETY_INFORMATION"
            }
        }],
        "sellingMode": {
            "price": {
                "amount": str(final_price),
                "currency": "PLN"
            }
        },
        "stock": {
            "available": item['amount']
        },
        "publication": {
            "status": "INACTIVE" if final_price == 1 else ("INACTIVE" if draft else "ACTIVE")
        },
        "images": combined_images,
        "delivery": {
            "handlingTime": "P2D",
            "shippingRates": {
                "id": shipping_rate_id
            }
        },
        "description": {
            "sections": description_sections
        }
    }
    def send_request():
        try:
            response = session.post(f'{ALLEGRO_API_URL}/sale/product-offers', headers=headers, json=offer_data)
            if response.status_code == 422:
                errors = response.json().get('errors', [])
                for error in errors:
                    if error.get('code') == "ConstraintViolationException.MissingRequiredParameters":
                        message = error.get('message', '')
                        missing_param_ids = list(map(int, re.findall(r'\d+', message)))
                        app.log_message(f"Missing parameters for product ID {product_id}: {missing_param_ids}")
                        product_parameters, offer_parameters = fetch_missing_parameters(app, product_id, missing_param_ids)
                        if product_parameters:
                            offer_data["productSet"][0]["product"]["parameters"] = product_parameters
                        if offer_parameters:
                            offer_data["parameters"] = offer_parameters
                        app.log_message(f"Retrying auction creation for product ID {product_id}...")
                        response = session.post(f'{ALLEGRO_API_URL}/sale/product-offers', headers=headers, json=offer_data)
                        response.raise_for_status()
                        offer_id = response.json()['id']
                        app.log_message(f"Auction created/updated for product ID {product_id}. Offer ID: {offer_id}")
                        save_offer_id_to_db(item['tecdoc_id'], offer_id, item['ean'])
                        return offer_id
            response.raise_for_status()
            offer_id = response.json()['id']
            app.log_message(f"Auction created/updated for product ID {product_id}. Offer ID: {offer_id}")
            save_offer_id_to_db(item['tecdoc_id'], offer_id, item['ean'])
            return offer_id
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                app.log_message("Access token expired. Refreshing token...")
                if check_and_get_access_token(app):
                    headers['Authorization'] = f'Bearer {ACCESS_TOKEN}'
                    return send_request()
            error_message = e.response.json().get('errors', [{}])[0].get('userMessage', str(e))
            app.log_message(f"Failed to create/update auction for product ID {product_id}: {error_message}")
            return None
    return send_request()

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
    patch_data = {'publication': {'status': 'ENDED'}}
    try:
        response = session.patch(f'{ALLEGRO_API_URL}/sale/product-offers/{offer_id}', headers=headers, json=patch_data)
        response.raise_for_status()
        app.log_message(f"Auction {offer_id} deleted successfully.")
        remove_offer_id_from_db(offer_id)
        return True
    except requests.exceptions.HTTPError as e:
        error_message = e.response.json().get('errors', [{}])[0].get('userMessage', str(e))
        app.log_message(f"Failed to delete auction {offer_id}: {error_message}")
        return False

def fetch_product_data(product_id):
    global ACCESS_TOKEN
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Accept': 'application/vnd.allegro.public.v1+json'
    }
    response = requests.get(f'{ALLEGRO_API_URL}/sale/products/{product_id}', headers=headers)
    response.raise_for_status()
    product_data = response.json()
    return product_data

def fetch_missing_parameters(app, product_id, missing_param_ids):
    global ACCESS_TOKEN
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Accept': 'application/vnd.allegro.public.v1+json'
    }
    app.log_message(f"Fetching missing parameters for product ID {product_id}...")
    response = requests.get(f'{ALLEGRO_API_URL}/sale/products/{product_id}', headers=headers)
    response.raise_for_status()
    product_data = response.json()
    category_id = product_data.get('category', {}).get('id')
    if not category_id:
        app.log_message(f"Category ID not found for product ID {product_id}")
        return [], []
    product_parameters = []
    offer_parameters = []
    for param_id in missing_param_ids:
        param_options, describes_product = fetch_parameter_options(app, category_id, param_id)
        if param_options:
            param_info = {"id": param_id, "values": [param_options[0]['value']]}
            if describes_product:
                product_parameters.append(param_info)
            else:
                offer_parameters.append(param_info)
            app.log_message(f"Setting parameter {param_id} to {param_info['values'][0]}")
        else:
            param_info = {"id": param_id, "values": ["brak informacji"]}
            offer_parameters.append(param_info)
            app.log_message(f"Setting parameter {param_id} to 'brak informacji'")
    return product_parameters, offer_parameters

def fetch_parameter_options(app, category_id, parameter_id):
    global ACCESS_TOKEN
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Accept': 'application/vnd.allegro.public.v1+json'
    }
    app.log_message(f"Fetching options for parameter {parameter_id} in category {category_id}...")
    response = requests.get(f'{ALLEGRO_API_URL}/sale/categories/{category_id}/parameters', headers=headers)
    response.raise_for_status()
    parameters = response.json().get('parameters', [])
    for parameter in parameters:
        if str(parameter['id']) == str(parameter_id):
            options = parameter.get('dictionary', [])
            describes_product = parameter.get('options', {}).get('describesProduct', False)
            for option in options:
                option['describesProduct'] = describes_product
            return options, describes_product
    return [], False

# You should also implement create_auction_description (and any other methods) exactly as in your original code.
# For brevity, here is a minimal implementation:
def create_auction_description(app, product_id, item, product_details, max_images=2):
    def format_parameters(parameters):
        formatted = ""
        for param in parameters:
            if 'valuesLabels' in param:
                values = ', '.join(param['valuesLabels'])
                formatted += f"<p><b>{param['name']}:</b> {values}</p>"
        return formatted
    def format_images(images):
        return [image.get('url') for image in images[:2]]
    parameters_html = format_parameters(product_details.get('parameters', []))
    images_urls = format_images(product_details.get('images', []))
    initial_content = f"<h2>{product_details.get('name')}</h2>{parameters_html}"
    description_sections = [
        {"items": [{"type": "TEXT", "content": "<p><b>Sprawdz kompatybilne auta...</b></p>"}]},
        {"items": [{"type": "TEXT", "content": initial_content},
                    {"type": "IMAGE", "url": images_urls[0] if images_urls else PLACEHOLDER_IMAGE_URL}]},
        {"items": [{"type": "TEXT", "content": "<p>Aukcja dotyczy 1szt/1m/1l.</p>"}]}
    ]
    return description_sections
