# utils.py
import time
import random
import re
import unicodedata
import ctypes
from functools import wraps
import logging

RETRY_COUNT = 5
RETRY_DELAY = 1

def retry(max_retries=5, delay=1, backoff=2, exceptions=(Exception,)):
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
                    logging.warning(f"Retry {retries}/{max_retries} for {func.__name__} in {sleep_time:.2f} seconds due to {e}")
                    time.sleep(sleep_time)
        return wrapper
    return decorator

def prevent_sleep():
    try:
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000002 | 0x80000000)
    except Exception:
        pass

def allow_sleep():
    try:
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)
    except Exception:
        pass

def normalize_unicode(text):
    if text is None:
        return ''
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')

def replace_special_characters(text):
    return re.sub(r'[^\w\s]', ' ', text)

def calculate_margin(price):
    if price < 3:
        return 2
    elif price < 10:
        return 1.6
    elif price < 20:
        return 1.29
    elif price < 32:
        return 1.26
    elif price < 80:
        return 1.24
    elif price < 178:
        return 1.23
    elif price < 350:
        return 1.21
    elif price < 600:
        return 1.19
    elif price < 1000:
        return 1.18
    elif price < 3500:
        return 1.15
    else:
        return 1.18

def is_valid_ean(ean):
    return ean and re.match(r'^\d+$', ean)
