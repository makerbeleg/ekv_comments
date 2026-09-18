
import os
import dotenv
import logging
import struct
import json
import random
import httpx
import asyncio
import ekv_db
from functools import wraps
from datetime import datetime, date, timedelta
from time import monotonic

dotenv.load_dotenv()

LOG_FILE = os.getenv("LOG_FILE", "ekv_bot.log")
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    force=True,
    style="{",
    format="{name}: {message}",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ])

_opener = httpx.AsyncClient(
    timeout=httpx.Timeout(10.0),
    limits=httpx.Limits(max_keepalive_connections=10, max_connections=110))

_RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", 3))
_BASE_DELAY = float(os.getenv("BASE_DELAY", 1.0)) #seconds
_DELAY_BACKOFF = int(os.getenv("DELAY_BACKOFF", 2)) #seconds
_LEAK_RATE_MULTIPLIER = int(os.getenv("LEAK_RATE_MULTIPLIER", 5)) #seconds
_API_PAUSE_DURATION = int(os.getenv("PAUSE_DURATION", 3600)) #seconds
_SLOW_THRESHOLD = float(os.getenv("SLOW_THRESHOLD", 1.0)) #seconds
_ERROR_THRESHOLD_MULTIPLIER = int(os.getenv("ERROR_THRESHOLD_MULTIPLIER", 2))
_CYCLE = int(os.getenv("CYCLE", 180)) #seconds

_error_lock = asyncio.Lock()
_error_counter, _pause_until = 0, 0

_bucket_tokens, _bucket_refill_time = 0.0, 0.0
_BUCKET_CAPACITY, _LEAK_RATE = 1, 1 #rate: tokens per second
_ERROR_THRESHOLD = 1
     
SLOW_SEMAPHORE, FAST_SEMAPHORE = asyncio.Semaphore(1), asyncio.Semaphore(1)



class APIPausedError(Exception): pass


def api_paused(): return _pause_until > monotonic()


async def reset_circuit_breaker():
    global _error_counter
    async with _error_lock: _error_counter = 0
    return True if _error_counter == 0 else False


async def manager(load_factor):
    global SLOW_SEMAPHORE, FAST_SEMAPHORE
    global _ERROR_THRESHOLD, _BUCKET_CAPACITY

    last_active = None
    last_factors = (None, None)
    while True:
        hour, minutes = datetime.now().hour, datetime.now().minute
        low_factor = 1 if load_factor == 1 else (load_factor if 8 <= hour < 22 else 1)
        high_factor = load_factor * 50 if 8 <= hour < 22 else load_factor * 5

        #----------------------
        if (low_factor, high_factor) != last_factors:
            last_factors = (low_factor, high_factor)
            SLOW_SEMAPHORE = asyncio.Semaphore(low_factor)
            FAST_SEMAPHORE = asyncio.Semaphore(high_factor)
            logger.info(f"Semaphore slow was set to {low_factor}, fast to {high_factor}")

        #----------------------
        active_users = await ekv_db.active_users()
        if active_users != last_active:
            _ERROR_THRESHOLD = max(active_users * _ERROR_THRESHOLD_MULTIPLIER, 3)
            _BUCKET_CAPACITY = max(_ERROR_THRESHOLD, 6)
            last_active = active_users

            logger.info(f"{active_users} active users were found. "
                f"Error threshold was set to {_ERROR_THRESHOLD}, bucket capacity to {_BUCKET_CAPACITY}")

        await asyncio.sleep(60 * (60 - minutes)) #Sleep till next hour


def _async_retry(attempts=_RETRY_ATTEMPTS):
    def retry_decorator(func):
        @wraps(func)
        async def retry_wrapper(*args, **kwargs):
            for attempt in range(1, attempts + 1):
                try:
                    await asyncio.sleep(random.uniform(0.001, 0.03)) #predelay
                    return await func(*args, **kwargs)

                except httpx.HTTPStatusError as error:
                    if error.response.status_code not in (408, 429, 500, 502, 503, 504):
                        logger.error(f"{error.response.status_code} on {func.__name__}: {error}.")
                        raise error

                    logger.warning(f"HTTP {error.response.status_code} on {func.__name__} (attempt {attempt}/{attempts})")

                except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.WriteError) as error:
                    logger.warning(f"An error occurred during {func.__name__}: {error} (attempt {attempt}/{attempts})")

                if attempt == attempts:
                    logger.error(f"All {attempts} attempts failed for {func.__name__}.")
                    raise

                delay = _BASE_DELAY * (_DELAY_BACKOFF ** attempt) + random.uniform(0.5, 1.5)
                logger.warning(f"Retrying {func.__name__} in {delay:.1f}s")
                await asyncio.sleep(delay)

        return retry_wrapper
    return retry_decorator


async def _leaky_bucket():
    global _bucket_tokens, _bucket_refill_time, _LEAK_RATE

    while True:
        now = monotonic()
        async with _error_lock:
            if _bucket_refill_time == 0: _bucket_refill_time = now

            elapsed = now - _bucket_refill_time
            if elapsed > _CYCLE:
                _bucket_tokens = 0
                _bucket_refill_time = now

            else:
                _LEAK_RATE = (_ERROR_THRESHOLD / (_error_counter * _LEAK_RATE_MULTIPLIER)) - 0.1 #tokens per second
                logger.info(f"Leak rate was set to {_LEAK_RATE:.2f}")

                new_tokens = elapsed * _LEAK_RATE
                _bucket_tokens = min(_BUCKET_CAPACITY, _bucket_tokens + new_tokens)
                _bucket_refill_time = now
                logger.info(f"Bucket contains {_bucket_tokens:.2f} tokens")

            if _bucket_tokens >= 1:
                _bucket_tokens = _bucket_tokens - 1
                logger.info(f"Consumed 1 token. Bucket contains {_bucket_tokens:.2f} tokens")
                break

            else:
                wait_time = (1 - _bucket_tokens) / _LEAK_RATE
                logger.info(f"Waiting {wait_time:.2f}s to fill up bucket")
                await asyncio.sleep(wait_time)


def _circuit_breaker(func):
    @wraps(func)
    async def breaker_wrapper(dest, payload, cookies):
        global _error_counter, _pause_until

        if _pause_until > monotonic() and payload['method'] != "removeSignature":
            raise APIPausedError(f"API paused for {(_pause_until - monotonic()) / 60:.1f}m")

        if _ERROR_THRESHOLD > 2 and (_ERROR_THRESHOLD / 2 <= _error_counter < _ERROR_THRESHOLD):
            await _leaky_bucket()

        try:
            response = await func(dest, payload, cookies)

        except Exception as error:
            async with _error_lock: _error_counter = _error_counter + 1
            logger.info(f"Error count increased to {_error_counter}")
            if _error_counter >= _ERROR_THRESHOLD:
                _pause_until = monotonic() + _API_PAUSE_DURATION
                logger.error(f"Error threshold {_ERROR_THRESHOLD} reached: {error}")
                logger.error(f"Api paused for {_API_PAUSE_DURATION}s")

            else:
                logger.error(f"API call failed ({_error_counter}/{_ERROR_THRESHOLD}): {error}")

            raise
        else:
            elapsed = response.elapsed.total_seconds()
            if elapsed > _SLOW_THRESHOLD and payload['method'] == "getCards":
                async with _error_lock: _error_counter = _error_counter + 1
                logger.info(f"Error count increased to {_error_counter}")
                if _error_counter >= _ERROR_THRESHOLD:
                    _pause_until = monotonic() + _API_PAUSE_DURATION
                    logger.error(f"Error threshold {_ERROR_THRESHOLD} reached. Paused for {_API_PAUSE_DURATION}s")

            else:
                if _error_counter > 0:
                    async with _error_lock: _error_counter = _error_counter - 1
                    logger.info(f"Error count decreased to {_error_counter}")

            return response
    return breaker_wrapper


@_circuit_breaker
async def _call_api(dest, payload, cookies=None):
    address, headers = (
        f"http://212.45.19.34:8081/ekvEmc/{dest}", {
        'Host': "212.45.19.34:8081",
        'Accept': "*/*",
        'Accept-Language': "en-US,en;q=0.9",
        'Accept-Encoding': "gzip, deflate",
        'User-Agent': "Mozilla/5.0 (X11; Linux x86_64; rv:150.0) Gecko/20100101 Firefox/150.0",
        'Connection': "keep-alive",
        'Origin': "http://212.45.19.34:8081",
        'Referer': "http://212.45.19.34:8081/ekvEmc/"}
        )

    data = {'InputParams': (
        "blob",
        json.dumps(payload),
        "application/octet-stream")}

    response = await _opener.post(
        address,
        files=data,
        headers=headers,
        cookies=cookies)

    elapsed = response.elapsed.total_seconds()
    logger.info(f"{payload['method']} took {elapsed:.3f}s: {response.json()['result']}")

    return response


@_async_retry()
async def login(login_hash, password_hash, cookies=None):
    def _bigEndian_sha1(data):
        data = bytes.fromhex(data)
        return list(struct.unpack('>5i', data))

    login_list = _bigEndian_sha1(login_hash)
    password_list = _bigEndian_sha1(password_hash)
    payload = {'method': "login", 's1': login_list, 's2': password_list}
    response = await _call_api(
        "Login.aspx",
        payload,
        cookies=cookies)

    expires = [cookie.expires for cookie in response.cookies.jar if cookie.name == "ekvSession"]

    return {
        'login': login_hash,
        'response': response.json(),
        'cookies': response.cookies,
        'expires': expires[0] if expires else 0
        }


@_async_retry()
async def confirm(cookies, reprimands=False):
    payload = {'method': "confirm"}
    response = await _call_api(
        f"ConfirmUnworked{'Reprimands' if reprimands else 'Comments'}.aspx",
        payload,
        cookies=cookies)

    return response.json()


@_async_retry()
async def fetch_reprimands(cookies):
    payload = {'method': "getReprimands"}
    response = await _call_api(
        "Reprimands.aspx",
        payload,
        cookies=cookies)

    return response.json()


@_async_retry()
async def fetch_cards(cookies=None, draw_attention=False, card_number=None):
    payload = {'data': [
        {'name': "paramDateStart", 'value': (date.today() - timedelta(days=8)).strftime("%d.%m.%Y")},
        {'name': "paramTimeStart", 'value': "12:00"},
        {'name': "paramDateEnd", 'value': (date.today() + timedelta(days=1)).strftime("%d.%m.%Y")},
        {'name': "paramTimeEnd", 'value': "00:00"},
        {'name': "paramA010", 'value': card_number or ""},
        {'name': "paramPs", 'value': "1"},
        {'name': "paramBrNum", 'value': ""},
        {'name': "paramDrawAttention", **( {'id': 1} if draw_attention else {} ), 'value': ""},
        {'name': "paramFio", 'value': ""},
        {'name': "paramMkb", 'value': ""},
        {'name': "paramK010", 'value': ""},
        {'name': "paramAddr", 'value': ""},
        {'name': "paramOwner", 'value': ""},
        {'name': "sortField", 'value': "d050"},
        {'name': "sortDirection", 'value': "desc"}], 'method': "getCards"}

    response = await _call_api(
        "Role4.aspx",
        payload,
        cookies=cookies)

    return response.json()


@_async_retry()
async def fetch_diagnosis(card_number, cookies):
    payload = {'method': "getDiag", 'a010': card_number}
    response = await _call_api(
        "Role4.aspx",
        payload,
        cookies=cookies)

    return {'a010': card_number, 'diag_text': response.json()['diag_text']}


@_async_retry(attempts=5)
async def unsign(card_number, cookies):
    payload = {'method': "removeSignature", 'a010': card_number}
    response = await _call_api(
        "Role4.aspx",
        payload,
        cookies=cookies)

    return response.json()


async def close_connection():
    await _opener.aclose()
    logger.info("Opener was closed")

