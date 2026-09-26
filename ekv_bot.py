#!/usr/bin/env python

import os
import dotenv
import logging
import hashlib
import json
import ekv_api, ekv_db
import asyncio
import httpx
import random
import sys
from functools import wraps
from collections.abc import Callable
from datetime import datetime, timedelta
from time import monotonic

from telebot import asyncio_filters, types as TelebotTypes, logger as TelebotLogger
from telebot.apihelper import ApiTelegramException
from telebot.async_telebot import AsyncTeleBot
from telebot.asyncio_storage import StateMemoryStorage
from telebot.asyncio_handler_backends import State, StatesGroup

if not os.path.exists(".env"):
    with open(".env", "w", encoding="utf-8") as file:
        file.write("# EKV_BOT_TOKEN=\n")

dotenv.load_dotenv()

LOG_FILE = os.getenv("LOG_FILE", "ekv_bot.log")
TelebotLogger.setLevel(logging.WARNING)
logger = logging.getLogger("ekv_bot")
logging.basicConfig(
    level=logging.INFO,
    force=True,
    style="{",
    format="{name}: {message}",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ])

try:
    EKV_TOKEN = os.environ['EKV_BOT_TOKEN']
except KeyError:
    raise RuntimeError("Telegram bot token EKV_BOT_TOKEN is missing in .env file")

bot = AsyncTeleBot(EKV_TOKEN, state_storage=StateMemoryStorage())
bot.add_custom_filter(asyncio_filters.StateFilter(bot))

users_cache = {}
message_queue = None
LOCK = asyncio.Lock()
_LOAD_FACTOR = 1
_CYCLE = int(os.getenv("CYCLE", 300)) #seconds
_SCHEDULE_TIME = [int(x) for x in os.getenv("SCHEDULE_TIME", "15:00").split(":")]
_SLEEP_START = int(os.getenv("SLEEP_START", 23))
_SLEEP_WAKEUP = int(os.getenv("SLEEP_WAKEUP", 7))
_RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", 3))
_BASE_DELAY = float(os.getenv("BASE_DELAY", 1.0)) #seconds
_DELAY_BACKOFF = int(os.getenv("DELAY_BACKOFF", 2)) #seconds



class AuthenticationError(Exception): pass


class Registration(StatesGroup):
    login_state = State()
    password_state = State()
    remove_login_state = State()
    remove_password_state = State()


def sleep_scheduler(start=_SLEEP_START, wakeup=_SLEEP_WAKEUP):
    def sleep_decorator(func):
        @wraps(func)
        async def sleep_wrapper(*args, **kwargs):
            now = datetime.now()
            if now.hour >= start or now.hour < wakeup:
                wakeup_time = now.replace(hour=wakeup, minute=0)
                if now.hour >= start:
                    wakeup_time += timedelta(days=1)

                logger.info("Starting cleanup...")
                results = await ekv_db.cleanup()

                if results['comments'] > 0: logger.info(f"Purged {results['comments']} old ekv_comments")
                if results['reprimands'] > 0: logger.info(f"Purged {results['reprimands']} signed reprimands")

                logger.info(f"Now sleeping till {wakeup_time.strftime('%H:%M')}...")
                await asyncio.sleep((wakeup_time - now).total_seconds())
                await ekv_api.reset_circuit_breaker()

            return await func(*args, **kwargs)
        return sleep_wrapper
    return sleep_decorator


async def message_worker(queue: asyncio.Queue) -> None:
    while True:
        chat_id, text, *rest = await queue.get()
        markup = rest[0] if rest else None

        for attempt in range(1, _RETRY_ATTEMPTS + 1):
            try:
                await bot.send_message(chat_id, text, reply_markup=markup)

            except Exception as error:
                logger.warning(f"Error has occurred while sending message: {error}"
                    f"{chat_id}, {text}, {markup}")

                if (isinstance(error, ApiTelegramException) and
                    error.error_code == 429):
                    retry_after = error.result_json.get("parameters", {}).get("retry_after", 5)
                    logger.warning(f"Waiting {retry_after:.0f}s before retry")
                    await asyncio.sleep(retry_after + 1)
                    continue

                if attempt == _RETRY_ATTEMPTS:
                    logger.error(f"Failed to send message after {attempt} attempts")

                else:
                    delay = _BASE_DELAY * (_DELAY_BACKOFF ** attempt) + random.uniform(0.5, 1.5)
                    logger.warning(f"Retry {attempt}/{_RETRY_ATTEMPTS} in {delay:.2f}s")
                    await asyncio.sleep(delay)

            else:
                #Satisfy telegram 30 messages/sec limit
                await asyncio.sleep(0.05)
                break

        queue.task_done()


#Add user
@bot.message_handler(commands=['start'])
async def start_command(message: TelebotTypes.Message) -> None:
    await bot.set_state(message.from_user.id,
        Registration.login_state,
        message.chat.id)

    await message_queue.put((message.chat.id,
        "Введите логин от электронных карт"))

@bot.message_handler(state=Registration.login_state)
async def start_login_process(message: TelebotTypes.Message) -> None:
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        data['chat_id'] = message.chat.id
        data['login'] = message.text

    await bot.set_state(message.from_user.id,
        Registration.password_state,
        message.chat.id)

    await message_queue.put((message.chat.id,
        "Введите пароль от электронных карт"))

@bot.message_handler(state=Registration.password_state)
async def start_password_process(message: TelebotTypes.Message) -> None:
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        data['password'] = message.text

    await bot.delete_state(message.from_user.id, message.chat.id)
    try:
        response = await add_user(data['chat_id'], data['login'], data['password'])
        await message_queue.put((message.chat.id, response['response']))

    except Exception as error:
        logger.error(f"{error} for {data['login']}")

    else:
        if response['login']:
            try:
                await new_user_fetch(response['login'], message.chat.id)

            except Exception as error:
                logger.warning(error)

#Remove user
@bot.message_handler(commands=['remove'])
async def remove_command(message: TelebotTypes.Message) -> None:
    await bot.set_state(message.from_user.id,
        Registration.remove_login_state,
        message.chat.id)

    await message_queue.put((message.chat.id,
        "Введите логин от электронных карт для удаления"))

@bot.message_handler(state=Registration.remove_login_state)
async def remove_login_process(message: TelebotTypes.Message) -> None:
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        data['chat_id'] = message.chat.id
        data['login'] = message.text

    await bot.set_state(message.from_user.id,
        Registration.remove_password_state,
        message.chat.id)

    await message_queue.put((message.chat.id,
        "Введите пароль от электронных карт для удаления"))

@bot.message_handler(state=Registration.remove_password_state)
async def remove_password_process(message: TelebotTypes.Message) -> None:
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        data['password'] = message.text

    await bot.delete_state(message.from_user.id, message.chat.id)
    try:
        response = await remove_user(data['chat_id'], data['login'], data['password'])
        await message_queue.put((message.chat.id, response['response']))

    except Exception as error:
        logger.error(f"{error} for {data['login']}")



#Unsign button handler
@bot.callback_query_handler(func=lambda call: True)
async def handle_query(call: TelebotTypes.CallbackQuery) -> None:
    card_number, login_hash = json.loads(call.data)
    if login_hash not in users_cache:
        try:
            await bot.answer_callback_query(call.id, text="Действие невозможно. Пользователь не найден")
            await bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=call.message.text + "\n\nДействие невозможно. Пользователь не найден",
                reply_markup=None)

        except Exception as error:
            logger.warning(f"Failed to answer callback/edit message for {call.id}: {error}")

        return

    if "queue" not in users_cache[login_hash]:
        user_queue = asyncio.Queue()
        users_cache[login_hash]['queue'] = user_queue
        asyncio.create_task(unsign_worker(login_hash, user_queue))

    else:
        user_queue = users_cache[login_hash]['queue']

    await user_queue.put(call)

    try:
        await bot.answer_callback_query(call.id)

    except Exception as error:
        logger.warning(f"Failed to answer callback for {call.id}: {error}")


#Per user queue worker for card unsigns
async def unsign_worker(login_hash: str, queue: asyncio.Queue) -> None:
    while True:
        try:
            call = await asyncio.wait_for(queue.get(), timeout=60.0)
            logger.info(f"Received {json.loads(call.data)[0]} from a queue")

        except asyncio.TimeoutError:
            async with LOCK: users_cache[login_hash].pop("queue")
            logger.info("Queue was empty for 60 seconds. Destroyed")
            break

        try:
            card_number = json.loads(call.data)[0]

            #Check for card status before unsign
            #also login if cookies expired/dead session
            async with ekv_api.FAST_SEMAPHORE:
                card = await fetch_cards(login_hash, [], users_cache[login_hash]['cookies'], card_number=card_number)

            cookies = users_cache[login_hash]['cookies'] #updated after fetch_cards

            response = None
            if (card_number in card and
                card[card_number]['state'] == "Необработанные комментарии"):

                if card[card_number]['signature']:
                    logger.info(f"Unworked comments on {card_number}. Unsigning")
                    async with ekv_api.FAST_SEMAPHORE:
                        response = await ekv_api.unsign(card_number, cookies)

                    if response['result'] is False:
                        raise AuthenticationError(response['result_text'])

                else:
                    logger.info(f"Unworked comments on {card_number}. Card was not signed")

                session_key = cookies['ekvSession'].split("=")[1]
                message = f"http://87.245.130.238:19910/#/login-proxy/{session_key}/?from=/karta/{card_number}/sbs"

            elif card_number in card:
                message = f"{card_number}: {card[card_number]['state']}"

            else:
                message = f"{card_number}: Карта не найдена"

        except Exception as error:
            logger.warning(f"Failed to unsign: {error}")

        else:
            try:
                await bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text=call.message.text + f"\n\n{message}",
                    reply_markup=None)

            except Exception as error:
                logger.warning(f"Failed to send link for {call.id}: {error}")

            if card_number in card:
                log_message = response['result_text'] if response else card[card_number]['state']

            else:
                log_message = f"{card_number}: Карта не найдена"

            logger.info(f"Successfully processed a queue item: {card_number}: {log_message}")

        queue.task_done()


def authentication(func):
    @wraps(func)
    async def auth_wrapper(login_hash: str, chat_ids: list[int], cookies: dict[str, str], **kwargs):
        for attempt in range(1, _RETRY_ATTEMPTS + 1):
            try:
                if datetime.now().timestamp() >= users_cache[login_hash]['expires']:
                    result = await ekv_db.update_user(login_hash, session="verified")
                    async with LOCK: users_cache[login_hash]['session'] = "verified"
                    raise AuthenticationError(f"Session expired for {login_hash}")

                return await func(login_hash, chat_ids, cookies, **kwargs)

            except AuthenticationError as error:
                logger.info(f"{func.__name__} returned: {error}. (attempt {attempt}/{_RETRY_ATTEMPTS})")

                user = await ekv_db.get_user(login=login_hash)
                if not user:
                    raise ValueError("User was not found in ekv_db")

                password_hash = user[login_hash]['password']
                async with ekv_api.SLOW_SEMAPHORE:
                    response = await ekv_api.login(login_hash, password_hash)

                if response['response']['result'] is False:
                    raise Exception(response['response']['result_text'])

                cookies, expires = response['cookies'], response['expires']

                result = await ekv_db.update_user(login_hash, cookies=cookies, expires=expires)
                async with LOCK:
                    if login_hash in users_cache:
                        users_cache[login_hash]['cookies'] = cookies
                        users_cache[login_hash]['expires'] = expires

                    else:
                        users_cache[login_hash] = {'cookies': cookies, 'expires': expires}

                logger.info("Updated cookies for user")

                response_path = response['response']['path']
                while response_path in ("ConfirmUnworkedComments.aspx", "ConfirmUnworkedReprimands.aspx"):
                    confirm_reprimands = (response_path == "ConfirmUnworkedReprimands.aspx") #returns True or False
                    async with ekv_api.FAST_SEMAPHORE:
                        response = await ekv_api.confirm(cookies, reprimands=confirm_reprimands)

                    logger.info(f"Confirmed {'reprimands' if confirm_reprimands else 'comments'}: {response['result_text']}")

                    if 'path' not in response: break
                    response_path = response['path']

                last_error = error
            await asyncio.sleep(0.2 * attempt)

        args = (login_hash, chat_ids, cookies)
        raise Exception(f"All {_RETRY_ATTEMPTS} failed for {func.__name__}"
            f"with {args}, {kwargs}: {last_error}")
    return auth_wrapper


async def add_user(chat_id: int, login: str, password: str) -> dict[str, str]:
    login_hash = hashlib.sha1(login.encode("utf-8")).hexdigest()
    password_hash = hashlib.sha1(password.encode("utf-8")).hexdigest()

    user_exists = await ekv_db.get_user(login=login_hash)
    if user_exists:
        if user_exists[login_hash]['password'] != password_hash:
            return {'response': "Пользователь не найден или неверный пароль.", 'login': None}

        user_ids = user_exists[login_hash]['chat_ids']
        if chat_id in user_ids:
            return {'response': f"Пользователь {login} уже существует.", 'login': None}

        user_ids.append(chat_id)
        result = await ekv_db.update_user(login_hash, chat_ids=user_ids)
        async with LOCK:
            if login_hash in users_cache:
                users_cache[login_hash]['chat_ids'] = user_ids

            else:
                users_cache[login_hash] = {'chat_ids': user_ids}

        logger.info("Updated telegram ids for user")
        return {'response': f"Пользователь {login} добавлен.", 'login': login_hash}

    async with ekv_api.SLOW_SEMAPHORE:
        try:
            response = await ekv_api.login(login_hash, password_hash)

        except ekv_api.APIPausedError:
            return {'response': "Сервер недоступен", 'login': None}

    if response['response']['result'] is False:
        return {'response': response['response']['result_text'], 'login': None}

    result = await ekv_db.add_user(
        [chat_id],
        login_hash,
        password_hash,
        response['cookies'],
        response['expires'])

    async with LOCK:
        users_cache[login_hash] = {
            'chat_ids': [chat_id],
            'password': password_hash,
            'cookies': dict(response['cookies']),
            'expires': response['expires']}

    logger.info(f"Added {result} new user")

    return {'response': response['response']['result_text'], 'login': login_hash}


async def remove_user(chat_id: int, login: str, password: str) -> dict[str, str]:
    login_hash = hashlib.sha1(login.encode("utf-8")).hexdigest()
    password_hash = hashlib.sha1(password.encode("utf-8")).hexdigest()
    user_exists = await ekv_db.get_user(login=login_hash)

    if (not user_exists or
            user_exists[login_hash]['password'] != password_hash):
        return {'response': "Пользователь не найден или неверный пароль.", 'login': login}

    user_ids = user_exists[login_hash]['chat_ids']
    if len(user_ids) > 1:
        user_ids.remove(chat_id)

        result = await ekv_db.update_user(login_hash, chat_ids=user_ids)
        async with LOCK: users_cache[login_hash]['chat_ids'] = user_ids

        logger.info("Removed telegram ids for user")
        return {'response': f"Пользователь {login} удален.", 'login': login}

    else:
        result = await ekv_db.remove_user(login_hash)
        async with LOCK: users_cache.pop(login_hash)

        logger.info(f"Removed {result} user")
        return {'response': f"Пользователь {login} удален.", 'login': login}


async def fetch_new_diagnoses(item_list: dict[int, dict]) -> dict[int, dict]:
    async def fetch_diagnose(card, details):
        async with ekv_api.FAST_SEMAPHORE:
            response = await ekv_api.fetch_diagnosis(card, details['cookies'])
        return response

    diagnosis_tasks = [fetch_diagnose(card, details) for card, details in item_list.items()]
    diagnoses = await asyncio.gather(*diagnosis_tasks, return_exceptions=True)

    for card_number, card in zip(item_list.keys(), diagnoses):
        if isinstance(card, Exception):
            logger.warning(f"Failed to fetch diagnosis for {card_number}: {card}")
            item_list[card_number]['diagnosis'] = ""
            continue

        item_list[card_number]['diagnosis'] = card['diag_text']
    return item_list


@authentication
async def fetch_cards(login_hash: str, chat_ids: list[int], cookies: dict[str, str], card_number: int = None) -> dict[int, dict]:
    async with ekv_api.SLOW_SEMAPHORE:
        cards_responses = await ekv_api.fetch_cards(cookies=cookies, card_number=card_number, draw_attention=False)
        if not card_number:
            await asyncio.sleep((5 + random.uniform(0.5, 2)) / _LOAD_FACTOR)

    #Set a 3 hour cooldown if dead session
    if "items" not in cards_responses or len(cards_responses['items']) == 0:
        now = datetime.now()
        if now.timestamp() <= users_cache[login_hash]['expires']:
            if users_cache[login_hash].get("session", "unverified") == "unverified" and not card_number:
                new_time = now + timedelta(hours=3)
                result = await ekv_db.update_user(login_hash,
                    date=new_time.strftime("%Y-%m-%d %H:%M:%S"),
                    session="sleep")

                async with LOCK:
                    if login_hash in users_cache:
                        users_cache[login_hash]['date'] = new_time.strftime("%Y-%m-%d %H:%M:%S")
                        users_cache[login_hash]['session'] = "sleep"

                logger.info(f"Updated user with 'date' set to {new_time}")
                return {}

            if (users_cache[login_hash]['session'] == "sleep" or
                (card_number and users_cache[login_hash]['session'] != "verified")):

                result = await ekv_db.update_user(login_hash, session="verified")
                async with LOCK: users_cache[login_hash]['session'] = "verified"
                raise AuthenticationError("No items in 'items' of cards response. "
                    f"{cards_responses['result_text']}")

    result = await ekv_db.update_user(login_hash, session="unverified")
    async with LOCK: users_cache[login_hash]['session'] = "unverified"

    #If there are no cards or all are approved then set cooldown till nextday _SCHEDULE_TIME
    if (len(cards_responses['items']) == 0 or
        all(card.get("state_text") == "Карта вызова утверждена" for card in cards_responses['items'])):

        if not card_number:
            now = datetime.now()
            new_date = now.replace(hour=_SCHEDULE_TIME[0], minute=random.randint(0, 30))
            if now.hour >= _SCHEDULE_TIME[0]:
                new_date += timedelta(days=1)

            result = await ekv_db.update_user(login_hash, date=new_date.strftime("%Y-%m-%d %H:%M:%S"))
            async with LOCK:
                if login_hash in users_cache:
                    users_cache[login_hash]['date'] = new_date.strftime("%Y-%m-%d %H:%M:%S")
                    users_cache[login_hash]['session'] = 'sleep'

            logger.info(f"Updated user with 'date' set to {new_date}")
        return {}

    elif not card_number:
        result = await ekv_db.update_user(login_hash, date="")
        async with LOCK:
            if login_hash in users_cache:
                users_cache[login_hash]['date'] = None

    logger.info(f"Fetched {len(cards_responses['items'])} cards")

    #Fields: a010 = card_number, dt = datetime
    comments = {}
    for card in cards_responses['items']:
        if card['state_text'] == "Комментарии отработаны":
            result = await ekv_db.cleanup(card_number=card['a010'])
            logger.info(f"Removed {card['state_text']} card")

        if card['state_text'] == "Необработанные комментарии" or card_number:
            comments[card['a010']] = {'user': login_hash}
            comments[card['a010']]['chat_ids'] = chat_ids
            comments[card['a010']]['state'] = card['state_text']
            comments[card['a010']]['dt'] = card['dt']
            comments[card['a010']]['signature'] = card['signature']
            comments[card['a010']]['owner'] = card['owner']
            comments[card['a010']]['cookies'] = cookies

    return comments


@authentication
async def fetch_reprimands(login_hash: str, chat_ids: list[int], cookies: dict[str, str]) -> dict[int, dict]:
    async with ekv_api.SLOW_SEMAPHORE:
        reps_responses = await ekv_api.fetch_reprimands(cookies=cookies)
        await asyncio.sleep((5 + random.uniform(0.5, 2)) / _LOAD_FACTOR)

    #Set a 3 hour cooldown if dead session
    if "items" not in reps_responses:
        now = datetime.now()
        if now.timestamp() <= users_cache[login_hash]['expires']:
            if users_cache[login_hash].get("session", "unverified") == "unverified":
                new_time = now + timedelta(hours=3)
                result = await ekv_db.update_user(login_hash,
                    date=new_time.strftime("%Y-%m-%d %H:%M:%S"),
                    session="sleep")

                async with LOCK:
                    if login_hash in users_cache:
                        users_cache[login_hash]['date'] = new_time.strftime("%Y-%m-%d %H:%M:%S")
                        users_cache[login_hash]['session'] = "sleep"

                logger.info(f"Updated user with 'date' set to {new_time}")
                return {}

            if users_cache[login_hash]['session'] == "sleep":
                result = await ekv_db.update_user(login_hash, session="verified")
                async with LOCK: users_cache[login_hash]['session'] = "verified"
                raise AuthenticationError("No 'items' in reprimands response. "
                    f"{reps_responses['result_text']}")

    result = await ekv_db.update_user(login_hash, session="unverified")
    async with LOCK: users_cache[login_hash]['session'] = "unverified"

    logger.info(f"Fetched {len(reps_responses['items'])} reprimands")

    #Fields: a010 = card_number, dt = datetime
    reprimands = {}
    for reprimand in reps_responses['items']:
        if not reprimand['signature']:
            reprimands[reprimand['a010']] = {'chat_ids': chat_ids}
            reprimands[reprimand['a010']]['owner'] = reprimand['owner']
            reprimands[reprimand['a010']]['dt'] = reprimand['dt']
            reprimands[reprimand['a010']]['signature'] = reprimand['signature']
            reprimands[reprimand['a010']]['cookies'] = cookies

    return reprimands


async def comments_sender(comments: dict[int, dict]) -> None:
    for card_number, details in comments.items():
        message = f"Комментарий к карте вызова от {details['dt']} \"{details['diagnosis']}\""
        markup = TelebotTypes.InlineKeyboardMarkup()
        button = TelebotTypes.InlineKeyboardButton(f"Отозвать подпись {details['owner']}",
            callback_data=json.dumps([card_number, details['user']]))
        markup.add(button)

        for chat_id in details["chat_ids"]:
            if details['signature']:
                await message_queue.put((chat_id, message, markup))

            else:
                await message_queue.put((chat_id, message))


async def reprimands_sender(reprimands: dict[int, dict]) -> None:
    messages = {}
    for details in reprimands.values():
        owner = details['owner']
        if owner not in messages:
            messages[owner] = {'message': "", 'chat_ids': details['chat_ids']}

        messages[owner]['message'] += f"Замечание к карте от {details['dt']} \"{details['diagnosis']}\"\n"

    for owner, details in messages.items():
        for chat_id in details['chat_ids']:
            await message_queue.put((chat_id, details['message']))


#Initial fetch
async def new_user_fetch(login_hash: str, chat_id: int) -> None:
    cookies = users_cache[login_hash]['cookies']

    #Fetching reprimands
    reprimands = await fetch_reprimands(login_hash, [chat_id], cookies)
    if reprimands:
        reprimands_diag = await fetch_new_diagnoses(reprimands)
        result_sender, result_db_r = await asyncio.gather(
            reprimands_sender(reprimands_diag),
            ekv_db.save_to_db(reprimands_diag, table="reprimands"))

        if result_db_r > 0: logger.info(f"Saved {result_db_r} new reprimands to db")

    else:
        await message_queue.put((chat_id, "Неподписанных замечаний нет"))

    #Fetching comments
    comments = await fetch_cards(login_hash, [chat_id], cookies)
    if comments:
        comments_diag = await fetch_new_diagnoses(comments)
        result_sender, result_db_c = await asyncio.gather(
            comments_sender(comments_diag),
            ekv_db.save_to_db(comments_diag, table="comments"))

        if result_db_c > 0: logger.info(f"Saved {result_db_c} comments to db")

    else:
        await message_queue.put((chat_id, "Неотработанных комментариев нет"))


async def fetch_func(fetch_function: Callable) -> dict[int, dict]:
    if ekv_api.api_paused():
        logger.warning(f"API is paused. Skipping {fetch_function.__name__}")
        return {}

    tasks = []
    now = datetime.now()
    async with LOCK: users = list(users_cache.items())

    for login_hash, user_data in users:
        if (not user_data.get("date") or
            now > datetime.strptime(user_data['date'], "%Y-%m-%d %H:%M:%S")):

            tasks.append(fetch_function(
                login_hash,
                user_data['chat_ids'],
                user_data['cookies']))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    dict_results = {}
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"{fetch_function.__name__} failed: {result}")
            continue

        dict_results.update(result)
    return dict_results


@sleep_scheduler()
async def fetcher(delay: int) -> None:
    now = datetime.now()
    if now.hour == _SCHEDULE_TIME[0] and _SCHEDULE_TIME[1] <= now.minute < _SCHEDULE_TIME[1] + (_CYCLE / 60):
        start_time = monotonic()
        reprimands_ekv, reprimands_db = await asyncio.gather(
            fetch_func(fetch_reprimands),
            ekv_db.get_from_db(table="reprimands"))
    
        reprimands_new = {key: reprimands_ekv[key] for key
            in reprimands_ekv.keys() - reprimands_db.keys()}
    
        signed_reprimands = {key: reprimands_db[key] for key
            in reprimands_db.keys() - reprimands_ekv.keys()}
    
        reprimands_new_diag = await fetch_new_diagnoses(reprimands_new)
        await reprimands_sender(reprimands_new_diag)

        result_r, result_sr = await asyncio.gather(
            ekv_db.save_to_db(reprimands_new_diag, table="reprimands"),
            ekv_db.save_to_db(signed_reprimands, table="reprimands"))

        if result_r > 0: logger.info(f"Saved {result_r} new reprimands to db")
        if result_sr > 0: logger.info(f"Updated {result_sr} signed reprimands to db")
    
    #-----------------
        elapsed = monotonic() - start_time
        remaining = (delay / 2) - elapsed + random.uniform(1 / _LOAD_FACTOR, 10 / _LOAD_FACTOR)
        if remaining > 0:
            logger.info(f"Sleeping for {remaining:.0f}s after fetching reprimands")
            await asyncio.sleep(remaining)
    #-----------------

    comments_ekv, comments_db = await asyncio.gather(
        fetch_func(fetch_cards),
        ekv_db.get_from_db(table="comments"))

    comments_new = {key: comments_ekv[key] for key
        in comments_ekv.keys() - comments_db.keys()}

    comments_new_diag = await fetch_new_diagnoses(comments_new)
    await comments_sender(comments_new_diag)

    result_c = await ekv_db.save_to_db(comments_new_diag, table="comments")
    if result_c > 0: logger.info(f"Saved {result_c} comments to db")


async def main_loop():
    delay = _CYCLE / _LOAD_FACTOR
    logger.info("Starting main loop...")
    no_users = False
    while True:
        if not users_cache:
            if not no_users:
                logger.warning("No users were found. Aborted")
                no_users = True

            await asyncio.sleep(60)
            continue

        no_users = False

        start_time = monotonic()
        try:
            await fetcher(delay)

        except Exception as error:
            logger.error(error)

        elapsed = monotonic() - start_time
        sleep = delay - elapsed
        if sleep > 0:
            sleep_time = sleep + random.uniform(5 / _LOAD_FACTOR, 15 / _LOAD_FACTOR)
            logger.info(f"Sleeping for {sleep_time:.0f}s in main loop")
            await asyncio.sleep(sleep_time)


async def main():
    global users_cache, message_queue, _LOAD_FACTOR

    active_users = await ekv_db.active_users()
    _LOAD_FACTOR = round(max(active_users / 10, 1))

    try:
        message_queue = asyncio.Queue()
        asyncio.create_task(message_worker(message_queue))
        asyncio.create_task(ekv_api.manager(_LOAD_FACTOR))

        async with LOCK: users_cache = await ekv_db.get_user()
        await asyncio.sleep(0.5)

        asyncio.create_task(main_loop())

        logger.info("Starting polling...")
        while True:
            try:
                await bot.infinity_polling(skip_pending=True)

            except (httpx.ConnectError, httpx.TimeoutException) as error:
                logger.warning(f"Polling failed: {error}. Reconnecting...")
                await asyncio.sleep(5)

            except Exception as error:
                logger.error("Unexpected error has occurred during polling: "
                    f"{error}", exc_info=True)

                await asyncio.sleep(10)

    finally:
        await ekv_api.close_connection()
        await ekv_db.close_database()


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except (KeyboardInterrupt, EOFError):
        logger.info("Interrupted by user. Exiting")
        sys.exit(0)
