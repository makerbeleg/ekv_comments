
import os
import dotenv
import logging
import json
import aiosqlite
from datetime import datetime

dotenv.load_dotenv()

EKV_DB = os.getenv("EKV_DB", "EKV_DATABASE.db")
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

_connection = None



async def _get_connection():
    global _connection
    if _connection is None:
        _connection = await aiosqlite.connect(EKV_DB)
        _connection.row_factory = aiosqlite.Row

        await _connection.execute("""CREATE TABLE IF NOT EXISTS users (
            chat_ids TEXT,
            login TEXT PRIMARY KEY,
            password TEXT,
            cookies TEXT,
            expires INTEGER,
            session TEXT,
            date TEXT)""")
        await _connection.execute("""CREATE TABLE IF NOT EXISTS comments (
            card_number INTEGER PRIMARY KEY,
            owner TEXT,
            dt TEXT)""")
        await _connection.execute("""CREATE TABLE IF NOT EXISTS reprimands (
            card_number INTEGER PRIMARY KEY,
            signature TEXT,
            owner TEXT,
            dt TEXT)""")

        await _connection.commit()
    return _connection


async def get_user(login=None):
    if login:
        query, params = """SELECT * FROM users WHERE login = ?""", (login,)

    else:
        query, params = """SELECT * FROM users""", ()

    connection = await _get_connection()
    async with connection.execute(query, params) as cursor:
        rows = [await cursor.fetchone()] if login else await cursor.fetchall()

    results = {}
    for row in rows:
        if row:
            row_dict = dict(row)
            row_dict['chat_ids'] = json.loads(row_dict['chat_ids'])
            row_dict['cookies'] = json.loads(row_dict['cookies'])
            login = row_dict.pop("login")
            results[login] = row_dict

    return results


async def active_users():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    query = """SELECT COUNT(*) FROM users
        WHERE date IS NULL OR date < ?"""

    connection = await _get_connection()
    async with connection.execute(query, (now,)) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else 0


async def add_user(chat_id, login_hash, password_hash, cookies, expires):
    query = """INSERT INTO users (chat_ids, login, password, cookies, expires)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(login) DO NOTHING"""

    connection = await _get_connection()
    async with connection.execute(query, (
            json.dumps(chat_id),
            login_hash,
            password_hash,
            json.dumps(dict(cookies)),
            expires)) as cursor:
        await connection.commit()
        return cursor.rowcount


async def update_user(login, password=None, chat_ids=None, cookies=None, expires=None, session=None, date=None):
    updates = {}
    if password is not None: updates['password'] = password
    if chat_ids is not None: updates['chat_ids'] = json.dumps(chat_ids)
    if cookies is not None: updates['cookies'] = json.dumps(dict(cookies))
    if expires is not None: updates['expires'] = expires
    if session is not None: updates['session'] = session
    if date is not None: updates['date'] = date

    if not updates or login is None:
        return 0

    columns = ", ".join([ f"{column} = ?" for column in updates.keys() ])
    query = f"""UPDATE users SET {columns}
        WHERE login = ?"""
    parameters = list(updates.values())

    connection = await _get_connection()
    async with connection.execute(query, parameters + [login]) as cursor:
        await connection.commit()
        return cursor.rowcount


async def remove_user(login):
    query = """DELETE FROM users
        WHERE login = ?"""

    connection = await _get_connection()
    async with connection.execute(query, (login,)) as cursor:
        await connection.commit()
        return cursor.rowcount


async def get_from_db(table="comments"):
    if table == "reprimands": query = """SELECT * FROM reprimands"""
    elif table == "comments": query = """SELECT * FROM comments"""

    connection = await _get_connection()
    async with connection.execute(query) as cursor:
        rows = await cursor.fetchall()

    results = {}
    for row in rows:
        if row:
            row_dict = dict(row)
            card_number = row_dict.pop("card_number")
            results[card_number] = row_dict

    return results


async def save_to_db(items_list, table="comments"):
    if table == "reprimands":
        query = """INSERT INTO reprimands (card_number, signature, owner, dt)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(card_number) DO
            UPDATE SET signature = excluded.signature"""

    elif table == "comments":
        query = """INSERT INTO comments (card_number, owner, dt)
            VALUES (?, ?, ?)
            ON CONFLICT(card_number) DO NOTHING"""

    rows = []
    for card_number, details in items_list.items():
        dt = datetime.strptime(details['dt'], "%d.%m.%Y %H:%M")
        if table == "reprimands":
            rows.append((card_number, details['signature'], details['owner'], dt.strftime("%Y-%m-%d")))

        elif table == "comments":
            rows.append((card_number, details['owner'], dt.strftime("%Y-%m-%d")))

    connection = await _get_connection()
    async with connection.executemany(query, rows) as cursor:
        await connection.commit()
        return cursor.rowcount


async def cleanup(card_number=None):
    query_cn = """DELETE FROM comments
        WHERE card_number = ?"""

    query_c = """DELETE FROM comments
        WHERE dt < date('now', '-8 days')"""

    query_r = """DELETE FROM reprimands
        WHERE signature IS NOT NULL AND signature != ''"""

    connection = await _get_connection()
    if card_number:
        async with connection.execute(query_cn, (card_number,)) as query_cn:
            await connection.commit()
            return {'comments': query_cn.rowcount}

    async with (connection.execute(query_c) as cursor_c,
                connection.execute(query_r) as cursor_r):
        await connection.commit()
        return {'comments': cursor_c.rowcount, 'reprimands': cursor_r.rowcount}


async def close_database():
    global _connection

    if _connection:
        await _connection.close()
        logger.info("DB connection was closed")
        _connection = None

