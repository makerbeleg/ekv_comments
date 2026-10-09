
import os
import logging
import json
import asyncio
import aiosqlite
from contextlib import asynccontextmanager
from datetime import datetime

logger = logging.getLogger(__name__)

EKV_DB = os.getenv("EKV_DB", "EKV_DATABASE.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    chat_ids TEXT,
    login TEXT PRIMARY KEY,
    password TEXT,
    cookies TEXT,
    expires INTEGER,
    session TEXT,
    date TEXT);
CREATE TABLE IF NOT EXISTS comments (
    card_number INTEGER PRIMARY KEY,
    owner TEXT,
    dt TEXT);
CREATE TABLE IF NOT EXISTS reprimands (
    card_number INTEGER PRIMARY KEY,
    signature TEXT,
    owner TEXT,
    dt TEXT);
"""

_writer = None
_db_lock = asyncio.Lock()


@asynccontextmanager
async def _db_connection(*, write=False):
    global _writer

    if _writer is None:
        async with _db_lock:
            if _writer is None:
                _writer = await aiosqlite.connect(EKV_DB)
                _writer.row_factory = aiosqlite.Row
                await _writer.execute("PRAGMA journal_mode = WAL")
                await _writer.executescript(_SCHEMA)
                await _writer.commit()

    if not write:
        async with aiosqlite.connect(EKV_DB) as reader:
            reader.row_factory = aiosqlite.Row
            yield reader
        return

    async with _db_lock:
        try:
            yield _writer
            await _writer.commit()
    
        except BaseException:
            await _writer.rollback()
            raise


async def get_user(login_hash=None):
    if login_hash:
        query, parameters = """SELECT * FROM users WHERE login = ?""", (login_hash,)

    else:
        query, parameters = """SELECT * FROM users""", ()

    async with _db_connection(write=False) as connection:
        async with connection.execute(query, parameters) as cursor:
            rows = [await cursor.fetchone()] if login_hash else await cursor.fetchall()

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
    query = """SELECT COUNT(*) FROM users
        WHERE date IS NULL OR date < ?"""

    parameters = (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),)

    async with _db_connection(write=False) as connection:
        async with connection.execute(query, parameters) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0


async def add_user(chat_id, login_hash, password_hash, cookies, expires):
    query = """INSERT INTO users (chat_ids, login, password, cookies, expires)
        VALUES (:chat_ids, :login, :password, :cookies, :expires)
        ON CONFLICT(login) DO NOTHING"""

    parameters = {
        'chat_ids': json.dumps(chat_id),
        'login': login_hash,
        'password': password_hash,
        'cookies': json.dumps(dict(cookies)),
        'expires': expires
        }

    async with _db_connection(write=True) as connection:
        async with connection.execute(query, parameters) as cursor:
            return cursor.rowcount


async def update_user(login_hash, password=None, chat_ids=None, cookies=None, expires=None, session=None, date=None):
    updates = {}
    if password is not None: updates['password'] = password
    if chat_ids is not None: updates['chat_ids'] = json.dumps(chat_ids)
    if cookies is not None: updates['cookies'] = json.dumps(dict(cookies))
    if expires is not None: updates['expires'] = expires
    if session is not None: updates['session'] = session
    if date is not None: updates['date'] = date

    if not updates or login_hash is None:
        return 0

    columns = ", ".join(f"{column} = ?" for column in updates.keys())
    query = f"""UPDATE users SET {columns} WHERE login = ?"""
    parameters = tuple(updates.values()) + (login_hash,)

    async with _db_connection(write=True) as connection:
        async with connection.execute(query, parameters) as cursor:
            return cursor.rowcount


async def remove_user(login_hash):
    query, parameters = """DELETE FROM users WHERE login = ?""", (login_hash,)

    async with _db_connection(write=True) as connection:
        async with connection.execute(query, parameters) as cursor:
            return cursor.rowcount


async def get_from_db(table="comments"):
    if table == "reprimands": query = """SELECT * FROM reprimands"""
    elif table == "comments": query = """SELECT * FROM comments"""

    async with _db_connection(write=False) as connection:
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
            VALUES (:card_number, :signature, :owner, :dt)
            ON CONFLICT(card_number) DO
            UPDATE SET signature = excluded.signature"""

    elif table == "comments":
        query = """INSERT INTO comments (card_number, owner, dt)
            VALUES (:card_number, :owner, :dt)
            ON CONFLICT(card_number) DO NOTHING"""

    rows = []
    for card_number, details in items_list.items():
        dt = datetime.strptime(details['dt'], "%d.%m.%Y %H:%M")
        parameters = {'card_number': card_number, 'owner': details['owner'], 'dt': dt.strftime("%Y-%m-%d")}
        if table == "reprimands":
            parameters['signature'] = details['signature']

        rows.append(parameters)

    async with _db_connection(write=True) as connection:
        async with connection.executemany(query, rows) as cursor:
            return cursor.rowcount


async def cleanup(card_number=None):
    query_cn, parameters = """DELETE FROM comments
        WHERE card_number = ?""", (card_number,)

    query_c = """DELETE FROM comments
        WHERE dt < date('now', '-8 days')"""

    query_r = """DELETE FROM reprimands
        WHERE signature IS NOT NULL AND signature != ''"""

    async with _db_connection(write=True) as connection:
        if card_number is not None:
            async with connection.execute(query_cn, parameters) as cursor_cn:
                return {'comments': cursor_cn.rowcount, 'reprimands': 0}

        async with (connection.execute(query_c) as cursor_c,
                    connection.execute(query_r) as cursor_r):
            return {'comments': cursor_c.rowcount, 'reprimands': cursor_r.rowcount}


async def close_database():
    global _writer

    async with _db_lock:
        if _writer:
            await _writer.close()
            logger.info("DB connection was closed")

        _writer = None
