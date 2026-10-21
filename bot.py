import asyncio

import os

import json

import hashlib

from datetime import datetime, timedelta

from zoneinfo import ZoneInfo

import asyncpg

from aiohttp import web, ClientSession, ClientTimeout

from aiogram import Bot, Dispatcher, F, Router

from aiogram.filters import Command, CommandStart

from aiogram.types import (

    Message,

    CallbackQuery,

    InlineKeyboardMarkup,

    InlineKeyboardButton,

)

from aiogram.enums import ParseMode

from aiogram.client.default import DefaultBotProperties

from aiogram.exceptions import TelegramBadRequest

# =========================================================

# НАЛАШТУВАННЯ

# =========================================================

TOKEN = os.getenv("BOT_TOKEN")

DATABASE_URL = os.getenv("DATABASE_URL")

if not TOKEN:

    raise RuntimeError("BOT_TOKEN environment variable is not set")

if not DATABASE_URL:

    raise RuntimeError("DATABASE_URL environment variable is not set")

TZ = ZoneInfo("Europe/Kyiv")

SCHEDULE_URL = "https://be-svitlo.oe.if.ua/schedule-by-queue"

QUEUES = [

    f"{number}.{sub}"

    for number in range(1, 7)

    for sub in (1, 2)

]

# Перевірка графіка кожні 5 хвилин

CHECK_INTERVAL = 300

# Попередження перед відключенням

WARNING_MINUTES = 30

router = Router()

db_pool = None

http_session = None

# =========================================================

# POSTGRESQL / SUPABASE

# =========================================================

async def init_database():

    global db_pool

    db_pool = await asyncpg.create_pool(

        DATABASE_URL,

        min_size=1,

        max_size=5,

        command_timeout=30,

    )

    async with db_pool.acquire() as conn:

        await conn.execute("""

            CREATE TABLE IF NOT EXISTS users (

                telegram_id BIGINT PRIMARY KEY,

                queue TEXT,

                notifications BOOLEAN NOT NULL DEFAULT TRUE,

                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()

            )

        """)

        await conn.execute("""

            CREATE TABLE IF NOT EXISTS sent_notifications (

                notification_key TEXT PRIMARY KEY,

                sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW()

            )

        """)

        await conn.execute("""

            CREATE TABLE IF NOT EXISTS schedule_cache (

                queue TEXT PRIMARY KEY,

                schedule_hash TEXT,

                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()

            )

        """)

    print(

        "PostgreSQL database initialized",

        flush=True

    )

async def get_user(user_id):

    async with db_pool.acquire() as conn:

        row = await conn.fetchrow(

            """

            SELECT queue, notifications

            FROM users

            WHERE telegram_id=$1

            """,

            user_id,

        )

    return row

async def save_user_queue(user_id, queue):

    async with db_pool.acquire() as conn:

        await conn.execute(

            """

            INSERT INTO users (

                telegram_id,

                queue,

                notifications,

                created_at,

                updated_at

            )

            VALUES ($1, $2, TRUE, NOW(), NOW())

            ON CONFLICT (telegram_id)

            DO UPDATE SET

                queue=EXCLUDED.queue,

                updated_at=NOW()

            """,

            user_id,

            queue,

        )

async def set_notifications(user_id, value):

    async with db_pool.acquire() as conn:

        await conn.execute(

            """

            UPDATE users

            SET notifications=$1,

                updated_at=NOW()

            WHERE telegram_id=$2

            """,

            value,

            user_id,

        )

async def get_notification_users():

    async with db_pool.acquire() as conn:

        rows = await conn.fetch(

            """

            SELECT telegram_id, queue

            FROM users

            WHERE notifications=TRUE

              AND queue IS NOT NULL

            """

        )

    return rows

async def notification_was_sent(key):

    async with db_pool.acquire() as conn:

        row = await conn.fetchrow(

            """

            SELECT notification_key

            FROM sent_notifications

            WHERE notification_key=$1

            """,

            key,

        )

    return row is not None

async def mark_notification_sent(key):

    async with db_pool.acquire() as conn:

        await conn.execute(

            """

            INSERT INTO sent_notifications (

                notification_key,

                sent_at

            )

            VALUES ($1, NOW())

            ON CONFLICT (notification_key)

            DO NOTHING

            """,

            key,

        )

async def get_cached_schedule_hash(queue):

    async with db_pool.acquire() as conn:

        return await conn.fetchval(

            """

            SELECT schedule_hash

            FROM schedule_cache

            WHERE queue=$1

            """,

            queue,

        )

async def save_schedule_hash(queue, schedule_hash):

    async with db_pool.acquire() as conn:

        await conn.execute(

            """

            INSERT INTO schedule_cache (

                queue,

                schedule_hash,

                updated_at

            )

            VALUES ($1, $2, NOW())

            ON CONFLICT (queue)

            DO UPDATE SET

                schedule_hash=EXCLUDED.schedule_hash,

                updated_at=NOW()

            """,

            queue,

            schedule_hash,

        )

# =========================================================

# КЛАВІАТУРИ

# =========================================================

def queue_keyboard():

    rows = []

    for i in range(0, len(QUEUES), 2):

        rows.append([

            InlineKeyboardButton(

                text=QUEUES[i],

                callback_data=f"queue:{QUEUES[i]}"

            ),

            InlineKeyboardButton(

                text=QUEUES[i + 1],

                callback_data=f"queue:{QUEUES[i + 1]}"

            ),

        ])

    rows.append([

        InlineKeyboardButton(

            text="🔎 Не знаю свою підчергу",

            url="https://svitlo.oe.if.ua/"

        )

    ])

    return InlineKeyboardMarkup(

        inline_keyboard=rows

    )

def main_keyboard(notifications_enabled=True):

    notification_text = (

        "🔔 Сповіщення: УВІМК."

        if notifications_enabled

        else "🔕 Сповіщення: ВИМК."

    )

    return InlineKeyboardMarkup(

        inline_keyboard=[

            [

                InlineKeyboardButton(

                    text="⚡ Стан зараз",

                    callback_data="status"

                ),

                InlineKeyboardButton(

                    text="📅 Сьогодні",

                    callback_data="today"

                ),

            ],

            [

                InlineKeyboardButton(

                    text="📆 Завтра",

                    callback_data="tomorrow"

                ),

                InlineKeyboardButton(

                    text="🔄 Змінити підчергу",

                    callback_data="change_queue"

                ),

            ],

            [

                InlineKeyboardButton(

                    text=notification_text,

                    callback_data="notifications"

                )

            ],

        ]

    )

# =========================================================

# TELEGRAM HELPERS

# =========================================================

async def safe_edit(

    message,

    text,

    reply_markup=None

):

    try:

        await message.edit_text(

            text,

            reply_markup=reply_markup

        )

    except TelegramBadRequest as error:

        if "message is not modified" in str(error).lower():

            return

        raise

async def get_main_keyboard_for_user(user_id):

    user = await get_user(user_id)

    enabled = True

    if user is not None:

        enabled = bool(user["notifications"])

    return main_keyboard(enabled)

# =========================================================

# API ПРИКАРПАТТЯОБЛЕНЕРГО

# =========================================================

async def fetch_schedule(queue):

    global http_session

    headers = {

        "User-Agent": (

            "Mozilla/5.0 "

            "(iPhone; CPU iPhone OS 18_0 like Mac OS X) "

            "AppleWebKit/605.1.15 "

            "(KHTML, like Gecko) "

            "Version/18.0 Mobile/15E148 Safari/604.1"

        ),

        "Accept": "application/json, text/plain, */*",

        "Origin": "https://svitlo.oe.if.ua",

        "Referer": "https://svitlo.oe.if.ua/",

    }

    async with http_session.get(

        SCHEDULE_URL,

        params={"queue": queue},

        headers=headers,

    ) as response:

        print(

            f"SCHEDULE {queue} STATUS:",

            response.status,

            flush=True

        )

        if response.status != 200:

            raise RuntimeError(

                f"Schedule server returned HTTP {response.status}"

            )

        data = await response.json(

            content_type=None

        )

        print(

            f"SCHEDULE {queue} DATA:",

            data,

            flush=True

        )

        return data

# =========================================================

# ДАТИ / ЧАС

# =========================================================

def normalize_date(value):

    if not value:

        return None

    value = str(value).strip()

    formats = [

        "%d.%m.%Y",

        "%Y-%m-%d",

        "%Y-%m-%dT%H:%M:%S",

        "%Y-%m-%dT%H:%M:%S.%f",

    ]

    for fmt in formats:

        try:

            return datetime.strptime(

                value,

                fmt

            ).date()

        except ValueError:

            continue

    try:

        return datetime.fromisoformat(

            value.replace("Z", "+00:00")

        ).date()

    except Exception:

        return None

def find_day(data, target_date):

    if not isinstance(data, list):

        return None

    matches = []

    for item in data:

        if not isinstance(item, dict):

            continue

        event_date = normalize_date(

            item.get("eventDate")

        )

        if event_date == target_date:

            matches.append(item)

    if not matches:

        return None

    return matches[-1]

def get_periods(day_data, queue):

    if not isinstance(day_data, dict):

        return []

    queues = day_data.get(

        "queues",

        {}

    )

    if not isinstance(queues, dict):

        return []

    periods = queues.get(

        queue,

        []

    )

    if not isinstance(periods, list):

        return []

    return periods

def time_to_minutes(value):

    try:

        parts = str(value).split(":")

        hour = int(parts[0])

        minute = int(parts[1])

        return hour * 60 + minute

    except Exception:

        return 0

def period_contains_now(period, now):

    start = time_to_minutes(

        period.get("from", "00:00")

    )

    end = time_to_minutes(

        period.get("to", "00:00")

    )

    current = (

        now.hour * 60

        + now.minute

    )

    if end == 0 and start > 0:

        end = 24 * 60

    return start <= current < end

def minutes_until(time_value, now):

    target = time_to_minutes(

        time_value

    )

    current = (

        now.hour * 60

        + now.minute

    )

    return target - current

# =========================================================

# СТАТУСИ ГРАФІКА

# =========================================================

def parse_status(value):

    try:

        return int(value)

    except Exception:

        return 0

def status_text(status):

    status = parse_status(status)

    if status == 1:

        return "🔴 Вимкнення за графіком"

    if status != 0:

        return "🟡 Можливе вимкнення"

    return "🟢 Світло має бути"

def format_period(period):

    start = period.get(

        "from",

        "--:--"

    )

    end = period.get(

        "to",

        "--:--"

    )

    status = parse_status(

        period.get("status", 0)

    )

    if status == 1:

        icon = "🔴"

        label = "вимкнення"

    elif status != 0:

        icon = "🟡"

        label = "можливе вимкнення"

    else:

        icon = "🟢"

        label = "світло має бути"

    return (

        f"{icon} "

        f"<b>{start}–{end}</b> — "

        f"{label}"

    )

# =========================================================

# КОЛИ АКТИВНОГО ГРАФІКА НЕМАЄ

# =========================================================

def no_active_schedule_text(queue, mode):

    if mode == "status":

        return (

            "<b>⚡ Стан зараз</b>\n"

            f"Підчерга: <b>{queue}</b>\n\n"

            "🟢 <b>Активний графік ГПВ "

            "зараз не опублікований.</b>\n\n"

            "За даними сервісу Прикарпаттяобленерго "

            "для цієї підчерги активного графіка "

            "погодинних вимкнень немає.\n\n"

            "⚠️ Аварійні або локальні планові "

            "відключення можуть відбуватися "

            "незалежно від ГПВ.\n\n"

            "Джерело: Прикарпаттяобленерго"

        )

    if mode == "today":

        return (

            "<b>📅 Графік на сьогодні</b>\n"

            f"Підчерга: <b>{queue}</b>\n\n"

            "🟢 <b>Активний графік ГПВ "

            "на сьогодні не опублікований.</b>\n\n"

            "Джерело: Прикарпаттяобленерго"

        )

    return (

        "<b>📆 Графік на завтра</b>\n"

        f"Підчерга: <b>{queue}</b>\n\n"

        "ℹ️ <b>Активний графік ГПВ "

        "на завтра поки не опублікований.</b>\n\n"

        "Якщо Прикарпаттяобленерго опублікує "

        "новий графік, бот автоматично "

        "побачить зміну.\n\n"

        "Джерело: Прикарпаттяобленерго"

    )

# =========================================================

# ФОРМАТУВАННЯ ГРАФІКА

# =========================================================

def format_day(

    day_data,

    queue,

    title,

    mode

):

    if not day_data:

        return no_active_schedule_text(

            queue,

            mode

        )

    periods = get_periods(

        day_data,

        queue

    )

    lines = [

        f"<b>{title}</b>",

        f"Підчерга: <b>{queue}</b>",

        "",

    ]

    if not periods:

        lines.append(

            "🟢 Запланованих вимкнень "

            "для цієї підчерги не знайдено."

        )

    else:

        for period in periods:

            lines.append(

                format_period(period)

            )

    approved = day_data.get(

        "scheduleApprovedSince"

    )

    if approved:

        lines.extend([

            "",

            f"🕐 Оновлено: {approved}"

        ])

    lines.extend([

        "",

        "Джерело: Прикарпаттяобленерго"

    ])

    return "\n".join(lines)

def format_current(day_data, queue):

    if not day_data:

        return no_active_schedule_text(

            queue,

            "status"

        )

    now = datetime.now(TZ)

    periods = get_periods(

        day_data,

        queue

    )

    current_period = None

    for period in periods:

        if period_contains_now(

            period,

            now

        ):

            current_period = period

            break

    if current_period:

        state = status_text(

            current_period.get(

                "status",

                0

            )

        )

        until = current_period.get(

            "to",

            "--:--"

        )

        text = (

            "<b>⚡ Стан зараз</b>\n"

            f"Підчерга: <b>{queue}</b>\n\n"

            f"{state}\n"

            f"До: <b>{until}</b>"

        )

    else:

        text = (

            "<b>⚡ Стан зараз</b>\n"

            f"Підчерга: <b>{queue}</b>\n\n"

            "🟢 <b>За графіком світло має бути.</b>\n\n"

            "На цей час вимкнення "

            "за опублікованим графіком "

            "не заплановане."

        )

    approved = day_data.get(

        "scheduleApprovedSince"

    )

    if approved:

        text += (

            f"\n\n🕐 Графік оновлено: "

            f"{approved}"

        )

    text += (

        "\n\n⚠️ Це інформація за графіком, "

        "а не перевірка фактичної наявності "

        "електроенергії.\n\n"

        "Джерело: Прикарпаттяобленерго"

    )

    return text

# =========================================================

# ОТРИМАННЯ ТЕКСТУ ГРАФІКА

# =========================================================

async def build_schedule_text(

    queue,

    mode

):

    data = await fetch_schedule(

        queue

    )

    if not data:

        return no_active_schedule_text(

            queue,

            mode

        )

    now = datetime.now(TZ)

    if mode == "status":

        day_data = find_day(

            data,

            now.date()

        )

        return format_current(

            day_data,

            queue

        )

    if mode == "today":

        day_data = find_day(

            data,

            now.date()

        )

        return format_day(

            day_data,

            queue,

            "📅 Графік на сьогодні",

            "today"

        )

    day_data = find_day(

        data,

        (

            now

            + timedelta(days=1)

        ).date()

    )

    return format_day(

        day_data,

        queue,

        "📆 Графік на завтра",

        "tomorrow"

    )

# =========================================================

# HASH ГРАФІКА

# =========================================================

def make_schedule_hash(data):

    encoded = json.dumps(

        data,

        ensure_ascii=False,

        sort_keys=True,

        separators=(",", ":")

    ).encode("utf-8")

    return hashlib.sha256(

        encoded

    ).hexdigest()

# =========================================================

# СПОВІЩЕННЯ ПРО ЗМІНУ ГРАФІКА

# =========================================================

async def notify_schedule_change(

    bot,

    queue,

    data

):

    new_hash = make_schedule_hash(

        data

    )

    old_hash = await get_cached_schedule_hash(

        queue

    )

    # Перший запуск:

    # просто запам'ятовуємо поточний стан,

    # щоб не розіслати старий графік усім.

    if old_hash is None:

        await save_schedule_hash(

            queue,

            new_hash

        )

        return

    if old_hash == new_hash:

        return

    await save_schedule_hash(

        queue,

        new_hash

    )

    async with db_pool.acquire() as conn:

        users = await conn.fetch(

            """

            SELECT telegram_id

            FROM users

            WHERE queue=$1

              AND notifications=TRUE

            """,

            queue,

        )

    if not users:

        return

    now = datetime.now(TZ)

    today_data = find_day(

        data,

        now.date()

    )

    tomorrow_data = find_day(

        data,

        (

            now

            + timedelta(days=1)

        ).date()

    )

    if today_data:

        body = format_day(

            today_data,

            queue,

            "📅 Оновлений графік на сьогодні",

            "today"

        )

    elif tomorrow_data:

        body = format_day(

            tomorrow_data,

            queue,

            "📆 Новий графік на завтра",

            "tomorrow"

        )

    elif not data:

        body = (

            f"Підчерга: <b>{queue}</b>\n\n"

            "🟢 Активний графік ГПВ "

            "зараз не опублікований."

        )

    else:

        body = (

            f"Підчерга: <b>{queue}</b>\n\n"

            "Прикарпаттяобленерго "

            "оновило інформацію щодо графіка.\n\n"

            "Перевірте актуальні дані в боті."

        )

    text = (

        "🔔 <b>Графік оновлено</b>\n\n"

        + body

    )

    for user in users:

        try:

            await bot.send_message(

                user["telegram_id"],

                text

            )

        except Exception as error:

            print(

                "CHANGE NOTIFICATION ERROR:",

                user["telegram_id"],

                repr(error),

                flush=True

            )

# =========================================================

# ПОПЕРЕДЖЕННЯ ПЕРЕД ВИМКНЕННЯМ

# =========================================================

async def check_upcoming_outages(

    bot,

    queue,

    data

):

    now = datetime.now(TZ)

    today_data = find_day(

        data,

        now.date()

    )

    if not today_data:

        return

    periods = get_periods(

        today_data,

        queue

    )

    async with db_pool.acquire() as conn:

        users = await conn.fetch(

            """

            SELECT telegram_id

            FROM users

            WHERE queue=$1

              AND notifications=TRUE

            """,

            queue,

        )

    if not users:

        return

    for period in periods:

        status = parse_status(

            period.get("status", 0)

        )

        # 0 трактуємо як період без ГПВ.

        # Інші значення поки логуються,

        # доки не перевіримо їх на живому графіку.

        if status == 0:

            continue

        if status not in (1, 2):

            print(

                "UNKNOWN SCHEDULE STATUS:",

                status,

                period,

                flush=True

            )

        start = period.get("from")

        end = period.get("to")

        if not start:

            continue

        remaining = minutes_until(

            start,

            now

        )

        # Ширше вікно, щоб 5-хвилинний цикл

        # не пропустив попередження.

        if not (

            WARNING_MINUTES - 5

            <= remaining

            <= WARNING_MINUTES

        ):

            continue

        for user in users:

            notification_key = (

                f"outage-warning:"

                f"{user['telegram_id']}:"

                f"{now.date()}:"

                f"{queue}:"

                f"{start}:"

                f"{end}:"

                f"{status}"

            )

            if await notification_was_sent(

                notification_key

            ):

                continue

            if status == 1:

                heading = (

                    "⚠️ <b>Наближається "

                    "відключення за графіком</b>"

                )

            else:

                heading = (

                    "⚠️ <b>Наближається період "

                    "можливого відключення</b>"

                )

            text = (

                f"{heading}\n\n"

                f"Підчерга: <b>{queue}</b>\n"

                f"Початок: <b>{start}</b>\n"

                f"До: <b>{end or '--:--'}</b>\n\n"

                f"До початку приблизно "

                f"<b>{remaining} хв.</b>\n\n"

                "Джерело: Прикарпаттяобленерго"

            )

            try:

                await bot.send_message(

                    user["telegram_id"],

                    text

                )

                await mark_notification_sent(

                    notification_key

                )

            except Exception as error:

                print(

                    "WARNING ERROR:",

                    user["telegram_id"],

                    repr(error),

                    flush=True

                )

# =========================================================

# ФОНОВИЙ МОНІТОРИНГ

# =========================================================

async def schedule_monitor(bot):

    await asyncio.sleep(10)

    while True:

        try:

            users = await get_notification_users()

            active_queues = sorted(

                {

                    row["queue"]

                    for row in users

                    if row["queue"]

                }

            )

            for queue in active_queues:

                try:

                    data = await fetch_schedule(

                        queue

                    )

                    await notify_schedule_change(

                        bot,

                        queue,

                        data

                    )

                    await check_upcoming_outages(

                        bot,

                        queue,

                        data

                    )

                except Exception as error:

                    print(

                        "MONITOR QUEUE ERROR:",

                        queue,

                        repr(error),

                        flush=True

                    )

                await asyncio.sleep(1)

        except asyncio.CancelledError:

            raise

        except Exception as error:

            print(

                "MONITOR ERROR:",

                repr(error),

                flush=True

            )

        await asyncio.sleep(

            CHECK_INTERVAL

        )

# =========================================================

# ГОЛОВНЕ МЕНЮ

# =========================================================

async def show_main_menu(message, user_id):

    user = await get_user(

        user_id

    )

    if not user or not user["queue"]:

        await message.answer(

            "⚡ <b>Струм ОК</b>\n\n"

            "Бот показує актуальні графіки "

            "відключення електроенергії "

            "для підчерг Прикарпаттяобленерго.\n\n"

            "Оберіть свою підчергу:",

            reply_markup=queue_keyboard()

        )

        return

    await message.answer(

        "⚡ <b>Струм ОК</b>\n\n"

        f"Ваша підчерга: "

        f"<b>{user['queue']}</b>\n\n"

        "Оберіть потрібний розділ:",

        reply_markup=main_keyboard(

            bool(user["notifications"])

        )

    )

# =========================================================

# /START

# =========================================================

@router.message(CommandStart())

async def start_command(message: Message):

    await show_main_menu(

        message,

        message.from_user.id

    )

# =========================================================

# /STATUS /TODAY /TOMORROW

# =========================================================

async def command_schedule(

    message,

    mode

):

    user = await get_user(

        message.from_user.id

    )

    if not user or not user["queue"]:

        await message.answer(

            "Спочатку оберіть свою підчергу:",

            reply_markup=queue_keyboard()

        )

        return

    try:

        text = await build_schedule_text(

            user["queue"],

            mode

        )

        await message.answer(

            text,

            reply_markup=main_keyboard(

                bool(user["notifications"])

            )

        )

    except Exception as error:

        print(

            "COMMAND SCHEDULE ERROR:",

            repr(error),

            flush=True

        )

        await message.answer(

            "⚠️ <b>Не вдалося отримати графік.</b>\n\n"

            "Сервіс Прикарпаттяобленерго "

            "тимчасово не відповідає.\n\n"

            "Спробуйте ще раз через кілька хвилин.",

            reply_markup=main_keyboard(

                bool(user["notifications"])

            )

        )

@router.message(Command("status"))

async def status_command(message: Message):

    await command_schedule(

        message,

        "status"

    )

@router.message(Command("today"))

async def today_command(message: Message):

    await command_schedule(

        message,

        "today"

    )

@router.message(Command("tomorrow"))

async def tomorrow_command(message: Message):

    await command_schedule(

        message,

        "tomorrow"

    )

# =========================================================

# /QUEUE

# =========================================================

@router.message(Command("queue"))

async def queue_command(message: Message):

    await message.answer(

        "🔄 <b>Зміна підчерги</b>\n\n"

        "Оберіть свою підчергу:",

        reply_markup=queue_keyboard()

    )

# =========================================================

# /NOTIFICATIONS

# =========================================================

@router.message(Command("notifications"))

async def notifications_command(

    message: Message

):

    user = await get_user(

        message.from_user.id

    )

    if not user or not user["queue"]:

        await message.answer(

            "Спочатку оберіть свою підчергу:",

            reply_markup=queue_keyboard()

        )

        return

    new_value = not bool(

        user["notifications"]

    )

    await set_notifications(

        message.from_user.id,

        new_value

    )

    if new_value:

        text = (

            "🔔 <b>Сповіщення увімкнено.</b>\n\n"

            "Бот стежитиме за змінами графіка "

            "для вашої підчерги."

        )

    else:

        text = (

            "🔕 <b>Сповіщення вимкнено.</b>\n\n"

            "Автоматичні повідомлення "

            "надсилатися не будуть."

        )

    await message.answer(

        text,

        reply_markup=main_keyboard(

            new_value

        )

    )

# =========================================================

# ВИБІР ПІДЧЕРГИ

# =========================================================

@router.callback_query(

    F.data.startswith("queue:")

)

async def choose_queue(

    callback: CallbackQuery

):

    queue = callback.data.split(

        ":",

        1

    )[1]

    if queue not in QUEUES:

        await callback.answer(

            "Невідома підчерга.",

            show_alert=True

        )

        return

    await save_user_queue(

        callback.from_user.id,

        queue

    )

    user = await get_user(

        callback.from_user.id

    )

    await callback.answer(

        "Підчергу збережено ✅"

    )

    await safe_edit(

        callback.message,

        (

            "✅ <b>Підчергу збережено</b>\n\n"

            f"Ваша підчерга: "

            f"<b>{queue}</b>\n\n"

            "Тепер можна переглядати графік "

            "та отримувати сповіщення."

        ),

        main_keyboard(

            bool(user["notifications"])

        )

    )

# =========================================================

# CALLBACK: ЗМІНИТИ ПІДЧЕРГУ

# =========================================================

@router.callback_query(

    F.data == "change_queue"

)

async def change_queue_callback(

    callback: CallbackQuery

):

    await callback.answer()

    await safe_edit(

        callback.message,

        "🔄 <b>Зміна підчерги</b>\n\n"

        "Оберіть свою підчергу:",

        queue_keyboard()

    )

# =========================================================

# CALLBACK: СПОВІЩЕННЯ

# =========================================================

@router.callback_query(

    F.data == "notifications"

)

async def notifications_callback(

    callback: CallbackQuery

):

    user = await get_user(

        callback.from_user.id

    )

    if not user or not user["queue"]:

        await callback.answer(

            "Спочатку оберіть підчергу.",

            show_alert=True

        )

        return

    new_value = not bool(

        user["notifications"]

    )

    await set_notifications(

        callback.from_user.id,

        new_value

    )

    if new_value:

        text = (

            "🔔 <b>Сповіщення увімкнено.</b>\n\n"

            f"Підчерга: <b>{user['queue']}</b>\n\n"

            "Бот стежитиме за змінами графіка."

        )

        alert = "Сповіщення увімкнено 🔔"

    else:

        text = (

            "🔕 <b>Сповіщення вимкнено.</b>\n\n"

            f"Підчерга: <b>{user['queue']}</b>\n\n"

            "Автоматичні повідомлення "

            "надсилатися не будуть."

        )

        alert = "Сповіщення вимкнено 🔕"

    await callback.answer(

        alert

    )

    await safe_edit(

        callback.message,

        text,

        main_keyboard(

            new_value

        )

    )

# =========================================================

# CALLBACK: ГРАФІК

# =========================================================

@router.callback_query(

    F.data.in_({

        "status",

        "today",

        "tomorrow"

    })

)

async def schedule_callback(

    callback: CallbackQuery

):

    user = await get_user(

        callback.from_user.id

    )

    if not user or not user["queue"]:

        await callback.answer(

            "Спочатку оберіть підчергу.",

            show_alert=True

        )

        return

    await callback.answer()

    try:

        text = await build_schedule_text(

            user["queue"],

            callback.data

        )

        await safe_edit(

            callback.message,

            text,

            main_keyboard(

                bool(user["notifications"])

            )

        )

    except Exception as error:

        print(

            "SCHEDULE CALLBACK ERROR:",

            repr(error),

            flush=True

        )

        await safe_edit(

            callback.message,

            (

                "⚠️ <b>Не вдалося отримати "

                "графік.</b>\n\n"

                "Сервіс Прикарпаттяобленерго "

                "тимчасово не відповідає.\n\n"

                "Спробуйте ще раз через "

                "кілька хвилин."

            ),

            main_keyboard(

                bool(user["notifications"])

            )

        )

# =========================================================

# HEALTH CHECK ДЛЯ RENDER

# =========================================================

async def health(request):

    return web.Response(

        text="Strum OK is running"

    )

# =========================================================

# ЗАПУСК

# =========================================================

async def main():

    global http_session

    print(

        "Starting Strum OK...",

        flush=True

    )

    # PostgreSQL / Supabase

    await init_database()

    # HTTP-сесія для API

    http_session = ClientSession(

        timeout=ClientTimeout(

            total=15

        )

    )

    # Health server для Render

    app = web.Application()

    app.router.add_get(

        "/",

        health

    )

    runner = web.AppRunner(

        app

    )

    await runner.setup()

    port = int(

        os.getenv(

            "PORT",

            10000

        )

    )

    site = web.TCPSite(

        runner,

        "0.0.0.0",

        port

    )

    await site.start()

    print(

        f"Health server started on port {port}",

        flush=True

    )

    # Telegram bot

    bot = Bot(

        TOKEN,

        default=DefaultBotProperties(

            parse_mode=ParseMode.HTML

        )

    )

    dp = Dispatcher()

    dp.include_router(

        router

    )

    monitor_task = asyncio.create_task(

        schedule_monitor(bot)

    )

    print(

        "Strum OK bot started",

        flush=True

    )

    try:

        await dp.start_polling(

            bot

        )

    finally:

        monitor_task.cancel()

        try:

            await monitor_task

        except asyncio.CancelledError:

            pass

        if http_session:

            await http_session.close()

        if db_pool:

            await db_pool.close()

        await bot.session.close()

        await runner.cleanup()

if __name__ == "__main__":

    asyncio.run(main())
