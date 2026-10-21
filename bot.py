import asyncio

import os

import sqlite3

import hashlib

import json

from datetime import datetime, timedelta

from zoneinfo import ZoneInfo

from aiohttp import web, ClientSession, ClientTimeout

from aiogram import Bot, Dispatcher, F, Router

from aiogram.filters import CommandStart

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

if not TOKEN:

    raise RuntimeError("BOT_TOKEN environment variable is not set")

TZ = ZoneInfo("Europe/Kyiv")

SCHEDULE_URL = "https://be-svitlo.oe.if.ua/schedule-by-queue"

QUEUES = [

    f"{number}.{sub}"

    for number in range(1, 7)

    for sub in (1, 2)

]

# Як часто перевіряємо графік

CHECK_INTERVAL = 300  # 5 хвилин

# За скільки хвилин попереджати

WARNING_MINUTES = 30

router = Router()

# =========================================================

# БАЗА ДАНИХ

# =========================================================

db = sqlite3.connect(

    "strum_ok.db",

    check_same_thread=False

)

db.execute("""

CREATE TABLE IF NOT EXISTS users (

    telegram_id INTEGER PRIMARY KEY,

    queue TEXT,

    notifications INTEGER NOT NULL DEFAULT 1,

    created_at TEXT NOT NULL

)

""")

db.execute("""

CREATE TABLE IF NOT EXISTS sent_notifications (

    notification_key TEXT PRIMARY KEY,

    sent_at TEXT NOT NULL

)

""")

db.execute("""

CREATE TABLE IF NOT EXISTS schedule_cache (

    queue TEXT PRIMARY KEY,

    schedule_hash TEXT,

    updated_at TEXT

)

""")

db.commit()

def get_user(user_id):

    return db.execute(

        """

        SELECT queue, notifications

        FROM users

        WHERE telegram_id=?

        """,

        (user_id,)

    ).fetchone()

def get_notification_users():

    return db.execute(

        """

        SELECT telegram_id, queue

        FROM users

        WHERE notifications=1

        AND queue IS NOT NULL

        """

    ).fetchall()

def notification_was_sent(key):

    row = db.execute(

        """

        SELECT notification_key

        FROM sent_notifications

        WHERE notification_key=?

        """,

        (key,)

    ).fetchone()

    return row is not None

def mark_notification_sent(key):

    db.execute(

        """

        INSERT OR IGNORE INTO sent_notifications (

            notification_key,

            sent_at

        )

        VALUES (?, ?)

        """,

        (

            key,

            datetime.now(TZ).isoformat()

        )

    )

    db.commit()

# =========================================================

# КНОПКИ

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

def main_keyboard():

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

                    text="🔔 Сповіщення",

                    callback_data="notifications"

                ),

            ],

            [

                InlineKeyboardButton(

                    text="🧪 Тест сповіщення",

                    callback_data="test_notification"

                ),

            ],

        ]

    )

# =========================================================

# БЕЗПЕЧНЕ РЕДАГУВАННЯ TELEGRAM

# =========================================================

async def safe_edit(message, text, reply_markup=None):

    try:

        await message.edit_text(

            text,

            reply_markup=reply_markup

        )

    except TelegramBadRequest as error:

        if "message is not modified" in str(error).lower():

            return

        raise

# =========================================================

# API ПРИКАРПАТТЯОБЛЕНЕРГО

# =========================================================

async def fetch_schedule(queue):

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

    timeout = ClientTimeout(total=15)

    async with ClientSession(timeout=timeout) as session:

        async with session.get(

            SCHEDULE_URL,

            params={"queue": queue},

            headers=headers

        ) as response:

            print(

                f"SCHEDULE {queue} STATUS:",

                response.status,

                flush=True

            )

            if response.status != 200:

                raise RuntimeError(

                    f"Schedule server returned {response.status}"

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

# РОБОТА З ДАТАМИ

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

    queues = day_data.get("queues", {})

    if not isinstance(queues, dict):

        return []

    periods = queues.get(queue, [])

    if not isinstance(periods, list):

        return []

    return periods

def time_to_minutes(value):

    try:

        hour, minute = str(value).split(":")[:2]

        return int(hour) * 60 + int(minute)

    except Exception:

        return 0

def period_contains_now(period, now):

    start = time_to_minutes(

        period.get("from", "00:00")

    )

    end = time_to_minutes(

        period.get("to", "00:00")

    )

    current = now.hour * 60 + now.minute

    if end == 0 and start > 0:

        end = 24 * 60

    return start <= current < end

def minutes_until(time_value, now):

    target_minutes = time_to_minutes(

        time_value

    )

    current_minutes = (

        now.hour * 60

        + now.minute

    )

    return target_minutes - current_minutes

# =========================================================

# СТАТУСИ

# =========================================================

def parse_status(value):

    try:

        return int(value)

    except Exception:

        return 0

def status_text(status):

    status = parse_status(status)

    if status == 1:

        return "🔴 Світла немає за графіком"

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

        "Якщо буде опублікований новий графік, "

        "бот автоматично побачить його.\n\n"

        "Джерело: Прикарпаттяобленерго"

    )

# =========================================================

# ГРАФІК НА ДЕНЬ

# =========================================================

def format_day(day_data, queue, title):

    if not day_data:

        return no_active_schedule_text(

            queue,

            "today"

            if "сьогодні" in title.lower()

            else "tomorrow"

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

# =========================================================

# СТАН ЗАРАЗ

# =========================================================

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

            "🟢 <b>Світло має бути</b>\n\n"

            "На цей час вимкнення "

            "за графіком не заплановане."

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

        "\n\nДжерело: "

        "Прикарпаттяобленерго"

    )

    return text

# =========================================================

# HASH ГРАФІКА

# =========================================================

def make_schedule_hash(data):

    encoded = json.dumps(

        data,

        ensure_ascii=False,

        sort_keys=True

    ).encode("utf-8")

    return hashlib.sha256(

        encoded

    ).hexdigest()

# =========================================================

# СПОВІЩЕННЯ ПРО НОВИЙ ГРАФІК

# =========================================================

async def notify_schedule_change(

    bot,

    queue,

    data

):

    new_hash = make_schedule_hash(

        data

    )

    row = db.execute(

        """

        SELECT schedule_hash

        FROM schedule_cache

        WHERE queue=?

        """,

        (queue,)

    ).fetchone()

    old_hash = (

        row[0]

        if row

        else None

    )

    # Перший запуск просто запам'ятовуємо.

    # Не спамимо користувачів.

    if old_hash is None:

        db.execute(

            """

            INSERT OR REPLACE INTO schedule_cache (

                queue,

                schedule_hash,

                updated_at

            )

            VALUES (?, ?, ?)

            """,

            (

                queue,

                new_hash,

                datetime.now(TZ).isoformat()

            )

        )

        db.commit()

        return

    if old_hash == new_hash:

        return

    db.execute(

        """

        UPDATE schedule_cache

        SET schedule_hash=?,

            updated_at=?

        WHERE queue=?

        """,

        (

            new_hash,

            datetime.now(TZ).isoformat(),

            queue

        )

    )

    db.commit()

    users = db.execute(

        """

        SELECT telegram_id

        FROM users

        WHERE queue=?

        AND notifications=1

        """,

        (queue,)

    ).fetchall()

    if not users:

        return

    now = datetime.now(TZ)

    today_data = find_day(

        data,

        now.date()

    )

    if today_data:

        schedule_text = format_day(

            today_data,

            queue,

            "📅 Оновлений графік на сьогодні"

        )

    else:

        schedule_text = (

            "🔔 <b>Струм ОК</b>\n\n"

            "Прикарпаттяобленерго "

            "оновило інформацію щодо ГПВ.\n\n"

            f"Ваша підчерга: <b>{queue}</b>\n\n"

            "Перевірте актуальний графік у боті."

        )

    for user in users:

        try:

            await bot.send_message(

                user[0],

                (

                    "🔔 <b>Графік оновлено</b>\n\n"

                    + schedule_text

                )

            )

        except Exception as error:

            print(

                "CHANGE NOTIFICATION ERROR:",

                user[0],

                repr(error),

                flush=True

            )

# =========================================================

# ПОПЕРЕДЖЕННЯ ЗА 30 ХВ

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

    users = db.execute(

        """

        SELECT telegram_id

        FROM users

        WHERE queue=?

        AND notifications=1

        """,

        (queue,)

    ).fetchall()

    if not users:

        return

    for period in periods:

        status = parse_status(

            period.get("status", 0)

        )

        # Попереджаємо лише про реальне

        # або можливе вимкнення.

        if status == 0:

            continue

        start = period.get("from")

        if not start:

            continue

        remaining = minutes_until(

            start,

            now

        )

        # Оскільки перевірка кожні 5 хв,

        # беремо невелике вікно.

        if (

            WARNING_MINUTES - 4

            <= remaining

            <= WARNING_MINUTES

        ):

            end = period.get(

                "to",

                "--:--"

            )

            for user in users:

                notification_key = (

                    f"warning:"

                    f"{user[0]}:"

                    f"{now.date()}:"

                    f"{queue}:"

                    f"{start}:"

                    f"{end}"

                )

                if notification_was_sent(

                    notification_key

                ):

                    continue

                if status == 1:

                    warning = (

                        "⚠️ <b>Через 30 хв "

                        "очікується відключення</b>"

                    )

                else:

                    warning = (

                        "⚠️ <b>Через 30 хв можливе "

                        "відключення</b>"

                    )

                text = (

                    f"{warning}\n\n"

                    f"Підчерга: <b>{queue}</b>\n"

                    f"🕐 {start}–{end}\n\n"

                    "Джерело: Прикарпаттяобленерго"

                )

                try:

                    await bot.send_message(

                        user[0],

                        text

                    )

                    mark_notification_sent(

                        notification_key

                    )

                except Exception as error:

                    print(

                        "WARNING ERROR:",

                        user[0],

                        repr(error),

                        flush=True

                    )

# =========================================================

# ФОНОВА ПЕРЕВІРКА

# =========================================================

async def schedule_monitor(bot):

    # Даємо боту нормально запуститися

    await asyncio.sleep(10)

    while True:

        try:

            users = get_notification_users()

            active_queues = sorted(

                set(

                    user[1]

                    for user in users

                    if user[1]

                )

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

# /START

# =========================================================

@router.message(CommandStart())

async def start(message: Message):

    user = get_user(

        message.from_user.id

    )

    if user and user[0]:

        await message.answer(

            "⚡ <b>Струм ОК</b>\n\n"

            f"Ваша підчерга: "

            f"<b>{user[0]}</b>\n\n"

            "Оберіть потрібний розділ:",

            reply_markup=main_keyboard()

        )

    else:

        await message.answer(

            "⚡ <b>Струм ОК</b>\n\n"

            "Бот показує актуальні графіки "

            "відключення електроенергії "

            "в Івано-Франківській області.\n\n"

            "Оберіть свою підчергу:",

            reply_markup=queue_keyboard()

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

    db.execute(

        """

        INSERT INTO users (

            telegram_id,

            queue,

            notifications,

            created_at

        )

        VALUES (?, ?, 1, ?)

        ON CONFLICT(telegram_id)

        DO UPDATE SET

            queue=excluded.queue

        """,

        (

            callback.from_user.id,

            queue,

            datetime.now(TZ).isoformat()

        )

    )

    db.commit()

    await callback.answer()

    await safe_edit(

        callback.message,

        (

            "✅ Підчергу збережено: "

            f"<b>{queue}</b>\n\n"

            "🔔 Сповіщення увімкнені.\n\n"

            "Тепер бот стежитиме за "

            "вашим графіком."

        ),

        main_keyboard()

    )

# =========================================================

# ЗМІНА ПІДЧЕРГИ

# =========================================================

@router.callback_query(

    F.data == "change_queue"

)

async def change_queue(

    callback: CallbackQuery

):

    await callback.answer()

    await safe_edit(

        callback.message,

        "Оберіть свою підчергу:",

        queue_keyboard()

    )

# =========================================================

# УВІМКНУТИ / ВИМКНУТИ СПОВІЩЕННЯ

# =========================================================

@router.callback_query(

    F.data == "notifications"

)

async def notifications(

    callback: CallbackQuery

):

    user = get_user(

        callback.from_user.id

    )

    if not user:

        await callback.answer(

            "Спочатку оберіть підчергу.",

            show_alert=True

        )

        return

    new_value = (

        0

        if user[1]

        else 1

    )

    db.execute(

        """

        UPDATE users

        SET notifications=?

        WHERE telegram_id=?

        """,

        (

            new_value,

            callback.from_user.id

        )

    )

    db.commit()

    if new_value:

        await callback.answer(

            "Сповіщення увімкнено 🔔",

            show_alert=True

        )

    else:

        await callback.answer(

            "Сповіщення вимкнено 🔕",

            show_alert=True

        )

# =========================================================

# ТЕСТ СПОВІЩЕННЯ

# =========================================================

@router.callback_query(

    F.data == "test_notification"

)

async def test_notification(

    callback: CallbackQuery

):

    user = get_user(

        callback.from_user.id

    )

    if not user:

        await callback.answer(

            "Спочатку оберіть підчергу.",

            show_alert=True

        )

        return

    queue = user[0]

    await callback.answer(

        "Тестове сповіщення надіслано ✅"

    )

    await callback.bot.send_message(

        callback.from_user.id,

        (

            "🧪 <b>Тестове сповіщення</b>\n\n"

            "⚠️ Через 30 хв за графіком "

            "очікується відключення "

            "електроенергії.\n\n"

            f"Підчерга: <b>{queue}</b>\n"

            "🕐 20:00–22:00\n\n"

            "Це лише тест. "

            "Реального відключення може не бути."

        )

    )

# =========================================================

# СТАН / СЬОГОДНІ / ЗАВТРА

# =========================================================

@router.callback_query(

    F.data.in_({

        "status",

        "today",

        "tomorrow"

    })

)

async def schedule(

    callback: CallbackQuery

):

    user = get_user(

        callback.from_user.id

    )

    if not user:

        await callback.answer(

            "Спочатку оберіть підчергу.",

            show_alert=True

        )

        return

    queue = user[0]

    await callback.answer()

    try:

        data = await fetch_schedule(

            queue

        )

        if not data:

            text = no_active_schedule_text(

                queue,

                callback.data

            )

            await safe_edit(

                callback.message,

                text,

                main_keyboard()

            )

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

        if callback.data == "status":

            text = format_current(

                today_data,

                queue

            )

        elif callback.data == "today":

            text = format_day(

                today_data,

                queue,

                "📅 Графік на сьогодні"

            )

        else:

            text = format_day(

                tomorrow_data,

                queue,

                "📆 Графік на завтра"

            )

        await safe_edit(

            callback.message,

            text,

            main_keyboard()

        )

    except Exception as error:

        print(

            "Schedule error:",

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

            main_keyboard()

        )

# =========================================================

# RENDER HEALTH CHECK

# =========================================================

async def health(request):

    return web.Response(

        text="Strum OK is running"

    )

# =========================================================

# ЗАПУСК

# =========================================================

async def main():

    # Сервер для Render

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

    # Telegram

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

    # Запускаємо фоновий моніторинг

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

        await bot.session.close()

if __name__ == "__main__":

    asyncio.run(main())
