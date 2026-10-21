import asyncio

import os

import sqlite3

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

TOKEN = os.getenv("BOT_TOKEN")

if not TOKEN:

    raise RuntimeError("BOT_TOKEN environment variable is not set")

TZ = ZoneInfo("Europe/Kyiv")

SCHEDULE_URL = "https://be-svitlo.oe.if.ua/schedule-by-queue"

QUEUES = [

    f"{n}.{s}"

    for n in range(1, 7)

    for s in (1, 2)

]

router = Router()

db = sqlite3.connect("strum_ok.db")

db.execute("""

CREATE TABLE IF NOT EXISTS users (

    telegram_id INTEGER PRIMARY KEY,

    queue TEXT,

    notifications INTEGER NOT NULL DEFAULT 1,

    created_at TEXT NOT NULL

)

""")

db.commit()

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

            )

        ])

    rows.append([

        InlineKeyboardButton(

            text="🔎 Не знаю свою підчергу",

            url="https://svitlo.oe.if.ua/"

        )

    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def main_keyboard():

    return InlineKeyboardMarkup(inline_keyboard=[

        [

            InlineKeyboardButton(

                text="⚡ Стан зараз",

                callback_data="status"

            ),

            InlineKeyboardButton(

                text="📅 Сьогодні",

                callback_data="today"

            )

        ],

        [

            InlineKeyboardButton(

                text="📆 Завтра",

                callback_data="tomorrow"

            ),

            InlineKeyboardButton(

                text="🔄 Змінити підчергу",

                callback_data="change_queue"

            )

        ],

        [

            InlineKeyboardButton(

                text="🔔 Сповіщення",

                callback_data="notifications"

            )

        ]

    ])

def get_user(user_id):

    return db.execute(

        """

        SELECT queue, notifications

        FROM users

        WHERE telegram_id=?

        """,

        (user_id,)

    ).fetchone()

async def fetch_schedule(queue):

    headers = {

        "User-Agent": "Mozilla/5.0",

        "Accept": "*/*",

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

            if response.status != 200:

                raise RuntimeError(

                    f"Schedule server returned {response.status}"

                )

            return await response.json()

def find_day(data, date):

    date_string = date.strftime("%d.%m.%Y")

    if not isinstance(data, list):

        return None

    matches = [

        item for item in data

        if item.get("eventDate") == date_string

    ]

    if not matches:

        return None

    return matches[-1]

def get_periods(day_data, queue):

    if not day_data:

        return []

    queues = day_data.get("queues", {})

    return queues.get(queue, [])

def time_to_minutes(value):

    try:

        hour, minute = value.split(":")

        return int(hour) * 60 + int(minute)

    except Exception:

        return 0

def period_contains_now(period, now):

    start = time_to_minutes(period.get("from", "00:00"))

    end = time_to_minutes(period.get("to", "00:00"))

    current = now.hour * 60 + now.minute

    if end == 0 and start > 0:

        end = 24 * 60

    return start <= current < end

def status_text(status):

    try:

        status = int(status)

    except Exception:

        status = 0

    if status == 1:

        return "🔴 Світла немає"

    if status != 0:

        return "🟡 Можливе вимкнення"

    return "🟢 Світло має бути"

def format_period(period):

    start = period.get("from", "--:--")

    end = period.get("to", "--:--")

    try:

        status = int(period.get("status", 0))

    except Exception:

        status = 0

    if status == 1:

        icon = "🔴"

        label = "вимкнення"

    else:

        icon = "🟡"

        label = "можливе вимкнення"

    return f"{icon} {start}–{end} — {label}"

def format_day(day_data, queue, title):

    if not day_data:

        return (

            f"<b>{title}</b>\n"

            f"Підчерга: <b>{queue}</b>\n\n"

            "ℹ️ Графік на цей день поки не опублікований."

        )

    periods = get_periods(day_data, queue)

    approved = day_data.get(

        "scheduleApprovedSince",

        "невідомо"

    )

    lines = [

        f"<b>{title}</b>",

        f"Підчерга: <b>{queue}</b>",

        "",

    ]

    if not periods:

        lines.append(

            "🟢 Запланованих вимкнень для цієї "

            "підчерги не знайдено."

        )

    else:

        for period in periods:

            lines.append(format_period(period))

    lines.extend([

        "",

        f"🕐 Графік затверджено/оновлено: {approved}",

        "",

        "Джерело: Прикарпаттяобленерго"

    ])

    return "\n".join(lines)

def format_current(day_data, queue):

    now = datetime.now(TZ)

    if not day_data:

        return (

            "<b>⚡ Стан зараз</b>\n"

            f"Підчерга: <b>{queue}</b>\n\n"

            "ℹ️ Актуальний графік на сьогодні "

            "поки не отримано."

        )

    periods = get_periods(day_data, queue)

    current_period = None

    for period in periods:

        if period_contains_now(period, now):

            current_period = period

            break

    if current_period:

        state = status_text(

            current_period.get("status", 0)

        )

        until = current_period.get("to", "--:--")

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

            "🟢 <b>Світло має бути</b>"

        )

    approved = day_data.get(

        "scheduleApprovedSince",

        "невідомо"

    )

    text += (

        f"\n\n🕐 Графік оновлено: {approved}"

        "\nДжерело: Прикарпаттяобленерго"

    )

    return text

@router.message(CommandStart())

async def start(message: Message):

    user = get_user(message.from_user.id)

    if user and user[0]:

        await message.answer(

            "⚡ <b>Струм ОК</b>\n\n"

            f"Ваша підчерга: <b>{user[0]}</b>\n"

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

@router.callback_query(F.data.startswith("queue:"))

async def choose_queue(callback: CallbackQuery):

    queue = callback.data.split(":", 1)[1]

    db.execute("""

        INSERT INTO users(

            telegram_id,

            queue,

            notifications,

            created_at

        )

        VALUES (?, ?, 1, ?)

        ON CONFLICT(telegram_id)

        DO UPDATE SET queue=excluded.queue

    """, (

        callback.from_user.id,

        queue,

        datetime.now(TZ).isoformat()

    ))

    db.commit()

    await callback.message.edit_text(

        f"✅ Підчергу збережено: <b>{queue}</b>\n\n"

        "Тепер бот показуватиме ваш "

        "персональний графік.",

        reply_markup=main_keyboard()

    )

    await callback.answer()

@router.callback_query(F.data == "change_queue")

async def change_queue(callback: CallbackQuery):

    await callback.message.edit_text(

        "Оберіть свою підчергу:",

        reply_markup=queue_keyboard()

    )

    await callback.answer()

@router.callback_query(F.data == "notifications")

async def notifications(callback: CallbackQuery):

    user = get_user(callback.from_user.id)

    if not user:

        await callback.answer(

            "Спочатку оберіть підчергу.",

            show_alert=True

        )

        return

    new_value = 0 if user[1] else 1

    db.execute(

        """

        UPDATE users

        SET notifications=?

        WHERE telegram_id=?

        """,

        (new_value, callback.from_user.id)

    )

    db.commit()

    state = (

        "увімкнено 🔔"

        if new_value

        else "вимкнено 🔕"

    )

    await callback.answer(

        f"Сповіщення {state}",

        show_alert=True

    )

@router.callback_query(

    F.data.in_({"status", "today", "tomorrow"})

)

async def schedule(callback: CallbackQuery):

    user = get_user(callback.from_user.id)

    if not user:

        await callback.answer(

            "Спочатку оберіть підчергу.",

            show_alert=True

        )

        return

    queue = user[0]

    await callback.answer()

    try:

        data = await fetch_schedule(queue)

        now = datetime.now(TZ)

        today_data = find_day(

            data,

            now.date()

        )

        tomorrow_data = find_day(

            data,

            (now + timedelta(days=1)).date()

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

        await callback.message.edit_text(

            text,

            reply_markup=main_keyboard()

        )

    except Exception as error:

        print(

            "Schedule error:",

            repr(error)

        )

        await callback.message.edit_text(

            "⚠️ <b>Не вдалося отримати графік.</b>\n\n"

            "Сервіс Прикарпаттяобленерго "

            "тимчасово не відповідає.\n"

            "Спробуйте ще раз через кілька хвилин.",

            reply_markup=main_keyboard()

        )

async def health(request):

    return web.Response(

        text="Strum OK is running"

    )

async def main():

    app = web.Application()

    app.router.add_get(

        "/",

        health

    )

    runner = web.AppRunner(app)

    await runner.setup()

    port = int(

        os.getenv("PORT", 10000)

    )

    site = web.TCPSite(

        runner,

        "0.0.0.0",

        port

    )

    await site.start()

    bot = Bot(

        TOKEN,

        default=DefaultBotProperties(

            parse_mode=ParseMode.HTML

        )

    )

    dp = Dispatcher()

    dp.include_router(router)

    await dp.start_polling(bot)

if __name__ == "__main__":

    asyncio.run(main())
