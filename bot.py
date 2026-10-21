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
from aiogram.exceptions import TelegramBadRequest


# =========================
# НАЛАШТУВАННЯ
# =========================

TOKEN = os.getenv("BOT_TOKEN")

if not TOKEN:
    raise RuntimeError(
        "BOT_TOKEN environment variable is not set"
    )

TZ = ZoneInfo("Europe/Kyiv")

SCHEDULE_URL = (
    "https://be-svitlo.oe.if.ua/schedule-by-queue"
)

QUEUES = [
    f"{number}.{sub}"
    for number in range(1, 7)
    for sub in (1, 2)
]


# =========================
# БАЗА ДАНИХ
# =========================

router = Router()

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


# =========================
# КНОПКИ
# =========================

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
                )
            ],
        ]
    )


# =========================
# БЕЗПЕЧНЕ ОНОВЛЕННЯ
# ПОВІДОМЛЕННЯ
# =========================

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
        error_text = str(error).lower()

        if "message is not modified" in error_text:
            return

        raise


# =========================
# API ПРИКАРПАТТЯОБЛЕНЕРГО
# =========================

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

    async with ClientSession(
        timeout=timeout
    ) as session:

        async with session.get(
            SCHEDULE_URL,
            params={"queue": queue},
            headers=headers
        ) as response:

            print(
                "SCHEDULE STATUS:",
                response.status,
                flush=True
            )

            if response.status != 200:
                body = await response.text()

                print(
                    "SCHEDULE ERROR BODY:",
                    body[:1000],
                    flush=True
                )

                raise RuntimeError(
                    "Schedule server returned "
                    f"{response.status}"
                )

            try:
                data = await response.json(
                    content_type=None
                )

            except Exception:
                body = await response.text()

                print(
                    "INVALID JSON:",
                    body[:2000],
                    flush=True
                )

                raise RuntimeError(
                    "Schedule server returned invalid JSON"
                )

            print(
                "SCHEDULE DATA:",
                data,
                flush=True
            )

            return data


# =========================
# ОБРОБКА ГРАФІКА
# =========================

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

    # Якщо сервер поверне ISO-дату
    # із часовою зоною
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
        value = str(value).strip()

        hour, minute = value.split(":")[:2]

        return (
            int(hour) * 60
            + int(minute)
        )

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

    # 00:00 у кінці інтервалу =
    # кінець доби
    if end == 0 and start > 0:
        end = 24 * 60

    return start <= current < end


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


# =========================
# ТЕКСТ КОЛИ ГПВ НЕМАЄ
# =========================

def no_active_schedule_text(
    queue,
    mode
):
    if mode == "status":
        return (
            "<b>⚡ Стан зараз</b>\n"
            f"Підчерга: <b>{queue}</b>\n\n"
            "🟢 <b>Активний графік ГПВ "
            "зараз не опублікований.</b>\n\n"
            "За даними сервісу "
            "Прикарпаттяобленерго для цієї "
            "підчерги активного графіка "
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
            "За даними сервісу "
            "Прикарпаттяобленерго графіка "
            "погодинних вимкнень для вашої "
            "підчерги немає.\n\n"
            "Джерело: Прикарпаттяобленерго"
        )

    return (
        "<b>📆 Графік на завтра</b>\n"
        f"Підчерга: <b>{queue}</b>\n\n"
        "ℹ️ <b>Активний графік ГПВ "
        "на завтра поки не опублікований.</b>\n\n"
        "Якщо Прикарпаттяобленерго "
        "опублікує графік, бот зможе "
        "отримати його через офіційний сервіс.\n\n"
        "Джерело: Прикарпаттяобленерго"
    )


# =========================
# ФОРМАТ ГРАФІКА НА ДЕНЬ
# =========================

def format_day(
    day_data,
    queue,
    title
):
    if not day_data:
        return (
            f"<b>{title}</b>\n"
            f"Підчерга: <b>{queue}</b>\n\n"
            "ℹ️ Активний графік на цей день "
            "поки не опублікований.\n\n"
            "Джерело: Прикарпаттяобленерго"
        )

    periods = get_periods(
        day_data,
        queue
    )

    approved = day_data.get(
        "scheduleApprovedSince"
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

    if approved:
        lines.extend([
            "",
            f"🕐 Оновлено: {approved}",
        ])

    lines.extend([
        "",
        "Джерело: Прикарпаттяобленерго"
    ])

    return "\n".join(lines)


# =========================
# СТАН ЗАРАЗ
# =========================

def format_current(
    day_data,
    queue
):
    now = datetime.now(TZ)

    if not day_data:
        return no_active_schedule_text(
            queue,
            "status"
        )

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


# =========================
# /START
# =========================

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


# =========================
# ВИБІР ПІДЧЕРГИ
# =========================

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

    await safe_edit(
        callback.message,
        (
            "✅ Підчергу збережено: "
            f"<b>{queue}</b>\n\n"
            "Тепер бот показуватиме "
            "ваш персональний графік."
        ),
        main_keyboard()
    )

    await callback.answer()


# =========================
# ЗМІНА ПІДЧЕРГИ
# =========================

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


# =========================
# СПОВІЩЕННЯ
# =========================

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
        0 if user[1] else 1
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
        state = "увімкнено 🔔"
    else:
        state = "вимкнено 🔕"

    await callback.answer(
        f"Сповіщення {state}",
        show_alert=True
    )


# =========================
# ГРАФІК
# =========================

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

    # Одразу закриваємо "годинник"
    # Telegram на кнопці
    await callback.answer()

    try:
        data = await fetch_schedule(
            queue
        )

        now = datetime.now(TZ)

        # --------------------------------
        # API повернув []
        # Це НЕ помилка Telegram/Render
        # --------------------------------

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

        # --------------------------------
        # API повернув графік
        # --------------------------------

        today_data = find_day(
            data,
            now.date()
        )

        tomorrow_date = (
            now + timedelta(days=1)
        ).date()

        tomorrow_data = find_day(
            data,
            tomorrow_date
        )

        if callback.data == "status":

            text = format_current(
                today_data,
                queue
            )

        elif callback.data == "today":

            if today_data:
                text = format_day(
                    today_data,
                    queue,
                    "📅 Графік на сьогодні"
                )
            else:
                text = no_active_schedule_text(
                    queue,
                    "today"
                )

        else:

            if tomorrow_data:
                text = format_day(
                    tomorrow_data,
                    queue,
                    "📆 Графік на завтра"
                )
            else:
                text = no_active_schedule_text(
                    queue,
                    "tomorrow"
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

        error_text = (
            "⚠️ <b>Не вдалося отримати "
            "графік.</b>\n\n"
            "Сервіс Прикарпаттяобленерго "
            "тимчасово не відповідає.\n\n"
            "Спробуйте ще раз через "
            "кілька хвилин."
        )

        try:
            await safe_edit(
                callback.message,
                error_text,
                main_keyboard()
            )

        except Exception as edit_error:
            print(
                "Telegram edit error:",
                repr(edit_error),
                flush=True
            )


# =========================
# RENDER HEALTH CHECK
# =========================

async def health(request):
    return web.Response(
        text="Strum OK is running"
    )


# =========================
# ЗАПУСК
# =========================

async def main():

    # Web server потрібен Render,
    # щоб Web Service залишався Live

    app = web.Application()

    app.router.add_get(
        "/",
        health
    )

    runner = web.AppRunner(app)

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

    print(
        "Strum OK bot started",
        flush=True
    )

    await dp.start_polling(
        bot
    )


if __name__ == "__main__":
    asyncio.run(main())
