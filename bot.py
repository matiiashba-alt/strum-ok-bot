import asyncio
import os
import json
import hashlib
import html
from dotenv import load_dotenv
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
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is not set")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

ADMIN_ID = 1962638884

TZ = ZoneInfo("Europe/Kyiv")

SCHEDULE_URL = "https://be-svitlo.oe.if.ua/schedule-by-queue"

QUEUES = [
    f"{number}.{sub}"
    for number in range(1, 7)
    for sub in (1, 2)
]

CHECK_INTERVAL = 300
WARNING_MINUTES = 30

router = Router()

db_pool = None
http_session = None

# Чернетки адмінської розсилки
broadcast_drafts = {}


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

        # Нові поля для адмін-статистики.
        # Старі користувачі НЕ видаляються.
        await conn.execute("""
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS username TEXT
        """)

        await conn.execute("""
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS first_name TEXT
        """)

        await conn.execute("""
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS last_name TEXT
        """)

        await conn.execute("""
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS last_active_at TIMESTAMPTZ
        """)

        await conn.execute("""
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS action_count BIGINT NOT NULL DEFAULT 0
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

    print("PostgreSQL database initialized", flush=True)


async def track_user(tg_user):

    if tg_user is None:
        return

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (
                telegram_id,
                username,
                first_name,
                last_name,
                notifications,
                created_at,
                updated_at,
                last_active_at,
                action_count
            )
            VALUES (
                $1, $2, $3, $4,
                TRUE,
                NOW(),
                NOW(),
                NOW(),
                1
            )
            ON CONFLICT (telegram_id)
            DO UPDATE SET
                username=EXCLUDED.username,
                first_name=EXCLUDED.first_name,
                last_name=EXCLUDED.last_name,
                updated_at=NOW(),
                last_active_at=NOW(),
                action_count=users.action_count + 1
            """,
            tg_user.id,
            tg_user.username,
            tg_user.first_name,
            tg_user.last_name,
        )


async def get_user(user_id):

    async with db_pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT
                queue,
                notifications,
                username,
                first_name,
                last_name,
                created_at,
                last_active_at,
                action_count
            FROM users
            WHERE telegram_id=$1
            """,
            user_id,
        )


async def save_user_queue(user_id, queue):

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (
                telegram_id,
                queue,
                notifications,
                created_at,
                updated_at,
                last_active_at
            )
            VALUES ($1, $2, TRUE, NOW(), NOW(), NOW())

            ON CONFLICT (telegram_id)
            DO UPDATE SET
                queue=EXCLUDED.queue,
                updated_at=NOW(),
                last_active_at=NOW()
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
                updated_at=NOW(),
                last_active_at=NOW()
            WHERE telegram_id=$2
            """,
            value,
            user_id,
        )


async def get_notification_users():

    async with db_pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT telegram_id, queue
            FROM users
            WHERE notifications=TRUE
              AND queue IS NOT NULL
            """
        )


async def get_all_user_ids():

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT telegram_id
            FROM users
            ORDER BY created_at ASC
            """
        )

    return [row["telegram_id"] for row in rows]


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
# АДМІН — СТАТИСТИКА
# =========================================================

def is_admin(user_id):
    return user_id == ADMIN_ID


async def get_admin_stats():

    now = datetime.now(TZ)
    today_start = now.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    week_start = today_start - timedelta(days=6)

    async with db_pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT
                COUNT(*) AS total_users,

                COUNT(*) FILTER (
                    WHERE created_at >= $1
                ) AS today_users,

                COUNT(*) FILTER (
                    WHERE created_at >= $2
                ) AS week_users,

                COUNT(*) FILTER (
                    WHERE notifications=TRUE
                ) AS notifications_on,

                COUNT(*) FILTER (
                    WHERE notifications=FALSE
                ) AS notifications_off,

                COALESCE(
                    SUM(action_count),
                    0
                ) AS total_actions

            FROM users
            """,
            today_start,
            week_start,
        )


async def get_queue_stats():

    async with db_pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT
                queue,
                COUNT(*) AS user_count
            FROM users
            WHERE queue IS NOT NULL
            GROUP BY queue
            ORDER BY queue
            """
        )


async def get_recent_users(limit=10):

    async with db_pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT
                telegram_id,
                username,
                first_name,
                last_name,
                queue,
                notifications,
                created_at,
                last_active_at,
                action_count
            FROM users
            ORDER BY created_at DESC
            LIMIT $1
            """,
            limit,
        )


async def get_active_users(limit=10):

    async with db_pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT
                telegram_id,
                username,
                first_name,
                last_name,
                queue,
                notifications,
                created_at,
                last_active_at,
                action_count
            FROM users
            ORDER BY
                last_active_at DESC NULLS LAST,
                updated_at DESC
            LIMIT $1
            """,
            limit,
        )


def format_admin_datetime(value):

    if not value:
        return "—"

    try:
        return value.astimezone(TZ).strftime(
            "%d.%m.%Y %H:%M"
        )
    except Exception:
        return str(value)


def user_display_name(row):

    first_name = row["first_name"] or ""
    last_name = row["last_name"] or ""

    full_name = f"{first_name} {last_name}".strip()

    if full_name:
        return html.escape(full_name)

    if row["username"]:
        return "@" + html.escape(row["username"])

    return f"ID {row['telegram_id']}"


async def build_admin_main():

    stats = await get_admin_stats()

    return (
        "👑 <b>Адмін-панель «Струм ОК»</b>\n\n"

        f"👥 Всього користувачів: "
        f"<b>{stats['total_users']}</b>\n"

        f"🆕 Нових сьогодні: "
        f"<b>{stats['today_users']}</b>\n"

        f"📅 Нових за 7 днів: "
        f"<b>{stats['week_users']}</b>\n\n"

        f"🔔 Сповіщення увімкнено: "
        f"<b>{stats['notifications_on']}</b>\n"

        f"🔕 Сповіщення вимкнено: "
        f"<b>{stats['notifications_off']}</b>\n\n"

        f"📊 Всього дій у боті: "
        f"<b>{stats['total_actions']}</b>\n\n"

        "Оберіть розділ:"
    )


async def build_queue_stats():

    rows = await get_queue_stats()

    counts = {
        row["queue"]: row["user_count"]
        for row in rows
    }

    lines = [
        "⚡ <b>Користувачі за підчергами</b>",
        "",
    ]

    total_with_queue = 0

    for queue in QUEUES:
        count = counts.get(queue, 0)
        total_with_queue += count

        lines.append(
            f"<b>{queue}</b> — {count}"
        )

    lines.extend([
        "",
        f"👥 Обрали підчергу: "
        f"<b>{total_with_queue}</b>"
    ])

    return "\n".join(lines)


def format_users_list(rows, title):

    lines = [
        f"<b>{title}</b>",
        "",
    ]

    if not rows:
        return (
            f"<b>{title}</b>\n\n"
            "Користувачів поки немає."
        )

    for index, row in enumerate(rows, start=1):

        name = user_display_name(row)

        username = (
            f"@{html.escape(row['username'])}"
            if row["username"]
            else "без username"
        )

        queue = row["queue"] or "—"

        notifications = (
            "🔔"
            if row["notifications"]
            else "🔕"
        )

        actions = row["action_count"] or 0

        lines.extend([
            f"<b>{index}. {name}</b>",
            f"   {username}",
            f"   ID: <code>{row['telegram_id']}</code>",
            f"   ⚡ {queue} · {notifications}",
            f"   📊 Дій: {actions}",
            f"   🕐 Активність: "
            f"{format_admin_datetime(row['last_active_at'])}",
            ""
        ])

    return "\n".join(lines)


def format_recent_users(rows):

    lines = [
        "🆕 <b>Останні користувачі</b>",
        "",
    ]

    if not rows:
        return (
            "🆕 <b>Останні користувачі</b>\n\n"
            "Користувачів поки немає."
        )

    for index, row in enumerate(rows, start=1):

        name = user_display_name(row)

        username = (
            f"@{html.escape(row['username'])}"
            if row["username"]
            else "без username"
        )

        queue = row["queue"] or "—"

        lines.extend([
            f"<b>{index}. {name}</b>",
            f"   {username}",
            f"   ⚡ Підчерга: {queue}",
            f"   📅 Приєднався: "
            f"{format_admin_datetime(row['created_at'])}",
            ""
        ])

    return "\n".join(lines)


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
                    text="✨ Мій день",
                    callback_data="my_day"
                ),
                InlineKeyboardButton(
                    text="📆 Завтра",
                    callback_data="tomorrow"
                ),
            ],
            [
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
            [
                InlineKeyboardButton(
                    text="🖕 Послати нах*й русню!",
                    callback_data="send_rusnya"
                )
            ],
        ]
    )


def admin_keyboard():

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📊 Статистика",
                    callback_data="admin:stats"
                ),
                InlineKeyboardButton(
                    text="⚡ Підчерги",
                    callback_data="admin:queues"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="👥 Користувачі",
                    callback_data="admin:users"
                ),
                InlineKeyboardButton(
                    text="🆕 Останні",
                    callback_data="admin:recent"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📢 Розсилка",
                    callback_data="admin:broadcast"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Оновити",
                    callback_data="admin:refresh"
                )
            ],
        ]
    )


def broadcast_cancel_keyboard():

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="❌ Скасувати",
                    callback_data="broadcast:cancel"
                )
            ]
        ]
    )


def broadcast_confirm_keyboard():

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Надіслати всім",
                    callback_data="broadcast:confirm"
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Скасувати",
                    callback_data="broadcast:cancel"
                )
            ]
        ]
    )


# =========================================================
# TELEGRAM HELPERS
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

    queues = day_data.get("queues", {})

    if not isinstance(queues, dict):
        return []

    periods = queues.get(queue, [])

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

    current = now.hour * 60 + now.minute

    if end == 0 and start > 0:
        end = 24 * 60

    return start <= current < end


def minutes_until(time_value, now):

    target = time_to_minutes(time_value)
    current = now.hour * 60 + now.minute

    return target - current


def duration_minutes(period):

    start = time_to_minutes(
        period.get("from", "00:00")
    )

    end = time_to_minutes(
        period.get("to", "00:00")
    )

    if end == 0 and start > 0:
        end = 24 * 60

    if end < start:
        end += 24 * 60

    return max(0, end - start)


def human_duration(minutes):

    minutes = max(0, int(minutes))

    hours = minutes // 60
    mins = minutes % 60

    if hours and mins:
        return f"{hours} год {mins} хв"

    if hours:
        return f"{hours} год"

    return f"{mins} хв"


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

    start = period.get("from", "--:--")
    end = period.get("to", "--:--")

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
# НЕМАЄ АКТИВНОГО ГРАФІКА
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

def format_day(day_data, queue, title, mode):

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
# МІЙ ДЕНЬ
# =========================================================

def format_my_day(day_data, queue):

    if not day_data:

        return (
            "<b>✨ Мій день</b>\n"
            f"Підчерга: <b>{queue}</b>\n\n"
            "🟢 <b>Активний графік ГПВ "
            "на сьогодні не опублікований.</b>\n\n"
            "На цей момент немає даних для "
            "розрахунку відключень на день.\n\n"
            "Як тільки Прикарпаттяобленерго "
            "опублікує графік, тут з'явиться "
            "короткий аналіз вашого дня.\n\n"
            "Джерело: Прикарпаттяобленерго"
        )

    periods = get_periods(
        day_data,
        queue
    )

    if not periods:

        return (
            "<b>✨ Мій день</b>\n"
            f"Підчерга: <b>{queue}</b>\n\n"
            "🟢 <b>На сьогодні запланованих "
            "вимкнень не знайдено.</b>\n\n"
            "Джерело: Прикарпаттяобленерго"
        )

    now = datetime.now(TZ)

    lines = [
        "<b>✨ Мій день</b>",
        f"Підчерга: <b>{queue}</b>",
        "",
    ]

    outage_minutes = 0
    possible_minutes = 0

    longest_light_minutes = 0
    longest_light_period = None

    next_outage = None
    next_outage_remaining = None

    for period in periods:

        lines.append(
            format_period(period)
        )

        status = parse_status(
            period.get("status", 0)
        )

        duration = duration_minutes(
            period
        )

        if status == 1:
            outage_minutes += duration

        elif status != 0:
            possible_minutes += duration

        else:

            if duration > longest_light_minutes:
                longest_light_minutes = duration
                longest_light_period = period

        if status != 0:

            start = period.get("from")

            if start:

                remaining = minutes_until(
                    start,
                    now
                )

                if remaining > 0:

                    if (
                        next_outage_remaining is None
                        or remaining < next_outage_remaining
                    ):
                        next_outage_remaining = remaining
                        next_outage = period

    lines.extend([
        "",
        "────────────",
        "",
        "<b>📊 Підсумок дня</b>",
    ])

    if outage_minutes > 0:

        lines.append(
            "🔴 Без світла за графіком: "
            f"<b>{human_duration(outage_minutes)}</b>"
        )

    else:

        lines.append(
            "🔴 Підтверджених відключень: "
            "<b>немає</b>"
        )

    if possible_minutes > 0:

        lines.append(
            "🟡 Можливі відключення: "
            f"<b>{human_duration(possible_minutes)}</b>"
        )

    if longest_light_period:

        light_start = longest_light_period.get(
            "from",
            "--:--"
        )

        light_end = longest_light_period.get(
            "to",
            "--:--"
        )

        lines.extend([
            "",
            "☀️ Найдовший період зі світлом:",
            f"<b>{light_start}–{light_end}</b> "
            f"({human_duration(longest_light_minutes)})"
        ])

    lines.append("")

    if next_outage:

        next_start = next_outage.get(
            "from",
            "--:--"
        )

        next_end = next_outage.get(
            "to",
            "--:--"
        )

        next_status = parse_status(
            next_outage.get(
                "status",
                0
            )
        )

        if next_status == 1:
            heading = "⚠️ Наступне відключення"
        else:
            heading = "⚠️ Наступне можливе відключення"

        lines.extend([
            f"<b>{heading}</b>",
            f"🕐 {next_start}–{next_end}",
            "⏳ До початку: "
            f"<b>{human_duration(next_outage_remaining)}</b>",
        ])

        if next_outage_remaining <= 30:

            lines.extend([
                "",
                "🔋 <b>Час перевірити заряд "
                "телефону та павербанка.</b>"
            ])

        elif next_outage_remaining <= 120:

            lines.extend([
                "",
                "🔌 Ще є час зарядити "
                "необхідні пристрої."
            ])

    else:

        current_outage = None

        for period in periods:

            status = parse_status(
                period.get("status", 0)
            )

            if (
                status != 0
                and period_contains_now(
                    period,
                    now
                )
            ):
                current_outage = period
                break

        if current_outage:

            end = current_outage.get(
                "to",
                "--:--"
            )

            lines.extend([
                "⚠️ <b>Зараз триває період "
                "відключення за графіком.</b>",
                f"Орієнтовно до: <b>{end}</b>"
            ])

        else:

            lines.append(
                "✅ <b>На сьогодні наступних "
                "відключень за графіком немає.</b>"
            )

    approved = day_data.get(
        "scheduleApprovedSince"
    )

    if approved:
        lines.extend([
            "",
            f"🕐 Графік оновлено: {approved}"
        ])

    lines.extend([
        "",
        "⚠️ Дані розраховані за опублікованим "
        "графіком і не показують аварійні "
        "відключення.",
        "",
        "Джерело: Прикарпаттяобленерго"
    ])

    return "\n".join(lines)


# =========================================================
# ОТРИМАННЯ ТЕКСТУ ГРАФІКА
# =========================================================

async def build_schedule_text(queue, mode):

    data = await fetch_schedule(queue)

    if not data:

        if mode == "my_day":
            return format_my_day(
                None,
                queue
            )

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

    if mode == "my_day":

        day_data = find_day(
            data,
            now.date()
        )

        return format_my_day(
            day_data,
            queue
        )

    day_data = find_day(
        data,
        (
            now + timedelta(days=1)
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

    new_hash = make_schedule_hash(data)

    old_hash = await get_cached_schedule_hash(
        queue
    )

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
            now + timedelta(days=1)
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

        if not (
            0 < remaining <= WARNING_MINUTES
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
                "🔋 Перевірте заряд телефону "
                "та павербанка.\n\n"
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

async def show_main_menu(
    message,
    user_id
):

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

    await track_user(
        message.from_user
    )

    await show_main_menu(
        message,
        message.from_user.id
    )


# =========================================================
# /ADMIN
# =========================================================

@router.message(Command("admin"))
async def admin_command(message: Message):

    if not is_admin(
        message.from_user.id
    ):
        return

    # Якщо була незавершена чернетка — скидаємо.
    broadcast_drafts.pop(
        message.from_user.id,
        None
    )

    await track_user(
        message.from_user
    )

    text = await build_admin_main()

    await message.answer(
        text,
        reply_markup=admin_keyboard()
    )


# =========================================================
# АДМІН CALLBACK
# =========================================================

@router.callback_query(
    F.data.startswith("admin:")
)
async def admin_callback(
    callback: CallbackQuery
):

    if not is_admin(
        callback.from_user.id
    ):

        await callback.answer(
            "Немає доступу.",
            show_alert=True
        )

        return

    await track_user(
        callback.from_user
    )

    action = callback.data.split(
        ":",
        1
    )[1]

    # -------------------------
    # РОЗСИЛКА
    # -------------------------

    if action == "broadcast":

        broadcast_drafts[
            callback.from_user.id
        ] = {
            "waiting": True
        }

        await callback.answer()

        await safe_edit(
            callback.message,
            (
                "📢 <b>Нова розсилка</b>\n\n"
                "Надішліть наступним повідомленням "
                "те, що потрібно розіслати "
                "користувачам бота.\n\n"
                "Можна надіслати текст, фото, "
                "відео, GIF або інше звичайне "
                "Telegram-повідомлення.\n\n"
                "👁 Перед розсилкою бот покаже "
                "вам попередній перегляд.\n\n"
                "⚠️ Без натискання "
                "«Надіслати всім» нічого "
                "не буде розіслано."
            ),
            broadcast_cancel_keyboard()
        )

        return

    await callback.answer()

    if action in (
        "stats",
        "refresh"
    ):

        text = await build_admin_main()

    elif action == "queues":

        text = await build_queue_stats()

    elif action == "users":

        rows = await get_active_users(10)

        text = format_users_list(
            rows,
            "👥 Останні активні користувачі"
        )

    elif action == "recent":

        rows = await get_recent_users(10)

        text = format_recent_users(
            rows
        )

    else:

        text = await build_admin_main()

    await safe_edit(
        callback.message,
        text,
        admin_keyboard()
    )


# =========================================================
# РОЗСИЛКА — СКАСУВАТИ
# =========================================================

@router.callback_query(
    F.data == "broadcast:cancel"
)
async def broadcast_cancel_callback(
    callback: CallbackQuery
):

    if not is_admin(
        callback.from_user.id
    ):

        await callback.answer(
            "Немає доступу.",
            show_alert=True
        )

        return

    broadcast_drafts.pop(
        callback.from_user.id,
        None
    )

    await callback.answer(
        "Розсилку скасовано"
    )

    text = await build_admin_main()

    await safe_edit(
        callback.message,
        text,
        admin_keyboard()
    )


# =========================================================
# РОЗСИЛКА — ПІДТВЕРДИТИ
# =========================================================

@router.callback_query(
    F.data == "broadcast:confirm"
)
async def broadcast_confirm_callback(
    callback: CallbackQuery
):

    if not is_admin(
        callback.from_user.id
    ):

        await callback.answer(
            "Немає доступу.",
            show_alert=True
        )

        return

    draft = broadcast_drafts.get(
        callback.from_user.id
    )

    if (
        not draft
        or draft.get("waiting")
        or not draft.get("message_id")
    ):

        await callback.answer(
            "Чернетку розсилки не знайдено.",
            show_alert=True
        )

        return

    # Видаляємо чернетку ДО розсилки,
    # щоб повторне натискання не запустило
    # другу таку саму розсилку.
    draft = broadcast_drafts.pop(
        callback.from_user.id
    )

    await callback.answer(
        "Розсилку запущено 📢"
    )

    await safe_edit(
        callback.message,
        (
            "📢 <b>Розсилка виконується...</b>\n\n"
            "Зачекайте. Після завершення "
            "тут з'явиться звіт."
        )
    )

    users = await get_all_user_ids()

    total = len(users)
    delivered = 0
    failed = 0

    for user_id in users:

        try:

            await callback.bot.copy_message(
                chat_id=user_id,
                from_chat_id=draft["chat_id"],
                message_id=draft["message_id"]
            )

            delivered += 1

        except Exception as error:

            failed += 1

            print(
                "BROADCAST ERROR:",
                user_id,
                repr(error),
                flush=True
            )

        # Щадний темп розсилки
        await asyncio.sleep(0.05)

    result_text = (
        "✅ <b>Розсилку завершено</b>\n\n"
        f"👥 Користувачів у базі: "
        f"<b>{total}</b>\n\n"
        f"✅ Надіслано: "
        f"<b>{delivered}</b>\n"
        f"❌ Не доставлено: "
        f"<b>{failed}</b>"
    )

    await safe_edit(
        callback.message,
        result_text,
        admin_keyboard()
    )


# =========================================================
# КОМАНДИ ГРАФІКА
# =========================================================

async def command_schedule(
    message,
    mode
):

    await track_user(
        message.from_user
    )

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


@router.message(Command("myday"))
async def my_day_command(message: Message):

    await command_schedule(
        message,
        "my_day"
    )


# =========================================================
# /QUEUE
# =========================================================

@router.message(Command("queue"))
async def queue_command(message: Message):

    await track_user(
        message.from_user
    )

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

    await track_user(
        message.from_user
    )

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

    await track_user(
        callback.from_user
    )

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

    await track_user(
        callback.from_user
    )

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

    await track_user(
        callback.from_user
    )

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

    await callback.answer(alert)

    await safe_edit(
        callback.message,
        text,
        main_keyboard(
            new_value
        )
    )


# =========================================================
# CALLBACK: ГРАФІК + МІЙ ДЕНЬ
# =========================================================

@router.callback_query(
    F.data.in_({
        "status",
        "today",
        "tomorrow",
        "my_day",
    })
)
async def schedule_callback(
    callback: CallbackQuery
):

    await track_user(
        callback.from_user
    )

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
# CALLBACK: ПОСЛАТИ РУСНЮ
# =========================================================

@router.callback_query(
    F.data == "send_rusnya"
)
async def send_rusnya_callback(
    callback: CallbackQuery
):

    await track_user(
        callback.from_user
    )

    await callback.answer()

    user = await get_user(
        callback.from_user.id
    )

    notifications_enabled = True

    if user is not None:
        notifications_enabled = bool(
            user["notifications"]
        )

    await safe_edit(
        callback.message,
        "🖕 <b>Ок, шлемо нах*й</b>",
        main_keyboard(
            notifications_enabled
        )
    )


# =========================================================
# ЛОВИМО ПОВІДОМЛЕННЯ ДЛЯ АДМІНСЬКОЇ РОЗСИЛКИ
#
# ВАЖЛИВО:
# цей handler стоїть ПІСЛЯ всіх команд.
# Тому /start, /admin, /status тощо працюють як раніше.
# =========================================================

@router.message()
async def broadcast_draft_message(
    message: Message
):

    if not is_admin(
        message.from_user.id
    ):
        return

    draft = broadcast_drafts.get(
        message.from_user.id
    )

    if not draft:
        return

    if not draft.get("waiting"):
        return

    # Не дозволяємо випадково взяти команду
    # як текст розсилки.
    if message.text and message.text.startswith("/"):

        await message.answer(
            "⚠️ Зараз бот очікує повідомлення "
            "для розсилки.\n\n"
            "Надішліть звичайний текст/фото/відео "
            "або натисніть «Скасувати».",
            reply_markup=broadcast_cancel_keyboard()
        )

        return

    broadcast_drafts[
        message.from_user.id
    ] = {
        "waiting": False,
        "chat_id": message.chat.id,
        "message_id": message.message_id,
    }

    await message.answer(
        "👁 <b>Попередній перегляд:</b>"
    )

    try:

        await message.bot.copy_message(
            chat_id=message.chat.id,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )

    except Exception as error:

        print(
            "BROADCAST PREVIEW ERROR:",
            repr(error),
            flush=True
        )

        broadcast_drafts.pop(
            message.from_user.id,
            None
        )

        await message.answer(
            "❌ Не вдалося підготувати "
            "це повідомлення для розсилки.\n\n"
            "Спробуйте інше повідомлення.",
            reply_markup=admin_keyboard()
        )

        return

    users = await get_all_user_ids()

    await message.answer(
        (
            "📢 <b>Готово до розсилки</b>\n\n"
            f"👥 Користувачів у базі: "
            f"<b>{len(users)}</b>\n\n"
            "Надіслати це повідомлення "
            "всім користувачам?"
        ),
        reply_markup=broadcast_confirm_keyboard()
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

    await init_database()

    http_session = ClientSession(
        timeout=ClientTimeout(
            total=15
        )
    )

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
