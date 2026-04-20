import asyncio
import json
import os
import re
from datetime import date, datetime, timedelta

import psycopg2
import psycopg2.extras
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from openai import OpenAI


# ================= SOZLAMALAR =================
API_TOKEN = os.getenv("API_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
BOSS_IDS = [5426806030, 6826780143]

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# ================= HOLATLAR =================
class AppStates(StatesGroup):
    waiting_trade = State()
    search_store = State()
    add_cash = State()
    add_return = State()
    add_new_sale_store = State()
    ai_chat = State()
    edit_store_info = State()
    add_worker_id = State()
    add_worker_name = State()
    pro_range_waiting = State()
    pro_quick_edit_waiting = State()
    pro_ai_waiting = State()


# ================= YORDAMCHI =================
WORKER_MENU_BUTTONS = {
    "📝 Savdo kiritish",
    "💵 Bugungi kassa",
    "🔍 Do'kon qidirish",
    "🤝 Qarzi borlar",
    "🧾 Do'konlarim",
    "📚 Oylik arxiv",
    "📈 Statistika",
    "💰 Oylik maosh",
    "🕘 Oxirgi amal",
    "🤖 AI Yordam",
}

BOSS_MENU_BUTTONS = {
    "📊 Boss Panel",
    "📅 Sana filter",
    "🤝 Qarzdorlar Pro",
    "👥 Ishchi statistikasi",
    "🏪 Do'kon reytingi",
    "🏪 Barcha do'konlar",
    "🔔 Eslatma",
    "💰 Kassa (Live)",
    "👥 Ishchilar",
    "🤖 AI Yordam",
    "🤖 AI Pro",
    "📅 Oylik arxiv",
    "📅 Oylik kassa",
    "💰 Oylik maosh",
}


def fmt(num):
    if num is None:
        return "0 $"
    try:
        n = round(float(num), 2)
        return f"{n:,.2f}".replace(".00", "").replace(",", " ") + " $"
    except Exception:
        return "0 $"


def normalize(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", "_", text.strip().lower())


def extract_date(text):
    if not text:
        return None, text
    match = re.search(r"(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})", text)
    if match:
        day, month, year = match.groups()
        year = year if len(year) == 4 else f"20{year}"
        date_str = f"{int(day):02d}.{int(month):02d}.{year}"
        clean_text = text.replace(match.group(0), "").strip()
        return date_str, clean_text
    return None, text


def safe_float(text, default=0.0):
    try:
        return float(str(text).replace(",", "."))
    except Exception:
        return default


def get_db():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL topilmadi!")
    return psycopg2.connect(database_url, sslmode="require")


def dict_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def get_worker_filter(uid):
    if uid in BOSS_IDS:
        return "", ()
    return "AND worker_id = %s", (uid,)


def get_worker_name(uid):
    try:
        conn = get_db()
        cur = dict_cursor(conn)
        cur.execute("SELECT name FROM users WHERE user_id = %s", (uid,))
        res = cur.fetchone()
        conn.close()
        return res["name"] if res and res.get("name") else f"ID:{uid}"
    except Exception:
        return f"ID:{uid}"


def date_head(text_value):
    if not text_value:
        return ""
    return str(text_value).strip().split()[0]


def parse_db_date_to_date(text_value):
    if not text_value:
        return None
    try:
        return datetime.strptime(date_head(text_value), "%d.%m.%Y").date()
    except Exception:
        try:
            return datetime.strptime(date_head(text_value), "%d.%m.%y").date()
        except Exception:
            return None


def extract_month_keys(text_value):
    if not text_value:
        return set()
    head = date_head(text_value)
    match = re.match(r"^(\d{2})\.(\d{2})\.(\d{2,4})$", head)
    if not match:
        return set()
    _, month, year = match.groups()
    keys = {f"{month}.{year}"}
    if len(year) == 2:
        keys.add(f"{month}.20{year}")
    else:
        keys.add(f"{month}.{year[-2:]}")
    return keys


def collect_available_months(rows):
    months = set()
    for row in rows:
        text_value = row["date"] if isinstance(row, dict) else row
        for key in extract_month_keys(text_value):
            month, year = key.split(".")
            if len(year) == 2:
                months.add(f"{month}.20{year}")
            else:
                months.add(f"{month}.{year}")
    return sorted(months, reverse=True)


def parse_date_range(text: str):
    text = (text or "").strip()
    if re.fullmatch(r"\d{2}\.\d{4}", text):
        return {"type": "month", "value": text}
    m = re.fullmatch(r"(\d{2}\.\d{2}\.\d{2,4})\s*-\s*(\d{2}\.\d{2}\.\d{2,4})", text)
    if m:
        return {"type": "range", "from": m.group(1), "to": m.group(2)}
    return None


def parse_any_date(text):
    for fmt_str in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(text, fmt_str).date()
        except Exception:
            pass
    return None


def in_selected_range(date_text: str, parsed):
    head = date_head(date_text)
    if parsed["type"] == "month":
        return parsed["value"] in extract_month_keys(head)
    current = parse_db_date_to_date(head)
    start = parse_any_date(parsed["from"])
    end = parse_any_date(parsed["to"])
    return bool(current and start and end and start <= current <= end)


def load_sales(worker_id=None):
    conn = get_db()
    cur = dict_cursor(conn)
    if worker_id is None:
        cur.execute(
            "SELECT id, store_name, normalized_store, total, cash, debt, txn_type, date, worker_id, worker_name FROM sales ORDER BY id DESC"
        )
    else:
        cur.execute(
            "SELECT id, store_name, normalized_store, total, cash, debt, txn_type, date, worker_id, worker_name FROM sales WHERE worker_id = %s ORDER BY id DESC",
            (worker_id,),
        )
    rows = cur.fetchall()
    conn.close()
    return rows


def load_workers():
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT user_id, name, active FROM users WHERE role = 'worker' ORDER BY name")
    rows = cur.fetchall()
    conn.close()
    return rows


def worker_summary(rows):
    stores = {r["normalized_store"] for r in rows if r.get("normalized_store")}
    total = sum((r["total"] or 0) for r in rows)
    cash = sum((r["cash"] or 0) for r in rows)
    debt = sum((r["total"] or 0) - (r["cash"] or 0) for r in rows)
    by_store = {}
    for r in rows:
        store = r.get("normalized_store")
        if not store:
            continue
        by_store.setdefault(store, 0)
        by_store[store] += r["total"] or 0
    best_store, best_value = ("yo'q", 0)
    if by_store:
        best_store, best_value = max(by_store.items(), key=lambda x: x[1])
    avg = total / len(rows) if rows else 0
    return {
        "stores": len(stores),
        "sales_count": len(rows),
        "total": total,
        "cash": cash,
        "debt": debt,
        "best_store": best_store,
        "best_value": best_value,
        "avg": avg,
    }


def fmt_card(store, total, cash, sale_date):
    return (
        f"🏪 Do'kon: {store}\n"
        f"💰 Savdo: {fmt(total)}\n"
        f"💵 Naqt: {fmt(cash)}\n"
        f"📉 Qarz: {fmt((total or 0) - (cash or 0))}\n"
        f"📅 Sana: {sale_date}"
    )


async def open_store_by_name(target, state: FSMContext, store: str, selected_worker_id=None):
    msg = target.message if isinstance(target, CallbackQuery) else target
    user_id = target.from_user.id if isinstance(target, CallbackQuery) else target.from_user.id

    if not store:
        if isinstance(target, CallbackQuery):
            return await target.answer("⚠️ Do'kon topilmadi.", show_alert=True)
        return await msg.answer("⚠️ Do'kon topilmadi.")

    await state.update_data(current_store=store)
    if selected_worker_id:
        await state.update_data(debt_worker_id=selected_worker_id)
    else:
        await state.update_data(debt_worker_id=None)

    data = await state.get_data()
    selected_uid = selected_worker_id or user_id
    w_cond, w_params = get_worker_filter(selected_uid)
    full_params = (store,) + w_params

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT COALESCE(SUM(total),0) AS total, COALESCE(SUM(cash),0) AS cash FROM sales WHERE normalized_store = %s " + w_cond, full_params)
    res = cur.fetchone()
    cur.execute("SELECT txn_type, total, cash, date FROM sales WHERE normalized_store = %s " + w_cond + " ORDER BY id DESC LIMIT 15", full_params)
    hist = cur.fetchall()
    conn.close()

    total = res["total"]
    cash = res["cash"]
    out = (
        f"🏪 **{store.upper()}** hisoboti:\n"
        f"💰 Umumiy savdo: {fmt(total)}\n"
        f"💵 Yig'ilgan: {fmt(cash)}\n"
        f"📉 Qoldiq qarz: {fmt(total - cash)}\n\n"
        f"📜 Harakatlar:\n"
    )
    for h in hist:
        if h["txn_type"] == "savdo":
            out += f"📅 {h['date']}\n💰 Savdo: {fmt(h['total'])}\n💵 Naqt: {fmt(h['cash'])}\n📉 Qarz: {fmt((h['total'] or 0) - (h['cash'] or 0))}\n\n"
        elif h["txn_type"] == "naqt":
            out += f"📅 {h['date']}\n💵 Naqt kiritildi: {fmt(h['cash'])}\n\n"
        elif h["txn_type"] == "qaytarish":
            out += f"📅 {h['date']}\n🔄 Qaytarish: {fmt(abs(h['total']))}\n\n"

    if selected_worker_id and user_id in BOSS_IDS:
        back_row = [InlineKeyboardButton(text="⬅️ Qarzdorlar", callback_data=f"boss_debt_uid_{selected_worker_id}")]
    else:
        back_row = [InlineKeyboardButton(text="⬅️", callback_data="back_main" if user_id in BOSS_IDS else "stores_list")]

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💵 Naqt", callback_data="act_cash"), InlineKeyboardButton(text="🔄 Qaytarish", callback_data="act_return")],
            [InlineKeyboardButton(text="💰 Savdo", callback_data="act_trade"), InlineKeyboardButton(text="👤 Do'konchi", callback_data="act_owner")],
            back_row,
        ]
    )
    try:
        if isinstance(target, CallbackQuery):
            await msg.edit_text(out, reply_markup=kb, parse_mode="Markdown")
        else:
            await msg.answer(out, reply_markup=kb, parse_mode="Markdown")
    except Exception:
        await msg.answer(out, reply_markup=kb, parse_mode="Markdown")


async def notify_boss(worker_uid, store, total, cash, txn_type, date_str):
    try:
        worker_name = get_worker_name(worker_uid)
        time_str = date_str.split()[1] if " " in date_str else ""
        msg = ""

        if txn_type in ["savdo", "savdo_yangi"]:
            debt = total - cash
            msg = (
                f"🔔 **Yangi savdo!**\n"
                f"👤 Ishchi: {worker_name}\n"
                f"🏪 Do'kon: `{store}`\n"
                f"💰 Savdo: {fmt(total)}\n"
                f"💵 Naqt: {fmt(cash)}\n"
                f"📉 Qarz: {fmt(debt)}\n"
                f"📅 Sana: {date_str.split()[0] if ' ' in date_str else date_str}"
            )
        elif txn_type == "naqt":
            msg = (
                f"💵 **Naqt kiritildi!**\n"
                f"👤 Ishchi: {worker_name}\n"
                f"🏪 {store} | 💵 {fmt(cash)} | 🕒 {time_str}\n"
                f"📅 {date_str.split()[0] if ' ' in date_str else date_str}"
            )
        elif txn_type == "qaytarish":
            msg = (
                f"🔄 **Qaytarildi!**\n"
                f"👤 Ishchi: {worker_name}\n"
                f"🏪 {store} | 🔄 {fmt(abs(total))} | 🕒 {time_str}\n"
                f"📅 {date_str.split()[0] if ' ' in date_str else date_str}"
            )

        if msg:
            for boss_id in BOSS_IDS:
                try:
                    await bot.send_message(boss_id, msg, parse_mode="Markdown")
                except Exception:
                    pass
    except Exception as e:
        print(f"Notify error: {e}")


def get_back_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ Orqaga")]],
        resize_keyboard=True,
    )


def get_worker_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📝 Savdo kiritish"), KeyboardButton(text="💵 Bugungi kassa")],
            [KeyboardButton(text="🔍 Do'kon qidirish"), KeyboardButton(text="🤝 Qarzi borlar")],
            [KeyboardButton(text="🧾 Do'konlarim"), KeyboardButton(text="📚 Oylik arxiv")],
            [KeyboardButton(text="📈 Statistika"), KeyboardButton(text="💰 Oylik maosh")],
            [KeyboardButton(text="🕘 Oxirgi amal"), KeyboardButton(text="🤖 AI Yordam")],
        ],
        resize_keyboard=True,
    )


def get_boss_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Boss Panel"), KeyboardButton(text="💰 Kassa (Live)")],
            [KeyboardButton(text="📅 Sana filter"), KeyboardButton(text="🤝 Qarzdorlar Pro")],
            [KeyboardButton(text="👥 Ishchi statistikasi"), KeyboardButton(text="🏪 Do'kon reytingi")],
            [KeyboardButton(text="🏪 Barcha do'konlar"), KeyboardButton(text="🔔 Eslatma")],
            [KeyboardButton(text="📅 Oylik arxiv"), KeyboardButton(text="📅 Oylik kassa")],
            [KeyboardButton(text="💰 Oylik maosh"), KeyboardButton(text="👥 Ishchilar")],
            [KeyboardButton(text="🤖 AI Yordam"), KeyboardButton(text="🤖 AI Pro")],
        ],
        resize_keyboard=True,
    )


def get_ai_questions_keyboard(is_boss=True):
    if is_boss:
        questions = [
            ["📊 Bugungi kassa qancha?", "💰 Bu oy qancha savdo?"],
            ["🏆 Eng yaxshi ishchi kim?", "💸 Eng ko'p qarzidor do'kon?"],
            ["⏰ Ishlamaydigan do'konlar?", "📉 Qaysi ishchi eng ko'p qarz?"],
            ["📅 Oylik hisobot", "📈 Umumiy statistika"],
        ]
    else:
        questions = [
            ["📊 Bugun qancha savdo?", "💰 Bu oy qancha yig'dim?"],
            ["💳 Qarzi bor do'konlarim?", "🏆 Eng yaxshi do'konim?"],
            ["📊 O'tgan oy vs Bu oy", "📅 Oylik hisobotim"],
            ["📈 Umumiy statistikam"],
        ]

    keyboard = []
    for i, row in enumerate(questions):
        keyboard.append(
            [InlineKeyboardButton(text=q, callback_data=f"ai_q_{i}_{j}") for j, q in enumerate(row)]
        )
    keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="ai_back_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            name TEXT,
            role TEXT DEFAULT 'worker',
            active INTEGER DEFAULT 1
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sales (
            id SERIAL PRIMARY KEY,
            store_name TEXT,
            normalized_store TEXT,
            total REAL,
            cash REAL,
            debt REAL,
            txn_type TEXT,
            date TEXT,
            worker_id BIGINT,
            worker_name TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stores_info (
            normalized_store TEXT PRIMARY KEY,
            owner_name TEXT,
            phone TEXT,
            location TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS deletion_requests (
            id SERIAL PRIMARY KEY,
            worker_id BIGINT,
            sale_id INTEGER,
            status TEXT DEFAULT 'pending',
            request_date TEXT
        )
        """
    )

    try:
        cur.execute("ALTER TABLE users ALTER COLUMN user_id TYPE BIGINT")
        cur.execute("ALTER TABLE sales ALTER COLUMN worker_id TYPE BIGINT")
        cur.execute("ALTER TABLE deletion_requests ALTER COLUMN worker_id TYPE BIGINT")
    except Exception:
        conn.rollback()

    conn.commit()
    conn.close()


async def route_menu_button(message: types.Message, state: FSMContext):
    text = (message.text or "").strip()

    if text == "⬅️ Orqaga":
        return await start_cmd(message, state)
    if text == "✍️ Savdo qo'shish" or text == "📝 Savdo kiritish":
        return await trade_init(message, state)
    if text == "📊 Kunlik kassa" or text == "💵 Bugungi kassa":
        return await daily_cash(message)
    if text == "🔍 Do'kon qidirish":
        return await search_prompt(message, state)
    if text == "🤝 Qarzi borlar":
        return await handle_debtors(message)
    if text == "🤝 Qarzdorlar Pro":
        return await handle_debtors(message)
    if text == "🏪 Do'konlarim" or text == "🧾 Do'konlarim":
        return await stores_list_cmd(message)
    if text == "📅 Oylik kassa" or text == "📚 Oylik arxiv":
        return await handle_monthly_cash(message)
    if text == "📅 Oylik hisobot":
        return await monthly_report(message)
    if text == "📈 Statistika":
        return await pro_my_stats(message)
    if text == "💰 Oylik maosh":
        return await calculate_salary(message)
    if text == "🕘 Oxirgi amal":
        return await pro_last_action(message, state)
    if text == "🤖 AI Yordam":
        return await ai_help_start(message, state)
    if text == "🤖 AI Pro" and message.from_user.id in BOSS_IDS:
        return await pro_ai_prompt(message, state)
    if text == "📊 Boss Panel":
        return await pro_boss_panel(message)
    if text == "📅 Sana filter":
        return await pro_range_prompt(message, state)
    if text == "👥 Ishchi statistikasi":
        return await pro_worker_stats(message)
    if text == "🏪 Do'kon reytingi":
        return await pro_store_ranking(message)
    if text == "🔔 Eslatma":
        return await pro_alerts(message)
    if text == "💰 Kassa (Live)":
        return await boss_kassa_live(message)
    if text == "👥 Ishchilar":
        return await boss_workers_list(message, state)
    if text == "📅 Oylik arxiv":
        return await boss_monthly_archive(message)
    if text == "🏪 Barcha do'konlar":
        return await boss_all_stores(message)
    if text == "📊 Eng yaxshi ishchilar":
        return await boss_top_workers(message)
    if text == "🏆 Eng yaxshi do'konlar":
        return await boss_top_stores(message)


# ================= START =================
@dp.message(Command("start"))
@dp.message(F.text == "⬅️ Orqaga")
async def start_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    init_db()

    uid = message.from_user.id
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT name, active FROM users WHERE user_id = %s", (uid,))
    user = cur.fetchone()
    conn.close()

    if uid in BOSS_IDS:
        return await message.answer("Xush kelibsiz, Boss 👑", reply_markup=get_boss_menu())
    if user and user.get("active") == 0:
        return await message.answer("🚫 Hisobingiz bloklangan. Boss bilan bog'laning.")

    kb = (
        get_worker_menu()
        if user
        else ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⏳ Kutilmoqda...")]],
            resize_keyboard=True,
        )
    )
    text = f"Assalomu alaykum, {user['name'] or 'Ishchi'}!" if user else f"Siz ro'yxatda yo'qsiz.\n🆔 ID: {uid}"
    await message.answer(text, reply_markup=kb)


# ================= BOSS =================
@dp.message(F.text == "💰 Kassa (Live)")
async def boss_kassa_live(message: types.Message):
    if message.from_user.id not in BOSS_IDS:
        return

    today = date.today().strftime("%d.%m.%Y")
    conn = get_db()
    cur = dict_cursor(conn)

    cur.execute(
        """
        SELECT u.name AS worker_name, s.store_name, COALESCE(SUM(s.cash), 0) AS cash
        FROM sales s
        JOIN users u ON s.worker_id = u.user_id
        WHERE s.date LIKE %s AND s.cash > 0
        GROUP BY u.name, s.store_name
        ORDER BY u.name, s.store_name
        """,
        (f"{today}%",),
    )
    rows = cur.fetchall()

    cur.execute(
        """
        SELECT u.name AS worker_name, COALESCE(SUM(s.cash), 0) AS total
        FROM sales s
        JOIN users u ON s.worker_id = u.user_id
        WHERE s.date LIKE %s AND s.cash > 0
        GROUP BY u.name
        """,
        (f"{today}%",),
    )
    worker_totals = {r["worker_name"]: r["total"] for r in cur.fetchall()}

    cur.execute("SELECT COALESCE(SUM(cash), 0) AS grand_total FROM sales WHERE date LIKE %s AND cash > 0", (f"{today}%",))
    grand_total = cur.fetchone()["grand_total"]
    conn.close()

    if not rows:
        return await message.answer("📅 Bugun hech qanday naqt tushumi yo'q.")

    out = f"💰 Kassa (Live) - {today}\n\n"
    current_worker = ""
    for r in rows:
        if r["worker_name"] != current_worker:
            if current_worker:
                out += f"\n✅ **Jami: {fmt(worker_totals.get(current_worker, 0))}**\n\n"
            current_worker = r["worker_name"]
            out += f"👤 **{current_worker}**:\n"
        out += f"🏪 {r['store_name']} - {fmt(r['cash'])}\n"
    if current_worker:
        out += f"\n✅ **Jami: {fmt(worker_totals.get(current_worker, 0))}**\n"
    out += f"\n{'=' * 30}\n💵 **UMUMIY JAMI: {fmt(grand_total)}**"
    await message.answer(out, parse_mode="Markdown")


@dp.message(F.text == "👥 Ishchilar")
async def boss_workers_list(message: types.Message, state: FSMContext):
    if message.from_user.id not in BOSS_IDS:
        return
    await state.clear()
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT name, user_id, active FROM users WHERE role = 'worker' ORDER BY name")
    workers = cur.fetchall()
    conn.close()

    kb = [[KeyboardButton(text=f"{'✅' if w['active'] else '🚫'} {w['name']}")] for w in workers]
    kb.append([KeyboardButton(text="➕ Yangi ishchi qo'shish")])
    kb.append([KeyboardButton(text="🚫 Ishdan bo'shatish (Inline)")])
    kb.append([KeyboardButton(text="⬅️ Orqaga")])
    await message.answer(
        "👥 Ishchilar ro'yxati (✅ faol / 🚫 blok):",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
    )


@dp.message(F.text == "➕ Yangi ishchi qo'shish")
async def add_worker_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in BOSS_IDS:
        return
    await state.clear()
    await message.answer("🆔 Ishchi Telegram ID sini kiriting:", reply_markup=get_back_kb())
    await state.set_state(AppStates.add_worker_id)


@dp.message(AppStates.add_worker_id)
async def add_worker_get_id(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga":
        await state.clear()
        return await boss_workers_list(message, state)
    if not message.text.isdigit():
        return await message.answer("⚠️ Faqat raqam kiriting.")

    await state.update_data(worker_id=int(message.text))
    await message.answer("📝 Ishchi ismini kiriting:", reply_markup=get_back_kb())
    await state.set_state(AppStates.add_worker_name)


@dp.message(AppStates.add_worker_name)
async def add_worker_get_name(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga":
        await state.clear()
        return await boss_workers_list(message, state)

    try:
        data = await state.get_data()
        worker_id = data.get("worker_id")
        if not worker_id:
            raise ValueError("ID topilmadi")

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users (user_id, name, role)
            VALUES (%s, %s, 'worker')
            ON CONFLICT (user_id) DO UPDATE SET name = EXCLUDED.name
            """,
            (worker_id, message.text.strip()),
        )
        conn.commit()
        conn.close()

        await state.clear()
        await message.answer(f"✅ Ishchi qo'shildi: {message.text.strip()}")
        await boss_workers_list(message, state)
    except Exception as e:
        await state.clear()
        await message.answer(f"❌ Xatolik: {e}")


@dp.message(F.text == "🚫 Ishdan bo'shatish (Inline)")
async def fire_worker_inline(message: types.Message):
    if message.from_user.id not in BOSS_IDS:
        return

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT name, user_id, active FROM users WHERE role = 'worker' ORDER BY name")
    workers = cur.fetchall()
    conn.close()

    if not workers:
        return await message.answer("🚫 Hozircha ishchilar yo'q.")

    kb = InlineKeyboardMarkup(inline_keyboard=[])
    row = []
    for w in workers:
        row.append(
            InlineKeyboardButton(
                text=f"{'✅' if w['active'] else '🚫'} {w['name']}",
                callback_data=f"fire_{w['user_id']}",
            )
        )
        if len(row) == 2:
            kb.inline_keyboard.append(row)
            row = []
    if row:
        kb.inline_keyboard.append(row)

    await message.answer("🚫 Ishchini tanlang:", reply_markup=kb)


@dp.callback_query(F.data.startswith("fire_"))
async def process_fire_worker(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id not in BOSS_IDS:
        return

    target_id = int(callback.data.replace("fire_", ""))
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT name, active FROM users WHERE user_id = %s", (target_id,))
    worker = cur.fetchone()
    if not worker:
        conn.close()
        return await callback.answer("⚠️ Ishchi topilmadi!", show_alert=True)

    new_status = 0 if worker["active"] == 1 else 1
    cur.execute("UPDATE users SET active = %s WHERE user_id = %s", (new_status, target_id))
    conn.commit()
    conn.close()

    await callback.message.edit_text(
        f"✅ {worker['name']} {'🚫 bloklandi' if new_status == 0 else '✅ faollashtirildi'}."
    )
    await fire_worker_inline(callback.message)


# ================= AI YORDAM =================
@dp.message(F.text == "🤖 AI Yordam")
async def ai_help_start(message: types.Message, state: FSMContext):
    await state.clear()
    is_boss = message.from_user.id in BOSS_IDS
    await message.answer(
        "🤖 **AI Yordamchi**\n\n📊 Tezkor savolni bosing yoki o'zingiz yozing.",
        reply_markup=get_ai_questions_keyboard(is_boss),
        parse_mode="Markdown",
    )
    await state.set_state(AppStates.ai_chat)


@dp.callback_query(F.data.startswith("ai_q_"))
async def handle_ai_question(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    parts = callback.data.split("_")
    row_idx = int(parts[2])
    col_idx = int(parts[3])
    is_boss = callback.from_user.id in BOSS_IDS

    if is_boss:
        questions = [
            ["Bugungi kassa qancha?", "Bu oy qancha savdo?"],
            ["Eng yaxshi ishchi kim?", "Eng ko'p qarzidor do'kon?"],
            ["Ishlamaydigan do'konlar?", "Qaysi ishchi eng ko'p qarz?"],
            ["Oylik hisobot", "Umumiy statistika"],
        ]
    else:
        questions = [
            ["Bugun qancha savdo?", "Bu oy qancha yig'dim?"],
            ["Qarzi bor do'konlarim?", "Eng yaxshi do'konim?"],
            ["O'tgan oy vs Bu oy", "Oylik hisobotim"],
            ["Umumiy statistikam"],
        ]

    await process_ai_question(callback.message, questions[row_idx][col_idx], state, is_boss, requester_id=callback.from_user.id)


@dp.callback_query(F.data == "ai_back_main")
async def ai_back_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    if callback.from_user.id in BOSS_IDS:
        await callback.message.answer("Xush kelibsiz, Boss 👑", reply_markup=get_boss_menu())
    else:
        await callback.message.answer("⬅️ Bosh menyu", reply_markup=get_worker_menu())


@dp.message(AppStates.ai_chat)
async def ai_handle_chat(message: types.Message, state: FSMContext):
    text = (message.text or "").strip()
    is_boss = message.from_user.id in BOSS_IDS
    allowed_menu = BOSS_MENU_BUTTONS if is_boss else WORKER_MENU_BUTTONS

    if text == "⬅️ Orqaga" or text in allowed_menu:
        await state.clear()
        return await route_menu_button(message, state)

    await process_ai_question(message, text, state, is_boss, requester_id=message.from_user.id)


async def process_ai_question(message: types.Message, question: str, state: FSMContext, is_boss: bool, requester_id=None):
    uid = requester_id or message.from_user.id
    msg = await message.answer("🔍 Ma'lumotlar tahlil qilinmoqda...")

    try:
        conn = get_db()
        cur = dict_cursor(conn)
        today = date.today().strftime("%d.%m.%Y")
        current_month = datetime.now().strftime("%m.%Y")
        last_month = (datetime.now() - timedelta(days=30)).strftime("%m.%Y")
        q_lower = question.lower()
        q_norm = (
            q_lower.replace("o‘", "o'")
            .replace("g‘", "g'")
            .replace("dokon", "do'kon")
            .replace("dokoni", "do'koni")
            .replace("dokonlar", "do'konlar")
            .replace("qarzi borlarim", "qarzi bor do'konlarim")
        )
        answer = ""

        worker_sales = []
        if not is_boss:
            cur.execute(
                "SELECT normalized_store, total, cash, date, txn_type FROM sales WHERE worker_id = %s ORDER BY id DESC",
                (uid,),
            )
            worker_sales = cur.fetchall()

        def summarize_month(rows, month_key):
            count = total = cash = debt = 0
            for row in rows:
                if month_key not in extract_month_keys(row["date"]):
                    continue
                count += 1
                total += row["total"] or 0
                cash += row["cash"] or 0
                debt += (row["total"] or 0) - (row["cash"] or 0)
            return {"count": count, "total": total, "cash": cash, "debt": debt}

        if "bugungi kassa" in q_norm or "bugun qancha savdo" in q_norm:
            if is_boss:
                cur.execute(
                    "SELECT COUNT(id) AS count, COALESCE(SUM(cash),0) AS cash, COALESCE(SUM(total),0) AS total FROM sales WHERE date LIKE %s",
                    (f"{today}%",),
                )
            else:
                cur.execute(
                    "SELECT COUNT(id) AS count, COALESCE(SUM(cash),0) AS cash, COALESCE(SUM(total),0) AS total FROM sales WHERE date LIKE %s AND worker_id = %s",
                    (f"{today}%", uid),
                )
            r = cur.fetchone()
            answer = f"📅 **Bugungi natijalar**\n💵 Kassa: {fmt(r['cash'])}\n💰 Savdo: {fmt(r['total'])}\n📝 Soni: {r['count']} ta"

        elif "bu oy" in q_norm and "qancha" in q_norm:
            if is_boss:
                cur.execute(
                    "SELECT COUNT(id) AS count, COALESCE(SUM(cash),0) AS cash, COALESCE(SUM(total),0) AS total FROM sales WHERE date LIKE %s",
                    (f"%{current_month}%",),
                )
                r = cur.fetchone()
            else:
                r = summarize_month(worker_sales, current_month)
            answer = f"📅 **Bu oy ({current_month})**\n💵 Yig'ilgan: {fmt(r['cash'])}\n💰 Savdo: {fmt(r['total'])}\n📝 Soni: {r['count']} ta"

        elif "eng yaxshi ishchi" in q_norm and is_boss:
            cur.execute(
                """
                SELECT u.name, COALESCE(SUM(s.total),0) AS total_sales
                FROM sales s
                JOIN users u ON s.worker_id = u.user_id
                GROUP BY u.user_id, u.name
                ORDER BY total_sales DESC
                LIMIT 1
                """
            )
            r = cur.fetchone()
            answer = f"🏆 **Eng yaxshi ishchi**\n👤 {r['name'] if r else 'Topilmadi'}\n💰 {fmt(r['total_sales']) if r else 0}"

        elif "qarzidor do'kon" in q_norm or "ko'p qarz" in q_norm:
            if is_boss:
                cur.execute(
                    """
                    SELECT normalized_store, COALESCE(SUM(total)-SUM(cash),0) AS debt
                    FROM sales
                    WHERE normalized_store IS NOT NULL AND normalized_store != ''
                    GROUP BY normalized_store
                    HAVING COALESCE(SUM(total)-SUM(cash),0) > 0
                    ORDER BY debt DESC
                    LIMIT 1
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT normalized_store, COALESCE(SUM(total)-SUM(cash),0) AS debt
                    FROM sales
                    WHERE worker_id = %s AND normalized_store IS NOT NULL AND normalized_store != ''
                    GROUP BY normalized_store
                    HAVING COALESCE(SUM(total)-SUM(cash),0) > 0
                    ORDER BY debt DESC
                    LIMIT 1
                    """,
                    (uid,),
                )
            r = cur.fetchone()
            answer = f"💳 **Eng ko'p qarzidor**\n🏪 {r['normalized_store'] if r else 'Yoq'}\n📉 Qarz: {fmt(r['debt']) if r else 0}"

        elif "ishlamay" in q_norm and is_boss:
            cur.execute(
                """
                SELECT normalized_store, MAX(date) AS last_sale
                FROM sales
                WHERE normalized_store IS NOT NULL AND normalized_store != ''
                GROUP BY normalized_store
                """
            )
            rows = cur.fetchall()
            inactive = []
            for row in rows:
                last_dt = parse_db_date_to_date(row["last_sale"])
                if last_dt and last_dt < (date.today() - timedelta(days=30)):
                    inactive.append((row["normalized_store"], row["last_sale"]))
            inactive.sort(key=lambda x: x[1])
            answer = (
                "⏰ **Faol bo'lmaganlar:**\n"
                + "\n".join([f"{i + 1}. {name} (oxirgi: {last_sale})" for i, (name, last_sale) in enumerate(inactive[:3])])
            ) if inactive else "✅ Barchasi faol!"

        elif (
            "qarzi bor do'kon" in q_norm
            or "qarzdor do'kon" in q_norm
            or "qarzi bor do'konlarim" in q_norm
            or "qarzdor do'konlarim" in q_norm
        ) and not is_boss:
            cur.execute(
                """
                SELECT normalized_store, COALESCE(SUM(total)-SUM(cash),0) AS debt
                FROM sales
                WHERE worker_id = %s AND normalized_store IS NOT NULL AND normalized_store != ''
                GROUP BY normalized_store
                HAVING COALESCE(SUM(total)-SUM(cash),0) > 0
                ORDER BY debt DESC
                """,
                (uid,),
            )
            res = cur.fetchall()
            answer = "💳 **Qarzlaringiz:**\n" + "\n".join(
                [f"{i + 1}. {r['normalized_store']} - {fmt(r['debt'])}" for i, r in enumerate(res)]
            ) if res else "✅ Qarz yo'q!"

        elif "o'tgan oy" in q_norm and "bu oy" in q_norm and not is_boss:
            this_month = summarize_month(worker_sales, current_month)
            last = summarize_month(worker_sales, last_month)
            answer = (
                f"📊 **SOLISHTIRISH**\n"
                f"📅 O'tgan oy: 💰{fmt(last['total'])} 💵{fmt(last['cash'])} 📉{fmt(last['debt'])}\n"
                f"📅 Bu oy: 💰{fmt(this_month['total'])} 💵{fmt(this_month['cash'])} 📉{fmt(this_month['debt'])}"
            )

        elif "statistika" in q_norm or "umumiy" in q_norm:
            if is_boss:
                cur.execute(
                    """
                    SELECT
                        COUNT(DISTINCT worker_id) AS workers,
                        COUNT(DISTINCT normalized_store) AS stores,
                        COUNT(id) AS sales_count,
                        COALESCE(SUM(total),0) AS total,
                        COALESCE(SUM(cash),0) AS cash,
                        COALESCE(SUM(total)-SUM(cash),0) AS debt
                    FROM sales
                    """
                )
                r = cur.fetchone()
                answer = (
                    "📊 **UMUMIY**\n"
                    f"👥 Ishchilar: {r['workers']} ta\n"
                    f"🏪 Do'konlar: {r['stores']} ta\n"
                    f"📝 Savdolar: {r['sales_count']} ta\n"
                    f"💰 Hajm: {fmt(r['total'])}\n"
                    f"💵 Naqt: {fmt(r['cash'])}\n"
                    f"📉 Qarz: {fmt(r['debt'])}"
                )
            else:
                stores = {row["normalized_store"] for row in worker_sales if row.get("normalized_store")}
                total = sum((row["total"] or 0) for row in worker_sales)
                cash = sum((row["cash"] or 0) for row in worker_sales)
                debt = sum((row["total"] or 0) - (row["cash"] or 0) for row in worker_sales)
                r = {"stores": len(stores), "sales_count": len(worker_sales), "total": total, "cash": cash, "debt": debt}
                answer = (
                    "📊 **SIZNING**\n"
                    f"🏪 Do'konlar: {r['stores']} ta\n"
                    f"📝 Savdolar: {r['sales_count']} ta\n"
                    f"💰 Hajm: {fmt(r['total'])}\n"
                    f"💵 Naqt: {fmt(r['cash'])}\n"
                    f"📉 Qarz: {fmt(r['debt'])}"
                )

        elif "hisobot" in q_norm:
            if is_boss:
                cur.execute(
                    """
                    SELECT COUNT(id) AS count, COALESCE(SUM(total),0) AS total,
                           COALESCE(SUM(cash),0) AS cash, COALESCE(SUM(total)-SUM(cash),0) AS debt
                    FROM sales
                    WHERE date LIKE %s
                    """,
                    (f"%{current_month}%",),
                )
                r = cur.fetchone()
            else:
                r = summarize_month(worker_sales, current_month)
            answer = f"📅 **Oylik hisobot**\n📝 {r['count']} ta | 💰 {fmt(r['total'])} | 💵 {fmt(r['cash'])} | 📉 {fmt(r['debt'])}"

        elif "eng yaxshi do'kon" in q_norm:
            if is_boss:
                cur.execute(
                    """
                    SELECT normalized_store, COALESCE(SUM(total),0) AS t
                    FROM sales
                    WHERE normalized_store IS NOT NULL AND normalized_store != ''
                    GROUP BY normalized_store
                    ORDER BY t DESC
                    LIMIT 1
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT normalized_store, COALESCE(SUM(total),0) AS t
                    FROM sales
                    WHERE worker_id = %s AND normalized_store IS NOT NULL AND normalized_store != ''
                    GROUP BY normalized_store
                    ORDER BY t DESC
                    LIMIT 1
                    """,
                    (uid,),
                )
            r = cur.fetchone()
            answer = f"🏆 **Eng yaxshi do'kon**\n🏪 {r['normalized_store'] if r else 'Yoq'}\n💰 {fmt(r['t']) if r else 0}"

        elif "ishchi eng ko'p qarz" in q_norm and is_boss:
            cur.execute(
                """
                SELECT u.name, COALESCE(SUM(s.total)-SUM(s.cash),0) AS d
                FROM sales s
                JOIN users u ON s.worker_id = u.user_id
                GROUP BY u.user_id, u.name
                HAVING COALESCE(SUM(s.total)-SUM(s.cash),0) > 0
                ORDER BY d DESC
                LIMIT 3
                """
            )
            res = cur.fetchall()
            answer = "💰 **Qarz yig'ganlar:**\n" + "\n".join(
                [f"{i + 1}. {r['name']} - {fmt(r['d'])}" for i, r in enumerate(res)]
            ) if res else "✅ Qarz yo'q!"

        else:
            if is_boss:
                cur.execute(
                    "SELECT COUNT(id) AS count, COALESCE(SUM(total),0) AS total, COALESCE(SUM(cash),0) AS cash, COALESCE(SUM(total)-SUM(cash),0) AS debt FROM sales"
                )
            else:
                cur.execute(
                    "SELECT COUNT(id) AS count, COALESCE(SUM(total),0) AS total, COALESCE(SUM(cash),0) AS cash, COALESCE(SUM(total)-SUM(cash),0) AS debt FROM sales WHERE worker_id = %s",
                    (uid,),
                )
                s = cur.fetchone()
            if not is_boss:
                this_month = summarize_month(worker_sales, current_month)
                best_store = ""
                best_total = 0
                store_totals = {}
                for row in worker_sales:
                    store = row.get("normalized_store")
                    if not store:
                        continue
                    store_totals.setdefault(store, 0)
                    store_totals[store] += row["total"] or 0
                if store_totals:
                    best_store, best_total = max(store_totals.items(), key=lambda x: x[1])
                ctx = (
                    f"Ishchi statistikasi. "
                    f"Jami savdolar: {len(worker_sales)}, "
                    f"Jami hajm: {fmt(sum((r['total'] or 0) for r in worker_sales))}, "
                    f"Jami naqt: {fmt(sum((r['cash'] or 0) for r in worker_sales))}, "
                    f"Jami qarz: {fmt(sum((r['total'] or 0) - (r['cash'] or 0) for r in worker_sales))}, "
                    f"Bu oy savdo: {fmt(this_month['total'])}, "
                    f"Bu oy naqt: {fmt(this_month['cash'])}, "
                    f"Bu oy soni: {this_month['count']}, "
                    f"Eng yaxshi do'kon: {best_store or 'yoq'} ({fmt(best_total)})."
                )
            else:
                ctx = f"Savdolar: {s['count']}, Hajm: {fmt(s['total'])}, Naqt: {fmt(s['cash'])}, Qarz: {fmt(s['debt'])}"
            if client:
                try:
                    res = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": f"MK Kassa yordamchisisiz. Faqat berilgan ma'lumotga tayangan holda, qisqa va aniq javob bering. Ma'lumot: {ctx}"},
                            {"role": "user", "content": question},
                        ],
                        max_tokens=150,
                    )
                    answer = res.choices[0].message.content
                except Exception:
                    answer = "❓ AI hozircha band. Boshqa savol bering."
            else:
                answer = f"📊 Ma'lumot: {ctx}"

        conn.close()
        await msg.edit_text(answer, parse_mode="Markdown")
        await message.answer("❓ **Boshqa savol:**", reply_markup=get_ai_questions_keyboard(is_boss), parse_mode="Markdown")
    except Exception as e:
        print(f"AI error: {e}")
        await msg.edit_text("❌ Ma'lumot yuklanmadi.")


@dp.message(F.text == "📅 Oylik arxiv")
async def boss_monthly_archive(message: types.Message):
    if message.from_user.id not in BOSS_IDS:
        return
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT user_id, name FROM users WHERE role = 'worker' AND active = 1 ORDER BY name")
    workers = cur.fetchall()

    curr = datetime.now().strftime("%m.%Y")
    out = f"📅 Oylik arxiv ({curr})\n\n"
    for worker in workers:
        cur.execute("SELECT total, cash, date FROM sales WHERE worker_id = %s", (worker["user_id"],))
        rows = cur.fetchall()
        total_all = sum((r["total"] or 0) for r in rows)
        cash_all = sum((r["cash"] or 0) for r in rows)
        month_rows = [r for r in rows if curr in extract_month_keys(r["date"])]
        month_total = sum((r["total"] or 0) for r in month_rows)
        month_cash = sum((r["cash"] or 0) for r in month_rows)
        month_debt = month_total - month_cash
        old_debt = max(0, (total_all - cash_all) - month_debt)
        current_balance = old_debt + month_debt
        out += (
            f"📅 Hisobot ({curr}):\n"
            f"ishchi {worker['name']}\n"
            f"📉 O'tgan: {fmt(old_debt)}\n"
            f"💰 Bu oy: {fmt(month_total)}\n"
            f"💵 Naqt: {fmt(month_cash)}\n"
            f"📉 Yangi: {fmt(month_debt)}\n"
            f"✅ Joriy: {fmt(current_balance)}\n\n"
        )
    conn.close()
    await message.answer(out)


async def render_boss_all_stores(target, state: FSMContext, filtered_stores=None, title="🏪 Barcha do'konlar"):
    is_callback = isinstance(target, CallbackQuery)
    msg = target.message if is_callback else target
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute(
        """
        SELECT s.normalized_store,
               COALESCE(MAX(u.name), '') AS worker_name,
               COALESCE(SUM(s.total),0) AS total_sales,
               COALESCE(SUM(s.cash),0) AS total_cash,
               COALESCE(SUM(s.total)-SUM(s.cash),0) AS total_debt,
               COUNT(s.id) AS sales_count
        FROM sales s
        JOIN users u ON s.worker_id = u.user_id
        WHERE s.normalized_store IS NOT NULL AND s.normalized_store != ''
        GROUP BY s.normalized_store
        ORDER BY s.normalized_store
        """
    )
    stores = cur.fetchall()
    conn.close()
    if filtered_stores is not None:
        stores = filtered_stores

    if not stores:
        if is_callback:
            return await msg.edit_text("🏪 Hozircha do'kon yo'q.")
        return await msg.answer("🏪 Hozircha do'kon yo'q.")

    debt_count = sum(1 for s in stores if (s["total_debt"] or 0) > 0)
    clean_count = len(stores) - debt_count
    total_sales = sum((s["total_sales"] or 0) for s in stores)
    total_cash = sum((s["total_cash"] or 0) for s in stores)

    store_map = {f"all_{i}": s["normalized_store"] for i, s in enumerate(stores)}
    await state.update_data(boss_all_store_map=store_map)

    out = (
        f"{title}\n\n"
        f"📊 Jami do'konlar: {len(stores)} ta\n"
        f"💰 Umumiy savdo: {fmt(total_sales)}\n"
        f"💵 Umumiy naqt: {fmt(total_cash)}\n"
        f"📉 Qarzdor do'konlar: {debt_count} ta\n"
        f"✅ Qarzsiz do'konlar: {clean_count} ta\n\n"
        "📋 Ro'yxat:\n"
    )
    for i, s in enumerate(stores, 1):
        out += (
            f"{i}. 🏪 {s['normalized_store']}\n"
            f"👤 Ishchi: {s['worker_name'] or 'nomaʼlum'}\n"
            f"💰 Savdo: {fmt(s['total_sales'])}\n"
            f"💵 Naqt: {fmt(s['total_cash'])}\n"
            f"📉 Qarz: {fmt(s['total_debt'])}\n\n"
        )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"🏪 {s['normalized_store']}", callback_data=f"bstore_all_{i}")]
            for i, s in enumerate(stores)
        ]
    )
    kb.inline_keyboard.append(
        [
            InlineKeyboardButton(text="📉 Qarzdorlar", callback_data="stores_filter_debt"),
            InlineKeyboardButton(text="✅ Qarzsizlar", callback_data="stores_filter_clean"),
        ]
    )
    kb.inline_keyboard.append(
        [
            InlineKeyboardButton(text="👤 Ishchi bo'yicha", callback_data="stores_filter_workers"),
            InlineKeyboardButton(text="💰 Savdo bo'yicha", callback_data="stores_filter_top"),
        ]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    if is_callback:
        await msg.edit_text(out, reply_markup=kb)
    else:
        await msg.answer(out, reply_markup=kb)


@dp.message(F.text == "🏪 Barcha do'konlar")
async def boss_all_stores(message: types.Message, state: FSMContext):
    if message.from_user.id not in BOSS_IDS:
        return
    await render_boss_all_stores(message, state)


@dp.callback_query(F.data.startswith("bstore_all_"))
async def boss_all_store_open(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    idx = callback.data.replace("bstore_all_", "")
    data = await state.get_data()
    store = (data.get("boss_all_store_map") or {}).get(f"all_{idx}")
    if not store:
        return await callback.answer("⚠️ Do'kon topilmadi.", show_alert=True)
    await open_store_by_name(callback, state, store)


@dp.callback_query(F.data == "stores_filter_debt")
async def stores_filter_debt(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute(
        """
        SELECT s.normalized_store,
               COALESCE(MAX(u.name), '') AS worker_name,
               COALESCE(SUM(s.total),0) AS total_sales,
               COALESCE(SUM(s.cash),0) AS total_cash,
               COALESCE(SUM(s.total)-SUM(s.cash),0) AS total_debt,
               COUNT(s.id) AS sales_count
        FROM sales s
        JOIN users u ON s.worker_id = u.user_id
        WHERE s.normalized_store IS NOT NULL AND s.normalized_store != ''
        GROUP BY s.normalized_store
        HAVING COALESCE(SUM(s.total)-SUM(s.cash),0) > 0
        ORDER BY total_debt DESC
        """
    )
    stores = cur.fetchall()
    conn.close()
    await render_boss_all_stores(callback, state, stores, "🏪 Qarzdor do'konlar")


@dp.callback_query(F.data == "stores_filter_clean")
async def stores_filter_clean(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute(
        """
        SELECT s.normalized_store,
               COALESCE(MAX(u.name), '') AS worker_name,
               COALESCE(SUM(s.total),0) AS total_sales,
               COALESCE(SUM(s.cash),0) AS total_cash,
               COALESCE(SUM(s.total)-SUM(s.cash),0) AS total_debt,
               COUNT(s.id) AS sales_count
        FROM sales s
        JOIN users u ON s.worker_id = u.user_id
        WHERE s.normalized_store IS NOT NULL AND s.normalized_store != ''
        GROUP BY s.normalized_store
        HAVING COALESCE(SUM(s.total)-SUM(s.cash),0) <= 0
        ORDER BY s.normalized_store
        """
    )
    stores = cur.fetchall()
    conn.close()
    await render_boss_all_stores(callback, state, stores, "🏪 Qarzsiz do'konlar")


@dp.callback_query(F.data == "stores_filter_top")
async def stores_filter_top(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute(
        """
        SELECT s.normalized_store,
               COALESCE(MAX(u.name), '') AS worker_name,
               COALESCE(SUM(s.total),0) AS total_sales,
               COALESCE(SUM(s.cash),0) AS total_cash,
               COALESCE(SUM(s.total)-SUM(s.cash),0) AS total_debt,
               COUNT(s.id) AS sales_count
        FROM sales s
        JOIN users u ON s.worker_id = u.user_id
        WHERE s.normalized_store IS NOT NULL AND s.normalized_store != ''
        GROUP BY s.normalized_store
        ORDER BY total_sales DESC
        """
    )
    stores = cur.fetchall()
    conn.close()
    await render_boss_all_stores(callback, state, stores, "🏪 Savdo bo'yicha do'konlar")


@dp.callback_query(F.data == "stores_filter_workers")
async def stores_filter_workers(callback: CallbackQuery):
    await callback.answer()
    workers = load_workers()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"👤 {w['name']}", callback_data=f"stores_by_worker_{w['user_id']}")] for w in workers]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="stores_filter_back_all")])
    await callback.message.edit_text("👥 Ishchini tanlang:", reply_markup=kb)


@dp.callback_query(F.data.startswith("stores_by_worker_"))
async def stores_by_worker(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    worker_id = int(callback.data.replace("stores_by_worker_", ""))
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute(
        """
        SELECT s.normalized_store,
               COALESCE(MAX(u.name), '') AS worker_name,
               COALESCE(SUM(s.total),0) AS total_sales,
               COALESCE(SUM(s.cash),0) AS total_cash,
               COALESCE(SUM(s.total)-SUM(s.cash),0) AS total_debt,
               COUNT(s.id) AS sales_count
        FROM sales s
        JOIN users u ON s.worker_id = u.user_id
        WHERE s.normalized_store IS NOT NULL AND s.normalized_store != '' AND s.worker_id = %s
        GROUP BY s.normalized_store
        ORDER BY s.normalized_store
        """,
        (worker_id,),
    )
    stores = cur.fetchall()
    cur.execute("SELECT name FROM users WHERE user_id = %s", (worker_id,))
    worker = cur.fetchone()
    conn.close()
    title = f"🏪 {worker['name'] if worker else 'Ishchi'} do'konlari"
    await render_boss_all_stores(callback, state, stores, title)


@dp.callback_query(F.data == "stores_filter_back_all")
async def stores_filter_back_all(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await render_boss_all_stores(callback, state)


@dp.message(F.text == "📊 Eng yaxshi ishchilar")
async def boss_top_workers(message: types.Message):
    if message.from_user.id not in BOSS_IDS:
        return
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute(
        """
        SELECT u.name AS worker_name,
               COALESCE(SUM(s.total),0) AS total_sales,
               COALESCE(SUM(s.cash),0) AS total_cash,
               COALESCE(SUM(s.total)-SUM(s.cash),0) AS total_debt,
               COUNT(s.id) AS sales_count
        FROM sales s
        JOIN users u ON s.worker_id = u.user_id
        GROUP BY u.name
        ORDER BY total_sales DESC
        """
    )
    res = cur.fetchall()
    conn.close()

    if not res:
        return await message.answer("📊 Ma'lumot yo'q.")

    out = "🏆 **Ishchilar reytingi**\n"
    for i, r in enumerate(res, 1):
        out += (
            f"{i}. {r['worker_name']} | "
            f"💰 {fmt(r['total_sales'])} | "
            f"💵 {fmt(r['total_cash'])} | "
            f"📉 {fmt(r['total_debt'])} | "
            f"🧾 {r['sales_count']}\n"
        )
    await message.answer(out, parse_mode="Markdown")


@dp.message(F.text == "🏆 Eng yaxshi do'konlar")
async def boss_top_stores(message: types.Message):
    if message.from_user.id not in BOSS_IDS:
        return
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute(
        """
        SELECT normalized_store, COALESCE(SUM(total),0) AS total_sales, COUNT(id) AS sales_count
        FROM sales
        WHERE normalized_store IS NOT NULL AND normalized_store != ''
        GROUP BY normalized_store
        ORDER BY total_sales DESC
        LIMIT 10
        """
    )
    res = cur.fetchall()
    conn.close()

    if not res:
        return await message.answer("📊 Ma'lumot yo'q.")

    out = "🏆 **TOP Do'konlar**\n"
    for i, r in enumerate(res, 1):
        out += f"{i}. 🏪 {r['normalized_store']} | 💰 {fmt(r['total_sales'])} | 🧾 {r['sales_count']}\n"
    await message.answer(out, parse_mode="Markdown")


# ================= OYLIK KASSA =================
@dp.message(F.text == "📅 Oylik kassa")
async def handle_monthly_cash(message: types.Message):
    uid = message.from_user.id

    if uid not in BOSS_IDS:
        conn = get_db()
        cur = dict_cursor(conn)
        cur.execute("SELECT date, cash FROM sales WHERE worker_id = %s AND cash > 0 ORDER BY date DESC", (uid,))
        raw_rows = cur.fetchall()
        conn.close()

        months = collect_available_months(raw_rows)
        if not months:
            return await message.answer("📅 Hozircha oylik kassa ma'lumoti yo'q.")

        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text=f"📅 {m}", callback_data=f"wm_{m}")] for m in months]
        )
        kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
        return await message.answer("📅 Oyni tanlang:", reply_markup=kb)

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT date FROM sales ORDER BY date DESC")
    months = collect_available_months(cur.fetchall())
    conn.close()
    if not months:
        return await message.answer("📅 Hozircha oylik kassa ma'lumoti yo'q.")
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"📅 {month}", callback_data=f"mc_month_{month}")] for month in months]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    await message.answer("📅 Boss uchun oyni tanlang:", reply_markup=kb)


@dp.callback_query(F.data.startswith("mc_month_"))
async def boss_month_mode(callback: CallbackQuery):
    await callback.answer()
    month = callback.data.replace("mc_month_", "")
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📅 Kunlik Umumiy", callback_data=f"mc_all_{month}")],
            [InlineKeyboardButton(text="👥 Ishchi bo'yicha", callback_data=f"mc_worker_{month}")],
            [InlineKeyboardButton(text="⬅️ Oylar", callback_data="boss_months_back")],
        ]
    )
    await callback.message.edit_text(f"📅 Oylik kassa ({month}) rejimi:", reply_markup=kb)


@dp.callback_query(F.data == "boss_months_back")
async def boss_months_back(callback: CallbackQuery):
    await callback.answer()
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT date FROM sales ORDER BY date DESC")
    months = collect_available_months(cur.fetchall())
    conn.close()
    if not months:
        return await callback.message.edit_text("📅 Hozircha oylik kassa ma'lumoti yo'q.")
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"📅 {month}", callback_data=f"mc_month_{month}")] for month in months]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    await callback.message.edit_text("📅 Boss uchun oyni tanlang:", reply_markup=kb)


@dp.callback_query(F.data.startswith("wm_"))
async def worker_month_dates(callback: CallbackQuery):
    await callback.answer()
    month = callback.data.replace("wm_", "")
    uid = callback.from_user.id
    if uid in BOSS_IDS:
        return

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT date, cash FROM sales WHERE worker_id = %s AND cash > 0 ORDER BY date DESC", (uid,))
    raw_rows = cur.fetchall()
    conn.close()

    grouped = {}
    for row in raw_rows:
        day = date_head(row["date"])
        if month not in extract_month_keys(row["date"]):
            continue
        grouped.setdefault(day, 0)
        grouped[day] += row["cash"] or 0

    rows = [{"d": d, "cash": grouped[d]} for d in sorted(grouped.keys(), reverse=True)]
    if not rows:
        return await callback.message.edit_text(f"📅 {month}: Ma'lumot yo'q.")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"📆 {r['d']} | {fmt(r['cash'])}", callback_data=f"day_{r['d']}")]
            for r in rows
        ]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Oylar", callback_data="worker_months_back")])
    await callback.message.edit_text(f"📅 {month} kassa:", reply_markup=kb)


@dp.callback_query(F.data == "worker_months_back")
async def worker_months_back(callback: CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid in BOSS_IDS:
        return

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT date FROM sales WHERE worker_id = %s ORDER BY date DESC", (uid,))
    months = collect_available_months(cur.fetchall())
    conn.close()

    if not months:
        return await callback.message.edit_text("📅 Hozircha oylik kassa ma'lumoti yo'q.")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"📅 {m}", callback_data=f"wm_{m}")] for m in months]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    await callback.message.edit_text("📅 Oyni tanlang:", reply_markup=kb)


@dp.callback_query(F.data.startswith("mc_all_"))
async def mc_all_dates(callback: CallbackQuery):
    await callback.answer()
    month = callback.data.replace("mc_all_", "")
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT date FROM sales ORDER BY date DESC")
    dates = sorted({date_head(r["date"]) for r in cur.fetchall() if month in extract_month_keys(r["date"])}, reverse=True)
    conn.close()
    if not dates:
        return await callback.message.edit_text("📭 Yo'q.")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"📆 {d}", callback_data=f"day_all_{d}")] for d in dates]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    await callback.message.edit_text(f"📅 {month} kunlari:", reply_markup=kb)


@dp.callback_query(F.data.startswith("day_all_"))
async def day_all_summary(callback: CallbackQuery):
    await callback.answer()
    day = callback.data.replace("day_all_", "")
    month_options = sorted(extract_month_keys(day), reverse=True)
    back_month = month_options[0] if month_options else datetime.now().strftime("%m.%Y")
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute(
        """
        SELECT u.name AS worker_name, s.store_name, COALESCE(SUM(s.cash),0) AS cash
        FROM sales s
        JOIN users u ON s.worker_id = u.user_id
        WHERE s.date LIKE %s AND s.cash > 0
        GROUP BY u.name, s.store_name
        ORDER BY u.name, s.store_name
        """,
        (f"{day}%",),
    )
    rows = cur.fetchall()
    cur.execute(
        """
        SELECT u.name AS worker_name, COALESCE(SUM(s.cash),0) AS total
        FROM sales s
        JOIN users u ON s.worker_id = u.user_id
        WHERE s.date LIKE %s AND s.cash > 0
        GROUP BY u.name
        """,
        (f"{day}%",),
    )
    totals = {r["worker_name"]: r["total"] for r in cur.fetchall()}
    conn.close()

    out = f"💰 {day}\n"
    current_worker = ""
    grand_total = 0
    for r in rows:
        if r["worker_name"] != current_worker:
            if current_worker:
                out += f"✅ {current_worker} - Jami: {fmt(totals.get(current_worker, 0))}\n\n"
            current_worker = r["worker_name"]
            out += f"👤 **{current_worker}**:\n"
        out += f"🏪 {r['store_name']} - {fmt(r['cash'])}\n"
        grand_total += r["cash"] or 0
    if current_worker:
        out += f"✅ {current_worker} - Jami: {fmt(totals.get(current_worker, 0))}"
    out += f"\n\n💵 UMUMIY JAMI: {fmt(grand_total)}"

    await callback.message.edit_text(
        out,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Sanalar", callback_data=f"mc_all_{back_month}")]]),
    )


@dp.callback_query(F.data.startswith("mc_worker_"))
async def mc_worker_list(callback: CallbackQuery):
    await callback.answer()
    month = callback.data.replace("mc_worker_", "")
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT name, user_id FROM users WHERE role = 'worker' AND active = 1 ORDER BY name")
    workers = cur.fetchall()
    conn.close()

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"👤 {w['name']}", callback_data=f"sel_worker_{w['user_id']}_{month}")] for w in workers]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data=f"mc_month_{month}")])
    await callback.message.edit_text("👥 Ishchini tanlang:", reply_markup=kb)


@dp.callback_query(F.data.startswith("sel_worker_"))
async def sel_worker_dates(callback: CallbackQuery):
    await callback.answer()
    payload = callback.data.replace("sel_worker_", "")
    uid_text, month = payload.split("_", 1)
    uid = int(uid_text)
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT name FROM users WHERE user_id = %s", (uid,))
    worker = cur.fetchone()
    if not worker:
        conn.close()
        return await callback.message.edit_text("⚠️ Ishchi topilmadi.")

    cur.execute("SELECT date FROM sales WHERE worker_id = %s ORDER BY date DESC", (uid,))
    dates = sorted({date_head(r["date"]) for r in cur.fetchall() if month in extract_month_keys(r["date"])}, reverse=True)
    conn.close()
    if not dates:
        return await callback.message.edit_text("📭 Yo'q.")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"📆 {d}", callback_data=f"day_worker_{uid}_{d}")] for d in dates]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Ishchilar", callback_data=f"mc_worker_{month}")])
    await callback.message.edit_text(f"👤 {worker['name']} sanalari ({month}):", reply_markup=kb)


@dp.callback_query(F.data.startswith("day_worker_"))
async def day_worker_summary(callback: CallbackQuery):
    await callback.answer()
    uid_text, day = callback.data.replace("day_worker_", "").split("_", 1)
    uid = int(uid_text)

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT name FROM users WHERE user_id = %s", (uid,))
    worker = cur.fetchone()
    cur.execute(
        """
        SELECT store_name, COALESCE(SUM(total),0) AS total, COALESCE(SUM(cash),0) AS cash,
               COALESCE(SUM(total)-SUM(cash),0) AS debt
        FROM sales
        WHERE worker_id = %s AND date LIKE %s
        GROUP BY store_name
        ORDER BY store_name
        """,
        (uid, f"{day}%"),
    )
    rows = cur.fetchall()
    conn.close()

    worker_name = worker["name"] if worker else f"ID:{uid}"
    out = f"👤 {worker_name} - {day}\n\n"
    total_cash = 0
    for r in rows:
        out += (
            f"🏪 Do'kon: {r['store_name']}\n"
            f"💰 Savdo: {fmt(r['total'])}\n"
            f"💵 Naqt: {fmt(r['cash'])}\n"
            f"📉 Qarz: {fmt(r['debt'])}\n\n"
        )
        total_cash += r["cash"] or 0
    out += f"💰 Jami naqt: {fmt(total_cash)}"

    await callback.message.edit_text(
        out,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Sanalar", callback_data=f"sel_worker_{uid}")]]),
    )


@dp.message(F.text == "💰 Oylik maosh")
async def calculate_salary(message: types.Message):
    uid = message.from_user.id
    month = datetime.now().strftime("%m.%Y")

    if uid in BOSS_IDS:
        conn = get_db()
        cur = dict_cursor(conn)
        cur.execute("SELECT user_id, name FROM users WHERE role = 'worker' AND active = 1 ORDER BY name")
        workers = cur.fetchall()
        out = f"💰 Oylik maosh hisoboti ({month}):\n\n"
        grand_total = 0
        for w in workers:
            cur.execute("SELECT cash, date FROM sales WHERE worker_id = %s AND cash > 0", (w["user_id"],))
            total_cash = 0
            for row in cur.fetchall():
                if month in extract_month_keys(row["date"]):
                    total_cash += row["cash"] or 0
            percent = total_cash * 0.08
            fixa = 150 if 1500 <= total_cash < 2000 else (200 if 2000 <= total_cash < 3000 else (300 if total_cash >= 3000 else 0))
            salary = percent + fixa
            grand_total += salary
            out += (
                f"👥 {w['name']}\n"
                f"📊 Yig'ilgan naqt: {fmt(total_cash)}\n"
                f"📈 8% ulush: {fmt(percent)}\n"
                f"🎁 Fiksa bonus: {fmt(fixa)}\n"
                f"✅ Jami maosh: {fmt(salary)}\n\n"
            )
        out += f"💰 JAMI MAOSH XARAJATI: {fmt(grand_total)}"
        conn.close()
        return await message.answer(out)

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT cash, date FROM sales WHERE worker_id = %s AND cash > 0", (uid,))
    total_cash = 0
    for row in cur.fetchall():
        if month in extract_month_keys(row["date"]):
            total_cash += row["cash"] or 0
    conn.close()

    percent = total_cash * 0.08
    fixa = 150 if 1500 <= total_cash < 2000 else (200 if 2000 <= total_cash < 3000 else (300 if total_cash >= 3000 else 0))
    await message.answer(
        f"💰 Oylik maosh hisoboti ({month}):\n\n"
        f"📊 Yig'ilgan naqt: {fmt(total_cash)}\n"
        f"📈 8% ulush: {fmt(percent)}\n"
        f"🎁 Fiksa bonus: {fmt(fixa)}\n"
        f"✅ Jami maosh: {fmt(percent + fixa)}"
    )


# ================= ISHCHI =================
@dp.message(F.text == "📊 Kunlik kassa")
async def daily_cash(message: types.Message):
    uid = message.from_user.id
    if uid in BOSS_IDS:
        return

    today = date.today().strftime("%d.%m.%Y")
    w_cond, w_params = get_worker_filter(uid)
    params = (f"{today}%",) + w_params
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT store_name, cash, date FROM sales WHERE date LIKE %s AND cash > 0 " + w_cond + " ORDER BY id DESC", params)
    rows = cur.fetchall()
    cur.execute("SELECT COALESCE(SUM(cash),0) AS total_cash FROM sales WHERE date LIKE %s AND cash > 0 " + w_cond, params)
    total_cash = cur.fetchone()["total_cash"]
    conn.close()

    if not rows:
        return await message.answer("📅 Bugun naqt yo'q.")

    out = f"📅 Bugun ({today}):\n"
    for r in rows:
        time_part = r["date"].split()[1] if " " in r["date"] else ""
        out += f"🏪 {r['store_name']} | 💵 {fmt(r['cash'])} | 🕒 {time_part}\n"
    out += f"\n💰 Jami: {fmt(total_cash)}"
    await message.answer(out)


@dp.message(F.text == "📝 Savdo kiritish")
async def pro_trade_alias(message: types.Message, state: FSMContext):
    await trade_init(message, state)


@dp.message(F.text == "💵 Bugungi kassa")
async def pro_daily_cash_alias(message: types.Message):
    await daily_cash(message)


@dp.message(F.text == "🧾 Do'konlarim")
async def pro_store_alias(message: types.Message):
    await stores_list_cmd(message)


@dp.message(F.text == "📚 Oylik arxiv")
async def pro_worker_archive_alias(message: types.Message):
    if message.from_user.id in BOSS_IDS:
        await boss_monthly_archive(message)
    else:
        await handle_monthly_cash(message)


@dp.message(F.text == "📈 Statistika")
async def pro_my_stats(message: types.Message):
    if message.from_user.id in BOSS_IDS:
        return
    rows = load_sales(message.from_user.id)
    s = worker_summary(rows)
    out = (
        "📈 Sizning statistikangiz\n\n"
        f"🧾 Savdolar: {s['sales_count']} ta\n"
        f"🏪 Do'konlar: {s['stores']} ta\n"
        f"💰 Jami savdo: {fmt(s['total'])}\n"
        f"💵 Jami naqt: {fmt(s['cash'])}\n"
        f"📉 Jami qarz: {fmt(s['debt'])}\n"
        f"🏆 Eng yaxshi do'kon: {s['best_store']} ({fmt(s['best_value'])})\n"
        f"📊 O'rtacha tushum: {fmt(s['avg'])}"
    )
    await message.answer(out)


@dp.message(F.text == "📊 Boss Panel")
async def pro_boss_panel(message: types.Message):
    if message.from_user.id not in BOSS_IDS:
        return
    rows = load_sales()
    today = date.today().strftime("%d.%m.%Y")
    month_key = datetime.now().strftime("%m.%Y")
    today_rows = [r for r in rows if r["date"].startswith(today)]
    month_rows = [r for r in rows if month_key in extract_month_keys(r["date"])]
    out = (
        "📊 Boss Panel\n\n"
        f"🧾 Bugungi savdolar: {len(today_rows)} ta\n"
        f"💵 Bugungi naqt: {fmt(sum((r['cash'] or 0) for r in today_rows))}\n"
        f"💰 Shu oy savdo: {fmt(sum((r['total'] or 0) for r in month_rows))}\n"
        f"📉 Shu oy qarz: {fmt(sum((r['total'] or 0) - (r['cash'] or 0) for r in month_rows))}\n"
        f"🏪 Do'konlar: {len({r['normalized_store'] for r in rows if r.get('normalized_store')})} ta\n"
        f"👥 Ishchilar: {len(load_workers())} ta"
    )
    await message.answer(out)


@dp.message(F.text == "👥 Ishchi statistikasi")
async def pro_worker_stats(message: types.Message):
    if message.from_user.id not in BOSS_IDS:
        return await pro_my_stats(message)
    workers = load_workers()
    rows = load_sales()
    out = "👥 Ishchi statistikasi\n\n"
    for worker in workers:
        w_rows = [r for r in rows if r["worker_id"] == worker["user_id"]]
        s = worker_summary(w_rows)
        out += (
            f"👤 {worker['name']}\n"
            f"💰 Oylik savdo: {fmt(s['total'])}\n"
            f"💵 Jami naqt: {fmt(s['cash'])}\n"
            f"🏆 Eng yaxshi do'kon: {s['best_store']}\n"
            f"📉 Eng katta qarz: {fmt(s['debt'])}\n"
            f"📊 O'rtacha tushum: {fmt(s['avg'])}\n\n"
        )
    await message.answer(out)


@dp.message(F.text == "🏪 Do'kon reytingi")
async def pro_store_ranking(message: types.Message):
    if message.from_user.id not in BOSS_IDS:
        return
    rows = load_sales()
    stats = {}
    for r in rows:
        store = r.get("normalized_store")
        if not store:
            continue
        stats.setdefault(store, {"total": 0, "cash": 0, "debt": 0, "last": None})
        stats[store]["total"] += r["total"] or 0
        stats[store]["cash"] += r["cash"] or 0
        stats[store]["debt"] += (r["total"] or 0) - (r["cash"] or 0)
        dt = parse_db_date_to_date(r["date"])
        if dt and (stats[store]["last"] is None or dt > stats[store]["last"]):
            stats[store]["last"] = dt
    if not stats:
        return await message.answer("🏪 Ma'lumot yo'q.")
    sales_top = sorted(stats.items(), key=lambda x: x[1]["total"], reverse=True)[:5]
    cash_top = sorted(stats.items(), key=lambda x: x[1]["cash"], reverse=True)[:5]
    debt_top = sorted(stats.items(), key=lambda x: x[1]["debt"], reverse=True)[:5]
    slow_top = sorted(stats.items(), key=lambda x: x[1]["last"] or date.min)[:5]
    out = "🏪 Do'kon reytingi\n\n"
    out += "💰 Eng ko'p savdo:\n" + "\n".join([f"{i+1}. {s} - {fmt(v['total'])}" for i, (s, v) in enumerate(sales_top)]) + "\n\n"
    out += "💵 Eng ko'p naqt:\n" + "\n".join([f"{i+1}. {s} - {fmt(v['cash'])}" for i, (s, v) in enumerate(cash_top)]) + "\n\n"
    out += "📉 Eng ko'p qarz:\n" + "\n".join([f"{i+1}. {s} - {fmt(v['debt'])}" for i, (s, v) in enumerate(debt_top)]) + "\n\n"
    out += "🐢 Eng sust do'kon:\n" + "\n".join([f"{i+1}. {s} - {(v['last'].strftime('%d.%m.%Y') if v['last'] else 'nomaʼlum')}" for i, (s, v) in enumerate(slow_top)])
    await message.answer(out)


@dp.message(F.text == "🔔 Eslatma")
async def pro_alerts(message: types.Message):
    if message.from_user.id not in BOSS_IDS:
        return
    rows = load_sales()
    stats = {}
    for r in rows:
        store = r.get("normalized_store")
        if not store:
            continue
        stats.setdefault(store, {"debt": 0, "last": None})
        stats[store]["debt"] += (r["total"] or 0) - (r["cash"] or 0)
        dt = parse_db_date_to_date(r["date"])
        if dt and (stats[store]["last"] is None or dt > stats[store]["last"]):
            stats[store]["last"] = dt
    debt_alerts = [(s, v["debt"]) for s, v in stats.items() if v["debt"] >= 1000]
    stale_alerts = [(s, v["last"]) for s, v in stats.items() if v["last"] and v["last"] < date.today() - timedelta(days=30)]
    out = "🔔 Eslatmalar\n\n"
    out += "📉 Katta qarzlar:\n"
    out += "\n".join([f"• {s} - {fmt(v)}" for s, v in debt_alerts[:10]]) if debt_alerts else "• yo'q"
    out += "\n\n⏰ Uzoqdan beri faol bo'lmaganlar:\n"
    out += "\n".join([f"• {s} - {d.strftime('%d.%m.%Y')}" for s, d in stale_alerts[:10]]) if stale_alerts else "• yo'q"
    await message.answer(out)


@dp.message(F.text == "📅 Sana filter")
async def pro_range_prompt(message: types.Message, state: FSMContext):
    await state.set_state(AppStates.pro_range_waiting)
    await message.answer("📅 Sana yuboring:\n`03.2026` yoki `01.03.2026 - 20.03.2026`", parse_mode="Markdown", reply_markup=get_back_kb())


@dp.message(AppStates.pro_range_waiting)
async def pro_range_report(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga":
        await state.clear()
        return await start_cmd(message, state)
    parsed = parse_date_range(message.text or "")
    if not parsed:
        return await message.answer("⚠️ Format noto'g'ri. Misol: `03.2026` yoki `01.03.2026 - 20.03.2026`", parse_mode="Markdown")
    rows = load_sales(None if message.from_user.id in BOSS_IDS else message.from_user.id)
    filtered = [r for r in rows if in_selected_range(r["date"], parsed)]
    total = sum((r["total"] or 0) for r in filtered)
    cash = sum((r["cash"] or 0) for r in filtered)
    debt = sum((r["total"] or 0) - (r["cash"] or 0) for r in filtered)
    await state.clear()
    await message.answer(
        f"📅 Filter natijasi: {message.text}\n\n🧾 Savdolar: {len(filtered)} ta\n💰 Savdo: {fmt(total)}\n💵 Naqt: {fmt(cash)}\n📉 Qarz: {fmt(debt)}",
        reply_markup=get_boss_menu() if message.from_user.id in BOSS_IDS else get_worker_menu(),
    )


@dp.message(F.text == "🕘 Oxirgi amal")
async def pro_last_action(message: types.Message, state: FSMContext):
    rows = load_sales(message.from_user.id)
    if not rows:
        return await message.answer("🕘 Oxirgi amal topilmadi.")
    last_row = rows[0]
    can_edit = False
    if " " in last_row["date"]:
        try:
            dt = datetime.strptime(last_row["date"], "%d.%m.%Y %H:%M")
            can_edit = datetime.now() - dt <= timedelta(minutes=5)
        except Exception:
            can_edit = False
    await state.update_data(pro_edit_sale_id=last_row["id"])
    kb_rows = []
    if can_edit:
        kb_rows.append([InlineKeyboardButton(text="✏️ Tez edit", callback_data="pro_quick_edit")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None
    await message.answer("🕘 Oxirgi amal\n\n" + fmt_card(last_row["store_name"], last_row["total"], last_row["cash"], last_row["date"]), reply_markup=kb)


@dp.callback_query(F.data == "pro_quick_edit")
async def pro_quick_edit_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AppStates.pro_quick_edit_waiting)
    await callback.message.answer("✏️ Yangi format yuboring:\n`summa naqt sana`\nMasalan: `3000 500 11.03.2026`", parse_mode="Markdown", reply_markup=get_back_kb())


@dp.message(AppStates.pro_quick_edit_waiting)
async def pro_quick_edit_save(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga":
        await state.clear()
        return await start_cmd(message, state)
    data = await state.get_data()
    sale_id = data.get("pro_edit_sale_id")
    m = re.match(r"^\s*([\d.,]+)\s+([\d.,]+)\s+(\d{2}\.\d{2}\.\d{2,4})\s*$", message.text or "")
    if not sale_id or not m:
        return await message.answer("⚠️ Format noto'g'ri. Misol: `3000 500 11.03.2026`", parse_mode="Markdown")
    total = safe_float(m.group(1))
    cash = safe_float(m.group(2))
    sale_date = m.group(3)
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("UPDATE sales SET total = %s, cash = %s, debt = %s, date = %s WHERE id = %s RETURNING store_name", (total, cash, total - cash, sale_date, sale_id))
    row = cur.fetchone()
    conn.commit()
    conn.close()
    await state.clear()
    await message.answer("✅ Oxirgi amal yangilandi.\n\n" + fmt_card(row["store_name"], total, cash, sale_date))


@dp.message(F.text == "🤖 AI Pro")
async def pro_ai_prompt(message: types.Message, state: FSMContext):
    await state.set_state(AppStates.pro_ai_waiting)
    await message.answer(
        "🤖 AI Pro savolingizni yozing.\nMasalan:\n`Ali bu oy qancha naqt yig'di?`\n`Bilol do'koni oxirgi marta qachon savdo qilgan?`\n`Qaysi ishchi bu hafta sust ishlagan?`",
        parse_mode="Markdown",
        reply_markup=get_back_kb(),
    )


@dp.message(AppStates.pro_ai_waiting)
async def pro_ai_answer(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga":
        await state.clear()
        return await start_cmd(message, state)
    q = (message.text or "").lower().replace("dokon", "do'kon").replace("dokoni", "do'koni")
    rows = load_sales()
    workers = load_workers()
    answer = "❓ Savolni tushunmadim."

    for worker in workers:
        name = worker["name"].lower()
        if name in q and "bu oy" in q and ("naqt" in q or "yig" in q):
            month_key = datetime.now().strftime("%m.%Y")
            cash = sum((r["cash"] or 0) for r in rows if r["worker_id"] == worker["user_id"] and month_key in extract_month_keys(r["date"]))
            answer = f"👤 {worker['name']}\n💵 Bu oy naqt: {fmt(cash)}"
            break

    if "oxirgi marta" in q and "do'koni" in q:
        for r in rows:
            store = r.get("normalized_store")
            if store and store.lower() in q:
                latest = next((x for x in rows if x.get("normalized_store") == store), None)
                if latest:
                    answer = f"🏪 {store}\n📅 Oxirgi savdo: {latest['date']}"
                break

    if "bu hafta" in q and "sust" in q:
        threshold = date.today() - timedelta(days=7)
        counts = {}
        for r in rows:
            dt = parse_db_date_to_date(r["date"])
            if dt and dt >= threshold:
                counts.setdefault(r["worker_name"], 0)
                counts[r["worker_name"]] += 1
        if counts:
            name, cnt = min(counts.items(), key=lambda x: x[1])
            answer = f"📉 Bu hafta eng sust ishchi: {name}\n🧾 Savdolar soni: {cnt} ta"

    if message.from_user.id in BOSS_IDS:
        await message.answer(answer)
        await message.answer("❓ Yana savol bering yoki `⬅️ Orqaga` bosing.", reply_markup=get_back_kb(), parse_mode="Markdown")
    else:
        await state.clear()
        await message.answer(answer, reply_markup=get_worker_menu())


@dp.callback_query(F.data.startswith("day_"))
async def show_day_details(callback: CallbackQuery):
    if callback.data.startswith("day_all_") or callback.data.startswith("day_worker_"):
        return
    await callback.answer()
    day = callback.data.replace("day_", "")
    uid = callback.from_user.id
    if uid in BOSS_IDS:
        return

    w_cond, w_params = get_worker_filter(uid)
    params = (f"{day}%",) + w_params
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute(
        "SELECT store_name, total, cash, txn_type, date FROM sales WHERE date LIKE %s AND cash > 0 " + w_cond + " ORDER BY id DESC",
        params,
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return await callback.message.edit_text(
            "📭 Yo'q.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️", callback_data="back_main")]]),
        )

    out = f"📅 {day} batafsil:\n"
    for r in rows:
        out += f"🏪 {r['store_name']} | 📦 {fmt(r['total'])} | 💵 {fmt(r['cash'])}\n"
    await callback.message.edit_text(
        out,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️", callback_data="back_main")]]),
    )


@dp.message(F.text == "📅 Oylik hisobot")
async def monthly_report(message: types.Message):
    uid = message.from_user.id
    if uid in BOSS_IDS:
        return

    w_cond, w_params = get_worker_filter(uid)
    curr = datetime.now().strftime("%m.%Y")
    params_all = w_params
    params_curr = (f"%{curr}%",) + w_params

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT COALESCE(SUM(total),0) AS total, COALESCE(SUM(cash),0) AS cash FROM sales WHERE 1=1 " + w_cond, params_all)
    total_all = cur.fetchone()
    cur.execute(
        """
        SELECT COALESCE(SUM(total),0) AS total,
               COALESCE(SUM(cash),0) AS cash,
               COALESCE(SUM(total)-SUM(cash),0) AS debt
        FROM sales
        WHERE date LIKE %s
        """
        + (" " + w_cond if w_cond else ""),
        params_curr,
    )
    current = cur.fetchone()
    conn.close()

    old_debt = max(0, ((total_all["total"] or 0) - (total_all["cash"] or 0)) - (current["debt"] or 0))
    await message.answer(
        f"📅 Hisobot ({curr}):\n"
        f"📉 O'tgan: {fmt(old_debt)}\n"
        f"💰 Bu oy: {fmt(current['total'])}\n"
        f"💵 Naqt: {fmt(current['cash'])}\n"
        f"📉 Yangi: {fmt(current['debt'])}\n"
        f"✅ Joriy: {fmt(old_debt + (current['debt'] or 0))}"
    )


# ================= QARZI BORLAR =================
@dp.message(F.text == "🤝 Qarzi borlar")
async def handle_debtors(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    conn = get_db()
    cur = dict_cursor(conn)

    if uid in BOSS_IDS:
        cur.execute(
            """
            SELECT u.user_id, u.name,
                   ROUND(CAST(COALESCE(SUM(s.total),0) - COALESCE(SUM(s.cash),0) AS numeric), 2) AS bal
            FROM sales s
            JOIN users u ON s.worker_id = u.user_id
            WHERE s.normalized_store IS NOT NULL AND s.normalized_store != ''
            GROUP BY u.user_id, u.name
            HAVING ROUND(CAST(COALESCE(SUM(s.total),0) - COALESCE(SUM(s.cash),0) AS numeric), 2) > 0
            ORDER BY bal DESC
            """
        )
        res = cur.fetchall()
        if not res:
            conn.close()
            return await message.answer("✅ Qarz yo'q.")

        out = "🤝 Qarzi bor ishchilar:\n"
        out += "\n".join([f"👤 {r['name']} - {fmt(r['bal'])}" for r in res])
        out += f"\n💰 Umumiy: {fmt(sum(r['bal'] for r in res))}"
        await state.update_data(boss_debt_workers={str(r["user_id"]): r["name"] for r in res})
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=f"👤 {r['name']} ({fmt(r['bal'])})", callback_data=f"boss_debt_uid_{r['user_id']}")]
                for r in res
            ]
        )
        conn.close()
        return await message.answer(out, reply_markup=kb)

    cur.execute(
        """
        SELECT normalized_store,
               ROUND(CAST(COALESCE(SUM(total),0) - COALESCE(SUM(cash),0) AS numeric), 2) AS bal
        FROM sales
        WHERE worker_id = %s AND normalized_store IS NOT NULL AND normalized_store != ''
        GROUP BY normalized_store
        HAVING ROUND(CAST(COALESCE(SUM(total),0) - COALESCE(SUM(cash),0) AS numeric), 2) > 0
        ORDER BY bal DESC
        """,
        (uid,),
    )
    res = cur.fetchall()
    conn.close()

    if not res:
        return await message.answer("✅ Qarz yo'q.")

    mapping = {f"s{i}": r["normalized_store"] for i, r in enumerate(res)}
    await state.update_data(worker_debt_map=mapping)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"📉 {r['normalized_store']} ({fmt(r['bal'])})", callback_data=f"wdebt_{code}")]
            for code, r in zip(mapping.keys(), res)
        ]
    )
    await message.answer("🤝 Qarzi bor do'konlar:", reply_markup=kb)


@dp.message(F.text == "🤝 Qarzdorlar Pro")
async def handle_debtors_pro(message: types.Message, state: FSMContext):
    await handle_debtors(message, state)


@dp.callback_query(F.data.startswith("wdebt_"))
async def worker_debt_store_open(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    code = callback.data.replace("wdebt_", "")
    data = await state.get_data()
    store = (data.get("worker_debt_map") or {}).get(code)
    if not store:
        return await callback.answer("⚠️ Do'kon topilmadi.", show_alert=True)
    await open_store_by_name(callback, state, store)


@dp.callback_query(F.data.startswith("boss_debt_uid_"))
async def boss_debt_detail(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        wid = int(callback.data.replace("boss_debt_uid_", ""))
        conn = get_db()
        cur = dict_cursor(conn)
        cur.execute("SELECT name FROM users WHERE user_id = %s", (wid,))
        worker = cur.fetchone()
        w_name = worker["name"] if worker else f"ID:{wid}"
        cur.execute(
            """
            SELECT normalized_store,
                   ROUND(CAST(COALESCE(SUM(total),0) AS numeric), 2) AS t,
                   ROUND(CAST(COALESCE(SUM(cash),0) AS numeric), 2) AS c
            FROM sales
            WHERE worker_id = %s AND normalized_store IS NOT NULL AND normalized_store != ''
            GROUP BY normalized_store
            HAVING ROUND(CAST(COALESCE(SUM(total),0) AS numeric), 2) > ROUND(CAST(COALESCE(SUM(cash),0) AS numeric), 2)
            ORDER BY (ROUND(CAST(COALESCE(SUM(total),0) AS numeric), 2) - ROUND(CAST(COALESCE(SUM(cash),0) AS numeric), 2)) DESC
            """,
            (wid,),
        )
        stores = cur.fetchall()
        conn.close()

        out = f"👤 {w_name} qarzlari:\n"
        out += "\n".join([f"🏪 {s['normalized_store']} | {fmt(s['t'] - s['c'])}" for s in stores]) if stores else "✅ Yo'q"

        await state.update_data(boss_debt_store_map={f"{wid}_{i}": s["normalized_store"] for i, s in enumerate(stores)})
        kb_rows = [
            [InlineKeyboardButton(text=f"📖 {s['normalized_store']}", callback_data=f"boss_debt_store_{wid}_{i}")]
            for i, s in enumerate(stores)
        ]
        kb_rows.append([InlineKeyboardButton(text="⬅️", callback_data="back_main")])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
        await callback.message.edit_text(out, reply_markup=kb)
    except Exception as e:
        print(f"Boss debt error: {e}")
        await callback.answer("❌ Xatolik", show_alert=True)


@dp.callback_query(F.data.startswith("boss_debt_store_"))
async def boss_debt_store_view(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    wid_text, idx = callback.data.replace("boss_debt_store_", "").split("_", 1)
    wid = int(wid_text)
    data = await state.get_data()
    store = (data.get("boss_debt_store_map") or {}).get(f"{wid}_{idx}")
    if not store:
        return await callback.answer("⚠️ Do'kon topilmadi.", show_alert=True)
    await open_store_by_name(callback, state, store, selected_worker_id=wid)


# ================= DO'KONLAR =================
@dp.message(F.text == "🏪 Do'konlarim")
async def stores_list_cmd(message: types.Message):
    uid = message.from_user.id
    if uid in BOSS_IDS:
        return
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT DISTINCT normalized_store FROM sales WHERE worker_id = %s AND normalized_store IS NOT NULL AND normalized_store != '' ORDER BY normalized_store", (uid,))
    stores = cur.fetchall()
    conn.close()
    if not stores:
        return await message.answer("🏪 Yo'q.")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"🏪 {s['normalized_store']}", callback_data=f"store_{s['normalized_store']}")] for s in stores]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️", callback_data="back_main")])
    await message.answer("🏪 Do'konlarim:", reply_markup=kb)


@dp.callback_query(F.data == "stores_list")
async def stores_list_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    if uid in BOSS_IDS:
        return
    w_cond, w_params = get_worker_filter(uid)
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT DISTINCT normalized_store FROM sales WHERE 1=1 " + w_cond + " AND normalized_store IS NOT NULL AND normalized_store != '' ORDER BY normalized_store", w_params)
    stores = cur.fetchall()
    conn.close()
    if not stores:
        return await callback.answer("🏪 Yo'q.")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"🏪 {s['normalized_store']}", callback_data=f"store_{s['normalized_store']}")] for s in stores]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️", callback_data="back_main")])
    await callback.message.edit_text("🏪 Do'konlar:", reply_markup=kb)


@dp.message(F.text == "🔍 Do'kon qidirish")
async def search_prompt(message: types.Message, state: FSMContext):
    if message.from_user.id in BOSS_IDS:
        return
    await message.answer("🔍 Do'kon nomini yozing:", reply_markup=get_back_kb())
    await state.set_state(AppStates.search_store)


@dp.message(AppStates.search_store)
async def search_handle(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga":
        await state.clear()
        return await start_cmd(message, state)
    uid = message.from_user.id
    if uid in BOSS_IDS:
        return

    w_cond, w_params = get_worker_filter(uid)
    search_term = f"%{message.text.lower().strip()}%"
    params = (search_term,) + w_params
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT DISTINCT normalized_store FROM sales WHERE normalized_store LIKE %s " + w_cond, params)
    res = cur.fetchall()
    conn.close()

    if not res:
        return await message.answer("🔍 Topilmadi.")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"🏪 {r['normalized_store']}", callback_data=f"store_{r['normalized_store']}")] for r in res]
    )
    await message.answer("🔍 Natijalar:", reply_markup=kb)
    await state.clear()


@dp.callback_query(F.data.startswith("store_"))
async def store_details(callback: CallbackQuery, state: FSMContext):
    store = callback.data[6:]
    if not store:
        return await callback.answer("⚠️ Xato", show_alert=True)

    await state.update_data(current_store=store)
    data = await state.get_data()
    selected_worker_id = data.get("debt_worker_id", callback.from_user.id)
    from_boss_debt = data.get("debt_worker_id")
    if from_boss_debt:
        await state.update_data(debt_worker_id=None)

    w_cond, w_params = get_worker_filter(selected_worker_id)
    full_params = (store,) + w_params
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT COALESCE(SUM(total),0) AS total, COALESCE(SUM(cash),0) AS cash FROM sales WHERE normalized_store = %s " + w_cond, full_params)
    res = cur.fetchone()
    cur.execute("SELECT txn_type, total, cash, date FROM sales WHERE normalized_store = %s " + w_cond + " ORDER BY id DESC LIMIT 10", full_params)
    hist = cur.fetchall()
    conn.close()

    total = res["total"]
    cash = res["cash"]
    out = (
        f"🏪 **{store.upper()}** hisoboti:\n"
        f"💰 Umumiy savdo: {fmt(total)}\n"
        f"💵 Yig'ilgan: {fmt(cash)}\n"
        f"📉 Qoldiq qarz: {fmt(total - cash)}\n\n"
        f"📜 Harakatlar:\n"
    )
    for h in hist:
        if h["txn_type"] == "savdo":
            out += f"📅 {h['date']}\n💰 Savdo: {fmt(h['total'])}\n💵 Naqt: {fmt(h['cash'])}\n📉 Qarz: {fmt((h['total'] or 0) - (h['cash'] or 0))}\n\n"
        elif h["txn_type"] == "naqt":
            out += f"📅 {h['date']}\n💵 Naqt kiritildi: {fmt(h['cash'])}\n\n"
        elif h["txn_type"] == "qaytarish":
            out += f"📅 {h['date']}\n🔄 Qaytarish: {fmt(abs(h['total']))}\n\n"

    if from_boss_debt:
        back_row = [InlineKeyboardButton(text="⬅️ Qarzdorlar", callback_data=f"boss_debt_uid_{selected_worker_id}")]
    else:
        back_row = [InlineKeyboardButton(text="⬅️", callback_data="back_main" if callback.from_user.id in BOSS_IDS else "stores_list")]

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💵 Naqt", callback_data="act_cash"), InlineKeyboardButton(text="🔄 Qaytarish", callback_data="act_return")],
            [InlineKeyboardButton(text="💰 Savdo", callback_data="act_trade"), InlineKeyboardButton(text="👤 Do'konchi", callback_data="act_owner")],
            back_row,
        ]
    )
    await callback.message.edit_text(out, reply_markup=kb, parse_mode="Markdown")


@dp.callback_query(F.data == "act_cash")
async def start_cash(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("💵 Summa kiriting. Masalan: `5000 11.01.25`", reply_markup=get_back_kb(), parse_mode="Markdown")
    await state.set_state(AppStates.add_cash)


@dp.message(AppStates.add_cash)
async def process_cash(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga":
        await state.clear()
        return await start_cmd(message, state)

    date_str, clean_text = extract_date(message.text)
    if not clean_text.replace(".", "", 1).replace(",", "", 1).isdigit():
        return await message.answer("⚠️ Faqat raqam kiriting.")

    amount = safe_float(clean_text)
    final_date = (date_str + " " + datetime.now().strftime("%H:%M")) if date_str else datetime.now().strftime("%d.%m.%Y %H:%M")
    await handle_store_action(message, state, "naqt", amount, date_override=final_date)


@dp.callback_query(F.data == "act_return")
async def start_return(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("🔄 Summa kiriting. Masalan: `200 11.01.25`", reply_markup=get_back_kb(), parse_mode="Markdown")
    await state.set_state(AppStates.add_return)


@dp.message(AppStates.add_return)
async def process_return(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga":
        await state.clear()
        return await start_cmd(message, state)

    date_str, clean_text = extract_date(message.text)
    if not clean_text.replace(".", "", 1).replace(",", "", 1).isdigit():
        return await message.answer("⚠️ Faqat raqam kiriting.")

    amount = safe_float(clean_text)
    final_date = (date_str + " " + datetime.now().strftime("%H:%M")) if date_str else datetime.now().strftime("%d.%m.%Y %H:%M")
    await handle_store_action(message, state, "qaytarish", amount, date_override=final_date)


@dp.callback_query(F.data == "act_trade")
async def start_trade(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("💰 Summa kiriting. Masalan: `1000 11.01.25`", reply_markup=get_back_kb(), parse_mode="Markdown")
    await state.set_state(AppStates.add_new_sale_store)


@dp.message(AppStates.add_new_sale_store)
async def process_new_sale_store(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga":
        await state.clear()
        return await start_cmd(message, state)

    data = await state.get_data()
    store = data.get("current_store")
    if not store:
        return await message.answer("⚠️ Do'kon tanlanmagan.")

    date_str, clean_text = extract_date(message.text)
    if not clean_text.replace(".", "", 1).replace(",", "", 1).isdigit():
        return await message.answer("⚠️ Faqat raqam kiriting.")

    amount = safe_float(clean_text)
    final_date = (date_str + " " + datetime.now().strftime("%H:%M")) if date_str else datetime.now().strftime("%d.%m.%Y %H:%M")
    await handle_store_action(message, state, "savdo_yangi", amount, store=store, date_override=final_date)


async def handle_store_action(message: types.Message, state: FSMContext, t_type: str, amount: float, store: str = None, date_override: str = None):
    if not store:
        data = await state.get_data()
        store = data.get("current_store")
        if not store:
            return await message.answer("⚠️ Do'kon tanlanmagan.")

    current_state = await state.get_data()
    target_worker_id = current_state.get("debt_worker_id", message.from_user.id if message.from_user.id not in BOSS_IDS else None)
    if message.from_user.id in BOSS_IDS and not target_worker_id:
        return await message.answer("⚠️ Boss uchun ishchi aniqlanmadi.")

    worker_id = target_worker_id if message.from_user.id in BOSS_IDS else message.from_user.id
    worker_name = get_worker_name(worker_id)
    now_str = date_override or datetime.now().strftime("%d.%m.%Y %H:%M")

    if t_type == "naqt":
        vals = (store, store, 0, amount, -amount, "naqt", now_str, worker_id, worker_name)
    elif t_type == "qaytarish":
        vals = (store, store, -amount, 0, -amount, "qaytarish", now_str, worker_id, worker_name)
    elif t_type == "savdo_yangi":
        vals = (store, store, amount, 0, amount, "savdo", now_str, worker_id, worker_name)
    else:
        return await message.answer("⚠️ Noto'g'ri amal.")

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute(
        """
        INSERT INTO sales (store_name, normalized_store, total, cash, debt, txn_type, date, worker_id, worker_name)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
        """,
        vals,
    )
    sale_id = cur.fetchone()["id"]
    conn.commit()
    conn.close()

    await notify_boss(worker_id, store, amount if t_type != "qaytarish" else -amount, amount if t_type == "naqt" else 0, t_type, now_str)

    if t_type == "naqt":
        txt = (
            f"✅ Saqlandi!\n"
            f"🏪 Do'kon: {store}\n"
            f"💵 Naqt: {fmt(amount)}\n"
            f"📅 Sana: {now_str.split()[0]}"
        )
    elif t_type == "qaytarish":
        txt = (
            f"✅ Saqlandi!\n"
            f"🏪 Do'kon: {store}\n"
            f"🔄 Qaytarish: {fmt(amount)}\n"
            f"📅 Sana: {now_str.split()[0]}"
        )
    else:
        txt = (
            f"✅ Saqlandi!\n"
            f"🏪 Do'kon: {store}\n"
            f"💰 Savdo: {fmt(amount)}\n"
            f"💵 Naqt: {fmt(0)}\n"
            f"📉 Qarz: {fmt(amount)}\n"
            f"📅 Sana: {now_str.split()[0]}"
        )
    await message.answer(
        txt,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Bekor", callback_data=f"cancel_req_{sale_id}")]]),
    )
    await state.clear()
    await send_store_details(message, store, state, force_worker_id=worker_id)


async def send_store_details(message: types.Message, store: str, state: FSMContext, force_worker_id=None):
    await state.update_data(current_store=store)
    uid = force_worker_id if force_worker_id else message.from_user.id
    await state.update_data(debt_worker_id=uid if message.from_user.id in BOSS_IDS else None)

    w_cond, w_params = get_worker_filter(uid)
    full_params = (store,) + w_params
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT COALESCE(SUM(total),0) AS total, COALESCE(SUM(cash),0) AS cash FROM sales WHERE normalized_store = %s " + w_cond, full_params)
    res = cur.fetchone()
    cur.execute("SELECT txn_type, total, cash, date FROM sales WHERE normalized_store = %s " + w_cond + " ORDER BY id DESC LIMIT 10", full_params)
    hist = cur.fetchall()
    conn.close()

    total = res["total"]
    cash = res["cash"]
    out = (
        f"🏪 **{store.upper()}** hisoboti:\n"
        f"💰 Umumiy savdo: {fmt(total)}\n"
        f"💵 Yig'ilgan: {fmt(cash)}\n"
        f"📉 Qoldiq qarz: {fmt(total - cash)}\n\n"
        f"📜 Harakatlar:\n"
    )
    for h in hist:
        if h["txn_type"] == "savdo":
            out += f"📅 {h['date']}\n💰 Savdo: {fmt(h['total'])}\n💵 Naqt: {fmt(h['cash'])}\n📉 Qarz: {fmt((h['total'] or 0) - (h['cash'] or 0))}\n\n"
        elif h["txn_type"] == "naqt":
            out += f"📅 {h['date']}\n💵 Naqt kiritildi: {fmt(h['cash'])}\n\n"
        elif h["txn_type"] == "qaytarish":
            out += f"📅 {h['date']}\n🔄 Qaytarish: {fmt(abs(h['total']))}\n\n"

    back_button = (
        InlineKeyboardButton(text="⬅️ Qarzdorlar", callback_data=f"boss_debt_uid_{uid}")
        if message.from_user.id in BOSS_IDS
        else InlineKeyboardButton(text="🔍 Boshqa", callback_data="stores_list")
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💵 Naqt", callback_data="act_cash"), InlineKeyboardButton(text="🔄 Qaytarish", callback_data="act_return")],
            [InlineKeyboardButton(text="💰 Savdo", callback_data="act_trade"), InlineKeyboardButton(text="👤 Do'konchi", callback_data="act_owner")],
            [back_button],
        ]
    )
    await message.answer(out, reply_markup=kb, parse_mode="Markdown")


async def send_owner_info(target, store, state: FSMContext):
    msg = target.message if isinstance(target, CallbackQuery) else target
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT owner_name, phone, location FROM stores_info WHERE normalized_store = %s", (store,))
    info = cur.fetchone()
    conn.close()

    name = info["owner_name"] if info and info.get("owner_name") else "⬜ Kiritilmagan"
    phone = info["phone"] if info and info.get("phone") else "⬜ Kiritilmagan"
    location = info["location"] if info and info.get("location") else "⬜ Kiritilmagan"

    out = f"👤 **{store}**\n📛 {name}\n📞 {phone}\n📍 {location}"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Ism", callback_data="edit_name"), InlineKeyboardButton(text="✏️ Tel", callback_data="edit_phone")],
            [InlineKeyboardButton(text="✏️ Manzil", callback_data="edit_loc"), InlineKeyboardButton(text="⬅️", callback_data=f"store_{store}")],
        ]
    )
    await msg.answer(out, reply_markup=kb, parse_mode="Markdown")


@dp.callback_query(F.data == "act_owner")
async def show_owner(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    await send_owner_info(callback, data.get("current_store", ""), state)


@dp.callback_query(F.data.startswith("edit_"))
async def edit_owner(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    field = callback.data.replace("edit_", "")
    await state.update_data(edit_field=field)
    titles = {"name": "ism", "phone": "telefon", "loc": "manzil"}
    await callback.message.answer(f"✍️ Yangi {titles.get(field, field)}:", reply_markup=get_back_kb())
    await state.set_state(AppStates.edit_store_info)


@dp.message(AppStates.edit_store_info)
async def save_owner_info(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga":
        await state.clear()
        return await start_cmd(message, state)

    data = await state.get_data()
    store = data.get("current_store", "")
    field = data.get("edit_field", "name")
    col = {"name": "owner_name", "phone": "phone", "loc": "location"}.get(field, "owner_name")

    owner_name = message.text.strip() if col == "owner_name" else ""
    phone = message.text.strip() if col == "phone" else ""
    location = message.text.strip() if col == "location" else ""

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute(
        f"""
        INSERT INTO stores_info (normalized_store, owner_name, phone, location)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (normalized_store) DO UPDATE SET {col} = EXCLUDED.{col}
        """,
        (store, owner_name, phone, location),
    )
    conn.commit()
    conn.close()

    await message.answer("✅ Saqlandi!")
    await state.clear()
    await send_owner_info(message, store, state)


@dp.message(Command("delete_store"))
async def delete_store_command(message: types.Message):
    if message.from_user.id not in BOSS_IDS:
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.answer("❌ /delete_store dokon")

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("DELETE FROM sales WHERE normalized_store = %s", (args[1],))
    cur.execute("DELETE FROM stores_info WHERE normalized_store = %s", (args[1],))
    conn.commit()
    conn.close()
    await message.answer(f"✅ {args[1]} o'chirildi!")


@dp.message(Command("delete_worker"))
async def delete_worker_command(message: types.Message):
    if message.from_user.id not in BOSS_IDS:
        return
    args = message.text.split()
    if len(args) < 2:
        return await message.answer("❌ /delete_worker id")
    try:
        wid = int(args[1])
        if wid in BOSS_IDS:
            return await message.answer("⚠️ Boss o'chirilmaydi!")

        conn = get_db()
        cur = dict_cursor(conn)
        cur.execute("SELECT name FROM users WHERE user_id = %s AND role = 'worker'", (wid,))
        if not cur.fetchone():
            conn.close()
            return await message.answer("⚠️ Topilmadi.")
        cur.execute("DELETE FROM deletion_requests WHERE worker_id = %s", (wid,))
        cur.execute("DELETE FROM sales WHERE worker_id = %s", (wid,))
        cur.execute("DELETE FROM users WHERE user_id = %s", (wid,))
        conn.commit()
        conn.close()
        await message.answer(f"✅ {wid} tozalandi!")
    except Exception as e:
        await message.answer(f"❌ {e}")


# ================= SAVDO =================
@dp.message(F.text == "✍️ Savdo qo'shish")
async def trade_init(message: types.Message, state: FSMContext):
    if message.from_user.id in BOSS_IDS:
        return
    await state.clear()
    await message.answer("📝 `Ali market 5000` yoki `Ali 5000 naxt 300 11.01.25`", reply_markup=get_back_kb(), parse_mode="Markdown")
    await state.set_state(AppStates.waiting_trade)


@dp.message(AppStates.waiting_trade)
async def handle_trade(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS:
        return
    if message.text == "⬅️ Orqaga":
        await state.clear()
        return await start_cmd(message, state)

    msg = await message.answer("🤖 Tahlil...")
    try:
        if client:
            res = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": """Sen savdo tahlilchisisiz. Matndan store, total, cash, date ajrat.
Qoidalar: 1. "naxt/naqt/pul" yonidagi raqam cash.
2. Yagona raqam bo'lsa total=raqam, cash=0.
3. Sana: DD.MM.YY yoki DD.MM.YYYY.
4. Faqat JSON qaytar: {"store":"str","total":num,"cash":num,"date":"DD.MM.YY" yoki null}""",
                    },
                    {"role": "user", "content": message.text},
                ],
                response_format={"type": "json_object"},
            )
            parsed = json.loads(res.choices[0].message.content)
            store = normalize(parsed.get("store", "noma'lum"))
            total = safe_float(parsed.get("total", 0))
            cash = safe_float(parsed.get("cash", 0))
            input_date = parsed.get("date")
        else:
            found_numbers = re.findall(r"\d+(?:[.,]\d+)?", message.text)
            if not found_numbers:
                raise Exception("Summani topolmadim.")
            total = safe_float(found_numbers[0])
            cash = safe_float(found_numbers[1]) if len(found_numbers) > 1 else 0
            input_date, clean_text = extract_date(message.text)
            store = normalize(re.sub(r"\d+(?:[.,]\d+)?", "", clean_text).replace("naxt", "").replace("naqt", "").strip() or "noma'lum")

        date_str = input_date if input_date and re.match(r"^\d{2}\.\d{2}\.\d{2,4}$", str(input_date)) else datetime.now().strftime("%d.%m.%Y")
        now_str = f"{date_str} {datetime.now().strftime('%H:%M')}"
        debt = round(total - cash, 2)

        conn = get_db()
        cur = dict_cursor(conn)
        cur.execute(
            """
            INSERT INTO sales (store_name, normalized_store, total, cash, debt, txn_type, date, worker_id, worker_name)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (store, store, total, cash, debt, "savdo", now_str, uid, message.from_user.full_name),
        )
        sale_id = cur.fetchone()["id"]
        conn.commit()
        conn.close()

        await notify_boss(uid, store, total, cash, "savdo", now_str)
        await msg.edit_text(
            f"✅ Saqlandi!\n"
            f"🏪 Do'kon: {store}\n"
            f"💰 Savdo: {fmt(total)}\n"
            f"💵 Naqt: {fmt(cash)}\n"
            f"📉 Qarz: {fmt(debt)}\n"
            f"📅 Sana: {date_str}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Bekor", callback_data=f"cancel_req_{sale_id}")]]),
        )
        await message.answer("📝 Yana savdo kiriting yoki `⬅️ Orqaga` bosing.", parse_mode="Markdown", reply_markup=get_back_kb())
        await state.set_state(AppStates.waiting_trade)
        return
    except Exception as e:
        await msg.edit_text(f"❌ {e}")
        return


@dp.callback_query(F.data.startswith("cancel_req_"))
async def request_cancel(callback: CallbackQuery):
    await callback.answer()
    sale_id = int(callback.data.replace("cancel_req_", ""))
    uid = callback.from_user.id

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT store_name, total, cash, date FROM sales WHERE id = %s AND worker_id = %s", (sale_id, uid))
    sale = cur.fetchone()
    if not sale:
        conn.close()
        return await callback.answer("⚠️ Topilmadi.", show_alert=True)

    cur.execute(
        "INSERT INTO deletion_requests (worker_id, sale_id, request_date) VALUES (%s,%s,%s)",
        (uid, sale_id, datetime.now().strftime("%d.%m.%Y %H:%M")),
    )
    conn.commit()
    conn.close()

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Bekor", callback_data=f"approve_{sale_id}"),
                InlineKeyboardButton(text="❌ Qoldir", callback_data=f"reject_{sale_id}"),
            ]
        ]
    )
    notif = f"🔔 **O'chirish**\n👤 {callback.from_user.full_name} | 🏪 {sale['store_name']} | 💰 {fmt(sale['total'])} | 📅 {sale['date']}"
    for bid in BOSS_IDS:
        try:
            await bot.send_message(bid, notif, reply_markup=kb, parse_mode="Markdown")
        except Exception:
            pass
    await callback.message.edit_text("✅ So'rov yuborildi.", reply_markup=None)


@dp.callback_query(F.data.startswith("approve_") | F.data.startswith("reject_"))
async def handle_deletion_request(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id not in BOSS_IDS:
        return

    action, sale_id_text = callback.data.split("_")
    sale_id = int(sale_id_text)
    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT worker_id FROM deletion_requests WHERE sale_id = %s", (sale_id,))
    row = cur.fetchone()
    wid = row["worker_id"] if row else None

    if action == "approve":
        cur.execute("DELETE FROM sales WHERE id = %s", (sale_id,))
        cur.execute("DELETE FROM deletion_requests WHERE sale_id = %s", (sale_id,))
        text = "✅ Bekor qilindi."
        worker_text = "✅ Boss tasdiqladi. Amal bekor qilindi."
    else:
        cur.execute("UPDATE deletion_requests SET status = 'rejected' WHERE sale_id = %s", (sale_id,))
        text = "❌ Rad etildi."
        worker_text = "❌ Boss rad etdi. Amal saqlandi."

    conn.commit()
    conn.close()
    await callback.message.edit_text(text)
    if wid:
        try:
            await bot.send_message(wid, worker_text)
        except Exception:
            pass


@dp.message(F.text)
async def worker_info(message: types.Message, state: FSMContext):
    if await state.get_state() is not None:
        return
    if message.from_user.id in BOSS_IDS:
        return

    conn = get_db()
    cur = dict_cursor(conn)
    cur.execute("SELECT name, user_id FROM users WHERE name = %s", (message.text,))
    worker = cur.fetchone()
    if not worker:
        conn.close()
        return

    cur.execute("SELECT COALESCE(SUM(total),0) AS total, COUNT(id) AS cnt FROM sales WHERE worker_id = %s", (worker["user_id"],))
    stats = cur.fetchone()
    conn.close()
    await message.answer(
        f"👤 {worker['name']}\n📊 Savdo: {fmt(stats['total'])}\n🧾 Soni: {stats['cnt']}",
        reply_markup=get_back_kb(),
    )


@dp.callback_query(F.data == "back_main")
async def back_main(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    uid = callback.from_user.id
    try:
        conn = get_db()
        cur = dict_cursor(conn)
        cur.execute("SELECT name, active FROM users WHERE user_id = %s", (uid,))
        user = cur.fetchone()
        conn.close()

        if uid in BOSS_IDS:
            kb = get_boss_menu()
            text = "Xush kelibsiz, Boss 👑"
        elif user and user.get("active") == 0:
            kb = get_back_kb()
            text = "🚫 Blok"
        else:
            kb = (
                get_worker_menu()
                if user
                else ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⏳")]], resize_keyboard=True)
            )
            text = f"Salom, {user['name'] or 'Ishchi'}!" if user else f"ID: {uid}"

        await callback.message.answer(text, reply_markup=kb)
    except Exception:
        await callback.message.answer("⬅️", reply_markup=get_boss_menu() if uid in BOSS_IDS else get_worker_menu())


async def main():
    init_db()
    print("✅ Bot ishga tushdi 🚀")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
