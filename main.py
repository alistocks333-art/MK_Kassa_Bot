import asyncio
import json
import os
import psycopg2
import psycopg2.extras
import re
from datetime import datetime, date

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from openai import OpenAI
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# ================= SOZLAMALAR =================
API_TOKEN = os.getenv('API_TOKEN')  
BOSS_IDS = [5426806030, 6826780143] 
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', "")

client = OpenAI(api_key=OPENAI_API_KEY)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ================= YORDAMCHI FUNKSIYALAR =================
def fmt(num):
    if num is None: return "0 $"
    n = round(float(num), 2)
    return f"{n:,.2f}".replace(".00", "").replace(",", " ") + " $"

def normalize(text):
    return text.strip().lower().replace("  ", " ")

# PostgreSQL uchun xavfsiz filter
def get_worker_filter(uid):
    if uid in BOSS_IDS: return "", ()
    return "AND worker_id = %s", (uid,)

def get_worker_name(uid):
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT name FROM users WHERE user_id = %s", (uid,))
    res = cur.fetchone(); conn.close()
    return res['name'] if res and res['name'] else f"ID:{uid}"

async def notify_boss(worker_uid, store, total, cash, txn_type, date_str):
    worker_name = get_worker_name(worker_uid)
    time_str = date_str.split()[1] if ' ' in date_str else ""
    msg = ""
    if txn_type in ['savdo', 'savdo_yangi']:
        debt = total - cash
        msg = (f"🔔 **Yangi savdo!**\n"
               f"👤 Ishchi: {worker_name}\n"
               f"🏪 Do'kon: `{store}`\n"
               f"💰 Savdo: {fmt(total)}\n"
               f"💵 Naqt: {fmt(cash)}\n"
               f"📉 Qarz: {fmt(debt)}")
    elif txn_type == 'naqt':
        msg = (f"💵 **Naqt kiritildi!**\n"
               f"👤 Ishchi: {worker_name}\n"
               f"🏪 {store} | 💵 {fmt(cash)} | 🕒 {time_str}")
    elif txn_type == 'qaytarish':
        msg = (f"🔄 **Qaytarildi!**\n"
               f"👤 Ishchi: {worker_name}\n"
               f"🏪 {store} | 🔄 {fmt(abs(total))} | 🕒 {time_str}")
    
    if msg:
        for boss_id in BOSS_IDS:
            try: await bot.send_message(boss_id, msg, parse_mode="Markdown")
            except: pass

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

# ================= BAZA (PostgreSQL uchun to'liq tuzatilgan) =================
def get_db():
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        raise Exception("DATABASE_URL topilmadi! Railway'da PostgreSQL ulanganini tekshiring.")
    # sslmode='require' Railway uchun majburiy
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def dict_cursor(conn):
    """PostgreSQL natijalarini lug'at (dict) sifatida qaytaradi"""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    conn = get_db()
    cur = dict_cursor(conn)
    # INTEGER PRIMARY KEY AUTOINCREMENT o'rniga SERIAL PRIMARY KEY ishlatiladi
    cur.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY, name TEXT, role TEXT DEFAULT 'worker', active INTEGER DEFAULT 1)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS sales (
        id SERIAL PRIMARY KEY, store_name TEXT, normalized_store TEXT,
        total REAL, cash REAL, debt REAL, txn_type TEXT, date TEXT, worker_id INTEGER, worker_name TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS stores_info (
        normalized_store TEXT PRIMARY KEY, owner_name TEXT, phone TEXT, location TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS deletion_requests (
        id SERIAL PRIMARY KEY, worker_id INTEGER, sale_id INTEGER, status TEXT DEFAULT 'pending', request_date TEXT)''')
    conn.commit()
    conn.close()

# ================= MENYULAR =================
def get_back_kb(): 
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ Orqaga")]], resize_keyboard=True)

def get_worker_menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✍️ Savdo qo'shish"), KeyboardButton(text="📊 Kunlik kassa")],
        [KeyboardButton(text="🔍 Do'kon qidirish"), KeyboardButton(text="🤝 Qarzi borlar")],
        [KeyboardButton(text="🏪 Do'konlarim"), KeyboardButton(text="📅 Oylik kassa")],
        [KeyboardButton(text="📅 Oylik hisobot"), KeyboardButton(text="💰 Oylik maosh")],
        [KeyboardButton(text="🤖 AI Yordam")]
    ], resize_keyboard=True)

def get_boss_menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="💰 Kassa (Live)"), KeyboardButton(text="🤝 Qarzi borlar")],
        [KeyboardButton(text="👥 Ishchilar"), KeyboardButton(text="🤖 AI Analitika")],
        [KeyboardButton(text="📅 Oylik arxiv"), KeyboardButton(text="🏪 Barcha do'konlar")],
        [KeyboardButton(text="📊 Eng yaxshi ishchilar"), KeyboardButton(text="🏆 Eng yaxshi do'konlar")],
        [KeyboardButton(text="📅 Oylik kassa"), KeyboardButton(text="💰 Oylik maosh")]
    ], resize_keyboard=True)

# ================= START =================
@dp.message(Command("start"))
@dp.message(F.text == "⬅️ Orqaga")
async def start_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    init_db()
    uid = message.from_user.id
    conn = get_db(); cur = dict_cursor(conn)
    # ? o'rniga %s
    cur.execute("SELECT name, active FROM users WHERE user_id = %s", (uid,))
    user = cur.fetchone(); conn.close()

    if uid in BOSS_IDS:
        return await message.answer("Xush kelibsiz, Boss 👑", reply_markup=get_boss_menu())
    if user and user['active'] == 0:
        return await message.answer("🚫 Hisobingiz bloklan. Boss bilan bog'laning.")

    kb = get_worker_menu() if user else ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⏳ Kutilmoqda...")]], resize_keyboard=True)
    text = f"Assalomu alaykum, {user['name'] or 'Ishchi'}!" if user else f"Siz ro'yxatda yo'qsiz.\n🆔 ID: {uid}"
    await message.answer(text, reply_markup=kb)

# ================= BOSS FUNKSIYALARI =================
@dp.message(F.text == "💰 Kassa (Live)")
async def boss_kassa_live(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    today = date.today().strftime("%d.%m.%Y")
    conn = get_db(); cur = dict_cursor(conn)
    
    # Barcha ? lar %s ga o'zgardi
    cur.execute("""SELECT u.name as worker_name, s.store_name, SUM(s.cash) as cash 
                   FROM sales s JOIN users u ON s.worker_id = u.user_id
                   WHERE s.date LIKE %s AND s.cash > 0 
                   GROUP BY u.name, s.store_name 
                   ORDER BY u.name, s.store_name""", (f"{today}%",))
    rows = cur.fetchall()
    
    cur.execute("""SELECT u.name as worker_name, SUM(s.cash) as total 
                   FROM sales s JOIN users u ON s.worker_id = u.user_id
                   WHERE s.date LIKE %s AND s.cash > 0 
                   GROUP BY u.name""", (f"{today}%",))
    worker_totals = {r['worker_name']: r['total'] for r in cur.fetchall()}
    
    cur.execute("""SELECT SUM(s.cash) as grand_total
                   FROM sales s
                   WHERE s.date LIKE %s AND s.cash > 0""", (f"{today}%",))
    grand_total = cur.fetchone()['grand_total'] or 0
    conn.close()
    
    if not rows: 
        return await message.answer("📅 Bugun hech qanday naqt tushumi yo'q.")
    
    out = f"💰 Kassa (Live) - {today}\n\n"
    current_worker = ""
    
    for r in rows:
        if r['worker_name'] != current_worker:
            if current_worker:
                out += f"\n✅ **Jami: {fmt(worker_totals.get(current_worker, 0))}**\n\n"
            current_worker = r['worker_name']
            out += f"👤 **{current_worker}**:\n"
        
        out += f"  🏪 {r['store_name']} - {fmt(r['cash'])}\n"
    
    if current_worker:
        out += f"\n✅ **Jami: {fmt(worker_totals.get(current_worker, 0))}**\n"
    
    out += f"\n{'='*30}\n"
    out += f"💵 **UMUMIY JAMI: {fmt(grand_total)}**\n"
    out += f"👥 Ishchilar soni: {len(worker_totals)} ta"
    
    await message.answer(out, parse_mode="Markdown")

@dp.message(F.text == "👥 Ishchilar")
async def boss_workers_list(message: types.Message, state: FSMContext):
    if message.from_user.id not in BOSS_IDS: return
    await state.clear()
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT name, user_id, active FROM users WHERE role = 'worker'")
    workers = cur.fetchall(); conn.close()
    kb = [[KeyboardButton(text=f"{'✅' if w['active'] else '🚫'} {w['name']}")] for w in workers]
    kb.append([KeyboardButton(text="➕ Yangi ishchi qo'shish")])
    kb.append([KeyboardButton(text="🚫 Ishdan bo'shatish (Inline)")])
    kb.append([KeyboardButton(text="⬅️ Orqaga")])
    await message.answer("👥 Ishchilar ro'yxati (✅Faol / 🚫Blok):", reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True))

@dp.message(F.text == "➕ Yangi ishchi qo'shish")
async def add_worker_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in BOSS_IDS: return
    await message.answer("🆔 Ishchi Telegram ID sini kiriting:", reply_markup=get_back_kb())
    await state.set_state(AppStates.add_worker_id)

@dp.message(AppStates.add_worker_id)
async def add_worker_get_id(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga": await state.clear(); return await boss_workers_list(message, state)
    if not message.text.isdigit(): return await message.answer("⚠️ Faqat raqam!")
    await state.update_data(worker_id=int(message.text))
    await message.answer("📝 Ishchi ismini kiriting:", reply_markup=get_back_kb())
    await state.set_state(AppStates.add_worker_name)

@dp.message(AppStates.add_worker_name)
async def add_worker_get_name(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga": await state.clear(); return await boss_workers_list(message, state)
    data = await state.get_data()
    conn = get_db(); cur = dict_cursor(conn)
    # PostgreSQL da INSERT OR REPLACE yo'q, ON CONFLICT ishlatiladi
    cur.execute("""
        INSERT INTO users (user_id, name, role) VALUES (%s, %s, 'worker')
        ON CONFLICT (user_id) DO UPDATE SET name = EXCLUDED.name, role = EXCLUDED.role
    """, (data['worker_id'], message.text.strip()))
    conn.commit(); conn.close()
    await message.answer(f"✅ Ishchi qo'shildi: {message.text}")
    await state.clear(); await boss_workers_list(message, state)

@dp.message(F.text == "🚫 Ishdan bo'shatish (Inline)")
async def fire_worker_inline(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT name, user_id, active FROM users WHERE role = 'worker'")
    workers = cur.fetchall(); conn.close()
    if not workers: return await message.answer("🚫 Hozircha ishchilar yo'q.")
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    row = []
    for w in workers:
        row.append(InlineKeyboardButton(text=f"{'✅' if w['active'] else '🚫'} {w['name']}", callback_data=f"fire_{w['user_id']}"))
        if len(row) == 2: kb.inline_keyboard.append(row); row = []
    if row: kb.inline_keyboard.append(row)
    await message.answer("🚫 Ishchini tanlang (statusni o'zgartirish uchun):", reply_markup=kb)

@dp.callback_query(F.data.startswith("fire_"))
async def process_fire_worker(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id not in BOSS_IDS: return
    target_id = int(callback.data.replace("fire_", ""))
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT name, active FROM users WHERE user_id = %s", (target_id,))
    w = cur.fetchone()
    if not w: return await callback.answer("⚠️ Ishchi topilmadi!", show_alert=True)
    new_status = 0 if w['active'] == 1 else 1
    cur.execute("UPDATE users SET active = %s WHERE user_id = %s", (new_status, target_id))
    conn.commit(); conn.close()
    await callback.message.edit_text(f"✅ {w['name']} {'🚫 Bloklandi' if new_status==0 else '✅ Faollashtirildi'}.")
    await fire_worker_inline(callback.message)

@dp.message(F.text == "🤖 AI Analitika")
async def boss_ai_analytics(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    if not OPENAI_API_KEY: return await message.answer("⚠️ OpenAI API kalit kiritilmagan!")
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT COUNT(id), SUM(total), SUM(cash), SUM(total)-SUM(cash) FROM sales")
    g = cur.fetchone()
    cur.execute("SELECT worker_name, SUM(total)-SUM(cash) as d FROM sales GROUP BY worker_name HAVING d > 0 ORDER BY d DESC LIMIT 3")
    debtors = cur.fetchall()
    cur.execute("SELECT normalized_store, SUM(total) as t FROM sales GROUP BY normalized_store ORDER BY t DESC LIMIT 3")
    top_stores = cur.fetchall()
    conn.close()
    context = (f"📊 BAZA HOLATI:\n"
               f"• Jami operatsiyalar: {g[0]} ta\n"
               f"• Umumiy savdo: {fmt(g[1])}, Yig'ilgan: {fmt(g[2])}, Qarz: {fmt(g[3])}\n"
               f"• Eng ko'p qarzdorlar: {', '.join([f'{r[0]}({fmt(r[1])})' for r in debtors]) or 'Yoq'}\n"
               f"• TOP do'konlar: {', '.join([f'{r[0]}({fmt(r[1])})' for r in top_stores]) or 'Yoq'}")
    try:
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[
            {"role": "system", "content": f"Siz MK Kassa boti boshqaruvchi AI tahlilchisisiz. {context}. Qisqa tahlil bering."},
            {"role": "user", "content": "Hozirgi holatni tahlil qiling"}])
        await message.answer(f"🤖 AI Tahlil:\n{res.choices[0].message.content}")
    except Exception as e:
        await message.answer(f"❌ AI xato: {e}")

@dp.message(F.text == "📅 Oylik arxiv")
async def boss_monthly_archive(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT user_id, name FROM users WHERE role = 'worker' AND active = 1")
    workers = cur.fetchall()
    curr = datetime.now().strftime("%m.%Y")
    out = f"📅 Oylik arxiv ({curr}):\n\n"
    grand_total_sales = grand_total_cash = grand_total_debt = 0
    for w in workers:
        cur.execute("SELECT SUM(total), SUM(cash), SUM(total)-SUM(cash) FROM sales WHERE worker_id = %s AND date LIKE %s", (w['user_id'], f"%{curr}%"))
        r = cur.fetchone()
        t, c, d = r[0] or 0, r[1] or 0, r[2] or 0
        out += f"👥 {w['name']}\n📉 O'tgan oydan qoldiq: 0 $\n💰 Bu oy savdo: {fmt(t)}\n💵 Bu oy naqt: {fmt(c)}\n📉 Bu oy yangi qarz: {fmt(d)}\n✅ Umumiy joriy qoldiq: {fmt(d)}\n\n"
        grand_total_sales += t; grand_total_cash += c; grand_total_debt += d
    out += f"📅 Oylik hisobot ({curr}): Umumiy\n"
    out += f"📉 O'tgan oydan qoldiq: 0 $\n💰 Bu oy savdo: {fmt(grand_total_sales)}\n💵 Bu oy naqt: {fmt(grand_total_cash)}\n📉 Bu oy yangi qarz: {fmt(grand_total_debt)}"
    conn.close()
    await message.answer(out)

@dp.message(F.text == "🏪 Barcha do'konlar")
async def boss_all_stores(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("""SELECT DISTINCT s.normalized_store, u.name FROM sales s 
                   JOIN users u ON s.worker_id = u.user_id ORDER BY u.name""")
    stores = cur.fetchall(); conn.close()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🏪 {s[0]} (👤 {s[1]})", callback_data=f"store_{s[0]}")] for s in stores])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    await message.answer("🏪 Barcha do'konlar:", reply_markup=kb)

@dp.message(F.text == "📊 Eng yaxshi ishchilar")
async def boss_top_workers(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("""SELECT u.name as worker_name, SUM(s.total) as ts, SUM(s.cash) as tc, 
                          SUM(s.total)-SUM(s.cash) as td, COUNT(s.id) as cnt 
                   FROM sales s JOIN users u ON s.worker_id = u.user_id 
                   GROUP BY u.name ORDER BY ts DESC""")
    res = cur.fetchall(); conn.close()
    out = "🏆 Ishchilar reytingi:\n"
    for i, r in enumerate(res, 1):
        out += f"{i}. {r['worker_name']} | 💰 {fmt(r['ts'])} | 💵 {fmt(r['tc'])} | 📉 {fmt(r['td'])} | 🧾 {r['cnt']} ta\n"
    await message.answer(out if out.strip() != "🏆 Ishchilar reytingi:\n" else "📊 Ma'lumot yo'q")

@dp.message(F.text == "🏆 Eng yaxshi do'konlar")
async def boss_top_stores(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT normalized_store, SUM(total) as ts, COUNT(id) as cnt FROM sales GROUP BY normalized_store ORDER BY ts DESC LIMIT 10")
    res = cur.fetchall(); conn.close()
    out = "🏆 TOP Do'konlar:\n"
    for i, r in enumerate(res, 1):
        out += f"{i}. 🏪 {r['normalized_store']} | 💰 {fmt(r['ts'])} | 🧾 {r['cnt']} marta\n"
    await message.answer(out if out.strip() != "🏆 TOP Do'konlar:\n" else "📊 Ma'lumot yo'q")

# ================= OYLIK KASSA =================
@dp.message(F.text == "📅 Oylik kassa")
async def handle_monthly_cash(message: types.Message):
    uid = message.from_user.id
    month = datetime.now().strftime("%m.%Y")

    if uid not in BOSS_IDS:
        conn = get_db(); cur = dict_cursor(conn)
        cur.execute("""SELECT SUBSTR(date, 1, 10) as d, SUM(cash) FROM sales 
                       WHERE worker_id = %s AND date LIKE %s AND cash > 0 GROUP BY d ORDER BY d DESC""", (uid, f"%{month}%"))
        rows = cur.fetchall(); conn.close()
        if not rows: return await message.answer(f"📅 {month} oyida naqt kassa harakati yo'q.")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📆 {r[0]} | {fmt(r[1])}", callback_data=f"day_{r[0]}")] for r in rows])
        kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
        return await message.answer(f"📅 {month} oylik kassa hisoboti (kun bo'yicha):", reply_markup=kb)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Kunlik Umumiy Hisobot", callback_data=f"mc_all_{month}")],
        [InlineKeyboardButton(text="👥 Ishchi bo'yicha Ko'rish", callback_data=f"mc_worker_{month}")]
    ])
    await message.answer(f"📅 Oylik kassa ({month}) rejimini tanlang:", reply_markup=kb)

@dp.callback_query(F.data.startswith("mc_all_"))
async def mc_all_dates(callback: CallbackQuery):
    await callback.answer()
    month = callback.data.replace("mc_all_", "")
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT DISTINCT SUBSTR(date, 1, 10) as d FROM sales WHERE date LIKE %s ORDER BY d DESC", (f"%{month}%",))
    dates = [r['d'] for r in cur.fetchall()]; conn.close()
    if not dates: return await callback.message.edit_text("📭 Bu oyda ma'lumot yo'q.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📆 {d}", callback_data=f"day_all_{d}")] for d in dates])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    await callback.message.edit_text(f"📅 {month} kunlarini tanlang:", reply_markup=kb)

@dp.callback_query(F.data.startswith("day_all_"))
async def day_all_summary(callback: CallbackQuery):
    await callback.answer()
    day = callback.data.replace("day_all_", "")
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("""SELECT u.name, s.store_name, SUM(s.cash) as cash 
                   FROM sales s JOIN users u ON s.worker_id = u.user_id 
                   WHERE s.date LIKE %s AND s.cash > 0 GROUP BY u.name, s.store_name""", (f"{day}%",))
    rows = cur.fetchall()
    cur.execute("""SELECT u.name, SUM(s.cash) as total 
                   FROM sales s JOIN users u ON s.worker_id = u.user_id 
                   WHERE s.date LIKE %s AND s.cash > 0 GROUP BY u.name""", (f"{day}%",))
    totals = {r['name']: r['total'] for r in cur.fetchall()}; conn.close()
    
    out = f"💰 Kassa (Live) - {day}\n"
    cur_w = ""
    for r in rows:
        if r['name'] != cur_w:
            if cur_w: out += f"👤 {cur_w} - Jami: {fmt(totals.get(cur_w, 0))}\n\n"
            cur_w = r['name']
        out += f"👤 {r['name']} - 🏪 {r['store_name']} - {fmt(r['cash'])}💵\n"
    out += f"👤 {cur_w} - Jami: {fmt(totals.get(cur_w, 0))}\n"
    await callback.message.edit_text(out, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Sanalar", callback_data="back_main")]]))

@dp.callback_query(F.data.startswith("mc_worker_"))
async def mc_worker_list(callback: CallbackQuery):
    await callback.answer()
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT name, user_id FROM users WHERE role='worker' AND active=1")
    workers = cur.fetchall(); conn.close()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"👤 {w['name']}", callback_data=f"sel_worker_{w['user_id']}")] for w in workers])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    await callback.message.edit_text("👥 Ishchini tanlang:", reply_markup=kb)

@dp.callback_query(F.data.startswith("sel_worker_"))
async def sel_worker_dates(callback: CallbackQuery):
    await callback.answer()
    uid = int(callback.data.replace("sel_worker_", ""))
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT name FROM users WHERE user_id=%s", (uid,)); w_name = cur.fetchone()['name']
    cur.execute("SELECT DISTINCT SUBSTR(date, 1, 10) as d FROM sales WHERE worker_id=%s ORDER BY d DESC", (uid,))
    dates = [r['d'] for r in cur.fetchall()]; conn.close()
    if not dates: return await callback.message.edit_text("📭 Ma'lumot yo'q.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📆 {d}", callback_data=f"day_worker_{uid}_{d}")] for d in dates])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Ishchilar", callback_data=f"mc_worker_{datetime.now().strftime('%m.%Y')}")])
    await callback.message.edit_text(f"👤 {w_name} uchun sanalar:", reply_markup=kb)

@dp.callback_query(F.data.startswith("day_worker_"))
async def day_worker_summary(callback: CallbackQuery):
    await callback.answer()
    parts = callback.data.replace("day_worker_", "").split("_")
    uid, day = int(parts[0]), parts[1]
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT name FROM users WHERE user_id=%s", (uid,)); w_name = cur.fetchone()['name']
    cur.execute("""SELECT store_name, SUM(cash) as cash FROM sales 
                   WHERE worker_id=%s AND date LIKE %s AND cash>0 GROUP BY store_name""", (uid, f"{day}%"))
    rows = cur.fetchall(); conn.close()
    out = f"💰 {w_name} - {day}\n"
    total = 0
    for r in rows:
        out += f"🏪 {r['store_name']} | 💵 {fmt(r['cash'])}\n"
        total += r['cash']
    out += f"\n💰 Jami: {fmt(total)}"
    await callback.message.edit_text(out, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Sanalar", callback_data=f"sel_worker_{uid}")]]))

@dp.message(F.text == "💰 Oylik maosh")
async def calculate_salary(message: types.Message):
    uid = message.from_user.id
    month = datetime.now().strftime("%m.%Y")
    
    if uid in BOSS_IDS:
        conn = get_db(); cur = dict_cursor(conn)
        cur.execute("SELECT user_id, name FROM users WHERE role = 'worker' AND active = 1")
        workers = cur.fetchall()
        out = f"💰 Oylik maosh hisoboti ({month}):\n\n"
        grand_salary = 0
        for w in workers:
            cur.execute("""SELECT SUM(cash) FROM sales WHERE worker_id = %s AND date LIKE %s AND cash > 0""", (w['user_id'], f"%{month}%"))
            result = cur.fetchone()
            total_cash = result[0] if result[0] else 0.0
            percent = total_cash * 0.08
            fixa = 150 if 1500 <= total_cash < 2000 else (200 if 2000 <= total_cash < 3000 else (300 if total_cash >= 3000 else 0))
            salary = percent + fixa; grand_salary += salary
            out += f"👥 {w['name']}\n📊 Yig'ilgan naqt: {fmt(total_cash)}\n📈 8% ulush: {fmt(percent)}\n🎁 Fiksa bonus: {fmt(fixa)}\n✅ Jami maosh: {fmt(salary)}\n\n"
        out += f"💰 JAMI MAOSH XARAJATI: {fmt(grand_salary)}"; conn.close()
        return await message.answer(out)
    
    w_cond, w_params = get_worker_filter(uid)
    params = (f"%{month}%",) + w_params
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute(f"SELECT SUM(cash) FROM sales WHERE date LIKE %s AND cash > 0 {w_cond}", params)
    total_cash = cur.fetchone()[0] or 0.0; conn.close()
    percent = total_cash * 0.08
    fixa = 150 if 1500 <= total_cash < 2000 else (200 if 2000 <= total_cash < 3000 else (300 if total_cash >= 3000 else 0))
    await message.answer(f"💰 Oylik maosh hisoboti ({month}):\n\n📊 Yig'ilgan naqt: {fmt(total_cash)}\n📈 8% ulush: {fmt(percent)}\n🎁 Fiksa bonus: {fmt(fixa)}\n✅ Itog (Jami maosh): {fmt(percent + fixa)}")

# ================= ISHCHI FUNKSIYALARI =================
@dp.message(F.text == "📊 Kunlik kassa")
async def daily_cash(message: types.Message):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    w_cond, w_params = get_worker_filter(uid)
    today = date.today().strftime("%d.%m.%Y")
    params = (f"{today}%",) + w_params
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute(f"SELECT store_name, cash, date FROM sales WHERE date LIKE %s AND cash > 0 {w_cond} ORDER BY id DESC", params)
    rows = cur.fetchall()
    cur.execute(f"SELECT SUM(cash) FROM sales WHERE date LIKE %s AND cash > 0 {w_cond}", params)
    total_cash = cur.fetchone()[0] or 0; conn.close()
    if not rows: return await message.answer("📅 Bugun naqt kiritilmagan.")
    out = f"📅 Bugungi naqt harakatlar ({today}):\n"
    for r in rows: out += f"🏪 {r['store_name']} | 💵 {fmt(r['cash'])} | 🕒 {r['date'].split()[1]}\n"
    out += f"\n💰 Jami yig'ilgan: {fmt(total_cash)}"
    await message.answer(out)

@dp.callback_query(F.data.startswith("day_"))
async def show_day_details(callback: CallbackQuery):
    if callback.data.startswith("day_all_") or callback.data.startswith("day_worker_"): return
    await callback.answer()
    day = callback.data.replace("day_", "")
    uid = callback.from_user.id
    if uid in BOSS_IDS: return
    w_cond, w_params = get_worker_filter(uid)
    params = (f"{day}%",) + w_params
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute(f"SELECT store_name, total, cash, txn_type, date FROM sales WHERE date LIKE %s AND cash > 0 {w_cond} ORDER BY id DESC", params)
    rows = cur.fetchall(); conn.close()
    if not rows: return await callback.message.edit_text("📭 Bu kunda naqt kassa harakati yo'q.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Oylik ro'yxat", callback_data="back_main")]]))
    out = f"📅 {day} kunlik batafsil kassa:\n"
    for r in rows:
        if r['txn_type'] == 'savdo': out += f"🏪 {r['store_name']} | 📦 {fmt(r['total'])} | 💵 {fmt(r['cash'])} naqt ({r['date'].split()[1]})\n"
        elif r['txn_type'] == 'naqt': out += f"🏪 {r['store_name']} | 💵 +{fmt(r['cash'])} ({r['date'].split()[1]})\n"
    await callback.message.edit_text(out, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")]]))

@dp.message(F.text == "📅 Oylik hisobot")
async def monthly_report(message: types.Message):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    w_cond, w_params = get_worker_filter(uid)
    curr = datetime.now().strftime("%m.%Y")
    params_curr = (f"%{curr}%",) + w_params
    params_all = w_params
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute(f"SELECT SUM(total), SUM(cash) FROM sales WHERE 1=1 {w_cond}", params_all)
    t_all = cur.fetchone()
    cur.execute(f"SELECT SUM(total), SUM(cash), SUM(total)-SUM(cash) FROM sales WHERE date LIKE %s {w_cond}", params_curr)
    t_curr = cur.fetchone(); conn.close()
    old_debt = ((t_all[0] or 0) - (t_all[1] or 0)) - ((t_curr[2] or 0) if t_curr[2] else 0)
    if old_debt < 0: old_debt = 0
    await message.answer(f"📅 Oylik hisobot ({curr}):\n📉 O'tgan oydan qoldiq: {fmt(old_debt)}\n💰 Bu oy savdo: {fmt(t_curr[0])}\n💵 Bu oy naqt: {fmt(t_curr[1])}\n📉 Bu oy yangi qarz: {fmt(t_curr[2])}\n✅ Umumiy joriy qoldiq: {fmt(old_debt + (t_curr[2] or 0))}")

# ================= QARZI BORLAR =================
@dp.message(F.text == "🤝 Qarzi borlar")
async def handle_debtors(message: types.Message):
    uid = message.from_user.id
    conn = get_db(); cur = dict_cursor(conn)

    if uid in BOSS_IDS:
        cur.execute("""
            SELECT u.user_id, u.name as worker_name,
                   ROUND(SUM(s.total) - SUM(s.cash), 2) as bal 
            FROM sales s
            JOIN users u ON s.worker_id = u.user_id
            WHERE s.normalized_store IS NOT NULL AND s.normalized_store != ''
            GROUP BY u.user_id, u.name
            HAVING bal > 0 
            ORDER BY bal DESC
        """)
        res = cur.fetchall()
        if not res:
            conn.close()
            return await message.answer("✅ Hozircha qarzi bor ishchilar yo'q.")
        out = "🤝 Qarzi bor ishchilar:\n"
        for r in res: out += f"👤 {r['worker_name']} - {fmt(r['bal'])}\n"
        out += f"\n💰 Umumiy qarz: {fmt(sum(r['bal'] for r in res))}"
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"👤 {r['worker_name']} ({fmt(r['bal'])})", callback_data=f"boss_debt_uid_{r['user_id']}")] for r in res])
        await message.answer(out, reply_markup=kb)
    else:
        cur.execute("SELECT normalized_store, SUM(total)-SUM(cash) as bal FROM sales WHERE worker_id = %s GROUP BY normalized_store HAVING bal > 0 ORDER BY bal DESC", (uid,))
        res = cur.fetchall()
        if not res:
            conn.close()
            return await message.answer("✅ Sizning qarzi bor do'konlaringiz yo'q.")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📉 {r[0]} ({fmt(r['bal'])})", callback_data=f"store_{r[0]}")] for r in res])
        await message.answer("🤝 Qarzi bor do'konlar:", reply_markup=kb)
    conn.close()

@dp.callback_query(F.data.startswith("boss_debt_uid_"))
async def boss_debt_detail(callback: CallbackQuery):
    await callback.answer()
    try:
        worker_id = int(callback.data.replace("boss_debt_uid_", ""))
        conn = get_db(); cur = dict_cursor(conn)
        
        cur.execute("SELECT name FROM users WHERE user_id = %s", (worker_id,))
        w_row = cur.fetchone()
        w_name = w_row['name'] if w_row else f"ID:{worker_id}"

        cur.execute("""
            SELECT normalized_store, 
                   ROUND(SUM(total), 2) as t, 
                   ROUND(SUM(cash), 2) as c
            FROM sales 
            WHERE worker_id = %s AND normalized_store IS NOT NULL AND normalized_store != ''
            GROUP BY normalized_store
            HAVING t > c
            ORDER BY (t - c) DESC
        """, (worker_id,))
        stores = cur.fetchall()
        conn.close()

        out = f"👤 {w_name} ning qarzdor do'konlari:\n"
        if not stores:
            out += "✅ Hozircha aniq qarzi bor do'kon topilmadi."
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")]])
        else:
            for s in stores:
                bal = s['t'] - s['c']
                out += f"🏪 {s['normalized_store']} | Qarz: {fmt(bal)}\n"
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"📖 {s['normalized_store']} ko'rish", callback_data=f"boss_debt_store_{worker_id}_{s['normalized_store']}")]
                for s in stores
            ])
            kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
            
        try:
            await callback.message.edit_text(out, reply_markup=kb)
        except Exception:
            await callback.message.answer(out, reply_markup=kb)
    except Exception as e:
        await callback.answer(f"❌ Xatolik: {e}", show_alert=True)

@dp.callback_query(F.data.startswith("boss_debt_store_"))
async def boss_debt_store_view(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    prefix = "boss_debt_store_"
    data_str = callback.data.replace(prefix, "")
    parts = data_str.split("_", 1)
    
    if len(parts) != 2: return
    worker_id = int(parts[0])
    store = parts[1]
    
    await state.update_data(debt_worker_id=worker_id, current_store=store)
    
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT SUM(total), SUM(cash) FROM sales WHERE normalized_store = %s AND worker_id = %s", (store, worker_id))
    res_total = cur.fetchone()
    cur.execute("SELECT txn_type, total, cash, date FROM sales WHERE normalized_store = %s AND worker_id = %s ORDER BY id DESC LIMIT 15", (store, worker_id))
    history = cur.fetchall(); conn.close()
    
    total = res_total[0] or 0; cash = res_total[1] or 0; balance = total - cash
    out = f"🏪 **{store.upper()}** hisoboti:\n💰 Umumiy savdo: {fmt(total)} | 💵 Yig'ilgan: {fmt(cash)}\n📉 **Qoldiq qarz: {fmt(balance)}**\n\n📜 Harakatlar tarixi:\n"
    for h in history:
        if h['txn_type'] in ['savdo', 'savdo_yangi']: 
            out += f"📅 {h['date']} | 📦 +{fmt(h['total'])} (💵 {fmt(h['cash'])} naqt)\n"
        elif h['txn_type'] == 'naqt': 
            out += f"📅 {h['date']} | 💵 +{fmt(h['cash'])} (Naqt kiritildi)\n"
        elif h['txn_type'] == 'qaytarish': 
            out += f"📅 {h['date']} | 🔄 -{fmt(abs(h['total']))} (Qaytarildi)\n"
            
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Naqt kiritish", callback_data="act_cash"),
         InlineKeyboardButton(text="🔄 Qaytarish", callback_data="act_return")],
        [InlineKeyboardButton(text="💰 Yangi savdo", callback_data="act_trade"),
         InlineKeyboardButton(text="👤 Do'konchi", callback_data="act_owner")],
        [InlineKeyboardButton(text="⬅️ Qarzdor do'konlar", callback_data=f"boss_debt_uid_{worker_id}")]
    ])
    
    try:
        await callback.message.edit_text(out, reply_markup=kb, parse_mode="Markdown")
    except Exception:
        await callback.message.answer(out, reply_markup=kb, parse_mode="Markdown")

# ================= DO'KONLAR =================
@dp.message(F.text == "🏪 Do'konlarim")
async def stores_list_cmd(message: types.Message): 
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT DISTINCT normalized_store FROM sales WHERE worker_id = %s ORDER BY normalized_store", (uid,))
    stores = cur.fetchall(); conn.close()
    if not stores: return await message.answer("🏪 Hozircha do'konlar yo'q.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🏪 {s[0]}", callback_data=f"store_{s[0]}")] for s in stores])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    await message.answer("🏪 Do'konlar ro'yxati:", reply_markup=kb)

@dp.callback_query(F.data == "stores_list")
async def stores_list_cb(callback: CallbackQuery): 
    uid = callback.from_user.id
    if uid in BOSS_IDS: return
    w_cond, w_params = get_worker_filter(uid)
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute(f"SELECT DISTINCT normalized_store FROM sales WHERE 1=1 {w_cond} ORDER BY normalized_store", w_params)
    stores = cur.fetchall(); conn.close()
    if not stores: return await callback.answer("🏪 Hozircha do'konlar yo'q.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🏪 {s['normalized_store']}", callback_data=f"store_{s['normalized_store']}")] for s in stores])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    await callback.message.edit_text("🏪 Do'konlar ro'yxati:", reply_markup=kb)

@dp.message(F.text == "🔍 Do'kon qidirish")
async def search_prompt(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    await message.answer("🔍 Do'kon nomini yozing:", reply_markup=get_back_kb())
    await state.set_state(AppStates.search_store)

@dp.message(AppStates.search_store)
async def search_handle(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Orqaga": await state.clear(); return await start_cmd(message, state)
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    w_cond, w_params = get_worker_filter(uid)
    search_term = f"%{message.text.lower().strip()}%"
    params = (search_term,) + w_params
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute(f"SELECT DISTINCT normalized_store FROM sales WHERE normalized_store LIKE %s {w_cond}", params)
    res = cur.fetchall(); conn.close()
    if not res: return await message.answer("🔍 Hech narsa topilmadi.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🏪 {r[0]}", callback_data=f"store_{r[0]}")] for r in res])
    await message.answer("🔍 Natijalar:", reply_markup=kb)
    await state.clear()

@dp.callback_query(F.data.startswith("store_"))
async def store_details(callback: CallbackQuery, state: FSMContext):
    store = callback.data[6:]
    if not store: return await callback.answer("⚠️ Xatolik: Do'kon nomi topilmadi.", show_alert=True)
    await state.update_data(current_store=store)
    
    data = await state.get_data()
    uid = data.get('debt_worker_id', callback.from_user.id)
    if data.get('debt_worker_id'): await state.update_data(debt_worker_id=None)
    
    w_cond, w_params = get_worker_filter(uid)
    full_params = (store,) + w_params
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute(f"SELECT SUM(total), SUM(cash) FROM sales WHERE normalized_store = %s {w_cond}", full_params)
    res_total = cur.fetchone()
    cur.execute(f"SELECT txn_type, total, cash, date FROM sales WHERE normalized_store = %s {w_cond} ORDER BY id DESC LIMIT 15", full_params)
    history = cur.fetchall(); conn.close()
    total = res_total[0] or 0; cash = res_total[1] or 0; balance = total - cash
    out = f"🏪 **{store.upper()}** hisoboti:\n💰 Umumiy savdo: {fmt(total)} | 💵 Yig'ilgan: {fmt(cash)}\n📉 **Qoldiq qarz: {fmt(balance)}**\n\n📜 Harakatlar tarixi:\n"
    for h in history:
        if h['txn_type'] in ['savdo', 'savdo_yangi']: 
            out += f"📅 {h['date']} | 📦 +{fmt(h['total'])} (💵 {fmt(h['cash'])} naqt)\n"
        elif h['txn_type'] == 'naqt': 
            out += f"📅 {h['date']} | 💵 +{fmt(h['cash'])} (Naqt kiritildi)\n"
        elif h['txn_type'] == 'qaytarish': 
            out += f"📅 {h['date']} | 🔄 -{fmt(abs(h['total']))} (Qaytarildi)\n"
            
    back_kb = []
    if data.get('debt_worker_id'):
        back_kb.append([InlineKeyboardButton(text="⬅️ Qarzdor do'konlar", callback_data=f"boss_debt_uid_{data.get('debt_worker_id')}")])
    elif uid in BOSS_IDS:
        back_kb.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    else:
        back_kb.append([InlineKeyboardButton(text="🔍 Boshqa do'kon", callback_data="stores_list")])
        
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Naqt kiritish", callback_data="act_cash"),
         InlineKeyboardButton(text="🔄 Qaytarish", callback_data="act_return")],
        [InlineKeyboardButton(text="💰 Yangi savdo", callback_data="act_trade"),
         InlineKeyboardButton(text="👤 Do'konchi", callback_data="act_owner")]
    ] + back_kb)
    
    await callback.message.edit_text(out, reply_markup=kb, parse_mode="Markdown")

@dp.callback_query(F.data == "act_cash")
async def start_cash(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    if uid in BOSS_IDS: return
    await callback.message.answer("💵 Qancha naqt kiritasiz? (faqat raqam):", reply_markup=get_back_kb())
    await state.set_state(AppStates.add_cash)

@dp.message(AppStates.add_cash)
async def process_cash(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    if message.text == "⬅️ Orqaga": await state.clear(); return await start_cmd(message, state)
    if not message.text.replace('.','',1).isdigit(): return await message.answer("⚠️ Faqat raqam!")
    await handle_store_action(message, state, "naqt", float(message.text))

@dp.callback_query(F.data == "act_return")
async def start_return(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    if uid in BOSS_IDS: return
    await callback.message.answer("🔄 Qancha qaytarilasiz? (faqat raqam):", reply_markup=get_back_kb())
    await state.set_state(AppStates.add_return)

@dp.message(AppStates.add_return)
async def process_return(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    if message.text == "⬅️ Orqaga": await state.clear(); return await start_cmd(message, state)
    if not message.text.replace('.','',1).isdigit(): return await message.answer("⚠️ Faqat raqam!")
    await handle_store_action(message, state, "qaytarish", float(message.text))

@dp.callback_query(F.data == "act_trade")
async def start_trade(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    if uid in BOSS_IDS: return
    await callback.message.answer("💰 Faqat summani yozing (bu summa shu do'kon savdosiga qo'shiladi):", reply_markup=get_back_kb())
    await state.set_state(AppStates.add_new_sale_store)

@dp.message(AppStates.add_new_sale_store)
async def process_new_sale_store(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    if message.text == "⬅️ Orqaga": await state.clear(); return await start_cmd(message, state)
    if not message.text.replace('.','',1).isdigit(): return await message.answer("⚠️ Faqat raqam!")
    data = await state.get_data()
    store = data.get('current_store')
    if not store: return await message.answer("⚠️ Xatolik: Do'kon tanlanmagan.")
    await handle_store_action(message, state, "savdo_yangi", float(message.text), store)

async def handle_store_action(message: types.Message, state: FSMContext, t_type: str, amount: float, store: str = None):
    if not store:
        data = await state.get_data()
        store = data.get('current_store')
        if not store: return await message.answer("⚠️ Xatolik: Do'kon tanlanmagan.")
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    conn = get_db(); cur = dict_cursor(conn)
    
    # PostgreSQL da RETURNING id ishlatiladi
    if t_type == "naqt": 
        cur.execute("""INSERT INTO sales (store_name, normalized_store, total, cash, debt, txn_type, date, worker_id, worker_name) 
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""", 
                    (store, store, 0, amount, -amount, 'naqt', now_str, message.from_user.id, message.from_user.full_name))
    elif t_type == "qaytarish": 
        cur.execute("""INSERT INTO sales (store_name, normalized_store, total, cash, debt, txn_type, date, worker_id, worker_name) 
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""", 
                    (store, store, -amount, 0, -amount, 'qaytarish', now_str, message.from_user.id, message.from_user.full_name))
    elif t_type == "savdo_yangi": 
        cur.execute("""INSERT INTO sales (store_name, normalized_store, total, cash, debt, txn_type, date, worker_id, worker_name) 
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""", 
                    (store, store, amount, 0, amount, 'savdo', now_str, message.from_user.id, message.from_user.full_name))
    else: return await message.answer("⚠️ Noto'g'ri amal.")
    
    sale_id = cur.fetchone()[0] # ID ni olish
    conn.commit(); conn.close()
    
    await notify_boss(message.from_user.id, store, amount if t_type!='qaytarish' else -amount, amount if t_type=='naqt' else 0, t_type, now_str)
    
    txt = f"✅ {fmt(amount)} naqt qabul qilindi!" if t_type == "naqt" else (f"✅ {fmt(amount)} qaytarildi!" if t_type == "qaytarish" else f"✅ {fmt(amount)} savdo qo'shildi!")
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Xato bo'lsa bekor qilish", callback_data=f"cancel_req_{sale_id}")]])
    await message.answer(txt, reply_markup=cancel_kb)
    await state.clear()
    if store: await send_store_details(message, store, state)

async def send_store_details(message: types.Message, store: str, state: FSMContext):
    await state.update_data(current_store=store)
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    w_cond, w_params = get_worker_filter(uid)
    full_params = (store,) + w_params
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute(f"SELECT SUM(total), SUM(cash) FROM sales WHERE normalized_store = %s {w_cond}", full_params)
    res = cur.fetchone()
    total = res[0] or 0; cash = res[1] or 0; balance = total - cash
    cur.execute(f"SELECT txn_type, total, cash, date FROM sales WHERE normalized_store = %s {w_cond} ORDER BY id DESC LIMIT 15", full_params)
    history = cur.fetchall(); conn.close()
    out = f"🏪 **{store.upper()}** hisoboti:\n💰 Umumiy savdo: {fmt(total)} | 💵 Yig'ilgan: {fmt(cash)}\n📉 **Qoldiq qarz: {fmt(balance)}**\n\n📜 Harakatlar tarixi:\n"
    for h in history:
        if h['txn_type'] == 'savdo': out += f"📅 {h['date']} | 📦 +{fmt(h['total'])} (Savdo)\n"
        elif h['txn_type'] == 'naqt': out += f"📅 {h['date']} | 💵 +{fmt(h['cash'])} (Naqt kiritildi)\n"
        elif h['txn_type'] == 'qaytarish': out += f"📅 {h['date']} | 🔄 -{fmt(abs(h['total']))} (Qaytarildi)\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Naqt kiritish", callback_data="act_cash"),
         InlineKeyboardButton(text="🔄 Qaytarish", callback_data="act_return")],
        [InlineKeyboardButton(text="💰 Yangi savdo", callback_data="act_trade"),
         InlineKeyboardButton(text="👤 Do'konchi", callback_data="act_owner")],
        [InlineKeyboardButton(text="🔍 Boshqa do'kon", callback_data="stores_list")]
    ])
    await message.answer(out, reply_markup=kb, parse_mode="Markdown")

async def send_owner_info(target, store, state: FSMContext):
    msg = target.message if isinstance(target, CallbackQuery) else target
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT owner_name, phone, location FROM stores_info WHERE normalized_store = %s", (store,))
    info = cur.fetchone(); conn.close()
    name = info['owner_name'] if info and info['owner_name'] else "⬜ Kiritilmagan"
    phone = info['phone'] if info and info['phone'] else "⬜ Kiritilmagan"
    loc = info['location'] if info and info['location'] else "⬜ Kiritilmagan"
    out = f"👤 **Do'konchi ma'lumotlari** (`{store}`):\n📛 Ism: `{name}`\n📞 Telefon: `{phone}`\n📍 Manzil: `{loc}`"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Ism o'zgartirish", callback_data="edit_name"),
         InlineKeyboardButton(text="✏️ Telefon", callback_data="edit_phone")],
        [InlineKeyboardButton(text="✏️ Manzil", callback_data="edit_loc"),
         InlineKeyboardButton(text="⬅️ Orqaga", callback_data=f"store_{store}")]
    ])
    await msg.answer(out, reply_markup=kb, parse_mode="Markdown")

@dp.callback_query(F.data == "act_owner")
async def show_owner(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    if uid in BOSS_IDS: return
    data = await state.get_data()
    await send_owner_info(callback, data.get('current_store', ''), state)

@dp.callback_query(F.data.startswith("edit_"))
async def edit_owner(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    if uid in BOSS_IDS: return
    field = callback.data.replace("edit_", "")
    await state.update_data(edit_field=field)
    titles = {"name": "Ism", "phone": "Telefon", "loc": "Manzil"}
    title_text = titles.get(field, "ma'lumot")
    await callback.message.answer(f"✍️ Yangi {title_text} ni yozing:", reply_markup=get_back_kb())
    await state.set_state(AppStates.edit_store_info)

@dp.message(AppStates.edit_store_info)
async def save_owner_info(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    if message.text == "⬅️ Orqaga": 
        await state.clear()
        return await start_cmd(message, state)
    data = await state.get_data()
    store = data.get('current_store', '')
    field = data.get('edit_field', 'name')
    col_map = {"name": "owner_name", "phone": "phone", "loc": "location"}
    col = col_map.get(field, "owner_name")
    conn = get_db(); cur = dict_cursor(conn)
    # ON CONFLICT ishlatish kerak
    cur.execute(f"""INSERT INTO stores_info (normalized_store, owner_name, phone, location) 
                    VALUES (%s, %s, %s, %s) 
                    ON CONFLICT (normalized_store) DO UPDATE SET {col} = EXCLUDED.{col}""", 
                (store, '' if col!='owner_name' else message.text.strip(), '' if col!='phone' else message.text.strip(), '' if col!='location' else message.text.strip()))
    conn.commit(); conn.close()
    await message.answer("✅ Saqlandi!")
    await state.clear()
    await send_owner_info(message, store, state)

# Faqat boss uchun do'kon o'chirish buyrug'i
@dp.message(Command("delete_store"))
async def delete_store_command(message: types.Message):
    if message.from_user.id not in BOSS_IDS:
        return
    
    args = message.text.split()
    if len(args) < 2:
        return await message.answer("❌ Misol: /delete_store qatortol")
    
    store_name = args[1]
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("DELETE FROM sales WHERE normalized_store = %s", (store_name,))
    cur.execute("DELETE FROM stores_info WHERE normalized_store = %s", (store_name,))
    conn.commit(); conn.close()
    
    await message.answer(f"✅ {store_name} o'chirildi!")

@dp.message(F.text == "✍️ Savdo qo'shish")
async def trade_init(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    await state.clear()
    await message.answer("📝 Misol: `Ali market 5000` yoki `Ali 5000 naxt 300`", reply_markup=get_back_kb())
    await state.set_state(AppStates.waiting_trade)

@dp.message(AppStates.waiting_trade)
async def handle_trade(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    if message.text == "⬅️ Orqaga": await state.clear(); return await start_cmd(message, state)
    msg = await message.answer("🤖 AI tahlil qilmoqda...")
    try:
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[
            {"role": "system", "content": """Sen savdo matnini tahlil qiluvchi AI yordamchisan.
Vazifang: Matndan do'kon(store), jami summa(total), naqt(cash) ajratish.
Qoidalar:
1. "naxt", "nax", "next", "naqt", "oldim", "pul", "qo'lda" so'zlari yoki ularga yopishgan raqamlar (masalan: nax40, next500) "naqt" ni bildiradi.
2. Agar faqat bitta raqam bo'lsa -> total = raqam, cash = 0.
3. Agar naqt so'zi va raqam bo'lsa -> cash = shu raqam, total = boshqa raqam (yoki cash bilan bir xil).
4. Do'kon nomini kichik harfda, bo'sh joylarsiz qaytar.
5. Faqat JSON formatda qaytar: {"store": "string", "total": number, "cash": number}"""},
            {"role": "user", "content": message.text}
        ], response_format={"type": "json_object"})
        d = json.loads(res.choices[0].message.content)
        store = normalize(d.get("store", "noma'lum"))
        total = float(d.get("total", 0)); cash = float(d.get("cash", 0)); debt = round(total - cash, 2)
        now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
        conn = get_db(); cur = dict_cursor(conn)
        # PostgreSQL da RETURNING id
        cur.execute("""INSERT INTO sales (store_name, normalized_store, total, cash, debt, txn_type, date, worker_id, worker_name) 
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                    (store, store, total, cash, debt, "savdo", now_str, message.from_user.id, message.from_user.full_name))
        sale_id = cur.fetchone()[0]; conn.commit(); conn.close()
        
        await notify_boss(message.from_user.id, store, total, cash, "savdo", now_str)
        
        await msg.edit_text(f"✅ Saqlandi!\n🏪 Dokon nomi - `{store}`\n💰 Savdo - {fmt(total)}\n💵 Naqt - {fmt(cash)}\n📉 Qarz: {fmt(debt)}", 
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Xato bo'lsa bekor qilish", callback_data=f"cancel_req_{sale_id}")]]))
    except Exception as e: 
        await msg.edit_text(f"❌ AI xato: {e}\nFormat: `dokon summa [naxt sum]`")
    await state.clear()

@dp.message(F.text == "🤖 AI Yordam")
async def ai_help_start(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    await state.clear()
    await message.answer("🤖 AI yordamchi tayyor. Misol: `5-aprelda qanaqa savdo bo'lgan?` yoki `Jami qancha naqt yig'dim?`", reply_markup=get_back_kb())
    await state.set_state(AppStates.ai_chat)

@dp.message(AppStates.ai_chat)
async def ai_handle_chat(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    if message.text == "⬅️ Orqaga": await state.clear(); return await start_cmd(message, state)
    msg = await message.answer("🔍 Ma'lumotlar tahlil qilinmoqda...")
    try:
        w_cond, w_params = get_worker_filter(uid)
        today_str = date.today().strftime("%d.%m.%Y") + "%"
        params_today = (today_str,) + w_params
        params_all = w_params
        conn = get_db(); cur = dict_cursor(conn)
        cur.execute(f"SELECT SUM(total)-SUM(cash) FROM sales WHERE 1=1 {w_cond}", params_all)
        total_debt = cur.fetchone()[0] or 0
        cur.execute(f"SELECT SUM(cash) FROM sales WHERE date LIKE %s {w_cond}", params_today)
        today_cash = cur.fetchone()[0] or 0
        cur.execute(f"SELECT COUNT(id) FROM sales WHERE date LIKE %s {w_cond}", params_today)
        today_sales = cur.fetchone()[0] or 0
        date_match = re.search(r'(\d{1,2})[-./](\d{1,2})', message.text)
        extra_context = ""
        if date_match:
            d, m = date_match.groups()
            q_date = f"{d.zfill(2)}.{m.zfill(2)}.{datetime.now().year}"
            cur.execute(f"SELECT store_name, total, cash, txn_type FROM sales WHERE date LIKE %s {w_cond}", (f"{q_date}%",) + w_params)
            res = cur.fetchall()
            extra_context = f"\n📅 {q_date} dagi harakatlar:\n" + "\n".join([f"- {r['store_name']}: {r['txn_type']} | Savdo:{fmt(r['total'])} | Naqt:{fmt(r['cash'])}" for r in res]) if res else f"\n📅 {q_date}: Ma'lumot yo'q."
        conn.close()
        context = (f"📊 UMUMIY HOLAT:\n"
                   f"• Jami qarz: {fmt(total_debt)}\n"
                   f"• Bugungi tushum: {fmt(today_cash)}\n"
                   f"• Bugungi savdolar soni: {today_sales} ta"
                   f"{extra_context}")
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[
            {"role": "system", "content": f"Siz MK Kassa boti yordamchisisiz. Foydalanuvchi savoliga **faqat** quyidagi aniq ma'lumotlar asosida javob bering. O'ylab topmang. Qisqa va aniq javob bering:\n{context}"},
            {"role": "user", "content": message.text}])
        await msg.edit_text(f"🤖 {res.choices[0].message.content}")
    except Exception as e: await msg.edit_text(f"❌ AI xato: {e}")

@dp.callback_query(F.data.startswith("cancel_req_"))
async def request_cancel(callback: CallbackQuery):
    await callback.answer()
    sale_id = int(callback.data.replace("cancel_req_", ""))
    uid = callback.from_user.id
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT store_name, total, cash, date FROM sales WHERE id = %s AND worker_id = %s", (sale_id, uid))
    sale = cur.fetchone()
    if not sale: return await callback.answer("⚠️ Bu amal allaqachon o'chirilgan yoki topilmadi.", show_alert=True)
    cur.execute("INSERT INTO deletion_requests (worker_id, sale_id, request_date) VALUES (%s,%s,%s)", (uid, sale_id, datetime.now().strftime("%d.%m.%Y %H:%M")))
    conn.commit(); conn.close()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Bekor qilsin", callback_data=f"approve_{sale_id}"), InlineKeyboardButton(text="❌ Qoldirsin", callback_data=f"reject_{sale_id}")]])
    notif = f"🔔 **O'chirish so'rovi**\n👤 Ishchi: {callback.from_user.full_name} (ID: `{uid}`)\n🏪 Do'kon: {sale['store_name']}\n💰 Summa: {fmt(sale['total'])} | 💵 Naqt: {fmt(sale['cash'])}\n📅 Sana: {sale['date']}"
    for boss_id in BOSS_IDS:
        try: await bot.send_message(boss_id, notif, reply_markup=kb, parse_mode="Markdown")
        except: pass
    await callback.message.edit_text(f"✅ Bekor qilish so'rovi bossga yuborildi.\nBoss tasdiqlagacha kuting...", reply_markup=None)

@dp.callback_query(F.data.startswith("approve_") | F.data.startswith("reject_"))
async def handle_deletion_request(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id not in BOSS_IDS: return
    action, sale_id = callback.data.split("_"); sale_id = int(sale_id)
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT worker_id FROM deletion_requests WHERE sale_id = %s", (sale_id,))
    req = cur.fetchone(); worker_id = req[0] if req else None
    if action == "approve":
        cur.execute("DELETE FROM sales WHERE id = %s", (sale_id,))
        cur.execute("DELETE FROM deletion_requests WHERE sale_id = %s", (sale_id,))
        conn.commit()
        await callback.message.edit_text(f"✅ Bekor qilish tasdiqlandi!\nSavdo ID: `{sale_id}` o'chirildi.")
        if worker_id: await bot.send_message(worker_id, "✅ Boss so'rovingizni tasdiqladi. Oxirgi amal bekor qilindi.")
    else:
        cur.execute("UPDATE deletion_requests SET status = 'rejected' WHERE sale_id = %s", (sale_id,))
        conn.commit()
        await callback.message.edit_text(f"❌ Rad etildi.\nSavdo o'zgarishsiz qoldi.")
        if worker_id: await bot.send_message(worker_id, "❌ Boss so'rovingizni rad etdi. Savdo saqlanib qoldi.")
    conn.close()

@dp.message(F.text)
async def worker_info(message: types.Message, state: FSMContext):
    if await state.get_state() is not None: return
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT name, user_id FROM users WHERE name = %s", (message.text,))
    w = cur.fetchone()
    if w:
        cur.execute("SELECT SUM(total), COUNT(id) FROM sales WHERE worker_id = %s", (w['user_id'],))
        r = cur.fetchone(); conn.close()
        await message.answer(f"👤 {w['name']}\n📊 Jami savdo: {fmt(r[0])}\n🧾 Savdolar soni: {r[1] or 0}", reply_markup=get_back_kb())

# ================= ORQAGA TUGMASI =================
@dp.callback_query(F.data == "back_main")
async def back_main(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    uid = callback.from_user.id
    try:
        conn = get_db(); cur = dict_cursor(conn)
        cur.execute("SELECT name, active FROM users WHERE user_id = %s", (uid,))
        user = cur.fetchone(); conn.close()
        
        if uid in BOSS_IDS:
            kb = get_boss_menu()
            text = "Xush kelibsiz, Boss 👑"
        elif user and user['active'] == 0:
            kb = get_back_kb()
            text = "🚫 Hisobingiz bloklan."
        else:
            kb = get_worker_menu() if user else ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⏳ Kutilmoqda...")]], resize_keyboard=True)
            text = f"Assalomu alaykum, {user['name'] or 'Ishchi'}!" if user else f"Siz ro'yxatda yo'qsiz.\n🆔 ID: {uid}"
            
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        kb = get_boss_menu() if uid in BOSS_IDS else get_worker_menu()
        await callback.message.answer(text, reply_markup=kb)

# ================= ASOSIY QISM =================
async def main():
    init_db()
    print("✅ Bot ishga tushdi 🚀")
    print("💡 Telegramda /start yuboring")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
