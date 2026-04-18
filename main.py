import asyncio
import json
import os
import psycopg2
import psycopg2.extras
import re
from datetime import datetime, date, timedelta

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
    try:
        n = round(float(num), 2)
        return f"{n:,.2f}".replace(".00", "").replace(",", " ") + " $"
    except: 
        return "0 $"

def normalize(text):
    if not text: return ""
    return text.strip().lower().replace("  ", " ").replace(" ", "_")

def extract_date(text):
    if not text: return None, text
    match = re.search(r'(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})', text)
    if match:
        day, month, year = match.groups()
        year = year if len(year) == 4 else f"20{year}"
        date_str = f"{int(day):02d}.{int(month):02d}.{year}"
        clean_text = text.replace(match.group(0), "").strip()
        return date_str, clean_text
    return None, text

def get_worker_filter(uid):
    if uid in BOSS_IDS: return "", ()
    return "AND worker_id = %s", (uid,)

def get_worker_name(uid):
    try:
        conn = get_db(); cur = dict_cursor(conn)
        cur.execute("SELECT name FROM users WHERE user_id = %s", (uid,))
        res = cur.fetchone(); conn.close()
        return res['name'] if res and res['name'] else f"ID:{uid}"
    except: 
        return f"ID:{uid}"

async def notify_boss(worker_uid, store, total, cash, txn_type, date_str):
    try:
        w_name = get_worker_name(worker_uid)
        time_str = date_str.split()[1] if ' ' in date_str else ""
        msg = ""
        if txn_type in ['savdo', 'savdo_yangi']:
            debt = total - cash
            msg = (f"🔔 **Yangi savdo!**\n"
                   f"👤 Ishchi: {w_name}\n"
                   f"🏪 Do'kon: `{store}`\n"
                   f"💰 Savdo: {fmt(total)}\n"
                   f"💵 Naqt: {fmt(cash)}\n"
                   f"📉 Qarz: {fmt(debt)}\n"
                   f"📅 Sana: {date_str.split()[0] if ' ' in date_str else date_str}")
        elif txn_type == 'naqt':
            msg = (f"💵 **Naqt kiritildi!**\n"
                   f"👤 Ishchi: {w_name}\n"
                   f"🏪 {store} | 💵 {fmt(cash)} | 🕒 {time_str}\n"
                   f"📅 {date_str.split()[0] if ' ' in date_str else date_str}")
        elif txn_type == 'qaytarish':
            msg = (f"🔄 **Qaytarildi!**\n"
                   f"👤 Ishchi: {w_name}\n"
                   f"🏪 {store} | 🔄 {fmt(abs(total))} | 🕒 {time_str}\n"
                   f"📅 {date_str.split()[0] if ' ' in date_str else date_str}")
        
        if msg:
            for bid in BOSS_IDS:
                try: await bot.send_message(bid, msg, parse_mode="Markdown")
                except: pass
    except Exception as e: 
        print(f"Notify error: {e}")

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

# ================= BAZA =================
def init_db():
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE users ALTER COLUMN user_id TYPE BIGINT")
        cur.execute("ALTER TABLE sales ALTER COLUMN worker_id TYPE BIGINT")
        cur.execute("ALTER TABLE deletion_requests ALTER COLUMN worker_id TYPE BIGINT")
        conn.commit()
    except:
        pass
    
    cur.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY, name TEXT, role TEXT DEFAULT 'worker', active INTEGER DEFAULT 1)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS sales (
        id SERIAL PRIMARY KEY, store_name TEXT, normalized_store TEXT,
        total REAL, cash REAL, debt REAL, txn_type TEXT, date TEXT, worker_id BIGINT, worker_name TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS stores_info (
        normalized_store TEXT PRIMARY KEY, owner_name TEXT, phone TEXT, location TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS deletion_requests (
        id SERIAL PRIMARY KEY, worker_id BIGINT, sale_id INTEGER, status TEXT DEFAULT 'pending', request_date TEXT)''')
    conn.commit()
    conn.close()

def get_db():
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        raise Exception("DATABASE_URL topilmadi!")
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def dict_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

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
        [KeyboardButton(text="👥 Ishchilar"), KeyboardButton(text="🤖 AI Yordam")],
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
    
    cur.execute("""SELECT u.name as worker_name, s.store_name, COALESCE(SUM(s.cash),0) as cash 
                   FROM sales s JOIN users u ON s.worker_id = u.user_id
                   WHERE s.date LIKE %s AND s.cash > 0 
                   GROUP BY u.name, s.store_name 
                   ORDER BY u.name, s.store_name""", (f"{today}%",))
    rows = cur.fetchall()
    
    cur.execute("""SELECT u.name as worker_name, COALESCE(SUM(s.cash),0) as total 
                   FROM sales s JOIN users u ON s.worker_id = u.user_id
                   WHERE s.date LIKE %s AND s.cash > 0 
                   GROUP BY u.name""", (f"{today}%",))
    worker_totals = {r['worker_name']: r['total'] for r in cur.fetchall()}
    
    cur.execute("""SELECT COALESCE(SUM(s.cash),0) as grand_total
                   FROM sales s
                   WHERE s.date LIKE %s AND s.cash > 0""", (f"{today}%",))
    grand_total = cur.fetchone()['grand_total']
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
    await state.clear()
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
    try:
        data = await state.get_data()
        worker_id = data.get('worker_id')
        if not worker_id: raise ValueError("ID topilmadi")
        
        conn = get_db(); cur = conn.cursor()
        cur.execute("""INSERT INTO users (user_id, name, role) VALUES (%s, %s, 'worker')
                       ON CONFLICT (user_id) DO UPDATE SET name = EXCLUDED.name""", (worker_id, message.text.strip()))
        conn.commit(); conn.close()
        
        await message.answer(f"✅ Ishchi qo'shildi: {message.text}")
        await state.clear()
        await boss_workers_list(message, state)
    except Exception as e:
        await message.answer(f"❌ Xatolik: {e}")
        await state.clear()

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

# ================= AI YORDAM (TUZATILGAN - Menyular bosilsa ishlash uchun) =================
def get_ai_questions_keyboard(is_boss=True):
    if is_boss:
        questions = [
            ["📊 Bugungi kassa qancha?", "💰 Bu oy qancha savdo?"],
            ["🏆 Eng yaxshi ishchi kim?", "💸 Eng ko'p qarzidor do'kon?"],
            ["⏰ Ishlamaydigan do'konlar?", "📉 Qaysi ishchi eng ko'p qarz?"],
            ["📅 Oylik hisobot", "📈 Umumiy statistika"]
        ]
    else:
        questions = [
            ["📊 Bugun qancha savdo?", "💰 Bu oy qancha yig'dim?"],
            ["💳 Qarzi bor do'konlarim?", "🏆 Eng yaxshi do'konim?"],
            ["📊 O'tgan oy vs Bu oy", "📅 Oylik hisobotim"],
            ["📈 Umumiy statistikam"]
        ]
    
    keyboard = []
    for i, row in enumerate(questions):
        keyboard.append([
            InlineKeyboardButton(text=q, callback_data=f"ai_q_{i}_{j}")
            for j, q in enumerate(row)
        ])
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="ai_back_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

@dp.message(F.text == "🤖 AI Yordam")
async def ai_help_start(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    is_boss = uid in BOSS_IDS
    kb = get_ai_questions_keyboard(is_boss)
    text = "🤖 **AI Yordamchi**\n\n📊 Tezkor savollardan birini bosing yoki o'zingiz yozing."
    await message.answer(text, reply_markup=kb, parse_mode="Markdown")
    await state.set_state(AppStates.ai_chat)

@dp.callback_query(F.data.startswith("ai_q_"))
async def handle_ai_question(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    parts = callback.data.split("_")
    row_idx, col_idx = int(parts[2]), int(parts[3])
    uid = callback.from_user.id
    is_boss = uid in BOSS_IDS
    
    if is_boss:
        questions = [["Bugungi kassa qancha?", "Bu oy qancha savdo?"], ["Eng yaxshi ishchi kim?", "Eng ko'p qarzidor do'kon?"], ["Ishlamaydigan do'konlar?", "Qaysi ishchi eng ko'p qarz?"], ["Oylik hisobot", "Umumiy statistika"]]
    else:
        questions = [["Bugun qancha savdo?", "Bu oy qancha yig'dim?"], ["Qarzi bor do'konlarim?", "Eng yaxshi do'konim?"], ["O'tgan oy vs Bu oy", "Oylik hisobotim"], ["Umumiy statistikam"]]
    
    await process_ai_question(callback.message, questions[row_idx][col_idx], state, is_boss)

@dp.callback_query(F.data == "ai_back_main")
async def ai_back_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    uid = callback.from_user.id
    if uid in BOSS_IDS:
        await callback.message.answer("Xush kelibsiz, Boss 👑", reply_markup=get_boss_menu())
    else:
        await callback.message.answer("⬅️ Bosh menyu", reply_markup=get_worker_menu())

# ✅ ENG MUHIM TUZATISH: AI ichida bo'lsangiz ham menyu tugmalari ishlashi uchun
@dp.message(AppStates.ai_chat)
async def ai_handle_chat(message: types.Message, state: FSMContext):
    text = message.text.strip()
    uid = message.from_user.id
    
    # 1. Orqaga tugmasi
    if text == "⬅️ Orqaga":
        await state.clear()
        return await start_cmd(message, state)

    # 2. Menyularni tekshirish (AI ichida bosilsa ham shu yerda ushlab qolamiz)
    if text == "✍️ Savdo qo'shish":
        await state.clear()
        return await trade_init(message, state)
    if text == "📊 Kunlik kassa":
        await state.clear()
        return await daily_cash(message)
    if text == "🔍 Do'kon qidirish":
        await state.clear()
        return await search_prompt(message, state)
    if text == "🤝 Qarzi borlar":
        await state.clear()
        return await handle_debtors(message)
    if text == "🏪 Do'konlarim":
        await state.clear()
        return await stores_list_cmd(message)
    if text == "📅 Oylik kassa":
        await state.clear()
        return await handle_monthly_cash(message)
    if text == "📅 Oylik hisobot":
        await state.clear()
        return await monthly_report(message)
    if text == "💰 Oylik maosh":
        await state.clear()
        return await calculate_salary(message)
    if text == "💰 Kassa (Live)":
        await state.clear()
        return await boss_kassa_live(message)
    if text == "👥 Ishchilar":
        await state.clear()
        return await boss_workers_list(message, state)
    if text == "📅 Oylik arxiv":
        await state.clear()
        return await boss_monthly_archive(message)
    if text == "🏪 Barcha do'konlar":
        await state.clear()
        return await boss_all_stores(message)
    if text == "📊 Eng yaxshi ishchilar":
        await state.clear()
        return await boss_top_workers(message)
    if text == "🏆 Eng yaxshi do'konlar":
        await state.clear()
        return await boss_top_stores(message)
    if text == "🤖 AI Yordam":
        await state.clear()
        return await ai_help_start(message, state)
        
    # 3. Agar menyu tugmasi bo'lmasa, AI ga yuborish
    await process_ai_question(message, text, state, uid in BOSS_IDS)

async def process_ai_question(message: types.Message, question: str, state: FSMContext, is_boss: bool):
    uid = message.from_user.id
    msg = await message.answer("🔍 Ma'lumotlar tahlil qilinmoqda...")
    
    try:
        conn = get_db(); cur = dict_cursor(conn)
        today = date.today().strftime("%d.%m.%Y")
        current_month = datetime.now().strftime("%m.%Y")
        last_month = (datetime.now() - timedelta(days=30)).strftime("%m.%Y")
        
        q_lower = question.lower()
        answer = ""
        
        if "bugungi kassa" in q_lower or "bugun qancha savdo" in q_lower:
            if is_boss:
                cur.execute("SELECT COUNT(id) as count, COALESCE(SUM(cash),0) as cash, COALESCE(SUM(total),0) as total FROM sales WHERE date LIKE %s", (f"{today}%",))
            else:
                cur.execute("SELECT COUNT(id) as count, COALESCE(SUM(cash),0) as cash, COALESCE(SUM(total),0) as total FROM sales WHERE date LIKE %s AND worker_id = %s", (f"{today}%", uid))
            r = cur.fetchone()
            answer = f"📅 **Bugungi natijalar**\n💵 Kassa: {fmt(r['cash'])}\n💰 Savdo: {fmt(r['total'])}\n📝 Soni: {r['count']} ta"
        
        elif "bu oy" in q_lower and "qancha" in q_lower:
            if is_boss:
                cur.execute("SELECT COUNT(id) as count, COALESCE(SUM(cash),0) as cash, COALESCE(SUM(total),0) as total FROM sales WHERE date LIKE %s", (f"%{current_month}%",))
            else:
                cur.execute("SELECT COUNT(id) as count, COALESCE(SUM(cash),0) as cash, COALESCE(SUM(total),0) as total FROM sales WHERE date LIKE %s AND worker_id = %s", (f"%{current_month}%", uid))
            r = cur.fetchone()
            answer = f"📅 **Bu oy ({current_month})**\n💵 Yig'ilgan: {fmt(r['cash'])}\n💰 Savdo: {fmt(r['total'])}\n📝 Soni: {r['count']} ta"
            
        elif "eng yaxshi ishchi" in q_lower and is_boss:
            cur.execute("SELECT u.name, COALESCE(SUM(s.total),0) as total_sales FROM sales s JOIN users u ON s.worker_id = u.user_id GROUP BY u.user_id, u.name ORDER BY total_sales DESC LIMIT 1")
            r = cur.fetchone()
            answer = f"🏆 **Eng yaxshi ishchi**\n👤 {r['name'] if r else 'Topilmadi'}\n💰 {fmt(r['total_sales']) if r else 0}"
            
        elif "qarzidor do'kon" in q_lower or "ko'p qarz" in q_lower:
            if is_boss:
                cur.execute("SELECT normalized_store, COALESCE(SUM(total)-SUM(cash),0) as debt FROM sales WHERE normalized_store IS NOT NULL GROUP BY normalized_store HAVING COALESCE(SUM(total)-SUM(cash),0) > 0 ORDER BY debt DESC LIMIT 1")
            else:
                cur.execute("SELECT normalized_store, COALESCE(SUM(total)-SUM(cash),0) as debt FROM sales WHERE worker_id = %s AND normalized_store IS NOT NULL GROUP BY normalized_store HAVING COALESCE(SUM(total)-SUM(cash),0) > 0 ORDER BY debt DESC LIMIT 1", (uid,))
            r = cur.fetchone()
            answer = f"💳 **Eng ko'p qarzidor**\n🏪 {r['normalized_store'] if r else 'Yoq'}\n📉 Qarz: {fmt(r['debt']) if r else 0}"
            
        elif "ishlamay" in q_lower and is_boss:
            cur.execute("SELECT normalized_store, MAX(date) as last_sale FROM sales WHERE normalized_store IS NOT NULL GROUP BY normalized_store HAVING MAX(date::date) < CURRENT_DATE - INTERVAL '30 days' ORDER BY last_sale ASC LIMIT 3")
            res = cur.fetchall()
            answer = "⏰ **Faol bo'lmaganlar:**\n" + "\n".join([f"{i+1}. {r['normalized_store']} (oxirgi: {r['last_sale']})" for i, r in enumerate(res)]) if res else "✅ Barchasi faol!"
            
        elif "qarzi bor do'kon" in q_lower and not is_boss:
            cur.execute("SELECT normalized_store, COALESCE(SUM(total)-SUM(cash),0) as debt FROM sales WHERE worker_id = %s AND normalized_store IS NOT NULL GROUP BY normalized_store HAVING COALESCE(SUM(total)-SUM(cash),0) > 0 ORDER BY debt DESC", (uid,))
            res = cur.fetchall()
            answer = "💳 **Qarzlaringiz:**\n" + "\n".join([f"{i+1}. {r['normalized_store']} - {fmt(r['debt'])}" for i, r in enumerate(res)]) if res else "✅ Qarz yo'q!"
            
        elif "o'tgan oy" in q_lower and "bu oy" in q_lower and not is_boss:
            cur.execute("SELECT COUNT(id), COALESCE(SUM(total),0), COALESCE(SUM(cash),0), COALESCE(SUM(total)-SUM(cash),0) FROM sales WHERE date LIKE %s AND worker_id = %s", (f"%{current_month}%", uid))
            this = cur.fetchone()
            cur.execute("SELECT COUNT(id), COALESCE(SUM(total),0), COALESCE(SUM(cash),0), COALESCE(SUM(total)-SUM(cash),0) FROM sales WHERE date LIKE %s AND worker_id = %s", (f"%{last_month}%", uid))
            last = cur.fetchone()
            answer = f"📊 **SOLISHTIRISH**\n📅 O'tgan oy: 💰{fmt(last[1])} 💵{fmt(last[2])} 📉{fmt(last[3])}\n📅 Bu oy: 💰{fmt(this[1])} 💵{fmt(this[2])} 📉{fmt(this[3])}"
            
        elif "statistika" in q_lower or "umumiy" in q_lower:
            if is_boss:
                cur.execute("SELECT COUNT(DISTINCT worker_id), COUNT(DISTINCT normalized_store), COUNT(id), COALESCE(SUM(total),0), COALESCE(SUM(cash),0), COALESCE(SUM(total)-SUM(cash),0) FROM sales")
            else:
                cur.execute("SELECT COUNT(DISTINCT normalized_store), COUNT(id), COALESCE(SUM(total),0), COALESCE(SUM(cash),0), COALESCE(SUM(total)-SUM(cash),0) FROM sales WHERE worker_id = %s", (uid,))
            r = cur.fetchone()
            prefix = "📊 **UMUMIY**\n" if is_boss else "📊 **SIZNING**\n"
            stats = f"🏪 Do'konlar: {r[0]} ta\n📝 Savdolar: {r[1]} ta\n💰 Hajm: {fmt(r[2])}\n💵 Naqt: {fmt(r[3])}\n📉 Qarz: {fmt(r[4])}"
            answer = prefix + stats
            
        elif "hisobot" in q_lower:
            if is_boss:
                cur.execute("SELECT COUNT(id), COALESCE(SUM(total),0), COALESCE(SUM(cash),0), COALESCE(SUM(total)-SUM(cash),0) FROM sales WHERE date LIKE %s", (f"%{current_month}%",))
            else:
                cur.execute("SELECT COUNT(id), COALESCE(SUM(total),0), COALESCE(SUM(cash),0), COALESCE(SUM(total)-SUM(cash),0) FROM sales WHERE date LIKE %s AND worker_id = %s", (f"%{current_month}%", uid))
            r = cur.fetchone()
            answer = f"📅 **Oylik hisobot**\n📝 {r[0]} ta | 💰 {fmt(r[1])} | 💵 {fmt(r[2])} | 📉 {fmt(r[3])}"
            
        elif "eng yaxshi do'kon" in q_lower:
            if is_boss:
                cur.execute("SELECT normalized_store, COALESCE(SUM(total),0) as t FROM sales WHERE normalized_store IS NOT NULL GROUP BY normalized_store ORDER BY t DESC LIMIT 1")
            else:
                cur.execute("SELECT normalized_store, COALESCE(SUM(total),0) as t FROM sales WHERE worker_id = %s AND normalized_store IS NOT NULL GROUP BY normalized_store ORDER BY t DESC LIMIT 1", (uid,))
            r = cur.fetchone()
            answer = f"🏆 **Eng yaxshi do'kon**\n🏪 {r['normalized_store'] if r else 'Yoq'}\n💰 {fmt(r['t']) if r else 0}"
            
        elif "ishchi eng ko'p qarz" in q_lower and is_boss:
            cur.execute("SELECT u.name, COALESCE(SUM(s.total)-SUM(s.cash),0) as d FROM sales s JOIN users u ON s.worker_id = u.user_id GROUP BY u.user_id, u.name HAVING COALESCE(SUM(s.total)-SUM(s.cash),0) > 0 ORDER BY d DESC LIMIT 3")
            res = cur.fetchall()
            answer = "💰 **Qarz yig'ganlar:**\n" + "\n".join([f"{i+1}. {r['name']} - {fmt(r['d'])}" for i, r in enumerate(res)]) if res else "✅ Qarz yo'q!"
            
        else:
            if is_boss:
                cur.execute("SELECT COUNT(id), COALESCE(SUM(total),0), COALESCE(SUM(cash),0), COALESCE(SUM(total)-SUM(cash),0) FROM sales")
            else:
                cur.execute("SELECT COUNT(id), COALESCE(SUM(total),0), COALESCE(SUM(cash),0), COALESCE(SUM(total)-SUM(cash),0) FROM sales WHERE worker_id = %s", (uid,))
            s = cur.fetchone()
            ctx = f"Savdolar: {s[0]}, Hajm: {fmt(s[1])}, Naqt: {fmt(s[2])}, Qarz: {fmt(s[3])}"
            try:
                res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"system","content":f"MK Kassa yordamchisisiz. Ma'lumot: {ctx}. Qisqa javob bering."},{"role":"user","content":question}], max_tokens=150)
                answer = res.choices[0].message.content
            except:
                answer = "❓ AI hozircha band. Boshqa savol bering."
        
        conn.close()
        await msg.edit_text(answer, parse_mode="Markdown")
        await message.answer("❓ **Boshqa savol:**", reply_markup=get_ai_questions_keyboard(is_boss))
    except Exception as e:
        print(f"AI error: {e}")
        await msg.edit_text("❌ Ma'lumot yuklanmadi.")

# ================= BOSS ARXIV VA DO'KONLAR =================
@dp.message(F.text == "📅 Oylik arxiv")
async def boss_monthly_archive(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT user_id, name FROM users WHERE role = 'worker' AND active = 1")
    workers = cur.fetchall()
    curr = datetime.now().strftime("%m.%Y")
    out = f"📅 **Oylik arxiv ({curr})**\n\n"
    for w in workers:
        cur.execute("SELECT COALESCE(SUM(total),0), COALESCE(SUM(cash),0), COALESCE(SUM(total)-SUM(cash),0) FROM sales WHERE worker_id = %s AND date LIKE %s", (w['user_id'], f"%{curr}%"))
        r = cur.fetchone()
        out += f"👤 {w['name']}\n💰 {fmt(r[0])} | 💵 {fmt(r[1])} | 📉 {fmt(r[2])}\n\n"
    conn.close()
    await message.answer(out)

@dp.message(F.text == "🏪 Barcha do'konlar")
async def boss_all_stores(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT DISTINCT s.normalized_store, u.name FROM sales s JOIN users u ON s.worker_id = u.user_id ORDER BY u.name")
    stores = cur.fetchall(); conn.close()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🏪 {s[0]} (👤 {s[1]})", callback_data=f"store_{s[0]}")] for s in stores])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    await message.answer("🏪 Barcha do'konlar:", reply_markup=kb)

@dp.message(F.text == "📊 Eng yaxshi ishchilar")
async def boss_top_workers(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT u.name, COALESCE(SUM(s.total),0), COALESCE(SUM(s.cash),0), COALESCE(SUM(s.total)-SUM(s.cash),0), COUNT(s.id) FROM sales s JOIN users u ON s.worker_id = u.user_id GROUP BY u.name ORDER BY 2 DESC")
    res = cur.fetchall(); conn.close()
    out = "🏆 **Reyting:**\n" + "\n".join([f"{i+1}. {r[0]} | 💰{fmt(r[1])} | 💵{fmt(r[2])} | 📉{fmt(r[3])} | 🧾{r[4]}" for i, r in enumerate(res)])
    await message.answer(out if out.strip() != "🏆 Reyting:\n" else "📊 Ma'lumot yo'q")

@dp.message(F.text == "🏆 Eng yaxshi do'konlar")
async def boss_top_stores(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT normalized_store, COALESCE(SUM(total),0), COUNT(id) FROM sales GROUP BY normalized_store ORDER BY 2 DESC LIMIT 10")
    res = cur.fetchall(); conn.close()
    out = "🏆 **TOP Do'konlar:**\n" + "\n".join([f"{i+1}. 🏪 {r[0]} | 💰 {fmt(r[1])} | 🧾 {r[2]}" for i, r in enumerate(res)])
    await message.answer(out if out.strip() != "🏆 TOP Do'konlar:\n" else "📊 Ma'lumot yo'q")

# ================= OYLIK KASSA =================
@dp.message(F.text == "📅 Oylik kassa")
async def handle_monthly_cash(message: types.Message):
    uid = message.from_user.id
    month = datetime.now().strftime("%m.%Y")
    if uid not in BOSS_IDS:
        conn = get_db(); cur = dict_cursor(conn)
        cur.execute("SELECT SUBSTR(date, 1, 10) as d, COALESCE(SUM(cash),0) FROM sales WHERE worker_id = %s AND date LIKE %s AND cash > 0 GROUP BY d ORDER BY d DESC", (uid, f"%{month}%"))
        rows = cur.fetchall(); conn.close()
        if not rows: return await message.answer(f"📅 {month}: Ma'lumot yo'q.")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📆 {r[0]} | {fmt(r[1])}", callback_data=f"day_{r[0]}")] for r in rows])
        kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
        return await message.answer(f"📅 {month} kassa:", reply_markup=kb)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Kunlik Umumiy", callback_data=f"mc_all_{month}")],
        [InlineKeyboardButton(text="👥 Ishchi bo'yicha", callback_data=f"mc_worker_{month}")]
    ])
    await message.answer(f"📅 Oylik kassa ({month}):", reply_markup=kb)

@dp.callback_query(F.data.startswith("mc_all_"))
async def mc_all_dates(callback: CallbackQuery):
    await callback.answer()
    month = callback.data.replace("mc_all_", "")
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT DISTINCT SUBSTR(date, 1, 10) as d FROM sales WHERE date LIKE %s ORDER BY d DESC", (f"%{month}%",))
    dates = [r['d'] for r in cur.fetchall()]; conn.close()
    if not dates: return await callback.message.edit_text("📭 Yo'q.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📆 {d}", callback_data=f"day_all_{d}")] for d in dates])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_main")])
    await callback.message.edit_text(f"📅 {month} kunlari:", reply_markup=kb)

@dp.callback_query(F.data.startswith("day_all_"))
async def day_all_summary(callback: CallbackQuery):
    await callback.answer()
    day = callback.data.replace("day_all_", "")
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT u.name, s.store_name, COALESCE(SUM(s.cash),0) FROM sales s JOIN users u ON s.worker_id = u.user_id WHERE s.date LIKE %s AND s.cash > 0 GROUP BY u.name, s.store_name", (f"{day}%",))
    rows = cur.fetchall()
    cur.execute("SELECT u.name, COALESCE(SUM(s.cash),0) FROM sales s JOIN users u ON s.worker_id = u.user_id WHERE s.date LIKE %s AND s.cash > 0 GROUP BY u.name", (f"{day}%",))
    totals = {r[0]: r[1] for r in cur.fetchall()}; conn.close()
    out, cur_w = f"💰 {day}\n", ""
    for r in rows:
        if r[0] != cur_w:
            if cur_w: out += f"👤 {cur_w} - Jami: {fmt(totals.get(cur_w, 0))}\n\n"
            cur_w = r[0]; out += f"👤 **{cur_w}**:\n"
        out += f"  🏪 {r[1]} - {fmt(r[2])}\n"
    out += f"👤 {cur_w} - Jami: {fmt(totals.get(cur_w, 0))}"
    await callback.message.edit_text(out, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Sanalar", callback_data="back_main")]]))

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
    if not dates: return await callback.message.edit_text("📭 Yo'q.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📆 {d}", callback_data=f"day_worker_{uid}_{d}")] for d in dates])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Ishchilar", callback_data=f"mc_worker_{datetime.now().strftime('%m.%Y')}")])
    await callback.message.edit_text(f"👤 {w_name} sanalari:", reply_markup=kb)

@dp.callback_query(F.data.startswith("day_worker_"))
async def day_worker_summary(callback: CallbackQuery):
    await callback.answer()
    uid, day = callback.data.replace("day_worker_", "").split("_")
    uid = int(uid)
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT name FROM users WHERE user_id=%s", (uid,)); w_name = cur.fetchone()['name']
    cur.execute("SELECT store_name, COALESCE(SUM(cash),0) FROM sales WHERE worker_id=%s AND date LIKE %s AND cash>0 GROUP BY store_name", (uid, f"{day}%"))
    rows = cur.fetchall(); conn.close()
    out, total = f"💰 {w_name} - {day}\n", 0
    for r in rows: out += f"🏪 {r[0]} | 💵 {fmt(r[1])}\n"; total += r[1]
    out += f"\n💰 Jami: {fmt(total)}"
    await callback.message.edit_text(out, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Sanalar", callback_data=f"sel_worker_{uid}")]]))

@dp.message(F.text == "💰 Oylik maosh")
async def calculate_salary(message: types.Message):
    uid = message.from_user.id
    month = datetime.now().strftime("%m.%Y")
    if uid in BOSS_IDS:
        conn = get_db(); cur = dict_cursor(conn)
        cur.execute("SELECT user_id, name FROM users WHERE role = 'worker' AND active = 1")
        workers = cur.fetchall(); out, grand = f"💰 Maosh ({month}):\n\n", 0
        for w in workers:
            cur.execute("SELECT COALESCE(SUM(cash),0) FROM sales WHERE worker_id = %s AND date LIKE %s AND cash > 0", (w['user_id'], f"%{month}%"))
            tc = cur.fetchone()[0]; pct, fix = tc * 0.08, (150 if 1500<=tc<2000 else (200 if 2000<=tc<3000 else (300 if tc>=3000 else 0)))
            sal = pct + fix; grand += sal
            out += f"👥 {w['name']}\n💵 {fmt(tc)} | 📈 {fmt(pct)} | 🎁 {fmt(fix)} | ✅ {fmt(sal)}\n\n"
        out += f"💰 JAMI: {fmt(grand)}"; conn.close()
        return await message.answer(out)
    w_cond, w_params = get_worker_filter(uid)
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT COALESCE(SUM(cash),0) FROM sales WHERE date LIKE %s AND cash > 0 " + w_cond, (f"%{month}%",) + w_params)
    tc = cur.fetchone()[0]; conn.close()
    pct, fix = tc * 0.08, (150 if 1500<=tc<2000 else (200 if 2000<=tc<3000 else (300 if tc>=3000 else 0)))
    await message.answer(f"💰 Maosh ({month}):\n💵 {fmt(tc)} | 📈 {fmt(pct)} | 🎁 {fmt(fix)} | ✅ {fmt(pct+fix)}")

# ================= ISHCHI FUNKSIYALARI =================
@dp.message(F.text == "📊 Kunlik kassa")
async def daily_cash(message: types.Message):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    today = date.today().strftime("%d.%m.%Y")
    w_cond, w_params = get_worker_filter(uid)
    params = (f"{today}%",) + w_params
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT store_name, cash, date FROM sales WHERE date LIKE %s AND cash > 0 " + w_cond + " ORDER BY id DESC", params)
    rows = cur.fetchall()
    cur.execute("SELECT COALESCE(SUM(cash),0) FROM sales WHERE date LIKE %s AND cash > 0 " + w_cond, params)
    total_cash = cur.fetchone()[0]; conn.close()
    if not rows: return await message.answer("📅 Bugun naqt yo'q.")
    out = f"📅 Bugun ({today}):\n" + "\n".join([f"🏪 {r['store_name']} | 💵 {fmt(r['cash'])}" for r in rows])
    out += f"\n💰 Jami: {fmt(total_cash)}"
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
    cur.execute("SELECT store_name, total, cash, txn_type, date FROM sales WHERE date LIKE %s AND cash > 0 " + w_cond + " ORDER BY id DESC", params)
    rows = cur.fetchall(); conn.close()
    if not rows: return await callback.message.edit_text("📭 Yo'q.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️", callback_data="back_main")]]))
    out = f"📅 {day} batafsil:\n" + "\n".join([f"🏪 {r['store_name']} | 📦 {fmt(r['total'])} | 💵 {fmt(r['cash'])}" for r in rows])
    await callback.message.edit_text(out, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️", callback_data="back_main")]]))

@dp.message(F.text == "📅 Oylik hisobot")
async def monthly_report(message: types.Message):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    w_cond, w_params = get_worker_filter(uid)
    curr = datetime.now().strftime("%m.%Y")
    params_all = w_params
    params_curr = (f"%{curr}%",) + w_params
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT COALESCE(SUM(total),0), COALESCE(SUM(cash),0) FROM sales WHERE 1=1 " + w_cond, params_all)
    t_all = cur.fetchone()
    cur.execute("SELECT COALESCE(SUM(total),0), COALESCE(SUM(cash),0), COALESCE(SUM(total)-SUM(cash),0) FROM sales WHERE date LIKE %s " + w_cond, params_curr)
    t_curr = cur.fetchone(); conn.close()
    old_debt = max(0, ((t_all[0] or 0) - (t_all[1] or 0)) - (t_curr[2] or 0))
    await message.answer(f"📅 Hisobot ({curr}):\n📉 O'tgan: {fmt(old_debt)}\n💰 Bu oy: {fmt(t_curr[0])}\n💵 Naqt: {fmt(t_curr[1])}\n📉 Yangi: {fmt(t_curr[2])}\n✅ Joriy: {fmt(old_debt + (t_curr[2] or 0))}")

# ================= QARZI BORLAR =================
@dp.message(F.text == "🤝 Qarzi borlar")
async def handle_debtors(message: types.Message):
    uid = message.from_user.id
    conn = get_db(); cur = dict_cursor(conn)
    if uid in BOSS_IDS:
        cur.execute("""SELECT u.user_id, u.name, ROUND(CAST(COALESCE(SUM(s.total),0) - COALESCE(SUM(s.cash),0) AS numeric), 2) as bal 
                       FROM sales s JOIN users u ON s.worker_id = u.user_id WHERE s.normalized_store IS NOT NULL 
                       GROUP BY u.user_id, u.name HAVING ROUND(CAST(COALESCE(SUM(s.total),0) - COALESCE(SUM(s.cash),0) AS numeric), 2) > 0 ORDER BY bal DESC""")
        res = cur.fetchall()
        if not res: conn.close(); return await message.answer("✅ Qarz yo'q.")
        out = "🤝 Qarzi borlar:\n" + "\n".join([f"👤 {r[1]} - {fmt(r[2])}" for r in res])
        out += f"\n💰 Umumiy: {fmt(sum(r[2] for r in res))}"
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"👤 {r[1]}", callback_data=f"boss_debt_uid_{r[0]}")] for r in res])
        await message.answer(out, reply_markup=kb)
    else:
        cur.execute("SELECT normalized_store, ROUND(CAST(COALESCE(SUM(total),0)-COALESCE(SUM(cash),0) AS numeric), 2) as bal FROM sales WHERE worker_id = %s GROUP BY normalized_store HAVING bal > 0 ORDER BY bal DESC", (uid,))
        res = cur.fetchall()
        if not res: conn.close(); return await message.answer("✅ Qarz yo'q.")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📉 {r[0]} ({fmt(r[1])})", callback_data=f"store_{r[0]}")] for r in res])
        await message.answer("🤝 Qarzi bor do'konlar:", reply_markup=kb)
    conn.close()

@dp.callback_query(F.data.startswith("boss_debt_uid_"))
async def boss_debt_detail(callback: CallbackQuery):
    await callback.answer()
    try:
        wid = int(callback.data.replace("boss_debt_uid_", ""))
        conn = get_db(); cur = dict_cursor(conn)
        cur.execute("SELECT name FROM users WHERE user_id = %s", (wid,))
        w_name = (cur.fetchone() or {}).get('name', f"ID:{wid}")
        cur.execute("""SELECT normalized_store, ROUND(CAST(COALESCE(SUM(total),0) AS numeric), 2) as t, ROUND(CAST(COALESCE(SUM(cash),0) AS numeric), 2) as c 
                       FROM sales WHERE worker_id = %s AND normalized_store IS NOT NULL GROUP BY normalized_store HAVING t > c ORDER BY (t-c) DESC""", (wid,))
        stores = cur.fetchall(); conn.close()
        out = f"👤 {w_name} qarzlari:\n" + ("\n".join([f"🏪 {s['normalized_store']} | {fmt(s['t']-s['c'])}" for s in stores]) or "✅ Yo'q")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📖 {s['normalized_store']}", callback_data=f"boss_debt_store_{wid}_{s['normalized_store']}")] for s in stores])
        kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️", callback_data="back_main")])
        await callback.message.edit_text(out, reply_markup=kb)
    except Exception as e:
        print(f"Boss debt error: {e}")
        await callback.answer("❌ Xatolik", show_alert=True)

@dp.callback_query(F.data.startswith("boss_debt_store_"))
async def boss_debt_store_view(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    wid, store = callback.data.replace("boss_debt_store_", "").split("_", 1)
    wid = int(wid)
    await state.update_data(debt_worker_id=wid, current_store=store)
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT COALESCE(SUM(total),0), COALESCE(SUM(cash),0) FROM sales WHERE normalized_store = %s AND worker_id = %s", (store, wid))
    res = cur.fetchone()
    cur.execute("SELECT txn_type, total, cash, date FROM sales WHERE normalized_store = %s AND worker_id = %s ORDER BY id DESC LIMIT 10", (store, wid))
    hist = cur.fetchall(); conn.close()
    out = f"🏪 **{store.upper()}**\n💰 {fmt(res[0])} | 💵 {fmt(res[1])} | 📉 {fmt(res[0]-res[1])}\n\n📜:\n"
    out += "\n".join([f"📅 {h['date']} | {'📦' if h['txn_type']=='savdo' else '💵' if h['txn_type']=='naqt' else '🔄'} {fmt(h['total'] if h['txn_type']!='qaytarish' else -h['total'])}" for h in hist])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Naqt", callback_data="act_cash"), InlineKeyboardButton(text="🔄 Qaytarish", callback_data="act_return")],
        [InlineKeyboardButton(text="💰 Savdo", callback_data="act_trade"), InlineKeyboardButton(text="👤 Do'konchi", callback_data="act_owner")],
        [InlineKeyboardButton(text="⬅️ Qarzdorlar", callback_data=f"boss_debt_uid_{wid}")]
    ])
    try: await callback.message.edit_text(out, reply_markup=kb, parse_mode="Markdown")
    except: await callback.message.answer(out, reply_markup=kb, parse_mode="Markdown")

# ================= DO'KONLAR VA HARAKATLAR =================
@dp.message(F.text == "🏪 Do'konlarim")
async def stores_list_cmd(message: types.Message): 
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT DISTINCT normalized_store FROM sales WHERE worker_id = %s ORDER BY normalized_store", (uid,))
    stores = cur.fetchall(); conn.close()
    if not stores: return await message.answer("🏪 Yo'q.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🏪 {s[0]}", callback_data=f"store_{s[0]}")] for s in stores])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️", callback_data="back_main")])
    await message.answer("🏪 Do'konlarim:", reply_markup=kb)

@dp.callback_query(F.data == "stores_list")
async def stores_list_cb(callback: CallbackQuery): 
    uid = callback.from_user.id
    if uid in BOSS_IDS: return
    w_cond, w_params = get_worker_filter(uid)
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT DISTINCT normalized_store FROM sales WHERE 1=1 " + w_cond + " ORDER BY normalized_store", w_params)
    stores = cur.fetchall(); conn.close()
    if not stores: return await callback.answer("🏪 Yo'q.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🏪 {s['normalized_store']}", callback_data=f"store_{s['normalized_store']}")] for s in stores])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️", callback_data="back_main")])
    await callback.message.edit_text("🏪 Do'konlar:", reply_markup=kb)

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
    cur.execute("SELECT DISTINCT normalized_store FROM sales WHERE normalized_store LIKE %s " + w_cond, params)
    res = cur.fetchall(); conn.close()
    if not res: return await message.answer("🔍 Topilmadi.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🏪 {r[0]}", callback_data=f"store_{r[0]}")] for r in res])
    await message.answer("🔍 Natijalar:", reply_markup=kb)
    await state.clear()

@dp.callback_query(F.data.startswith("store_"))
async def store_details(callback: CallbackQuery, state: FSMContext):
    store = callback.data[6:]
    if not store: return await callback.answer("⚠️ Xato", show_alert=True)
    await state.update_data(current_store=store)
    data = await state.get_data()
    uid = data.get('debt_worker_id', callback.from_user.id)
    if data.get('debt_worker_id'): await state.update_data(debt_worker_id=None)
    w_cond, w_params = get_worker_filter(uid)
    full_params = (store,) + w_params
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT COALESCE(SUM(total),0), COALESCE(SUM(cash),0) FROM sales WHERE normalized_store = %s " + w_cond, full_params)
    res = cur.fetchone()
    cur.execute("SELECT txn_type, total, cash, date FROM sales WHERE normalized_store = %s " + w_cond + " ORDER BY id DESC LIMIT 10", full_params)
    hist = cur.fetchall(); conn.close()
    out = f"🏪 **{store.upper()}**\n💰 {fmt(res[0])} | 💵 {fmt(res[1])} | 📉 {fmt(res[0]-res[1])}\n\n📜:\n"
    out += "\n".join([f"📅 {h['date']} | {'📦' if h['txn_type']=='savdo' else '💵' if h['txn_type']=='naqt' else '🔄'} {fmt(h['total'] if h['txn_type']!='qaytarish' else -h['total'])}" for h in hist])
    back_kb = [InlineKeyboardButton(text="⬅️ Qarzdorlar", callback_data=f"boss_debt_uid_{data.get('debt_worker_id')}")] if data.get('debt_worker_id') else [InlineKeyboardButton(text="⬅️", callback_data="back_main")]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Naqt", callback_data="act_cash"), InlineKeyboardButton(text="🔄 Qaytarish", callback_data="act_return")],
        [InlineKeyboardButton(text="💰 Savdo", callback_data="act_trade"), InlineKeyboardButton(text="👤 Do'konchi", callback_data="act_owner")]
    ] + [back_kb])
    await callback.message.edit_text(out, reply_markup=kb, parse_mode="Markdown")

@dp.callback_query(F.data == "act_cash")
async def start_cash(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    if uid in BOSS_IDS: return
    await callback.message.answer("💵 Summa (masalan: 5000 11.01.25):", reply_markup=get_back_kb())
    await state.set_state(AppStates.add_cash)

@dp.message(AppStates.add_cash)
async def process_cash(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    if message.text == "⬅️ Orqaga": await state.clear(); return await start_cmd(message, state)
    date_str, clean_text = extract_date(message.text)
    if not clean_text.replace('.','',1).isdigit(): return await message.answer("⚠️ Faqat raqam!")
    await handle_store_action(message, state, "naqt", float(clean_text), date_override=date_str or datetime.now().strftime("%d.%m.%Y %H:%M"))

@dp.callback_query(F.data == "act_return")
async def start_return(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    if uid in BOSS_IDS: return
    await callback.message.answer("🔄 Summa (masalan: 200 11.01.25):", reply_markup=get_back_kb())
    await state.set_state(AppStates.add_return)

@dp.message(AppStates.add_return)
async def process_return(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    if message.text == "⬅️ Orqaga": await state.clear(); return await start_cmd(message, state)
    date_str, clean_text = extract_date(message.text)
    if not clean_text.replace('.','',1).isdigit(): return await message.answer("⚠️ Faqat raqam!")
    await handle_store_action(message, state, "qaytarish", float(clean_text), date_override=date_str or datetime.now().strftime("%d.%m.%Y %H:%M"))

@dp.callback_query(F.data == "act_trade")
async def start_trade(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    if uid in BOSS_IDS: return
    await callback.message.answer("💰 Summa (masalan: 1000 11.01.25):", reply_markup=get_back_kb())
    await state.set_state(AppStates.add_new_sale_store)

@dp.message(AppStates.add_new_sale_store)
async def process_new_sale_store(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    if message.text == "⬅️ Orqaga": await state.clear(); return await start_cmd(message, state)
    data = await state.get_data()
    store = data.get('current_store')
    if not store: return await message.answer("⚠️ Do'kon tanlanmagan.")
    date_str, clean_text = extract_date(message.text)
    if not clean_text.replace('.','',1).isdigit(): return await message.answer("⚠️ Faqat raqam!")
    await handle_store_action(message, state, "savdo_yangi", float(clean_text), store=store, date_override=date_str or datetime.now().strftime("%d.%m.%Y %H:%M"))

async def handle_store_action(message: types.Message, state: FSMContext, t_type: str, amount: float, store: str = None, date_override: str = None):
    if not store:
        data = await state.get_data()
        store = data.get('current_store')
        if not store: return await message.answer("⚠️ Do'kon tanlanmagan.")
    now_str = date_override or datetime.now().strftime("%d.%m.%Y %H:%M")
    conn = get_db(); cur = dict_cursor(conn)
    if t_type == "naqt": vals = (store, store, 0, amount, -amount, 'naqt', now_str, message.from_user.id, message.from_user.full_name)
    elif t_type == "qaytarish": vals = (store, store, -amount, 0, -amount, 'qaytarish', now_str, message.from_user.id, message.from_user.full_name)
    else: vals = (store, store, amount, 0, amount, 'savdo', now_str, message.from_user.id, message.from_user.full_name)
    cur.execute("INSERT INTO sales (store_name, normalized_store, total, cash, debt, txn_type, date, worker_id, worker_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id", vals)
    sale_id = cur.fetchone()[0]; conn.commit(); conn.close()
    await notify_boss(message.from_user.id, store, amount if t_type!='qaytarish' else -amount, amount if t_type=='naqt' else 0, t_type, now_str)
    txt = f"✅ {fmt(amount)} qabul! ({now_str.split()[0]})" if t_type == "naqt" else (f"✅ {fmt(amount)} qaytarildi!" if t_type == "qaytarish" else f"✅ {fmt(amount)} savdo!")
    await message.answer(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Bekor", callback_data=f"cancel_req_{sale_id}")]]))
    await state.clear()
    if store: await send_store_details(message, store, state)

async def send_store_details(message: types.Message, store: str, state: FSMContext):
    await state.update_data(current_store=store)
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    w_cond, w_params = get_worker_filter(uid)
    full_params = (store,) + w_params
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT COALESCE(SUM(total),0), COALESCE(SUM(cash),0) FROM sales WHERE normalized_store = %s " + w_cond, full_params)
    res = cur.fetchone()
    cur.execute("SELECT txn_type, total, cash, date FROM sales WHERE normalized_store = %s " + w_cond + " ORDER BY id DESC LIMIT 10", full_params)
    hist = cur.fetchall(); conn.close()
    out = f"🏪 **{store.upper()}**\n💰 {fmt(res[0])} | 💵 {fmt(res[1])} | 📉 {fmt(res[0]-res[1])}\n\n📜:\n"
    out += "\n".join([f"📅 {h['date']} | {fmt(h['total'] if h['txn_type']!='qaytarish' else -h['total'])}" for h in hist])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Naqt", callback_data="act_cash"), InlineKeyboardButton(text="🔄 Qaytarish", callback_data="act_return")],
        [InlineKeyboardButton(text="💰 Savdo", callback_data="act_trade"), InlineKeyboardButton(text="👤 Do'konchi", callback_data="act_owner")],
        [InlineKeyboardButton(text="🔍 Boshqa", callback_data="stores_list")]
    ])
    await message.answer(out, reply_markup=kb, parse_mode="Markdown")

async def send_owner_info(target, store, state: FSMContext):
    msg = target.message if isinstance(target, CallbackQuery) else target
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT owner_name, phone, location FROM stores_info WHERE normalized_store = %s", (store,))
    info = cur.fetchone(); conn.close()
    out = f"👤 **{store}**\n📛 {info['owner_name'] if info else '⬜'}\n📞 {info['phone'] if info else '⬜'}\n📍 {info['location'] if info else '⬜'}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Ism", callback_data="edit_name"), InlineKeyboardButton(text="✏️ Tel", callback_data="edit_phone")],
        [InlineKeyboardButton(text="✏️ Manzil", callback_data="edit_loc"), InlineKeyboardButton(text="⬅️", callback_data=f"store_{store}")]
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
    await callback.message.answer(f"✍️ Yangi {field}:", reply_markup=get_back_kb())
    await state.set_state(AppStates.edit_store_info)

@dp.message(AppStates.edit_store_info)
async def save_owner_info(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    if message.text == "⬅️ Orqaga": await state.clear(); return await start_cmd(message, state)
    data = await state.get_data()
    store, field = data.get('current_store', ''), data.get('edit_field', 'name')
    col = {"name": "owner_name", "phone": "phone", "loc": "location"}.get(field, "owner_name")
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute(f"INSERT INTO stores_info (normalized_store, owner_name, phone, location) VALUES (%s, %s, %s, %s) ON CONFLICT (normalized_store) DO UPDATE SET {col} = EXCLUDED.{col}", 
                (store, message.text.strip() if col=='owner_name' else '', message.text.strip() if col=='phone' else '', message.text.strip() if col=='location' else ''))
    conn.commit(); conn.close()
    await message.answer("✅ Saqlandi!"); await state.clear()
    await send_owner_info(message, store, state)

@dp.message(Command("delete_store"))
async def delete_store_command(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    args = message.text.split()
    if len(args) < 2: return await message.answer("❌ /delete_store dokon")
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("DELETE FROM sales WHERE normalized_store = %s", (args[1],))
    cur.execute("DELETE FROM stores_info WHERE normalized_store = %s", (args[1],))
    conn.commit(); conn.close()
    await message.answer(f"✅ {args[1]} o'chirildi!")

@dp.message(Command("delete_worker"))
async def delete_worker_command(message: types.Message):
    if message.from_user.id not in BOSS_IDS: return
    args = message.text.split()
    if len(args) < 2: return await message.answer("❌ /delete_worker id")
    try:
        wid = int(args[1])
        if wid in BOSS_IDS: return await message.answer("⚠️ Boss o'chirilmaydi!")
        conn = get_db(); cur = dict_cursor(conn)
        cur.execute("SELECT name FROM users WHERE user_id = %s AND role = 'worker'", (wid,))
        if not cur.fetchone(): conn.close(); return await message.answer("⚠️ Topilmadi.")
        cur.execute("DELETE FROM deletion_requests WHERE worker_id = %s", (wid,))
        cur.execute("DELETE FROM sales WHERE worker_id = %s", (wid,))
        cur.execute("DELETE FROM users WHERE user_id = %s", (wid,))
        conn.commit(); conn.close()
        await message.answer(f"✅ {wid} tozalandi!", parse_mode="Markdown")
    except Exception as e: await message.answer(f"❌ {e}")

@dp.message(F.text == "✍️ Savdo qo'shish")
async def trade_init(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    await state.clear()
    await message.answer("📝 `Ali market 5000` yoki `Ali 5000 naxt 300 11.01.25`", reply_markup=get_back_kb())
    await state.set_state(AppStates.waiting_trade)

@dp.message(AppStates.waiting_trade)
async def handle_trade(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    if message.text == "⬅️ Orqaga": await state.clear(); return await start_cmd(message, state)
    msg = await message.answer("🤖 Tahlil...")
    try:
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[
            {"role": "system", "content": """Sen savdo tahlilchisisiz. Matndan store, total, cash, date ajrat.
Qoidalar: 1. "naxt/naqt/pul" yonidagi raqam cash. 2. Yagona raqam bo'lsa total=raqam, cash=0. 3. Sana: DD.MM.YY. 4. JSON: {"store":"str","total":num,"cash":num,"date":"DD.MM.YY" yoki null}"""},
            {"role": "user", "content": message.text}
        ], response_format={"type": "json_object"})
        d = json.loads(res.choices[0].message.content)
        store = normalize(d.get("store", "noma'lum"))
        total, cash = float(d.get("total", 0)), float(d.get("cash", 0))
        date_str = d.get("date") if d.get("date") and re.match(r'^\d{2}\.\d{2}\.\d{2,4}$', str(d.get("date"))) else datetime.now().strftime("%d.%m.%Y")
        now_str = f"{date_str} {datetime.now().strftime('%H:%M')}"
        conn = get_db(); cur = dict_cursor(conn)
        cur.execute("INSERT INTO sales (store_name, normalized_store, total, cash, debt, txn_type, date, worker_id, worker_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                    (store, store, total, cash, round(total-cash,2), "savdo", now_str, uid, message.from_user.full_name))
        sale_id = cur.fetchone()[0]; conn.commit(); conn.close()
        await notify_boss(uid, store, total, cash, "savdo", now_str)
        await msg.edit_text(f"✅ Saqlandi!\n🏪 {store}\n💰 {fmt(total)} | 💵 {fmt(cash)} | 📉 {fmt(total-cash)}\n📅 {date_str}", 
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Bekor", callback_data=f"cancel_req_{sale_id}")]]))
    except Exception as e: await msg.edit_text(f"❌ {e}")
    await state.clear()

@dp.callback_query(F.data.startswith("cancel_req_"))
async def request_cancel(callback: CallbackQuery):
    await callback.answer()
    sale_id = int(callback.data.replace("cancel_req_", ""))
    uid = callback.from_user.id
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT store_name, total, cash, date FROM sales WHERE id = %s AND worker_id = %s", (sale_id, uid))
    sale = cur.fetchone()
    if not sale: return await callback.answer("⚠️ Topilmadi.", show_alert=True)
    cur.execute("INSERT INTO deletion_requests (worker_id, sale_id, request_date) VALUES (%s,%s,%s)", (uid, sale_id, datetime.now().strftime("%d.%m.%Y %H:%M")))
    conn.commit(); conn.close()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Bekor", callback_data=f"approve_{sale_id}"), InlineKeyboardButton(text="❌ Qoldir", callback_data=f"reject_{sale_id}")]])
    notif = f"🔔 **O'chirish**\n👤 {callback.from_user.full_name} | 🏪 {sale['store_name']} | 💰 {fmt(sale['total'])} | 📅 {sale['date']}"
    for bid in BOSS_IDS:
        try: await bot.send_message(bid, notif, reply_markup=kb, parse_mode="Markdown")
        except: pass
    await callback.message.edit_text("✅ So'rov yuborildi.", reply_markup=None)

@dp.callback_query(F.data.startswith("approve_") | F.data.startswith("reject_"))
async def handle_deletion_request(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id not in BOSS_IDS: return
    action, sale_id = callback.data.split("_"); sale_id = int(sale_id)
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT worker_id FROM deletion_requests WHERE sale_id = %s", (sale_id,))
    wid = (cur.fetchone() or {}).get('worker_id')
    if action == "approve":
        cur.execute("DELETE FROM sales WHERE id = %s", (sale_id,))
        cur.execute("DELETE FROM deletion_requests WHERE sale_id = %s", (sale_id,))
        txt = "✅ Bekor qilindi."
    else:
        cur.execute("UPDATE deletion_requests SET status = 'rejected' WHERE sale_id = %s", (sale_id,))
        txt = "❌ Rad etildi."
    conn.commit(); conn.close()
    await callback.message.edit_text(txt)
    if wid: await bot.send_message(wid, txt.replace("✅", "Boss tasdiqladi").replace("❌", "Boss rad etdi"))

@dp.message(F.text)
async def worker_info(message: types.Message, state: FSMContext):
    if await state.get_state() is not None: return
    uid = message.from_user.id
    if uid in BOSS_IDS: return
    conn = get_db(); cur = dict_cursor(conn)
    cur.execute("SELECT name, user_id FROM users WHERE name = %s", (message.text,))
    w = cur.fetchone()
    if w:
        cur.execute("SELECT COALESCE(SUM(total),0), COUNT(id) FROM sales WHERE worker_id = %s", (w['user_id'],))
        r = cur.fetchone(); conn.close()
        await message.answer(f"👤 {w['name']}\n📊 Savdo: {fmt(r[0])}\n🧾 Soni: {r[1]}", reply_markup=get_back_kb())

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
            kb, text = get_boss_menu(), "Xush kelibsiz, Boss 👑"
        elif user and user['active'] == 0:
            kb, text = get_back_kb(), "🚫 Blok"
        else:
            kb = get_worker_menu() if user else ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⏳")]], resize_keyboard=True)
            text = f"Salom, {user['name'] or 'Ishchi'}!" if user else f"ID: {uid}"
        await callback.message.answer(text, reply_markup=kb)
    except:
        await callback.message.answer("⬅️", reply_markup=get_boss_menu() if uid in BOSS_IDS else get_worker_menu())

async def main():
    init_db()
    print("✅ Bot ishga tushdi 🚀")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
