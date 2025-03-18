import os
import asyncio
import logging
import datetime
import gspread
import hashlib
import numpy as np
import matplotlib.pyplot as plt

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, FSInputFile

# ======== Настройка токена и логирования ==========
logging.basicConfig(level=logging.INFO)

# Получаем токен из переменной окружения "API_TOKEN"
API_TOKEN = os.getenv("API_TOKEN")
if not API_TOKEN:
    logging.error("Переменная окружения API_TOKEN не установлена.")
    exit(1)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Путь к файлу с учетными данными Google Sheets (используется относительный путь или переменная окружения)
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

# ======== Глобальные переменные ==========
pending_inputs = {}         # Для хранения выбора пользователя: {user_id: {"type": str, "category": str}}
records = []                # Все финансовые записи, загружаемые из Google Sheets
registered_users = set()    # ID пользователей для автоматической рассылки отчётов

# ======== Инициализация Google Sheets ==========
try:
    gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
    spreadsheet = gc.open("FinancialRecords")  # Название таблицы
    income_sheet = spreadsheet.worksheet("Доходы")
    expense_sheet = spreadsheet.worksheet("Расходы")
    balance_sheet = spreadsheet.worksheet("Баланс")  # Лист для агрегированных данных
except Exception as e:
    logging.error(f"Ошибка инициализации Google Sheets: {e}")
    gc = None

# ---------------------------------------------------------------------------- #
#                      Функции работы с записями и графиками                     #
# ---------------------------------------------------------------------------- #
def get_record_id(record: dict) -> str:
    """Генерирует уникальный ID для записи на основе её полей."""
    hash_input = f"{record['date']}-{record['type']}-{record['category']}-{record['amount']}-{record['comment']}"
    return hashlib.md5(hash_input.encode('utf-8')).hexdigest()

def load_records() -> None:
    """Загружает все записи напрямую из Google Sheets."""
    global records
    records = []
    if gc:
        try:
            # Проходим по каждому листу и соответствующему типу транзакции
            for sheet, trans_type in [(income_sheet, "доход"), (expense_sheet, "расход")]:
                try:
                    data = sheet.get_all_records()
                except Exception as e:
                    logging.error(f"Ошибка получения данных с листа ({trans_type}): {e}")
                    continue
                for row in data:
                    # Проверка обязательных полей
                    if not row.get("date") or not row.get("amount"):
                        continue
                    try:
                        record_date = row.get("date")
                        # Попытка конвертации даты
                        datetime.datetime.strptime(record_date, "%Y-%m-%d %H:%M:%S")
                        record_amount = float(row.get("amount"))
                        rec = {
                            "date": record_date,
                            "type": trans_type,
                            "category": row.get("category"),
                            "amount": record_amount,
                            "comment": row.get("comment", "")
                        }
                        records.append(rec)
                    except Exception as ex:
                        logging.error(f"Ошибка обработки строки ({trans_type}): {row} - {ex}")
            try:
                records.sort(key=lambda r: datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S"))
            except Exception as e:
                logging.error(f"Ошибка сортировки записей: {e}")
            logging.info("Данные успешно загружены из Google Sheets.")
        except Exception as e:
            logging.error(f"Ошибка загрузки данных из Google Sheets: {e}")
    else:
        logging.error("Google Sheets недоступен. Записи не загружены.")

def save_record_to_sheet(record: dict) -> None:
    """Сохраняет новую запись в соответствующий лист Google Sheets."""
    row = [record['date'], record['category'], record['amount'], record['comment']]
    try:
        if record["type"] == "доход":
            income_sheet.append_row(row)
        else:
            expense_sheet.append_row(row)
        logging.info(f"Запись сохранена в листе {record['type']}.")
    except Exception as e:
        logging.error(f"Ошибка сохранения записи в лист: {e}")

def save_record(record: dict) -> bool:
    """
    Добавляет запись, если её нет (по уникальному ID),
    сохраняет её в Google Sheets и обновляет глобальный список.
    """
    record["id"] = get_record_id(record)
    if any(get_record_id(r) == record["id"] for r in records):
        logging.info("Дублирующая запись, не добавляем.")
        return False
    try:
        save_record_to_sheet(record)
        records.append(record)
        try:
            records.sort(key=lambda r: datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            logging.error(f"Ошибка сортировки записей: {e}")
        return True
    except Exception as e:
        logging.error(f"Ошибка сохранения записи в Google Sheets: {e}")
        return False

def generate_chart(period_records: list, title: str) -> str:
    """
    Генерирует группированный столбчатый график для переданного периода,
    группируя данные по категориям для доходов и расходов.
    Возвращает имя файла с сохраненным графиком.
    """
    try:
        categories = sorted(list({r["category"] for r in period_records}))
    except Exception as e:
        logging.error(f"Ошибка при сборе категорий: {e}")
        categories = []
    income_by_cat = {cat: 0 for cat in categories}
    expense_by_cat = {cat: 0 for cat in categories}
    for r in period_records:
        if r["type"] == "доход":
            income_by_cat[r["category"]] += r["amount"]
        elif r["type"] == "расход":
            expense_by_cat[r["category"]] += r["amount"]
    incomes = [income_by_cat.get(cat, 0) for cat in categories]
    expenses = [expense_by_cat.get(cat, 0) for cat in categories]

    x = np.arange(len(categories))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(x - width/2, incomes, width, label='Доходы')
    ax.bar(x + width/2, expenses, width, label='Расходы')
    ax.set_ylabel('Сумма (руб.)')
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(categories, rotation=45, ha='right')
    ax.legend()
    plt.tight_layout()
    filename = title.replace(" ", "_") + ".png"
    try:
        plt.savefig(filename)
    except Exception as e:
        logging.error(f"Ошибка сохранения графика: {e}")
    plt.close(fig)
    return filename

def generate_weekly_chart() -> str:
    now = datetime.datetime.now()
    week_ago = now - datetime.timedelta(days=7)
    period_records = [r for r in records if datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S") >= week_ago]
    title = f"Недельный отчёт ({(now - datetime.timedelta(days=7)).strftime('%d.%m')}–{now.strftime('%d.%m')})"
    return generate_chart(period_records, title)

def generate_monthly_chart() -> str:
    now = datetime.datetime.now()
    period_records = [r for r in records if (datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S").year == now.year and
                                              datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S").month == now.month)]
    title = f"Месячный отчёт за {now.strftime('%B %Y')}"
    return generate_chart(period_records, title)

def generate_yearly_chart() -> str:
    now = datetime.datetime.now()
    period_records = [r for r in records if datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S").year == now.year]
    title = f"Годовой отчёт за {now.year}"
    return generate_chart(period_records, title)

def get_current_balance() -> float:
    overall_income = sum(r["amount"] for r in records if r["type"] == "доход")
    overall_expense = sum(r["amount"] for r in records if r["type"] == "расход")
    return overall_income - overall_expense

def generate_daily_summary() -> str:
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    daily = [r for r in records if r["date"].startswith(today_str)]
    incomes = [r for r in daily if r["type"] == "доход"]
    expenses = [r for r in daily if r["type"] == "расход"]
    total_income = sum(r["amount"] for r in incomes)
    total_expense = sum(r["amount"] for r in expenses)
    balance_day = total_income - total_expense
    msg = f"🗓️ Отчёт за {datetime.datetime.now().strftime('%d %B %Y')}:\n\n"
    msg += "✅ Доходы:\n" + ("\n".join(f"- {r['category']}: {r['amount']} руб. {r['comment']}" for r in incomes) if incomes else "Нет записей") + "\n\n"
    msg += "❌ Расходы:\n" + ("\n".join(f"- {r['category']}: {r['amount']} руб. {r['comment']}" for r in expenses) if expenses else "Нет записей") + "\n\n"
    msg += f"📌 Итого:\nДоходы: {total_income} руб.\nРасходы: {total_expense} руб.\nБаланс за день: {'+' if balance_day >= 0 else ''}{balance_day} руб."
    return msg

def generate_weekly_summary() -> str:
    now = datetime.datetime.now()
    week_ago = now - datetime.timedelta(days=7)
    weekly = [r for r in records if datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S") >= week_ago]
    total_income = sum(r["amount"] for r in weekly if r["type"] == "доход")
    total_expense = sum(r["amount"] for r in weekly if r["type"] == "расход")
    balance_week = total_income - total_expense
    text = f"📆 Недельный отчёт ({(now - datetime.timedelta(days=7)).strftime('%d.%m')}–{now.strftime('%d.%m')}):\n\n"
    text += f"✅ Доход: {total_income} руб.\n❌ Расход: {total_expense} руб.\n💰 Баланс: {'+' if balance_week >= 0 else ''}{balance_week} руб."
    return text

def generate_monthly_summary() -> str:
    now = datetime.datetime.now()
    monthly = [r for r in records if (datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S").year == now.year and
                                       datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S").month == now.month)]
    total_income = sum(r["amount"] for r in monthly if r["type"] == "доход")
    total_expense = sum(r["amount"] for r in monthly if r["type"] == "расход")
    balance_month = total_income - total_expense
    text = f"📈 Месячный отчёт за {now.strftime('%B %Y')}:\n\n"
    text += f"✅ Доход: {total_income} руб.\n❌ Расход: {total_expense} руб.\n💳 Баланс: {'+' if balance_month >= 0 else ''}{balance_month} руб."
    return text

def generate_yearly_summary() -> str:
    now = datetime.datetime.now()
    yearly = [r for r in records if datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S").year == now.year]
    total_income = sum(r["amount"] for r in yearly if r["type"] == "доход")
    total_expense = sum(r["amount"] for r in yearly if r["type"] == "расход")
    balance_year = total_income - total_expense
    text = f"📊 Годовой отчёт за {now.year}:\n\n"
    text += f"✅ Доход: {total_income} руб.\n❌ Расход: {total_expense} руб.\n💵 Баланс: {'+' if balance_year >= 0 else ''}{balance_year} руб."
    return text

def update_balance_sheet() -> None:
    """
    Обновляет лист "Баланс" в Google Sheets, записывая агрегированные данные:
      1. Общий баланс
      2. Данные недели
      3. Данные месяца
      4. Данные года
    """
    if not gc:
        logging.error("Google Sheets недоступен. Не удается обновить баланс.")
        return
    try:
        overall_income = sum(r["amount"] for r in records if r["type"] == "доход")
        overall_expense = sum(r["amount"] for r in records if r["type"] == "расход")
        overall_balance = overall_income - overall_expense

        now = datetime.datetime.now()
        week_ago = now - datetime.timedelta(days=7)
        weekly_income = sum(r["amount"] for r in records 
                            if r["type"] == "доход" and datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S") >= week_ago)
        weekly_expense = sum(r["amount"] for r in records 
                             if r["type"] == "расход" and datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S") >= week_ago)
        weekly_balance = weekly_income - weekly_expense

        monthly_income = sum(r["amount"] for r in records 
                             if r["type"] == "доход" and datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S").year == now.year and
                             datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S").month == now.month)
        monthly_expense = sum(r["amount"] for r in records 
                              if r["type"] == "расход" and datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S").year == now.year and
                              datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S").month == now.month)
        monthly_balance = monthly_income - monthly_expense

        yearly_income = sum(r["amount"] for r in records 
                            if r["type"] == "доход" and datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S").year == now.year)
        yearly_expense = sum(r["amount"] for r in records 
                             if r["type"] == "расход" and datetime.datetime.strptime(r["date"], "%Y-%m-%d %H:%M:%S").year == now.year)
        yearly_balance = yearly_income - yearly_expense

        balance_sheet.update([["Общий баланс", overall_balance, "", ""]], "A1:D1")
        balance_sheet.update([["Данные недели", weekly_income, weekly_expense, weekly_balance]], "A2:D2")
        balance_sheet.update([["Данные месяца", monthly_income, monthly_expense, monthly_balance]], "A3:D3")
        balance_sheet.update([["Данные года", yearly_income, yearly_expense, yearly_balance]], "A4:D4")
        logging.info("Лист 'Баланс' успешно обновлен.")
    except Exception as e:
        logging.error(f"Ошибка обновления листа 'Баланс': {e}")

# ---------------------------------------------------------------------------- #
#                     Функции создания клавиатур для меню                        #
# ---------------------------------------------------------------------------- #
def get_main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Доход"), KeyboardButton(text="➖ Расход")],
            [KeyboardButton(text="💰 Баланс"), KeyboardButton(text="📊 Отчёты")]
        ],
        resize_keyboard=True
    )

def get_income_keyboard() -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="Бизнес", callback_data="income_business")],
            [types.InlineKeyboardButton(text="Инвестиции", callback_data="income_investments")],
            [types.InlineKeyboardButton(text="Пассивный Доход", callback_data="income_passive")],
            [types.InlineKeyboardButton(text="Дополнительный Доход", callback_data="income_additional")]
        ]
    )

def get_expense_keyboard() -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="Жилье", callback_data="expense_housing"),
             types.InlineKeyboardButton(text="Продукты", callback_data="expense_products"),
             types.InlineKeyboardButton(text="Кафе и Рестораны", callback_data="expense_restaurants")],
            [types.InlineKeyboardButton(text="Развитие", callback_data="expense_development"),
             types.InlineKeyboardButton(text="Транспорт", callback_data="expense_transport"),
             types.InlineKeyboardButton(text="Развлечения", callback_data="expense_entertainment")],
            [types.InlineKeyboardButton(text="Здоровье", callback_data="expense_health"),
             types.InlineKeyboardButton(text="Стиль", callback_data="expense_style"),
             types.InlineKeyboardButton(text="Непредвиденные Расходы", callback_data="expense_unexpected")]
        ]
    )

def get_reports_menu_keyboard() -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="🗓️ Ежедневный", callback_data="report_daily")],
            [types.InlineKeyboardButton(text="📆 Недельный", callback_data="report_weekly")],
            [types.InlineKeyboardButton(text="📈 Месячный", callback_data="report_monthly")],
            [types.InlineKeyboardButton(text="📊 Годовой", callback_data="report_yearly")],
            [types.InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
        ]
    )

# ---------------------------------------------------------------------------- #
#                    Обработчики команд и событий бота                         #
# ---------------------------------------------------------------------------- #
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    registered_users.add(message.from_user.id)
    logging.info(f"Пользователь {message.from_user.id} запустил бота.")
    await message.answer("Добро пожаловать! Выберите действие:", reply_markup=get_main_menu_keyboard())

@dp.message(lambda m: m.text == "➕ Доход")
async def choose_income_handler(message: types.Message):
    logging.info(f"Пользователь {message.from_user.id} выбрал доход.")
    await message.answer("Выберите категорию дохода:", reply_markup=get_income_keyboard())

@dp.message(lambda m: m.text == "➖ Расход")
async def choose_expense_handler(message: types.Message):
    logging.info(f"Пользователь {message.from_user.id} выбрал расход.")
    await message.answer("Выберите категорию расхода:", reply_markup=get_expense_keyboard())

@dp.message(lambda m: m.text == "💰 Баланс")
async def show_balance_handler(message: types.Message):
    balance = get_current_balance()
    logging.info(f"Пользователь {message.from_user.id} запросил баланс: {balance} руб.")
    await message.answer(f"Твой текущий баланс: {'+' if balance >= 0 else ''}{balance} руб.", reply_markup=get_main_menu_keyboard())

@dp.message(lambda m: m.text == "📊 Отчёты")
async def choose_reports_handler(message: types.Message):
    logging.info(f"Пользователь {message.from_user.id} запросил отчёты.")
    await message.answer("Выберите тип отчёта:", reply_markup=get_reports_menu_keyboard())

@dp.callback_query(lambda c: c.data.startswith("income_"))
async def process_income_category(callback: types.CallbackQuery):
    data = callback.data[len("income_"):]
    mapping = {
        "business": "Бизнес",
        "investments": "Инвестиции",
        "passive": "Пассивный Доход",
        "additional": "Дополнительный Доход"
    }
    chosen = mapping.get(data, "Неизвестная категория")
    pending_inputs[callback.from_user.id] = {"type": "доход", "category": chosen}
    logging.info(f"Пользователь {callback.from_user.id} выбрал категорию дохода: {chosen}")
    await callback.answer()
    await bot.send_message(callback.from_user.id,
                           f"Вы выбрали: {chosen}\nВведите сумму и опциональный комментарий")

@dp.callback_query(lambda c: c.data.startswith("expense_") and not c.data.startswith("expense_group_"))
async def process_expense_category(callback: types.CallbackQuery):
    data = callback.data[len("expense_"):]
    mapping = {
        "housing": "Жилье",
        "products": "Продукты",
        "restaurants": "Кафе и Рестораны",
        "development": "Развитие",
        "transport": "Транспорт",
        "entertainment": "Развлечения",
        "health": "Здоровье",
        "style": "Стиль",
        "unexpected": "Непредвиденные Расходы"
    }
    chosen = mapping.get(data, "Неизвестная категория")
    pending_inputs[callback.from_user.id] = {"type": "расход", "category": chosen}
    logging.info(f"Пользователь {callback.from_user.id} выбрал категорию расхода: {chosen}")
    await callback.answer()
    await bot.send_message(callback.from_user.id,
                           f"Вы выбрали: {chosen}\nВведите сумму и опциональный комментарий")

@dp.callback_query(lambda c: c.data == "report_daily")
async def process_report_daily(callback: types.CallbackQuery):
    logging.info(f"Пользователь {callback.from_user.id} запросил ежедневный отчёт.")
    await callback.answer()
    text_report = generate_daily_summary()
    await bot.send_message(callback.from_user.id, text_report)
    await bot.send_message(callback.from_user.id, "Главное меню:", reply_markup=get_main_menu_keyboard())

@dp.callback_query(lambda c: c.data == "report_weekly")
async def process_report_weekly(callback: types.CallbackQuery):
    logging.info(f"Пользователь {callback.from_user.id} запросил недельный отчёт.")
    await callback.answer()
    text_report = generate_weekly_summary()
    chart_file = generate_weekly_chart()
    photo = FSInputFile(chart_file)
    await bot.send_photo(callback.from_user.id, photo=photo, caption=text_report)
    await bot.send_message(callback.from_user.id, "Главное меню:", reply_markup=get_main_menu_keyboard())

@dp.callback_query(lambda c: c.data == "report_monthly")
async def process_report_monthly(callback: types.CallbackQuery):
    logging.info(f"Пользователь {callback.from_user.id} запросил месячный отчёт.")
    await callback.answer()
    text_report = generate_monthly_summary()
    chart_file = generate_monthly_chart()
    photo = FSInputFile(chart_file)
    await bot.send_photo(callback.from_user.id, photo=photo, caption=text_report)
    await bot.send_message(callback.from_user.id, "Главное меню:", reply_markup=get_main_menu_keyboard())

@dp.callback_query(lambda c: c.data == "report_yearly")
async def process_report_yearly(callback: types.CallbackQuery):
    logging.info(f"Пользователь {callback.from_user.id} запросил годовой отчёт.")
    await callback.answer()
    text_report = generate_yearly_summary()
    chart_file = generate_yearly_chart()
    photo = FSInputFile(chart_file)
    await bot.send_photo(callback.from_user.id, photo=photo, caption=text_report)
    await bot.send_message(callback.from_user.id, "Главное меню:", reply_markup=get_main_menu_keyboard())

@dp.message(lambda message: message.from_user.id in pending_inputs)
async def process_manual_input(message: types.Message):
    user_id = message.from_user.id
    pending = pending_inputs.get(user_id)
    if not pending:
        return
    text = message.text.strip()
    logging.info(f"Пользователь {user_id} ввёл: {text}")
    parts = text.split(maxsplit=1)
    if not parts:
        await message.reply("Пожалуйста, введите сумму и комментарий.")
        return
    try:
        amount = float(parts[0])
    except ValueError:
        await message.reply("Неверный формат суммы. Введите число в начале сообщения.")
        logging.warning(f"Неверный формат суммы от пользователя {user_id}: {parts[0]}")
        return
    comment = parts[1] if len(parts) > 1 else ""
    record = {
        "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "type": pending["type"],
        "category": pending["category"],
        "amount": amount,
        "comment": comment
    }
    if save_record(record):
        await message.reply(
            f"Запись сохранена:\nДата: {record['date']}\nТип: {record['type']}\nКатегория: {record['category']}\n"
            f"Сумма: {record['amount']}\nКомментарий: {record['comment']}"
        )
        logging.info(f"Запись успешно добавлена пользователем {user_id}.")
    else:
        await message.reply("Такая запись уже существует.")
        logging.info(f"Попытка дублирования записи пользователем {user_id}.")
    del pending_inputs[user_id]
    await bot.send_message(message.from_user.id, "Главное меню:", reply_markup=get_main_menu_keyboard())

# ---------------------------------------------------------------------------- #
#         Фоновые задачи для обновления листа "Баланс" и отправки отчётов       #
# ---------------------------------------------------------------------------- #
async def update_balance_task():
    """Фоновая задача, которая каждые 5 минут обновляет данные из Google Sheets и лист 'Баланс'."""
    while True:
        load_records()
        update_balance_sheet()
        await asyncio.sleep(300)

async def daily_summary_task():
    while True:
        now = datetime.datetime.now()
        target = now.replace(hour=20, minute=0, second=0, microsecond=0)
        if now >= target:
            target += datetime.timedelta(days=1)
        delay = (target - now).total_seconds()
        await asyncio.sleep(delay)
        load_records()
        text_report = generate_daily_summary()
        for user_id in registered_users:
            try:
                await bot.send_message(user_id, text_report)
            except Exception as e:
                logging.error(f"Ошибка отправки ежедневного отчёта пользователю {user_id}: {e}")
        await asyncio.sleep(60)

async def weekly_summary_task():
    while True:
        now = datetime.datetime.now()
        days_ahead = 6 - now.weekday()
        target = now.replace(hour=20, minute=0, second=0, microsecond=0) + datetime.timedelta(days=days_ahead)
        if now >= target:
            target += datetime.timedelta(weeks=1)
        delay = (target - now).total_seconds()
        await asyncio.sleep(delay)
        load_records()
        text_report = generate_weekly_summary()
        chart_file = generate_weekly_chart()
        photo = FSInputFile(chart_file)
        for user_id in registered_users:
            try:
                await bot.send_photo(user_id, photo=photo, caption=text_report)
            except Exception as e:
                logging.error(f"Ошибка отправки недельного отчёта пользователю {user_id}: {e}")
        await asyncio.sleep(60)

async def monthly_summary_task():
    while True:
        now = datetime.datetime.now()
        if now.day == 1 and now.hour < 10:
            target = now.replace(hour=10, minute=0, second=0, microsecond=0)
        else:
            if now.month == 12:
                target = datetime.datetime(now.year + 1, 1, 1, 10, 0, 0)
            else:
                target = datetime.datetime(now.year, now.month + 1, 1, 10, 0, 0)
        delay = (target - now).total_seconds()
        await asyncio.sleep(delay)
        load_records()
        text_report = generate_monthly_summary()
        chart_file = generate_monthly_chart()
        photo = FSInputFile(chart_file)
        for user_id in registered_users:
            try:
                await bot.send_photo(user_id, photo=photo, caption=text_report)
            except Exception as e:
                logging.error(f"Ошибка отправки месячного отчёта пользователю {user_id}: {e}")
        await asyncio.sleep(60)

async def yearly_summary_task():
    while True:
        # Обновление годового отчёта раз в 24 часа
        await asyncio.sleep(86400)
        load_records()
        text_report = generate_yearly_summary()
        chart_file = generate_yearly_chart()
        photo = FSInputFile(chart_file)
        for user_id in registered_users:
            try:
                await bot.send_photo(user_id, photo=photo, caption=text_report)
            except Exception as e:
                logging.error(f"Ошибка отправки годового отчёта пользователю {user_id}: {e}")

# ---------------------------------------------------------------------------- #
#                             Основная функция                               #
# ---------------------------------------------------------------------------- #
async def main():
    try:
        load_records()  # Начальная загрузка данных из Google Sheets
    except Exception as e:
        logging.error(f"Ошибка при начальной загрузке записей: {e}")
    asyncio.create_task(update_balance_task())
    asyncio.create_task(daily_summary_task())
    asyncio.create_task(weekly_summary_task())
    asyncio.create_task(monthly_summary_task())
    asyncio.create_task(yearly_summary_task())
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logging.error(f"Ошибка в процессе polling: {e}")

if __name__ == '__main__':
    asyncio.run(main())
