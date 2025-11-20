import asyncio
import urllib.parse
import logging
import re
import csv
import os

from dotenv import load_dotenv
from io import StringIO
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, BufferedInputFile, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, Location, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from datetime import datetime, timedelta, timezone
from database import *
from keyboards import *
from notifications import notify_new_order_in_group # Уведомление о новом заказе в чат
import subprocess  # Импорты для webapp
import sys         # Импорты для webapp
from webapp import *
from webapp import start_webapp
#from untils import *  #ИМПОРТ UNTILS ДЛЯ ГЕНЕРАЦИИ ИЗОБРАЖЕНИЙ

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("❌ Переменная BOT_TOKEN не задана в .env")

# ADMINS — строка вида "123,456", превращаем в set(int)
ADMINS_RAW = os.getenv("ADMINS", "")
if ADMINS_RAW:
    ADMINS = {int(x.strip()) for x in ADMINS_RAW.split(",") if x.strip().isdigit()}
else:
    ADMINS = set()
    logging.warning("⚠️ В .env не указаны ADMINS.")

COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", 120))
UNCLAIMED_SECONDS = int(os.getenv("UNCLAIMED_SECONDS", 120))
CANCEL_SECONDS = int(os.getenv("CANCEL_SECONDS", 120))
STALE_SECONDS = int(os.getenv("STALE_SECONDS", 120))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()

unclaimed_tasks = {}   # для auto_cancel_unclaimed_order
selection_tasks = {}   # для auto_cancel_order (выбор водителя)
stale_tasks = {}       # для auto_cancel_stale_order (висячий заказ)
order_cooldown = {} # для cooldown (таймер на повтор заказа)
client_bid_messages = {}
driver_order_messages = {}
client_order_messages = {}
#order_recipients = {}

class RatingStates(StatesGroup):
    waiting_for_low_rating_comment = State()

class ClientStates(StatesGroup):
    waiting_for_order = State()
    waiting_for_rating = State()
    waiting_for_passengers = State()
    sending_location = State()

class DriverStates(StatesGroup):
    waiting_for_car = State()
    waiting_for_rating = State()
    waiting_for_arrival_time = State()
    opening_shift = State()

class AdminStates(StatesGroup):
    waiting_for_verification_date = State()
    waiting_for_user_search = State()
    waiting_for_broadcast_text = State()
    broadcast_target = State()
    waiting_for_broadcast_schedule = State()
    waiting_for_new_car_info = State()
    waiting_for_ad_message = State()
    waiting_for_ad_url = State()
    waiting_for_rating_edit = State()

class DisputeStates(StatesGroup):
    waiting_for_dispute_text = State()
    waiting_for_dispute_photo = State()

# Клавиатура с кнопкой "Назад"
def admin_back_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ Вернутся назад")]],
        resize_keyboard=True,
        one_time_keyboard=False  # остаётся до явного удаления
    )

async def send_immediate_broadcast(user_ids: list, original_message: Message):
    if not user_ids:
        return 0

    # Сохраняем рассылку с количеством получателей
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO broadcasts 
            (target, message_text, photo_file_id, document_file_id, caption, is_sent, total_recipients)
            VALUES (?, ?, ?, ?, ?, 1, ?)
        """, (
            "temp",
            original_message.text,
            original_message.photo[-1].file_id if original_message.photo else None,
            original_message.document.file_id if original_message.document else None,
            original_message.caption,
            len(user_ids)  # ← сохраняем количество
        ))
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cursor:
            broadcast_id = (await cursor.fetchone())[0]

    success = 0
    for user_id in user_ids:
        try:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Прочитано", callback_data=f"receipt_{broadcast_id}")]
            ])
            if original_message.text:
                await bot.send_message(
                    user_id,
                    original_message.text,
                    parse_mode="HTML",
                    reply_markup=kb
                )
            elif original_message.photo:
                await bot.send_photo(
                    user_id,
                    photo=original_message.photo[-1].file_id,
                    caption=original_message.caption,
                    parse_mode="HTML",
                    reply_markup=kb
                )
            elif original_message.document:
                await bot.send_document(
                    user_id,
                    document=original_message.document.file_id,
                    caption=original_message.caption,
                    parse_mode="HTML",
                    reply_markup=kb
                )
            success += 1
        except:
            pass
    return success  # ← ЭТА СТРОКА ОБЯЗАТЕЛЬНА!

async def send_partner_ad(user_id: int):
    """Отправляет партнёрскую рекламу и логирует ПОКАЗ."""
    ad = await get_random_partner_ad()
    if not ad:
        return
    ad_id, message_text, photo_file_id, url = ad

    try:
        # ВАЖНО: кнопка — callback, НЕ url!
        if photo_file_id:
            await bot.send_photo(
                user_id,
                photo=photo_file_id,
                caption=message_text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Подробнее", callback_data=f"ad_click_{ad_id}")]
                ])
            )
        else:
            await bot.send_message(
                user_id,
                message_text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Подробнее", callback_data=f"ad_click_{ad_id}")]
                ])
            )

        # Логируем ПОКАЗ
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO ad_stats (ad_id, user_id, event_type, timestamp)
                VALUES (?, ?, 'impression', ?)
            """, (ad_id, user_id, datetime.now().isoformat()))
            await db.commit()

        logging.info(f"[partner_ad] Показана реклама {ad_id} пользователю {user_id}")

    except Exception as e:
        logging.warning(f"[partner_ad] Не удалось отправить рекламу {user_id}: {e}")

#ФУНКЦИИ COOLDOWN
def is_order_allowed(user_id: int) -> bool:
    """Проверяет, прошёл ли cooldown для пользователя."""
    last_time = order_cooldown.get(user_id)
    if last_time is None:
        return True
    return (datetime.now().timestamp() - last_time) >= COOLDOWN_SECONDS

def set_order_cooldown(user_id: int):
    """Фиксирует время последнего заказа."""
    order_cooldown[user_id] = datetime.now().timestamp()

async def broadcast_scheduler():
    """Фоновая задача: проверяет и отправляет запланированные рассылки."""
    while True:
        try:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            async with aiosqlite.connect(DB_PATH) as db:
                # Получаем неотправленные рассылки, время которых наступило
                async with db.execute("""
                    SELECT id, target, message_text, photo_file_id, 
                           document_file_id, caption, scheduled_at
                    FROM broadcasts 
                    WHERE is_sent = 0 AND scheduled_at <= ?
                """, (now,)) as cursor:
                    broadcasts = await cursor.fetchall()

                for b in broadcasts:
                    b_id, target, text, photo, doc, caption, _ = b
                    await send_scheduled_broadcast(b_id, target, text, photo, doc, caption)
                    # Помечаем как отправленную
                    await db.execute("UPDATE broadcasts SET is_sent = 1 WHERE id = ?", (b_id,))
                    await db.commit()

        except Exception as e:
            logging.error(f"Ошибка в планировщике рассылок: {e}")

        await asyncio.sleep(30)  # Проверяем каждые 30 секунд

async def send_scheduled_broadcast(broadcast_id: int, target: str, text: str, photo: str, doc: str, caption: str):
    # Получаем получателей
    user_ids = await get_broadcast_recipients(target)

    # Сохраняем total_recipients
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE broadcasts SET total_recipients = ? WHERE id = ?",
            (len(user_ids), broadcast_id)
        )
        await db.commit()

    # Отправляем каждому
    for user_id in user_ids:
        try:
            # Создаём кнопку подтверждения
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="✅ Прочитано",
                    callback_data=f"receipt_{broadcast_id}"
                )]
            ])

            if text:
                await bot.send_message(user_id, text, parse_mode="HTML", reply_markup=kb)
            elif photo:
                await bot.send_photo(user_id, photo, caption=caption, parse_mode="HTML", reply_markup=kb)
            elif doc:
                await bot.send_document(user_id, doc, caption=caption, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            logging.warning(f"Не удалось отправить рассылку {broadcast_id} пользователю {user_id}: {e}")

@router.callback_query(F.data.startswith("change_car_"))
async def request_new_car_info(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return

    try:
        user_id = int(callback.data.split("_")[2])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    await state.update_data(target_user_id=user_id)
    await state.set_state(AdminStates.waiting_for_new_car_info)

    await callback.message.answer(
        "✏️ Введите новую марку и госномер автомобиля в формате:\n\n*МАРКА ГОСНОМЕР*",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )
    await callback.answer()

@router.callback_query(F.data.startswith("orders_page_"))
async def navigate_orders_page(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    try:
        page = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    await show_orders_page(callback.message, page)
    await callback.answer()

@router.callback_query(F.data.startswith("admin_cancel_order_"))
async def admin_cancel_order(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    try:
        order_id = int(callback.data.split("_")[3])
    except (ValueError, IndexError):
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    order = await get_order(order_id)
    if not order:
        await callback.answer("❌ Заказ не найден.", show_alert=True)
        return

    status = order["status"]
    if status in ("completed", "cancelled"):
        await callback.answer("❌ Заказ уже завершён или отменён.", show_alert=True)
        return

    # Отменяем заказ
    await cancel_order_with_reason(order_id, f"admin_{callback.from_user.id}")

    # Удаляем сообщения о заказе у водителей
    messages_to_delete = await get_driver_order_messages(order_id)
    for chat_id, msg_id, driver_id in messages_to_delete:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception as e:
            logging.warning(f"Не удалось удалить сообщение заказа у водителя {driver_id}: {e}")
    await delete_driver_order_messages(order_id)

    # Уведомляем участников
    client_id = order["client_id"]
    driver_id = order["driver_id"]

    try:
        await bot.send_message(client_id, f"❌ Ваш заказ №{order_id} отменён администратором.")
    except:
        pass

    if driver_id:
        try:
            await bot.send_message(driver_id, f"❌ Заказ №{order_id}, который вы обслуживали, отменён администратором.")
        except:
            pass

    await callback.answer(f"✅ Заказ №{order_id} отменён.", show_alert=True)

    # Обновляем страницу
    current_page_msg = callback.message.text
    if "страница" in current_page_msg:
        # Попробуем определить номер страницы из сообщения
        # Это упрощённый способ — в реальном проекте лучше хранить page в FSM
        import re
        match = re.search(r"страница (\d+)", current_page_msg)
        if match:
            page = int(match.group(1))
            await show_orders_page(callback.message, page)
        else:
            await callback.message.delete()
            await show_orders_page(callback.message, page=1)
    else:
        await callback.message.delete()
        await show_orders_page(callback.message, page=1)

@router.callback_query(F.data.startswith("drivers_page_"))
async def navigate_drivers_page(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    try:
        page = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    await show_drivers_page(callback.message, page)
    await callback.answer()

@router.callback_query(F.data.startswith("view_profile_"))
async def view_driver_profile(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    try:
        user_id = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    # Открываем карточку пользователя (как в поиске)
    await search_user_by_id(callback.message, user_id)
    await callback.answer()

@router.callback_query(F.data.startswith("change_role_"))
async def change_user_role(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    try:
        # Формат: change_role_{user_id}_{new_role}
        parts = callback.data.split("_")
        if len(parts) != 4:
            raise ValueError("Неверное количество частей")
        user_id = int(parts[2])  # Третий элемент (индекс 2)
        new_role = parts[3]      # Четвёртый элемент (индекс 3)
        if new_role not in ("client", "driver"):
            raise ValueError("Недопустимая роль")
    except (ValueError, IndexError):
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    # Обновляем роль в БД
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET role = ? WHERE user_id = ?", (new_role, user_id))
        await db.commit()

    # Уведомляем пользователя
    try:
        await bot.send_message(
            user_id,
            f"🔄 Ваша роль изменена на: {'🚗 Водитель' if new_role == 'driver' else '👤 Клиент'}.\n"
            "Перезапустите бота командой /start, чтобы обновить меню."
        )
    except:
        pass

    await callback.answer(f"✅ Роль пользователя {user_id} изменена на {new_role}.", show_alert=True)
    await search_user_by_id(callback.message, user_id)

@router.callback_query(F.data.startswith("receipt_"))
async def handle_receipt(callback: CallbackQuery):
    broadcast_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id

    # Сохраняем подтверждение
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO broadcast_receipts (broadcast_id, user_id) VALUES (?, ?)",
            (broadcast_id, user_id)
        )
        await db.commit()

    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.answer("Спасибо! Сообщение прочитано.", show_alert=True)

@router.callback_query(F.data.startswith("admin_verify_"))
async def admin_verify_driver(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    try:
        driver_id = int(callback.data.split("_")[2])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверный ID водителя.", show_alert=True)
        return

    await state.update_data(driver_id=driver_id)

    msg = await callback.message.answer(
        "📅 Укажите дату окончания верификации в формате ДД.ММ.ГГГГ (например, 30.09.2025).\n"
        "Отправьте «-», чтобы установить бессрочную верификацию."
    )
    await state.update_data(date_request_message_id=msg.message_id)

    await state.set_state(AdminStates.waiting_for_verification_date)
    await callback.answer()

@router.callback_query(F.data.startswith("verify_driver_"))
async def request_verification_date(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    parts = callback.data.split("_")
    if len(parts) < 3:
        await callback.answer("❌ Неверный формат.", show_alert=True)
        return
    driver_id = int(parts[2])
    message_id = int(parts[3]) if len(parts) > 3 else None

    # Сохраняем данные, включая ID карточки
    await state.update_data(
        driver_id=driver_id,
        verify_message_id=message_id,
        chat_id=callback.message.chat.id
    )

    # Отправляем запрос на дату и сохраняем его ID
    msg = await callback.message.answer(
        "📅 Укажите дату окончания верификации в формате ДД.ММ.ГГГГ (например, 30.09.2025).\n"
        "Отправьте «-», чтобы установить бессрочную верификацию."
    )
    await state.update_data(date_request_message_id=msg.message_id)

    await state.set_state(AdminStates.waiting_for_verification_date)
    await callback.answer()

@router.callback_query(F.data.startswith("reject_driver_"))
async def reject_driver(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    parts = callback.data.split("_")
    driver_id = int(parts[2])
    message_id = int(parts[3]) if len(parts) > 3 else None

    # Отправляем уведомление водителю
    try:
        await bot.send_message(
            driver_id,
            "❌ Ваши данные не прошли верификацию. Обратитесь в поддержку: @AnatolyElizarev @azimut301"
        )
        # 🔥 Возвращаем в меню выбора роли
        await bot.send_message(
            driver_id,
            "👋 Пожалуйста, выберите роль заново:",
            reply_markup=start_keyboard()
        )
    except Exception as e:
        logging.warning(f"Не удалось отправить сообщение водителю {driver_id}: {e}")

    # Удаляем сообщение с карточкой у админа
    if message_id:
        try:
            await bot.delete_message(chat_id=callback.message.chat.id, message_id=message_id)
        except:
            pass

    await callback.answer("❌ Водитель отклонён.", show_alert=True)

@router.callback_query(F.data.startswith("unverify_"))
async def unverify_driver(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    user_id = int(callback.data.split("_")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_verified = 0 WHERE user_id = ?", (user_id,))
        await db.commit()
    await callback.message.edit_text("🔄 Верификация снята.")
    await callback.answer()

@router.callback_query(F.data == "admin_users_back")
async def admin_users_back(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.delete()
    await callback.answer()

@router.callback_query(F.data.startswith("ban_"))
async def ban_user_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    user_id = int(callback.data.split("_")[1])
    await ban_user(user_id)
    await callback.message.edit_text("🚫 Пользователь заблокирован.")
    await callback.answer()

@router.callback_query(F.data.startswith("unban_"))
async def unban_user_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    user_id = int(callback.data.split("_")[1])
    await unban_user(user_id)
    await callback.message.edit_text("🔓 Пользователь разблокирован.")
    await callback.answer()

@router.callback_query(F.data == "admin_back")
async def admin_back(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await callback.message.delete()
    await callback.message.answer("👮‍♂️ Панель администратора:", reply_markup=admin_menu())
    await callback.answer()

@router.callback_query(F.data.startswith("broadcast_"))
async def select_broadcast_target(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return

    target = callback.data.split("_")[1]  # all, drivers, clients
    await state.update_data(broadcast_target=target)
    await state.set_state(AdminStates.waiting_for_broadcast_text)

    audience = {"all": "всем", "drivers": "водителям", "clients": "клиентам"}
    await callback.message.edit_text(f"✏️ Отправьте сообщение для {audience[target]}:")
    await callback.answer()

@router.callback_query(F.data.startswith("repeat_order_"))
async def repeat_order(callback: CallbackQuery, state: FSMContext):
    client_id = callback.from_user.id
    # 🔥 Проверка cooldown
    if not is_order_allowed(client_id):
        remaining = int(COOLDOWN_SECONDS - (datetime.now().timestamp() - order_cooldown.get(client_id, 0)))
        await callback.answer(
            f"⏳ Вы можете создать новый заказ через {remaining} секунд.\n"
            "Это нужно, чтобы не перегружать водителей.",
            show_alert=True
        )
        return
    if await is_user_banned(client_id):
        await callback.answer("❌ Ваш аккаунт заблокирован.", show_alert=True)
        return
    # Проверка: есть ли активный заказ?
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM orders WHERE client_id = ? AND status IN ('pending', 'accepted')",
            (client_id,)
        ) as cursor:
            active_order = await cursor.fetchone()
    if active_order:
        await callback.answer("⚠️ У вас уже есть активный заказ. Дождитесь его завершения или отмените.", show_alert=True)
        return

    try:
        order_id = int(callback.data.split("_")[2])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверный номер заказа.", show_alert=True)
        return

    order = await get_order(order_id)
    if not order or order["client_id"] != client_id:
        await callback.answer("❌ Заказ не найден или не ваш.", show_alert=True)
        return

    pickup = order["pickup_address"]
    dropoff = order["dropoff_address"]
    comment = order["comment"] or ""

    # 🔥 Сохраняем данные и переходим к выбору пассажиров — как в обычном заказе
    await state.update_data(pickup=pickup, dropoff=dropoff, comment=comment.strip())
    await state.set_state(ClientStates.waiting_for_passengers)
    await callback.message.edit_text("👥 Сколько пассажиров будет ехать?")
    await callback.message.answer("Выберите количество:", reply_markup=passengers_keyboard())
    await callback.answer()

@router.message(F.text == "🤝 Партнёрская реклама")
async def partner_ads_menu(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "🤝 Управление партнёрской рекламой:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить", callback_data="partner_add")],
            [InlineKeyboardButton(text="📋 Список", callback_data="partner_list")],
            [InlineKeyboardButton(text="📊 Аналитика рекламы", callback_data="partner_analytics")],
        ])
    )

@router.callback_query(F.data == "partner_add")
async def partner_add_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer("✏️ Отправьте рекламное сообщение с фото (опционально).")
    await state.set_state(AdminStates.waiting_for_ad_message)
    await callback.answer()

@router.callback_query(F.data == "partner_list")
async def partner_list_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT id, partner_name, message_text, url, is_active
            FROM partner_ads
            ORDER BY id DESC
        """) as cursor:
            ads = await cursor.fetchall()

    if not ads:
        await callback.message.edit_text("📭 Нет партнёрских объявлений.")
        await callback.answer()
        return

    msg = "🤝 Список партнёрских объявлений:\n\n"
    buttons = []
    for ad_id, name, text, url, is_active in ads:
        status = "✅ Активно" if is_active else "❌ Неактивно"
        preview = (text[:30] + "...") if len(text) > 30 else text
        msg += f"ID: #{ad_id}\nПартнёр: {name}\nТекст: {preview}\nURL: {url}\nСтатус: {status}\n\n"

        # Кнопки управления каждым объявлением (опционально)
        buttons.append([
            InlineKeyboardButton(text=f"🗑 Удалить #{ad_id}", callback_data=f"partner_delete_{ad_id}")
         ])

    # Кнопка "Назад"
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="partner_ads_menu")])

    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )

    await callback.answer()

@router.callback_query(F.data == "partner_analytics")
async def partner_analytics(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT 
                p.id,
                p.partner_name,
                p.message_text,
                p.url,
                COUNT(CASE WHEN s.event_type = 'impression' THEN 1 END) as impressions,
                COUNT(CASE WHEN s.event_type = 'click' THEN 1 END) as clicks
            FROM partner_ads p
            LEFT JOIN ad_stats s ON p.id = s.ad_id
            GROUP BY p.id
            ORDER BY p.id DESC
        """) as cursor:
            rows = await cursor.fetchall()

    if not rows:
        await callback.message.edit_text("📭 Нет данных по партнёрской рекламе.")
        await callback.answer()
        return

    msg = "📊 <b>Аналитика партнёрской рекламы:</b>\n\n"
    for ad_id, partner, text, url, impressions, clicks in rows:
        ctr = (clicks / impressions * 100) if impressions > 0 else 0
        preview = (text[:30] + "...") if len(text) > 30 else text
        msg += (
            f"ID: {ad_id}\n"
            f"Партнёр: {partner}\n"
            f"Текст: {preview}\n"
            f"URL: {url}\n"
            f"Показы: {impressions}\n"
            f"Клики: {clicks}\n"
            f"CTR: {ctr:.2f}%\n"
            f"{'─' * 30}\n"
        )

    # Кнопка "Назад"
    back_button = [[InlineKeyboardButton(text="⬅️ Назад", callback_data="partner_ads_menu")]]

    await callback.message.edit_text(
        msg,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=back_button)
    )
    await callback.answer()

@router.callback_query(F.data.startswith("ad_click_"))
async def handle_ad_click(callback: CallbackQuery):
    try:
        ad_id = int(callback.data.split("_")[2])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверный формат.", show_alert=True)
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT url FROM partner_ads WHERE id = ?", (ad_id,)) as cursor:
            row = await cursor.fetchone()
    if not row or not row[0]:
        await callback.answer("❌ Ссылка недоступна.", show_alert=True)
        return

    url = row[0].strip()
    if not url:
        await callback.answer("❌ Ссылка пустая.", show_alert=True)
        return

    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    # Валидация (опционально)
    try:
        parsed = urllib.parse.urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid URL")
    except Exception:
        logging.error(f"[ad_click] Невалидный URL: {repr(row[0])}")
        await callback.answer("❌ Некорректная ссылка.", show_alert=True)
        return

    user_id = callback.from_user.id

    # 🔥 ЛОГИРУЕМ КЛИК
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO ad_stats (ad_id, user_id, event_type, timestamp)
            VALUES (?, ?, 'click', ?)
        """, (ad_id, user_id, datetime.now().isoformat()))
        await db.commit()

    logging.info(f"[ad_click] Клик по рекламе {ad_id} от {user_id}")

    # 🔥 ОТПРАВЛЯЕМ СООБЩЕНИЕ СО ССЫЛКОЙ (как в рассылках)
    try:
        await callback.message.answer(
            f'🔗 <a href="{url}">Перейти на сайт партнёра</a>',
            parse_mode="HTML"
        )
        await callback.answer("✅ Ссылка отправлена!", show_alert=False)
    except Exception as e:
        logging.error(f"[ad_click] Ошибка отправки ссылки: {e}")
        await callback.answer("❌ Не удалось отправить ссылку.", show_alert=True)

@router.callback_query(F.data.startswith("partner_delete_"))
async def partner_delete_ad(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    try:
        ad_id = int(callback.data.split("_")[2])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверный ID объявления.", show_alert=True)
        return

    async with aiosqlite.connect(DB_PATH) as db:
        # Проверяем, существует ли объявление
        async with db.execute("SELECT id FROM partner_ads WHERE id = ?", (ad_id,)) as cursor:
            exists = await cursor.fetchone()
        if not exists:
            await callback.answer("❌ Объявление не найдено.", show_alert=True)
            return

        # Удаляем
        await db.execute("DELETE FROM partner_ads WHERE id = ?", (ad_id,))
        await db.commit()

    await callback.answer(f"✅ Объявление ID #{ad_id} удалено.", show_alert=True)
    # Обновляем список
    await partner_list_handler(callback)

@router.message(AdminStates.waiting_for_ad_message)
async def partner_add_message(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.update_data(
        message_text=message.caption or message.text,
        photo_file_id=message.photo[-1].file_id if message.photo else None
    )
    await message.answer("🔗 Отправьте ПАРТНЁРСКУЮ ссылку (обязательно с вашим ref-параметром):")
    await state.set_state(AdminStates.waiting_for_ad_url)

@router.message(AdminStates.waiting_for_ad_url)
async def partner_add_url(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    url = message.text.strip()
    if not url.startswith("http"):
        await message.answer("❌ Ссылка должна начинаться с http(s)://")
        return

    data = await state.get_data()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO partner_ads (partner_name, message_text, photo_file_id, url, is_active)
            VALUES (?, ?, ?, ?, 1)
        """, ("Новый партнёр", data["message_text"], data["photo_file_id"], url))
        await db.commit()

    await message.answer("✅ Партнёрская реклама добавлена!")
    await state.clear()

@router.callback_query(F.data == "partner_ads_menu")
async def back_to_partner_menu(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(
        "🤝 Управление партнёрской рекламой:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить", callback_data="partner_add")],
            [InlineKeyboardButton(text="📋 Список", callback_data="partner_list")],
            [InlineKeyboardButton(text="📊 Аналитика рекламы", callback_data="partner_analytics")],
        ])
    )
    await callback.answer()

@router.message(F.text == "📜 История заказов")
async def order_history(message: Message):
    user_id = message.from_user.id
    orders = await get_client_order_history(user_id)
    if not orders:
        await message.answer("📭 У вас пока нет заказов.")
        return

    msg = "📋 Ваши последние заказы:\n\n"
    for order_id, pickup, dropoff, created_at in orders:
        # Форматируем дату: 01.06.2024 14:30
        dt = datetime.fromisoformat(created_at)
        dt_str = dt.strftime("%d.%m.%Y %H:%M")
        msg += f"Заказ #{order_id} | {pickup} → {dropoff} | {dt_str}\n"

    await message.answer(msg)

    # Отправляем кнопки "Повторить" под каждым заказом
    for order_id, pickup, dropoff, _ in orders:
        await message.answer(
            f"🔁 Повторить заказ #{order_id}: {pickup} → {dropoff}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔁 Повторить", callback_data=f"repeat_order_{order_id}")]
            ])
        )

@router.message(F.text.startswith("💎 Ваш статус"))
async def show_client_status_info(message: Message):
    user_id = message.from_user.id
    rides = await get_monthly_rides(user_id)
    status_name, status_emoji = get_client_status(rides)

    # Определяем привилегии по статусу
    if status_name == "Платина":
        privileges = (
            "• Приоритет в подаче такси\n"
            "• Бесплатное ожидание до 15 минут\n"
            "• Персональный менеджер\n"
            "• Скидка 15% на все поездки"
            "• Скидки у наших партнеров\n"
        )
    elif status_name == "Золото":
        privileges = (
            "• Ускоренная подача\n"
            "• Бесплатное ожидание до 10 минут\n"
            "• Скидка 10% на все поездки"
            "• Скидки у наших партнеров\n"
        )
    elif status_name == "Серебро":
        privileges = (
            "• Быстрая подача\n"
            "• Бесплатное ожидание до 7 минут\n"
            "• Скидка 5% на все поездки"
            "• Скидки у наших партнеров\n"
        )
    else:  # Базовый
        privileges = (
            "• Стандартная подача\n"
            "• Бесплатное ожидание до 5 минут\n"
            "• Скидки у наших партнеров\n"
            "• Возможность повысить статус — совершайте больше поездок!"
        )

    msg = (
        f"🌟 <b>Вы — особенный клиент ТаксиБарс!</b>\n\n"
        f"Ваш текущий статус: <b>{status_emoji}{status_name}</b>\n"
        f"Поездок в этом месяце: <b>{rides}</b>\n\n"
        f"<b>Ваши привилегии:</b>\n{privileges}\n\n"
        "💡 <i>Совершайте больше поездок — получайте больше бонусов!</i>"
    )
    await message.answer(msg, parse_mode="HTML")

@router.message(F.text == "📤 Рассылка")
async def admin_broadcast(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("📤 Выберите аудиторию:", reply_markup=admin_broadcast_menu())

@router.message(F.text == "📈 Статистика рассылок")
async def broadcast_stats(message: Message):
    if not is_admin(message.from_user.id):
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT b.id, b.target, b.total_recipients,
                   (SELECT COUNT(*) FROM broadcast_receipts r WHERE r.broadcast_id = b.id) as receipts
            FROM broadcasts b
            ORDER BY b.created_at DESC
            LIMIT 5
        """) as cursor:
            recent = await cursor.fetchall()

    msg = "📤 Статистика рассылок:\n\n"
    for b_id, target, total_recipients, receipts in recent:
        rate = f"{receipts}/{total_recipients} ({receipts/total_recipients*100:.1f}%)" if total_recipients > 0 else "0/0"
        msg += f"ID {b_id} ({target}) — {rate} подтверждений\n"

    await message.answer(msg)

@router.message(AdminStates.waiting_for_broadcast_text)
async def send_broadcast(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    # Спрашиваем, отправить сейчас или отложить
    await state.update_data(
        original_message=message,
        broadcast_target=(await state.get_data())["broadcast_target"]
    )
    await state.set_state(AdminStates.waiting_for_broadcast_schedule)

    await message.answer(
        "🕗 Отправить сейчас или запланировать?\n\n"
        "Отправьте дату и время в формате: `ДД.ММ.ГГГГ ЧЧ:ММ`\n"
        "Или напишите «сейчас» для немедленной отправки.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="сейчас")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )

@router.message(AdminStates.waiting_for_broadcast_schedule)
async def handle_broadcast_schedule(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    data = await state.get_data()
    target = data["broadcast_target"]
    original_msg = data["original_message"]

    text = original_msg.text
    photo = original_msg.photo[-1].file_id if original_msg.photo else None
    doc = original_msg.document.file_id if original_msg.document else None
    caption = original_msg.caption

    if message.text.strip().lower() == "сейчас":
        # Отправляем немедленно (как раньше)
        user_ids = await get_broadcast_recipients(target)
        success = await send_immediate_broadcast(user_ids, original_msg)
        await message.answer(f"✅ Рассылка отправлена {success} из  {len(user_ids)} пользователей.")
    else:
        # Планируем
        try:
            dt = datetime.strptime(message.text, "%d.%m.%Y %H:%M")
            scheduled_at = dt.strftime("%Y-%m-%d %H:%M:%S")

            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("""
                    INSERT INTO broadcasts 
                    (target, message_text, photo_file_id, document_file_id, caption, scheduled_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (target, text, photo, doc, caption, scheduled_at))
                await db.commit()

            await message.answer(f"✅ Рассылка запланирована на {message.text}.")
        except ValueError:
            await message.answer("❌ Неверный формат даты. Попробуйте снова.")
            return

    await state.clear()

@router.message(AdminStates.waiting_for_user_search, F.text == "⬅️ Вернутся назад")
async def back_to_admin_menu_from_search(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("👮‍♂️ Панель администратора:", reply_markup=admin_menu())

@router.message(F.text == "👥 Пользователи")
async def admin_users(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    await message.answer(
        "👥 Введите ID пользователя или его @username (если известен), чтобы найти.\n"
        "Пример: `123456789` или `@ivan`",
        reply_markup=admin_back_keyboard()
    )
    await state.set_state(AdminStates.waiting_for_user_search)


@router.message(AdminStates.waiting_for_user_search)
async def search_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    query = message.text.strip()
    user_id = None
    username = None
    if query.isdigit():
        user_id = int(query)
    elif query.startswith("@"):
        username = query[1:]  # убираем @
    else:
        await message.answer("❌ Неверный формат. Используйте ID (123456) или @username.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        if user_id is not None:
            cursor = await db.execute("""
                SELECT 
                    u.user_id, u.username, u.role, u.car_brand, u.car_number, 
                    u.is_verified, u.verification_expires, u.is_banned, u.created_at,
                    (SELECT AVG(rating) FROM ratings WHERE target_id = u.user_id) as avg_rating
                FROM users u 
                WHERE user_id = ?
            """, (user_id,))
        else:
            cursor = await db.execute("""
                SELECT 
                    u.user_id, u.username, u.role, u.car_brand, u.car_number, 
                    u.is_verified, u.verification_expires, u.is_banned, u.created_at,
                    (SELECT AVG(rating) FROM ratings WHERE target_id = u.user_id) as avg_rating
                FROM users u 
                WHERE username = ?
            """, (username,))
        row = await cursor.fetchone()

    if not row:
        await message.answer("❌ Пользователь не найден.")
        return

    uid, uname, role, brand, number, is_verified, expires, is_banned, created_at, avg_rating = row
    rating_text = f"{round(avg_rating, 2)}" if avg_rating is not None else "—"

    # Проверка активности верификации
    is_verification_active = False
    if is_verified:
        if expires is None:
            is_verification_active = True
        else:
            from datetime import date
            try:
                expire_date = date.fromisoformat(expires)
                is_verification_active = expire_date >= date.today()
            except:
                is_verification_active = False

    verified_status = "✅ Активна" if is_verification_active else ("❌ Истекла" if is_verified else "❌ Нет")
    expires_text = expires if expires else "Бессрочно"
    created_text = created_at if created_at else "—"

    msg = (
        f"👤 <b>ID:</b> {uid}\n"
        f"🔖 <b>Username:</b> @{uname if uname else '—'}\n"
        f"🎭 <b>Роль:</b> {'🚗 Водитель' if role == 'driver' else '👤 Клиент'}\n"
        f"⭐ <b>Рейтинг:</b> {rating_text}\n"
        f"✅ <b>Верификация:</b> {verified_status}\n"
        f"📅 <b>Дата регистрации:</b> {created_text}\n"
    )
    if role == "driver":
        msg += f"🚘 <b>Авто:</b> {brand or '—'} {number or '—'}\n"
        msg += f"🗓 <b>До:</b> {expires_text}\n"

    # Кнопки
    buttons = []
    if role == "client":
        buttons.append([InlineKeyboardButton(text="🔄 Сделать водителем", callback_data=f"change_role_{uid}_driver")])
    else:
        buttons.append([InlineKeyboardButton(text="🔄 Сделать клиентом", callback_data=f"change_role_{uid}_client")])
    if role == "driver":
        buttons.append([InlineKeyboardButton(text="✏️ Изменить авто", callback_data=f"change_car_{uid}")])
        if is_verification_active:
            buttons.append([InlineKeyboardButton(text="🔁 Снять верификацию", callback_data=f"unverify_{uid}")])
        elif not is_verified or not is_verification_active:
            buttons.append([InlineKeyboardButton(text="✅ Верифицировать", callback_data=f"admin_verify_{uid}")])
    if is_banned:
        buttons.append([InlineKeyboardButton(text="🔓 Разблокировать", callback_data=f"unban_{uid}")])
    else:
        buttons.append([InlineKeyboardButton(text="🚫 Заблокировать", callback_data=f"ban_{uid}")])
    # buttons.append([InlineKeyboardButton(text="⭐ Изменить рейтинг", callback_data=f"edit_rating_{uid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users_back")])

    sent_msg = await message.answer(
        msg,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await message.answer("↩️ Введите новый ID или @username.", reply_markup=ReplyKeyboardRemove())
    await state.clear()

@router.message(Command("admin"))
async def admin_panel(message: Message):
    if not is_admin(message.from_user.id):
        # Не отвечаем ничего, если не админ (или можно отправить "нет доступа")
        return
    await message.answer("👮‍♂️ Панель администратора:", reply_markup=admin_menu())

@router.message(Command("panik"))
async def panic_support(message: Message):
    await message.answer("🛠 Выберите опцию:", reply_markup=support_keyboard())

@router.message(Command("disput"))
async def start_dispute(message: Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or "—"
    role = await get_user_role(user_id) or "—"
    await message.answer(
        "Здравствуйте, уважаемый пользователь!"
        "Вы открыли раздел <b>Диспут / Репорт</b>."
        "⚠️ Пожалуйста, укажите:"
        "• Номер заказа <b>ИЛИ</b> @username пользователя, с которым произошёл инцидент;"
        "• Подробно опишите ситуацию."
        "После отправки текста вы сможете <b>прикрепить скриншот</b> (опционально).",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )
    await state.set_state(DisputeStates.waiting_for_dispute_text)
    await state.update_data(
        user_id=user_id,
        username=username,
        role=role,
        timestamp=datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    )

@router.message(DisputeStates.waiting_for_dispute_text, F.text == "❌ Отмена")
async def cancel_dispute_text(message: Message, state: FSMContext):
    user_id = message.from_user.id
    await state.clear()
    role = await get_user_role(user_id)
    if role == "driver":
        shift_opened = await is_shift_opened(user_id)
        menu = driver_menu(shift_opened)
    else:
        menu = await get_client_menu_with_rating_and_status(user_id)
    await message.answer("❌ Обращение отменено.", reply_markup=menu)

@router.message(DisputeStates.waiting_for_dispute_text)
async def handle_dispute_text(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Пожалуйста, отправьте текстовое описание.")
        return

    user_text = message.text.strip()
    await state.update_data(dispute_text=user_text)

    await message.answer(
        "📸 Хотите прикрепить скриншот? Отправьте фото, нажмите «Пропустить» или «❌ Отмена».",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⏭ Пропустить")],
                [KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )
    await state.set_state(DisputeStates.waiting_for_dispute_photo)

@router.message(DisputeStates.waiting_for_dispute_photo, F.text == "❌ Отмена")
async def cancel_dispute_photo(message: Message, state: FSMContext):
    user_id = message.from_user.id
    await state.clear()
    role = await get_user_role(user_id)
    if role == "driver":
        shift_opened = await is_shift_opened(user_id)
        menu = driver_menu(shift_opened)
    else:
        menu = await get_client_menu_with_rating_and_status(user_id)
    await message.answer("❌ Обращение отменено.", reply_markup=menu)

@router.message(DisputeStates.waiting_for_dispute_photo, F.text == "⏭ Пропустить")
async def skip_photo(message: Message, state: FSMContext):
    await state.update_data(photo_file_id=None)
    await send_dispute_to_admins(message, state)


@router.message(DisputeStates.waiting_for_dispute_photo, F.photo)
async def handle_dispute_photo(message: Message, state: FSMContext):
    photo_file_id = message.photo[-1].file_id
    await state.update_data(photo_file_id=photo_file_id)
    await send_dispute_to_admins(message, state)


@router.message(DisputeStates.waiting_for_dispute_photo)
async def invalid_photo_input(message: Message):
    await message.answer(
        "⚠️ Пожалуйста, отправьте фото <b>или</b> нажмите «Пропустить».",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⏭ Пропустить")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )

async def send_dispute_to_admins(message: Message, state: FSMContext):
    data = await state.get_data()
    user_id = data["user_id"]
    username = data["username"]
    role = data["role"]
    timestamp = data["timestamp"]
    user_text = data["dispute_text"]
    photo_file_id = data.get("photo_file_id")

    # Формируем текст для админа
    admin_message = (
        "🚨 <b>Новое обращение в Диспут / Репорт</b>\n"
        f"👤 <b>От кого:</b> ID {user_id} (@{username})\n"
        f"🎭 <b>Роль:</b> {role}\n"
        f"📅 <b>Дата и время:</b> {timestamp}\n"
        f"💬 <b>Сообщение:</b>\n{user_text}\n"
    )

    # Попытка найти номер заказа
    import re
    order_id_match = re.search(r'#?(\d+)', user_text)
    if order_id_match:
        try:
            order_id = int(order_id_match.group(1))
            order = await get_order(order_id)
            if order:
                client_id, driver_id, pickup, dropoff, comment, status, created_at = order["client_id"], order["driver_id"], "pickup_address", order["dropoff_address"], order["comment"], order["status"], order["created_at"]
                admin_message += (
                    f"📦 <b>Данные по заказу #{order_id}:</b>\n"
                    f"   Статус: {status}\n"
                    f"   Откуда: {pickup}\n"
                    f"   Куда: {dropoff}\n"
                    f"   Комментарий: {comment or '—'}\n"
                    f"   Создан: {created_at}\n"
                    f"   Клиент: {client_id}\n"
                    f"   Водитель: {driver_id or '—'}\n"
                )
        except Exception as e:
            logging.warning(f"Ошибка при получении данных заказа в disput: {e}")

    admin_message += "⚠️ Пожалуйста, примите меры."

    sent_to_any = False
    for admin_id in ADMINS:
        try:
            if photo_file_id:
                await bot.send_photo(
                    admin_id,
                    photo=photo_file_id,
                    caption=admin_message,
                    parse_mode="HTML"
                )
            else:
                await bot.send_message(admin_id, admin_message, parse_mode="HTML")
            sent_to_any = True
        except Exception as e:
            logging.error(f"Не удалось отправить disput админу {admin_id}: {e}")

    # === 🔥 ВОЗВРАЩАЕМ ОСНОВНОЕ МЕНЮ ПОЛЬЗОВАТЕЛЮ ===
    if sent_to_any:
        await message.answer("✅ Ваше обращение отправлено администратору. Спасибо!")
    else:
        await message.answer("❌ Не удалось отправить сообщение. Попробуйте позже.")

    # Определяем роль и возвращаем меню
    user_role = await get_user_role(user_id)
    if user_role == "driver":
        shift_opened = await is_shift_opened(user_id)
        menu = driver_menu(shift_opened)
    else:
        menu = await get_client_menu_with_rating_and_status(user_id)

    await message.answer("📍 Главное меню:", reply_markup=menu)
    await state.clear()

@router.message(F.text == "🔐 Верификация")
async def verification_menu(message: Message):
    if not is_admin(message.from_user.id):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, username, car_brand, car_number FROM users 
            WHERE role = 'driver' AND is_verified = 0
        """) as cursor:
            drivers = await cursor.fetchall()
    if not drivers:
        await message.answer("✅ Нет водителей на верификации.")
        return

    for driver_id, username, brand, number in drivers:
        # 1. Отправляем текстовое сообщение БЕЗ кнопок
        username_display = f"@{username}" if username else "-"
        sent_msg = await message.answer(
            f"Авто: {brand or '—'} {number or '—'} \n Водитель: ID {driver_id} ({username_display})"
        )
        # 2. Добавляем кнопки к этому сообщению
        await sent_msg.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"verify_driver_{driver_id}_{sent_msg.message_id}")],
                [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_driver_{driver_id}_{sent_msg.message_id}")]
            ])
        )

# --- СТАРТ ---
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username
    user = await get_user(user_id)

    if user:
        # Уже зарегистрирован — обычное приветствие
        await save_user(user_id, username=username)
        role = await get_user_role(user_id)
        if role == "driver":
            shift_open = await is_shift_opened(user_id)
            await message.answer("Вы вошли как 🚗 Водитель.", reply_markup=driver_menu(shift_open))
        else:
            await message.answer("Вы вошли как 👤 Клиент.", reply_markup=await get_client_menu_with_rating_and_status(user_id))
    else:
        # НОВЫЙ ПОЛЬЗОВАТЕЛЬ
        await save_user(user_id, username=username)

        # Показываем соглашение
        agreement_text = (
            "⚠️ <b>ВНИМАНИЕ!</b>\n"
            'Перед началом использования ознакомьтесь с '
            '<a href="https://taxibarsnz24.ru/agreement.html">пользовательским соглашением</a>.\n'
            "При использовании сервиса вы автоматически даёте согласие на обработку данных."
        )
        await message.answer(
            agreement_text,
            parse_mode="HTML",
            disable_web_page_preview=True
        )

        # Проверяем, разрешена ли роль водителя
        driver_role_enabled = await get_setting("driver_role_enabled", "1") == "1"

        if driver_role_enabled:
            # Даём выбор
            await message.answer(
                "👋 Добро пожаловать в ТаксиБарс!\nВыберите роль:",
                reply_markup=start_keyboard()
            )
        else:
            # Автоматически делаем клиентом
            await save_user(user_id, role="client", username=username)
            await message.answer("✅ Вы зарегистрированы как клиент.", reply_markup=await get_client_menu_with_rating_and_status(user_id))

# --- ВЫБОР РОЛИ ---
@router.message(F.text.in_({"🚗 Водитель", "👤 Клиент"}))
async def choose_role(message: Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username
    role = "driver" if "Водитель" in message.text else "client"
    await save_user(user_id, role=role, username=username)

    if role == "driver":
        car_info = await get_driver_info(user_id)
        if not car_info or not car_info[0]:
            await message.answer("Пожалуйста, укажите марку и госномер автомобиля в формате:\n\n*МАРКА ГОСНОМЕР (Toyota A123BC)*", parse_mode="Markdown")
            await state.set_state(DriverStates.waiting_for_car)
        else:
            shift_open = await is_shift_opened(user_id)
            await message.answer("✅ Вы зарегистрированы как водитель. \n 📎 Для начала работы необходима верификация администрацией сервиса!", reply_markup=driver_menu(shift_open))
    else:
        await message.answer("✅ Вы зарегистрированы как клиент.", reply_markup=await get_client_menu_with_rating_and_status(user_id))

# --- ВВОД АВТОМОБИЛЯ ---
@router.message(DriverStates.waiting_for_car)
async def enter_car_info(message: Message, state: FSMContext):
    user_id = message.from_user.id
    text = message.text.strip()

    # Регулярное выражение для поиска госномера в формате РФ
    # Поддерживаем: А123БВ, А123БВ77, Е901КХ150, Т555ТТ и т.п.
    plate_pattern = r'\b([АВЕКМНОРСТУХ]\d{3}[АВЕКМНОРСТУХ]{2}(?:\d{2,3})?)\b'
    match = re.search(plate_pattern, text.upper())

    if not match:
        await message.answer(
            "❌ Неверный формат госномера.\n"
            "Поддерживаются только российские номера: <b>А123БВ77</b>, <b>Т555ТТ</b> и т.п.\n"
            "Пример: <code>Toyota A123BC77</code>",
            parse_mode="HTML"
        )
        return

    plate = match.group(1)
    # Всё до номера — это марка (удаляем лишние пробелы)
    brand_part = text[:match.start()].strip()

    if not brand_part:
        await message.answer("❌ Укажите марку автомобиля перед госномером.\nПример: <code>Toyota A123BC77</code>", parse_mode="HTML")
        return

    # Опционально: проверим, что марка содержит хотя бы одну букву
    if not re.search(r'[a-zA-Zа-яА-Я]', brand_part):
        await message.answer("❌ Марка должна содержать буквы.\nПример: <code>Hyundai A123BC</code>", parse_mode="HTML")
        return

    # Сохраняем данные
    await save_car_info(user_id, brand_part, plate)
    shift_open = await is_shift_opened(user_id)
    await message.answer("✅ Данные автомобиля сохранены.", reply_markup=driver_menu(shift_open))
    await state.clear()

# --- МЕНЮ ВОДИТЕЛЯ ---
@router.message(F.text == "✅ Открыть смену")
async def open_shift_start(message: Message, state: FSMContext):
    if await is_user_banned(message.from_user.id):
        await message.answer("❌ Ваш аккаунт заблокирован. Обратитесь в поддержку: @AnatolyElizarev @azimut301")
        return

    user_id = message.from_user.id
    if not await is_driver_verified(user_id):
        await message.answer(
            "⚠️ Ваш аккаунт водителя не верифицирован или срок верификации истёк."
            "Обратитесь в поддержку: @AnatolyElizarev @azimut301"
        )
        return

    # Проверяем, включена ли функция штурмана
    co_driver_enabled = await get_setting("co_driver_enabled", "1") == "1"

    if co_driver_enabled:
        # Даём выбор
        await message.answer(
            "👥 Будете работать один или с штурманом (пассажиром)?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👤 Один", callback_data="shift_alone")],
                [InlineKeyboardButton(text="👥 С штурманом", callback_data="shift_with_co")]
            ])
        )
        await state.set_state(DriverStates.opening_shift)
    else:
        # Автоматически открываем смену в одиночку
        await set_shift(user_id, True, has_co_driver=0)
        await message.answer("🟢 Смена открыта! Вы работаете в одиночку.", reply_markup=driver_menu(True))

@router.message(F.text == "🔴 Закрыть смену")
async def close_shift(message: Message):
    user_id = message.from_user.id
    await set_shift(user_id, False, has_co_driver=0)
    await message.answer("🔴 Смена закрыта.", reply_markup=driver_menu(False))

@router.message(F.text == "🛠 Техническая поддержка")
async def support(message: Message):
    await message.answer("🛠 Выберите опцию:", reply_markup=support_keyboard())

@router.callback_query(F.data == "open_disput_from_support")
async def open_disput_from_support(callback: CallbackQuery, state: FSMContext):
    # Повторяем логику команды /disput
    user_id = callback.from_user.id
    username = callback.from_user.username or "—"
    role = await get_user_role(user_id) or "—"
    await callback.message.answer(
        "Здравствуйте, уважаемый пользователь!"
        "Вы открыли раздел <b>Диспут / Репорт</b>.\n"
        "⚠️ Пожалуйста, укажите:\n"
        "• Номер заказа <b>ИЛИ</b> @username пользователя, с которым произошёл инцидент;\n"
        "• Подробно опишите ситуацию.\n"
        "После отправки текста вы сможете <b>прикрепить скриншот</b> (опционально).",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )
    await state.set_state(DisputeStates.waiting_for_dispute_text)
    await state.update_data(
        user_id=user_id,
        username=username,
        role=role,
        timestamp=datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    )
    await callback.answer()

@router.callback_query(DriverStates.opening_shift, F.data == "shift_alone")
@router.callback_query(DriverStates.opening_shift, F.data == "shift_with_co")
async def confirm_shift_open(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    has_co = 1 if callback.data == "shift_with_co" else 0
    await set_shift(user_id, True, has_co_driver=has_co)  # ← обновлённая функция
    await state.clear()
    status = "с штурманом" if has_co else "в одиночку"
    await callback.message.delete()
    await callback.message.answer(f"🟢 Смена открыта! Вы работаете {status}.", reply_markup=driver_menu(True))
    await callback.answer()

# --- МЕНЮ КЛИЕНТА ---
@router.message(F.text.startswith("🚕 Сделать заказ"))
async def make_order_start(message: Message, state: FSMContext):
    user_id = message.from_user.id

    # 🔥 Проверка cooldown
    if not is_order_allowed(user_id):
        remaining = int(COOLDOWN_SECONDS - (datetime.now().timestamp() - order_cooldown[user_id]))
        await message.answer(
            f"⏳ Вы можете создать новый заказ через {remaining} секунд.\n"
            "Это нужно, чтобы не перегружать водителей."
        )
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM orders WHERE client_id = ? AND status IN ('pending', 'accepted')",
            (user_id,)
        ) as cursor:
            active_order = await cursor.fetchone()

            if active_order:
                await message.answer("⚠️ У вас уже есть активный заказ. Дождитесь его завершения или отмените.")
                return

    await message.answer(
        "📍 Введите адрес подачи и назначения в одном сообщении, например:\n"
        "*Улица Ленина, 10 → Проспект Мира, 25* \n"
        "Можно добавить комментарий через новую строку, например: \n *Нужен заезд или детское кресло* \n\n"
        "Или нажмите «❌ Назад», чтобы отменить.",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Назад")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )
    await state.set_state(ClientStates.waiting_for_order)

@router.message(ClientStates.waiting_for_order, F.text == "❌ Назад")
async def cancel_order_creation(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    role = await get_user_role(user_id)

    if  role == "client":
        await message.answer("✅ Создание заказа отменено.", reply_markup=await get_client_menu_with_rating_and_status(user_id))
    elif role == "driver":
        shift_opened = await is_shift_opened(user_id)
        await message.answer("✅ Создание заказа отменено.", reply_markup=driver_menu(shift_opened))
    else:
        await message.answer("✅ Создание заказа отменено.", reply_markup=start_keyboard())

def parse_addresses(text: str):
    """
    Гибко парсит текст и возвращает (pickup, dropoff) или (None, None).
    Поддерживает разделители: →, ->, -, —, :, ;, \n, или просто два блока через пробел.
    """
    text = text.strip()
    if not text:
        return None, None

    # Удаляем лишние пробелы и нормализуем
    text = re.sub(r'\s+', ' ', text)

    # Попробуем разделители в порядке приоритета
    separators = [
        r'\s*→\s*',      # →
        r'\s*->\s*',     # ->
        r'\s*[-—:;]\s*', # -, —, :, ;
        r'\n\s*',        # новая строка
    ]

    for sep in separators:
        parts = re.split(sep, text, maxsplit=1)
        if len(parts) == 2 and parts[0].strip() and parts[1].strip():
            return parts[0].strip(), parts[1].strip()

    # Если не сработало — попробуем разделить по "половине слов"
    words = text.split()
    if len(words) >= 4:
        mid = len(words) // 2
        pickup = " ".join(words[:mid]).strip()
        dropoff = " ".join(words[mid:]).strip()
        if pickup and dropoff:
            return pickup, dropoff

    return None, None

@router.callback_query(ClientStates.waiting_for_passengers, F.data.startswith("passengers_"))
async def handle_passenger_count(callback: CallbackQuery, state: FSMContext):
    try:
        count = int(callback.data.split("_")[1])
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка выбора.", show_alert=True)
        return

    user_id = callback.from_user.id

    # 🔥 Проверка cooldown
    if not is_order_allowed(user_id):
        remaining = int(COOLDOWN_SECONDS - (datetime.now().timestamp() - order_cooldown.get(user_id, 0)))
        await callback.answer(f"⏳ Подождите {remaining} сек.", show_alert=True)
        await state.clear()
        return

    # Проверка активного заказа
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM orders WHERE client_id = ? AND status IN ('pending', 'accepted')",
            (user_id,)
        ) as cursor:
            if await cursor.fetchone():
                await callback.answer("⚠️ У вас уже есть активный заказ.", show_alert=True)
                await state.clear()
                return

    # 🔥 Получаем ВСЕ данные из FSM, включая комментарий
    data = await state.get_data()
    pickup = data.get("pickup")
    dropoff = data.get("dropoff")
    comment = data.get("comment", "").strip()  # ← вот он!

    if not pickup or not dropoff:
        await callback.answer("❌ Не удалось восстановить адреса заказа.", show_alert=True)
        await state.clear()
        return

    # ✅ Теперь создаём заказ с комментарием
    order_id = await create_order(user_id, pickup, dropoff, comment)

    await notify_new_order_in_group(bot, order_id)
    set_order_cooldown(user_id)

    drivers = await get_drivers_with_open_shift()
    if not drivers:
        await cancel_order_with_reason(order_id, "no_drivers")
        await callback.message.edit_text("⚠️ Нет свободных водителей. Заказ отменён.")
        await callback.message.answer("📍 Главное меню:", reply_markup=await get_client_menu_with_rating_and_status(user_id))
        await state.clear()
        await callback.answer()
        return

# # ============================== ГЕНЕРАЦИЯ ИЗОБРАЖЕНИЯ =====================================
#     client_rating = await get_user_rating(user_id) or 0.0
#     rides = await get_monthly_rides(user_id)
#     status_name, status_emoji = get_client_status(rides)
#
#     card_image = generate_modern_order_card(
#
#         order_id=order_id,
#         pickup=pickup,
#         dropoff=dropoff,
#         comment=comment,
#         passengers=count,
#         client_rating=round(client_rating or 0.0, 1),
#         client_status=status_name,
#         status_emoji=status_emoji
#     )
#
#     for driver_id in drivers:
#         try:
#             await bot.send_photo(
#                 driver_id,
#                 photo=BufferedInputFile(card_image.read(), filename=f"order_{order_id}.png"),
#                 caption="🆕 Новый заказ! Нажмите «Откликнуться», чтобы взять.",
#                 reply_markup=accept_order_button(order_id)
#             )
#
#             card_image.seek(0)  # ← обязательно!
#         except Exception as e:
#             logging.warning(f"Не удалось отправить карточку водителю {driver_id}: {e}")
# # ======================================================================================



#=========================== СТАНДАРТНЫЙ ТЕКСТОВЫЙ =============================
 #   Отправляем водителям с комментарием
    client_rating = await get_user_rating(user_id)
    rides = await get_monthly_rides(user_id)
    status_name, status_emoji = get_client_status(rides)
    client_status_line = f"{status_emoji} Статус клиента: <b>{status_name}</b>"

    for driver_id in drivers:
        try:
            sent_msg = await bot.send_message(
                driver_id,
                f"🔥 Новый заказ №<b>{order_id}</b>!\n\n"
                f"📍 Откуда: <b>{pickup}</b>\n"
                f"🏁 Куда: <b>{dropoff}</b>\n"
                f"📝 Комментарий: {comment if comment else '—'}\n"
                f"👥 Пассажиров: <b>{count}</b>\n\n"
                f"⭐ Рейтинг клиента: <b>{client_rating}</b>\n{client_status_line}",
                reply_markup=accept_order_button(order_id), parse_mode="HTML"
            )
            await save_driver_order_message(order_id, driver_id, sent_msg.chat.id, sent_msg.message_id)
        except Exception as e:
            logging.warning(f"Не удалось отправить заказ водителю {driver_id}: {e}")
#===========================================================================================================

    # Запускаем таймер
    if order_id not in unclaimed_tasks:
        task = asyncio.create_task(auto_cancel_unclaimed_order(order_id))
        unclaimed_tasks[order_id] = task
        logging.info(f"[handle_passenger_count] Запущен unclaimed-таймер для {order_id}")

    await state.clear()
    await callback.message.delete()

    role = await get_user_role(user_id)
    menu = driver_menu(await is_shift_opened(user_id)) if role == "driver" else await get_client_menu_with_rating_and_status(user_id)
    await callback.message.answer("✅ Ваш заказ отправлен водителям!", reply_markup=menu)
    await callback.answer()

@router.message(ClientStates.waiting_for_order)
async def process_order(message: Message, state: FSMContext):
    if await is_user_banned(message.from_user.id):
        await message.answer("❌ Ваш аккаунт заблокирован. Обратитесь в поддержку: @AnatolyElizarev @azimut301")
        return

    user_id = message.from_user.id
    text = message.text.strip()

    # Извлекаем комментарий
    comment = ""
    if "\n" in text:
        main_part, comment = text.split("\n", 1)
    else:
        main_part = text

    pickup, dropoff = parse_addresses(main_part)
    if not pickup or not dropoff:
        await message.answer(
            "❌ Не удалось определить адреса.\n"
            "Пожалуйста, укажите **откуда и куда** в одном из форматов:\n"
            "• `Ленина, 11 → Мира, 25`\n"
            "• `Ленина 11 Мира 25`\n"
            "• `Ул. Ленина д.11\nПр. Мира д.25`\n"
            "Можно добавить комментарий через пустую строку."
        )
        return

    # Сохраняем данные в FSM и переходим к выбору пассажиров
    await state.update_data(pickup=pickup, dropoff=dropoff, comment=comment.strip())
    await state.set_state(ClientStates.waiting_for_passengers)

    await message.answer(
        "👥 Сколько пассажиров будет ехать?",
        reply_markup=passengers_keyboard()
    )

@router.callback_query(DriverStates.waiting_for_arrival_time, F.data.startswith("arrival_time_"))
async def handle_arrival_time_inline(callback: CallbackQuery, state: FSMContext):
    try:
        minutes = int(callback.data.split("_")[2])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка выбора времени.", show_alert=True)
        return

    # Создаём MockMessage, НО НЕ удаляем сообщение и НЕ перезаписываем FSM
    class MockMessage:
        def __init__(self, text, chat):
            self.text = text
            self.chat = chat
            self.from_user = callback.from_user
        async def answer(self, *args, **kwargs):
            return await bot.send_message(self.chat.id, *args, **kwargs)

    mock_msg = MockMessage(str(minutes), callback.message.chat)
    await handle_arrival_time(mock_msg, state)
    await callback.answer()

@router.message(DriverStates.waiting_for_arrival_time)
async def handle_arrival_time(message: Message, state: FSMContext):
    driver_id = message.from_user.id
    try:
        minutes = int(message.text.strip())
        if minutes < 1 or minutes > 60:
            raise ValueError
    except:
        await message.answer("❌ Введите целое число от 1 до 60.")
        return

    data = await state.get_data()
    order_id = data["order_id"]

    # 🔑 Используем .get(), чтобы избежать KeyError
    arrival_time_msg_id = data.get("arrival_time_message_id")

    # ✅ Удаляем сообщение с кнопками выбора времени (если оно есть)
    if arrival_time_msg_id:
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=arrival_time_msg_id)
        except Exception as e:
            logging.warning(f"Не удалось удалить сообщение с выбором времени: {e}")

    success = await create_bid(order_id, driver_id, minutes)
    if not success:
        await message.answer("✅ Вы уже откликнулись на этот заказ.")
        await state.clear()
        return

    order = await get_order(order_id)
    if not order or order["status"] != "pending":
        await message.answer("❌ Заказ уже закрыт.")
        await state.clear()
        return

    client_id = order["client_id"]

    # Отменяем unclaimed-таймер
    task = unclaimed_tasks.pop(order_id, None)
    if task:
        task.cancel()

    # === Автообновление списка водителей у клиента ===
    # === Проверка авто-принятия при первом отклике ===
    auto_accept_enabled = await get_setting("auto_accept_on_first_bid", "0") == "1"
    bids = await get_bids_for_order(order_id)

    if auto_accept_enabled and len(bids) == 1:
        first_driver_id = bids[0][0]
        success = await accept_bid(order_id, first_driver_id)
        if success:
            # Отменяем unclaimed и selection таймеры
            for task_dict in [unclaimed_tasks, selection_tasks]:
                task = task_dict.pop(order_id, None)
                if task:
                    task.cancel()

            # Запускаем stale-таймер
            if order_id not in stale_tasks:
                task = asyncio.create_task(auto_cancel_stale_order(order_id))
                stale_tasks[order_id] = task

            # 🔥 Повторно получаем актуальные данные заказа
            order = await get_order(order_id)
            if not order:
                logging.error(f"[auto_accept] Заказ {order_id} исчез после accept_bid")
                return

            client_id = order["client_id"]
            comment_text = (order["comment"] or "").strip()
            comment_block = f"\n📝 Комментарий: {comment_text}" if comment_text else ""

            # === 1. Получаем данные водителя ===
            car_info = await get_driver_info(first_driver_id)
            car_text = f"{car_info[0]} {car_info[1]}" if car_info else "Не указано"
            rating = await get_driver_rating(first_driver_id)

            # === 2. Отправляем клиенту сообщение С КОММЕНТАРИЕМ ===
            try:
                menu_msg = await bot.send_message(
                    client_id,
                    f"✅ Водитель автоматически выбран!\n"
                    f"🚗 {car_text}\n"
                    f"⭐ {rating}\n"
                    f"📍 {order['pickup_address']} → {order['dropoff_address']}"
                    f"{comment_block}",
                    reply_markup=client_order_menu(first_driver_id, order_id)
                )
                client_order_messages[order_id] = (menu_msg.chat.id, menu_msg.message_id)
            except TelegramBadRequest as e:
                logging.error(f"[auto_accept] Не удалось отправить меню клиенту {client_id}: {e}")
                # Опционально: отменить заказ, если клиент недоступен
                await cancel_order_with_reason(order_id, "client_unreachable")
                return
            except Exception as e:
                logging.error(f"[auto_accept] Неизвестная ошибка при отправке клиенту {client_id}: {e}")
                return

            # === 3. Отправляем водителю сообщение С КОММЕНТАРИЕМ ===
            try:
                await bot.send_message(
                    first_driver_id,
                    f"✅ Клиент выбрал вас для заказа!\n"
                    f"📍 {order['pickup_address']} → {order['dropoff_address']}"
                    f"{comment_block}",
                    reply_markup=driver_order_menu(client_id, order_id)
                )
            except Exception as e:
                logging.warning(f"[auto_accept] Не удалось уведомить водителя {first_driver_id}: {e}")

            # === 4. Удаляем сообщения у всех водителей ===
            messages_to_delete = await get_driver_order_messages(order_id)
            for chat_id, msg_id, driver_id in messages_to_delete:
                try:
                    await bot.delete_message(chat_id=chat_id, message_id=msg_id)
                except:
                    pass
            await delete_driver_order_messages(order_id)

            logging.info(f"[auto_accept] Заказ {order_id} автоматически принят за водителем {first_driver_id}")
        else:
            logging.warning(f"[auto_accept] Не удалось автоматически принять заказ {order_id}")
    else:
        # Стандартное поведение: показываем список или уведомление
        source = order["source"]
        if len(order) > 9:  # Индекс source — 9
            source = order['source']
        if len(bids) == 1 and source == "telegram":
            comment_text = order["comment"].strip() if order["comment"] else ""
            await bot.send_message(
                client_id,
                f"🆕 Нашёлся водитель на ваш заказ! \n"
                f"📍{order['pickup_address']} → {order['dropoff_address']} \n"
                f"📝 Комментарий: {comment_text} \n" if comment_text else ""
                f"⏳ У вас есть 2 минуты, чтобы выбрать водителя.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="👥 Показать водителей", callback_data=f"show_bids_{order_id}")]
                ])
            )
        else:
            if order_id in client_bid_messages:
                chat_id, msg_id = client_bid_messages[order_id]
                buttons = []
                for d_id, brand, number, arr_min, has_co in bids:
                    r = await get_driver_rating(d_id)
                    name = f"{brand} {number} ⭐{r} ({arr_min} мин)"
                    buttons.append([InlineKeyboardButton(text=name, callback_data=f"select_driver_{order_id}_{d_id}")])
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=msg_id,
                        text="🚕 Выберите водителя:",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
                    )
                except Exception as e:
                    logging.warning(f"Автообновление списка водителей не удалось: {e}")
                    client_bid_messages.pop(order_id, None)

        # Запускаем таймер выбора (если ещё не запущен)
        if order_id not in selection_tasks:
            task = asyncio.create_task(auto_cancel_order(order_id))
            selection_tasks[order_id] = task

    await message.answer("✅ Ваша заявка отправлена. Ожидайте выбора клиента.")
    await state.clear()

# --- ПРИНЯТИЕ ЗАКАЗА ---
@router.callback_query(F.data.startswith("accept_"))
async def handle_accept_order(callback: CallbackQuery, state: FSMContext):
    driver_id = callback.from_user.id
    order_id = int(callback.data.split("_")[1])
    # Проверяем, есть ли у водителя активный заказ
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM orders WHERE driver_id = ? AND status = 'accepted'",
            (driver_id,)
        ) as cursor:
            has_active_order = await cursor.fetchone()
    if has_active_order:
        await callback.answer("⚠️ У вас уже есть активный заказ. Завершите его, чтобы взять новый.", show_alert=True)
        return
    # Получаем заказ, чтобы узнать ID клиента
    order = await get_order(order_id)
    if not order or order["status"] != "pending":
        await callback.answer("❌ Заказ уже закрыт.", show_alert=True)
        return
    client_id = order["client_id"]
    if driver_id == client_id:
        await callback.answer("❌ Нельзя брать свой заказ.", show_alert=True)
        return

    # Сохраняем данные
    await state.update_data(order_id=order_id, client_id=client_id)

    # Отправляем клавиатуру и сохраняем message_id
    sent_msg = await callback.message.answer(
        "⏱ Укажите, через сколько минут вы сможете быть у клиента: \n"
        "📌Если нет нужного ответа - отправьте его в чат (целое число, например: 5).",
        reply_markup=arrival_time_inline_keyboard()
    )
    await state.update_data(arrival_time_message_id=sent_msg.message_id)  # ← КЛЮЧЕВАЯ СТРОКА
    await state.set_state(DriverStates.waiting_for_arrival_time)
    await callback.answer()

# --- ЗАВЕРШЕНИЕ И ОТМЕНА ---
@router.callback_query(F.data.startswith("complete_"))
async def complete_order_handler(callback: CallbackQuery, state: FSMContext):
    order_id = int(callback.data.split("_")[1])
    driver_id = callback.from_user.id
    order = await get_order(order_id)
    if not order:
        await callback.answer("❌ Заказ не найден.", show_alert=True)
        return
    actual_driver_id = order["driver_id"]
    actual_status = order["status"]
    if actual_driver_id != driver_id:
        await callback.answer(f"❌ Это не ваш заказ. (Водитель: {actual_driver_id})", show_alert=True)
        return
    if actual_status != "accepted":
        await callback.answer(f"❌ Заказ в статусе '{actual_status}', ожидался 'accepted'.", show_alert=True)
        return
    client_id = order["client_id"]

    if order_id in driver_order_messages:
        del driver_order_messages[order_id]

    # === 🔥 Удаляем/обновляем меню заказа у КЛИЕНТА ===
    if order_id in client_order_messages:
        chat_id, msg_id = client_order_messages[order_id]
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text="✅ Заказ завершён. Спасибо, что выбрали ТаксиБарс!",
                reply_markup=None
            )
        except Exception as e:
            logging.warning(f"Не удалось обновить меню заказа у клиента {client_id}: {e}")
        del client_order_messages[order_id]

    # Завершаем заказ в БД
    await complete_order(order_id)
    await increment_monthly_rides(client_id) #Бонусная программа

    # Водитель уже в процессе оценки — запрашиваем оценку клиента
    await callback.message.edit_text(f"🏁 Заказ №{order_id} завершён. Оцените клиента:")
    await callback.message.answer("Поставьте оценку клиенту от 1 до 5:", reply_markup=rating_keyboard(client_id, order_id))
    await state.set_state(DriverStates.waiting_for_rating)
    await state.update_data(order_id=order_id, target_id=client_id)

    # Отменяем stale-таймер
    task = stale_tasks.pop(order_id, None)
    if task is not None:
        try:
            task.cancel()
            logging.info(f"[complete] Отменен stale-таймер для {order_id}")
        except Exception as e:
            logging.info(f"[complete] Ошибка при отмене задачи для {order_id}: {e}")

    # Отправляем запрос на оценку ВОДИТЕЛЯ клиенту
    already_client_rated = await has_user_rated(order_id, client_id)
    if not already_client_rated:
        # 🔥 Определяем источник заказа
        source = order.get("source", "telegram")
        if source == "telegram":
            try:
                await bot.send_message(
                    client_id,
                f"🏁 Заказ №{order_id} завершён! Пожалуйста, оцените водителя:",
                    reply_markup=rating_keyboard(driver_id, order_id)
                )
            except Exception as e:
                logging.warning(f"Не удалось отправить запрос на оценку клиенту {client_id}: {e}")

    # Отправляем партнёрскую рекламу
    asyncio.create_task(send_partner_ad(client_id))
    asyncio.create_task(send_partner_ad(driver_id))
    await callback.answer()

@router.callback_query(F.data.startswith("rate_"))
async def handle_rating(callback: CallbackQuery, state: FSMContext):
    try:
        data = callback.data.split("_")
        if len(data) != 4:
            await callback.answer("❌ Неверные данные.", show_alert=True)
            return
        target_id = int(data[1])
        order_id = int(data[2])
        rating = int(data[3])
        rater_id = callback.from_user.id

        if await has_user_rated(order_id, rater_id):
            await callback.answer("✅ Вы уже поставили оценку по этому заказу.", show_alert=True)
            await callback.message.delete()
            return

        if not (1 <= rating <= 5):
            await callback.answer("❌ Оценка должна быть от 1 до 5.", show_alert=True)
            return

        # Сохраняем оценку сразу — чтобы избежать повторной отправки
        await save_rating(order_id, rater_id, target_id, rating)

        if rating <= 3:
            # Запрашиваем комментарий
            await state.update_data(
                order_id=order_id,
                rater_id=rater_id,
                target_id=target_id,
                rating=rating
            )
            await state.set_state(RatingStates.waiting_for_low_rating_comment)
            await callback.message.edit_text(
                f"⭐ Вы поставили оценку {rating}. Пожалуйста, кратко укажите причину (1–2 предложения):"
            )
            await callback.answer()
        else:
            # Оценка 4–5 — завершаем без комментария
            await finalize_rating_flow(callback.message, rater_id, rating)
            await callback.answer()

    except Exception as e:
        logging.error(f"Ошибка при установке рейтинга: {e}")
        await callback.answer("❌ Произошла ошибка. Попробуйте позже.", show_alert=True)

@router.message(RatingStates.waiting_for_low_rating_comment)
async def handle_low_rating_comment(message: Message, state: FSMContext):
    comment = message.text.strip()
    if not comment:
        await message.answer("⚠️ Пожалуйста, введите текстовое объяснение.")
        return

    data = await state.get_data()
    order_id = data["order_id"]
    rater_id = data["rater_id"]
    target_id = data["target_id"]
    rating = data["rating"]

    # 1. Отправляем комментарий **целевому пользователю**
    try:
        await bot.send_message(
            target_id,
            f"💬 Пользователь оставил комментарий к оценке {rating} за заказ №{order_id}:\n{comment}"
        )
    except Exception as e:
        logging.warning(f"Не удалось отправить комментарий к оценке пользователю {target_id}: {e}")

    # 2. Отправляем **копию администраторам**
    rater_role = await get_user_role(rater_id) or "—"
    target_role = await get_user_role(target_id) or "—"
    rater_username = (await bot.get_chat(rater_id)).username or "—"
    target_username = (await bot.get_chat(target_id)).username or "—"

    admin_msg = (
        f"⚠️ <b>Комментарий к низкой оценке</b>\n"
        f"Заказ: #{order_id}\n"
        f"Оценил: ID {rater_id} (@{rater_username}) — {rater_role}\n"
        f"Получил: ID {target_id} (@{target_username}) — {target_role}\n"
        f"Оценка: {rating}\n"
        f"Комментарий:\n{comment}"
    )

    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, admin_msg, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Не удалось отправить комментарий админу {admin_id}: {e}")

    # 3. Завершаем flow для оценившего
    await finalize_rating_flow(message, rater_id, rating)
    await state.clear()

@router.message(RatingStates.waiting_for_low_rating_comment)
async def handle_low_rating_comment(message: Message, state: FSMContext):
    comment = message.text.strip()
    if not comment:
        await message.answer("⚠️ Пожалуйста, введите текстовое объяснение.")
        return

    data = await state.get_data()
    order_id = data["order_id"]
    rater_id = data["rater_id"]
    target_id = data["target_id"]
    rating = data["rating"]

    # 1. Отправляем комментарий **целевому пользователю**
    try:
        await bot.send_message(
            target_id,
            f"💬 Пользователь оставил комментарий к оценке {rating} за заказ №{order_id}:\n{comment}"
        )
    except Exception as e:
        logging.warning(f"Не удалось отправить комментарий к оценке пользователю {target_id}: {e}")

    # 2. Отправляем **копию администраторам**
    rater_role = await get_user_role(rater_id) or "—"
    target_role = await get_user_role(target_id) or "—"
    rater_username = (await bot.get_chat(rater_id)).username or "—"
    target_username = (await bot.get_chat(target_id)).username or "—"

    admin_msg = (
        f"⚠️ <b>Комментарий к низкой оценке</b>\n"
        f"Заказ: #{order_id}\n"
        f"Оценил: ID {rater_id} (@{rater_username}) — {rater_role}\n"
        f"Получил: ID {target_id} (@{target_username}) — {target_role}\n"
        f"Оценка: {rating}\n"
        f"Комментарий:\n{comment}"
    )

    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, admin_msg, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Не удалось отправить комментарий админу {admin_id}: {e}")

    # 3. Завершаем flow для оценившего
    await finalize_rating_flow(message, rater_id, rating)
    await state.clear()

@router.callback_query(F.data.startswith("cancel_client_"))
async def request_cancel_confirmation_client(callback: CallbackQuery, state: FSMContext):
    try:
        order_id = int(callback.data.split("_")[2])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверный формат заказа.", show_alert=True)
        return

    user_id = callback.from_user.id
    order = await get_order(order_id)
    if not order or order["client_id"] != user_id or order["status"] not in ("pending", "accepted"):
        await callback.answer("❌ Заказ не найден или уже завершён.", show_alert=True)
        return

    # 🔥 Сохраняем данные заказа для восстановления
    await state.update_data(
        order_id=order_id,
        pickup=order["pickup_address"],
        dropoff=order["dropoff_address"],
        driver_id=order["driver_id"],
        status=order["status"]
    )

    await callback.message.edit_text(
        "❓ Вы уверены, что хотите отменить заказ?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_cancel_client_{order_id}"),
                InlineKeyboardButton(text="❌ Нет", callback_data=f"back_to_order_menu_client")
            ]
        ])
    )
    await callback.answer()


@router.callback_query(F.data.startswith("cancel_driver_"))
async def request_cancel_confirmation_driver(callback: CallbackQuery, state: FSMContext):
    try:
        order_id = int(callback.data.split("_")[2])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверный формат заказа.", show_alert=True)
        return

    user_id = callback.from_user.id
    order = await get_order(order_id)
    if not order or order["driver_id"] != user_id or order["status"] != "accepted":
        await callback.answer("❌ Это не ваш активный заказ.", show_alert=True)
        return

    await state.update_data(
        order_id=order_id,
        pickup=order["pickup_address"],
        dropoff=order["dropoff_address"],
        client_id=order["client_id"]
    )

    await callback.message.edit_text(
        "❓ Вы уверены, что хотите отменить заказ?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_cancel_driver_{order_id}"),
                InlineKeyboardButton(text="❌ Нет", callback_data=f"back_to_order_menu_driver")
            ]
        ])
    )
    await callback.answer()

@router.callback_query(F.data.startswith("confirm_cancel_client_"))
async def confirm_cancel_client(callback: CallbackQuery):
    try:
        order_id = int(callback.data.split("_")[3])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных.", show_alert=True)
        return

    user_id = callback.from_user.id
    order = await get_order(order_id)
    if not order or order["client_id"] != user_id or order["status"] in ("completed", "cancelled"):
        await callback.answer("❌ Заказ уже завершён или не ваш.", show_alert=True)
        return

    client_id = user_id
    driver_id = order["driver_id"]
    status = order["status"]

    # Удаляем меню у клиента
    if order_id in client_order_messages:
        chat_id, msg_id = client_order_messages[order_id]
        try:
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except:
            pass
        del client_order_messages[order_id]

    # Отменяем таймеры
    for task_dict in [unclaimed_tasks, selection_tasks, stale_tasks]:
        if order_id in task_dict:
            task_dict[order_id].cancel()
            del task_dict[order_id]

    await cancel_order_with_reason(order_id, f"client_{user_id}")
    client_bid_messages.pop(order_id, None)

    # Уведомляем водителя
    if driver_id and status == "accepted":
        try:
            await bot.send_message(
                driver_id,
                f"❌ Клиент отменил заказ №{order_id}.\n📍 Откуда: {order['pickup_address']}\n🏁 Куда: {order['dropoff_address']}",
                reply_markup=None
            )
        except:
            pass
    elif status == "pending":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT driver_id FROM bids WHERE order_id = ?", (order_id,)) as cursor:
                bids = await cursor.fetchall()
                for (d_id,) in bids:
                    try:
                        await bot.send_message(d_id, f"❌ Клиент отменил заказ №{order_id}.")
                    except:
                        pass

    # Возвращаем в меню
    role = await get_user_role(user_id)
    menu = await get_client_menu_with_rating_and_status(user_id) if role == "client" else driver_menu(await is_shift_opened(user_id))

    # 🔥 Безопасное редактирование сообщения
    try:
        await callback.message.edit_text("❌ Ваш заказ отменён.", reply_markup=None)
    except TelegramBadRequest as e:
        if "message to edit not found" in str(e):
            logging.warning(f"Сообщение не найдено при отмене заказа {order_id}")
            await callback.message.answer("❌ Ваш заказ отменён.")
        else:
            raise

    await callback.message.answer("📍 Главное меню:", reply_markup=menu)
    await callback.answer()


@router.callback_query(F.data.startswith("confirm_cancel_driver_"))
async def confirm_cancel_driver(callback: CallbackQuery):
    try:
        order_id = int(callback.data.split("_")[3])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных.", show_alert=True)
        return

    user_id = callback.from_user.id
    order = await get_order(order_id)
    if not order or order["driver_id"] != user_id or order["status"] in ("completed", "cancelled"):
        await callback.answer("❌ Заказ уже завершён или не ваш.", show_alert=True)
        return

    client_id = order["client_id"]
    await cancel_order_with_reason(order_id, f"driver_{user_id}")

    # 🔥 Безопасное редактирование сообщения
    try:
        await callback.message.edit_text("❌ Заказ отменён.", reply_markup=None)
    except TelegramBadRequest as e:
        if "message to edit not found" in str(e):
            logging.warning(f"Сообщение не найдено при отмене заказа {order_id}")
            await callback.message.answer("❌ Заказ отменён.")
        else:
            raise

    source = order.get("source", "telegram")

    # Отправляем уведомление клиенту
    if source == "telegram":
        try:
            await bot.send_message(
                client_id,
                f"❌ Водитель отменил ваш заказ №{order_id}.\n📍 Откуда: {order['pickup_address']}\n🏁 Куда: {order['dropoff_address']}",
                reply_markup=None
            )
        except:
            pass

    if order_id in driver_order_messages:
        del driver_order_messages[order_id]

    shift_opened = await is_shift_opened(user_id)
    await callback.message.answer("📍 Главное меню:", reply_markup=driver_menu(shift_opened))
    await callback.answer()

@router.callback_query(F.data == "back_to_order_menu_client")
async def back_to_client_order_menu(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("order_id")
    pickup = data.get("pickup")
    dropoff = data.get("dropoff")
    driver_id = data.get("driver_id")
    status = data.get("status")

    if not all([order_id, pickup, dropoff]):
        await callback.message.edit_text("❌ Не удалось восстановить меню заказа.")
        await state.clear()
        return

    # Если заказ в статусе "accepted" — восстанавливаем меню с проверкой
    if status == "accepted":
        # Проверяем, есть ли сохранённое сообщение меню
        if order_id in client_order_messages:
            chat_id, msg_id = client_order_messages[order_id]
            try:
                # Пробуем обновить — это и есть проверка на существование
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=(
                        f"✅ Вы выбрали водителя!\n"
                        f"📍 {pickup} → {dropoff}"
                    ),
                    reply_markup=client_order_menu(driver_id, order_id)
                )
                await callback.answer()
                return
            except TelegramBadRequest as e:
                if "message to edit not found" in str(e):
                    logging.warning(f"Меню клиента для заказа {order_id} утеряно. Переотправка...")
                    client_order_messages.pop(order_id, None)
                else:
                    raise

        # Если дошли сюда — меню нет или недоступно → отправляем заново
        try:
            new_msg = await bot.send_message(
                callback.from_user.id,
                text=(
                    f"✅ Вы выбрали водителя!\n"
                    f"📍 {pickup} → {dropoff}"
                ),
                reply_markup=client_order_menu(driver_id, order_id)
            )
            client_order_messages[order_id] = (new_msg.chat.id, new_msg.message_id)
        except Exception as e:
            logging.error(f"Не удалось восстановить меню клиента {callback.from_user.id} для заказа {order_id}: {e}")
            await callback.message.answer("❌ Не удалось восстановить меню заказа.")
    else:
        # Статус "pending" — показываем стандартное меню выбора водителей
        await callback.message.edit_text(
            f"📍 Откуда: {pickup}\n🏁 Куда: {dropoff}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👥 Показать водителей", callback_data=f"show_bids_{order_id}")],
                [InlineKeyboardButton(text="❌ Отменить заказ", callback_data=f"cancel_client_{order_id}")]
            ])
        )

    await callback.answer()

@router.callback_query(F.data == "back_to_order_menu_driver")
async def back_to_driver_order_menu(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("order_id")
    pickup = data.get("pickup")
    dropoff = data.get("dropoff")
    client_id = data.get("client_id")
    if not all([order_id, pickup, dropoff, client_id]):
        await callback.message.edit_text("❌ Не удалось восстановить меню заказа.")
        await state.clear()
        return

    # Проверяем, есть ли сохранённое сообщение меню у водителя
    if order_id in driver_order_messages:
        chat_id, msg_id = driver_order_messages[order_id]
        try:
            # Пробуем обновить — это и есть проверка на существование
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=f"📍 {pickup} → {dropoff}",
                reply_markup=driver_order_menu(client_id, order_id)
            )
            await callback.answer()
            return
        except TelegramBadRequest as e:
            if "message to edit not found" in str(e):
                logging.warning(f"Меню водителя для заказа {order_id} утеряно. Переотправка...")
                driver_order_messages.pop(order_id, None)
            else:
                raise

    # Если дошли сюда — меню нет или недоступно → отправляем заново
    try:
        new_msg = await bot.send_message(
            callback.from_user.id,
            f"📍 {pickup} → {dropoff}",
            reply_markup=driver_order_menu(client_id, order_id)
        )
        driver_order_messages[order_id] = (new_msg.chat.id, new_msg.message_id)
    except Exception as e:
        logging.error(f"Не удалось восстановить меню водителя {callback.from_user.id} для заказа {order_id}: {e}")
        await callback.message.answer("❌ Не удалось восстановить меню заказа.")

    await callback.answer()


 #Старая логика до восстановления активного меню
    # pickup = data.get("pickup")
    # dropoff = data.get("dropoff")
    #
    #
    # if not all([order_id, pickup, dropoff, client_id]):
    #     await callback.message.edit_text("❌ Не удалось восстановить меню заказа.")
    #     await state.clear()
    #     return
    #
    # await callback.message.edit_text(
    #     f"📍 {pickup} → {dropoff}",
    #     reply_markup=driver_order_menu(client_id, order_id)
    # )
    # await callback.answer()

@router.callback_query(F.data.startswith("show_bids_"))
async def show_bids(callback: CallbackQuery):
    order_id = int(callback.data.split("_")[2])
    client_id = callback.from_user.id
    order = await get_order(order_id)
    if not order or order["client_id"] != client_id or order["status"] != "pending":
        await callback.answer("❌ Заказ уже принят или отменён.", show_alert=True)
        return

    bids = await get_bids_for_order(order_id)
    if not bids:
        await callback.message.edit_text("📭 Пока нет откликнувшихся водителей.")
        client_bid_messages.pop(order_id, None)
        return

    # Проверяем, включена ли функция "штурман"
    co_driver_enabled = await get_setting("co_driver_enabled", "1") == "1"

    buttons = []
    has_any_co_driver = False

    for driver_id, car_brand, car_number, arrival_minutes, has_co in bids:
        if has_co:
            has_any_co_driver = True
        co_icon = " 👥" if has_co else ""
        rating = await get_driver_rating(driver_id)
        name = f"{car_brand} {car_number} ⭐{rating} ({arrival_minutes} мин){co_icon}"
        buttons.append([InlineKeyboardButton(text=name, callback_data=f"select_driver_{order_id}_{driver_id}")])

    # Формируем текст сообщения
    base_text = "🚕 Выберите водителя:"
    if co_driver_enabled and has_any_co_driver:
        info_line = "ℹ️ Водители с 👥 едут с штурманом (доп. пассажиром)."
        full_text = f"{info_line}\n{base_text}"
    else:
        full_text = base_text

    try:
        await callback.message.edit_text(
            full_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
        client_bid_messages[order_id] = (callback.message.chat.id, callback.message.message_id)
    except Exception as e:
        logging.warning(f"Не удалось обновить сообщение с водителями: {e}")
        client_bid_messages.pop(order_id, None)

    await callback.answer()

@router.callback_query(F.data.startswith("select_driver_"))
async def select_driver(callback: CallbackQuery):
    client_id = callback.from_user.id
    try:
        _, _, order_id, driver_id = callback.data.split("_")
        order_id = int(order_id)
        driver_id = int(driver_id)
    except Exception:
        await callback.answer("❌ Ошибка данных.", show_alert=True)
        return

    order = await get_order(order_id)
    if not order or order["client_id"] != client_id or order["status"] != "pending":
        await callback.answer("❌ Заказ уже принят или отменён.", show_alert=True)
        return

    # Проверяем, не взял ли водитель другой заказ
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM orders WHERE driver_id = ? AND status = 'accepted'", (driver_id,)
        ) as cursor:
            has_other_order = await cursor.fetchone()
    if has_other_order:
        await callback.answer("⚠️ Водитель уже выполняет другой заказ. Выберите другого.", show_alert=True)
        return

    success = await accept_bid(order_id, driver_id)
    if not success:
        await callback.answer("❌ Не удалось выбрать водителя. Возможно, заказ уже принят.", show_alert=True)
        return

    # Удаляем сообщение со списком водителей
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение со списком водителей: {e}")

    client_bid_messages.pop(order_id, None)

    # Отменяем таймер выбора
    task = selection_tasks.pop(order_id, None)
    if task:
        task.cancel()
        logging.info(f"[select_driver] Отменён selection-таймер для {order_id}")

    # Запускаем stale-таймер (на выполнение заказа)
    if order_id not in stale_tasks:
        task = asyncio.create_task(auto_cancel_stale_order(order_id))
        stale_tasks[order_id] = task
        logging.info(f"[select_driver] Запущен stale-таймер для {order_id}")

    # Уведомляем выбранного водителя
    comment_text = (order["comment"] or "").strip()
    comment_block = f"📝 Комментарий: {comment_text}\n" if comment_text else ""
    try:
        driver_menu_msg = await bot.send_message(
            driver_id,
            f"✅ Клиент выбрал вас для заказа!\n"
            f"📍 {order['pickup_address']} → {order['dropoff_address']}\n"
            f"{comment_block}",
            reply_markup=driver_order_menu(client_id, order_id)
        )
        driver_order_messages[order_id] = (driver_menu_msg.chat.id, driver_menu_msg.message_id)
    except Exception as e:
        logging.warning(f"Не удалось уведомить водителя {driver_id}: {e}")

    # Уведомляем остальных водителей
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT driver_id FROM bids WHERE order_id = ? AND driver_id != ?", (order_id, driver_id)
        ) as cursor:
            rejected_drivers = await cursor.fetchall()
            for (d_id,) in rejected_drivers:
                try:
                    await bot.send_message(d_id, f"❌ Заказ №{order_id} достался другому водителю.")
                except:
                    pass

    # === ОТПРАВКА МЕНЮ АКТИВНОГО ЗАКАЗА КЛИЕНТУ ===
    car_info = await get_driver_info(driver_id)
    car_text = f"{car_info[0]} {car_info[1]}" if car_info else "Не указано"
    rating = await get_driver_rating(driver_id)

    # Получаем время прибытия
    arrival_minutes = None
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT arrival_minutes FROM bids WHERE order_id = ? AND driver_id = ?",
            (order_id, driver_id)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                arrival_minutes = row[0]

    # Формируем текст с комментарием
    comment_text = (order["comment"] or "").strip()
    comment_block = f"\n📝 Комментарий: {comment_text}" if comment_text else ""

    # 🔥 ИСПРАВЛЕНИЕ: используем bot.send_message с явным указанием chat_id и message_thread_id
    chat_id = callback.message.chat.id
    thread_id = callback.message.message_thread_id
    if thread_id == "":
        thread_id = None

    menu_msg = await bot.send_message(
        chat_id=chat_id,
        message_thread_id=thread_id,
        text=(
            f"✅ Вы выбрали водителя!\n"
            f"🚗 {car_text}\n"
            f"⭐ {rating}\n\n"
            f"📍 {order['pickup_address']} → {order['dropoff_address']}"
            f"{comment_block}"
        ),
        reply_markup=client_order_menu(driver_id, order_id)
    )

    # Сохраняем ID меню клиента для будущего обновления/удаления
    client_order_messages[order_id] = (menu_msg.chat.id, menu_msg.message_id)

    # Отправляем уведомление о времени прибытия
    if arrival_minutes is not None:
        await bot.send_message(
            client_id,
            f"✳️ Водитель уже в пути! Будет у вас через {arrival_minutes} минут.",
            reply_markup=None
        )
    else:
        await bot.send_message(
            client_id,
            "Водитель уже в пути!",
            reply_markup=None
        )

    # Удаляем сообщения у всех водителей из БД
    messages_to_delete = await get_driver_order_messages(order_id)
    for chat_id_del, msg_id_del, d_id in messages_to_delete:
        try:
            await bot.delete_message(chat_id=chat_id_del, message_id=msg_id_del)
        except Exception as e:
            logging.warning(f"Не удалось удалить сообщение заказа у водителя {d_id}: {e}")
    await delete_driver_order_messages(order_id)

    await callback.answer("Водитель выбран!", show_alert=True)

# ФУНКЦИИ ТАЙМЕРОВ АВТООТМЕНЫ
async def auto_cancel_unclaimed_order(order_id: int):
    """Отменяет заказ, если за N секунд никто не откликнулся."""
    try:
        logging.info(f"[unclaimed] Таймер запущен для заказа {order_id}")
        await asyncio.sleep(UNCLAIMED_SECONDS)

        order = await get_order(order_id)
        if not order or order["status"] != "pending":
            logging.info(f"[unclaimed] Заказ {order_id} уже обработан — выход.")
            return

        # Проверяем, есть ли заявки
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT 1 FROM bids WHERE order_id = ? LIMIT 1", (order_id,)) as cursor:
                has_bids = await cursor.fetchone()

        if not has_bids:
            await cancel_order_with_reason(order_id, "unclaimed_timer")
            client_id = order["client_id"]
            try:
                await bot.send_message(client_id, "⏰ Никто из водителей не откликнулся. Заказ отменён.")
            except:
                pass
            logging.info(f"[unclaimed] Заказ {order_id} отменён (нет откликов).")

            # === УДАЛЯЕМ СООБЩЕНИЯ У ВСЕХ ВОДИТЕЛЕЙ ===
            messages_to_delete = await get_driver_order_messages(order_id)
            for chat_id, msg_id, driver_id in messages_to_delete:
                try:
                    await bot.delete_message(chat_id=chat_id, message_id=msg_id)
                except Exception as e:
                    logging.warning(f"Не удалось удалить сообщение заказа у водителя {driver_id}: {e}")
            await delete_driver_order_messages(order_id)
        else:
            logging.info(f"[unclaimed] На заказ {order_id} есть отклики — отмена НЕ нужна.")

    except Exception as e:
        logging.error(f"[unclaimed] Ошибка для заказа {order_id}: {e}")
    finally:
        unclaimed_tasks.pop(order_id, None)

async def auto_cancel_order(order_id: int):
    """Отменяет заказ, если клиент не выбрал водителя за N секунд."""
    try:
        logging.info(f"[selection] Таймер запущен для заказа {order_id}")
        await asyncio.sleep(CANCEL_SECONDS)

        order = await get_order(order_id)
        if not order or order["status"] != "pending":
            logging.info(f"[selection] Заказ {order_id} уже обработан — выход.")
            return

        # Отменяем заказ
        await cancel_order_with_reason(order_id, "selection_timer")
        client_id = order["client_id"]
        try:
            await bot.send_message(client_id, "⏰ Время на выбор водителя истекло. Заказ отменён.")
        except:
            pass

            # === УДАЛЯЕМ СООБЩЕНИЯ У ВСЕХ ВОДИТЕЛЕЙ (из БД) ===
        messages_to_delete = await get_driver_order_messages(order_id)
        for chat_id, msg_id, driver_id in messages_to_delete:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception as e:
                logging.warning(f"Не удалось удалить сообщение заказа у водителя {driver_id}: {e}")
        # Чистим БД
        await delete_driver_order_messages(order_id)

        # Уведомляем всех откликнувшихся водителей
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT driver_id FROM bids WHERE order_id = ?", (order_id,)) as cursor:
                drivers = await cursor.fetchall()
                for (driver_id,) in drivers:
                    try:
                        await bot.send_message(
                            driver_id,
                            f"⏰ Заказ №{order_id} отменён автоматически (клиент не выбрал водителя)."
                        )
                    except:
                        pass

        logging.info(f"[selection] Заказ {order_id} отменён (клиент не выбрал).")

    except Exception as e:
        logging.error(f"[selection] Ошибка для заказа {order_id}: {e}")
    finally:
        selection_tasks.pop(order_id, None)

async def auto_cancel_stale_order(order_id: int):
    """Отменяет заказ, если он в статусе 'accepted' дольше N секунд."""
    try:
        logging.info(f"[stale] Таймер запущен для заказа {order_id}")
        await asyncio.sleep(STALE_SECONDS)

        order = await get_order(order_id)
        if not order or order["status"] != "accepted":
            logging.info(f"[stale] Заказ {order_id} уже завершён/отменён — выход.")
            return

        await cancel_order_with_reason(order_id, "stale_timer")
        client_id = order["client_id"]
        driver_id = order["driver_id"]
        try:
            await bot.send_message(client_id, "⚠️ Заказ отменён из-за превышения времени выполнения.")
        except:
            pass
        try:
            await bot.send_message(driver_id, f"⚠️ Заказ №{order_id} отменён автоматически (превышено время).")
        except:
            pass

        logging.info(f"[stale] Заказ {order_id} отменён (превышено время).")

    except Exception as e:
        logging.error(f"[stale] Ошибка для заказа {order_id}: {e}")
    finally:
        stale_tasks.pop(order_id, None)

#ВОССТАНОВЛЕНИЕ АКТИВНЫХ ТАЙМЕРОВ ПОСЛЕ ПЕРЕЗАГРУЗКИ
async def restore_active_timers():
    """Восстанавливает таймеры после перезапуска бота."""
    logging.info("🔄 Восстановление активных таймеров...")
    now = datetime.now()

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT id, client_id, driver_id, status, created_at
            FROM orders
            WHERE status IN ('pending', 'accepted')
        """) as cursor:
            orders = await cursor.fetchall()

    for order_id, client_id, driver_id, status, created_at_str in orders:
        try:
            created_at = datetime.fromisoformat(created_at_str)
            elapsed = (now - created_at).total_seconds()

            if status == "pending":
                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute("SELECT 1 FROM bids WHERE order_id = ? LIMIT 1", (order_id,)) as cursor:
                        has_bids = await cursor.fetchone()

                if not has_bids:
                    # unclaimed-таймер
                    remaining = UNCLAIMED_SECONDS - elapsed
                    if remaining > 0 and order_id not in unclaimed_tasks:
                        task = asyncio.create_task(auto_cancel_unclaimed_order_with_delay(order_id, remaining))
                        unclaimed_tasks[order_id] = task
                        logging.info(
                            f"[restore] Восстановлен unclaimed-таймер для {order_id} (осталось {remaining:.1f} сек)")
                    elif remaining <= 0:
                        await cancel_order_with_reason(order_id, "unclaimed_timer")
                        try:
                            await bot.send_message(client_id, "⏰ Никто не откликнулся на ваш заказ. Заказ отменён.")
                        except:
                            pass
                        logging.info(f"[restore] Заказ {order_id} отменён (истёк срок unclaimed)")

                else:
                    # selection-таймер
                    remaining = CANCEL_SECONDS - elapsed
                    if remaining > 0 and order_id not in selection_tasks:
                        task = asyncio.create_task(auto_cancel_order_with_delay(order_id, remaining))
                        selection_tasks[order_id] = task
                        logging.info(
                            f"[restore] Восстановлен selection-таймер для {order_id} (осталось {remaining:.1f} сек)")
                    elif remaining <= 0:
                        await cancel_order_with_reason(order_id, "selection_timer")
                        try:
                            await bot.send_message(client_id, "⏰ Время на выбор водителя истекло. Заказ отменён.")
                        except:
                            pass
                        async with aiosqlite.connect(DB_PATH) as db:
                            async with db.execute("SELECT driver_id FROM bids WHERE order_id = ?",
                                                  (order_id,)) as cursor:
                                drivers = await cursor.fetchall()
                                for (d_id,) in drivers:
                                    try:
                                        await bot.send_message(d_id, f"⏰ Заказ №{order_id} отменён (клиент не выбрал).")
                                    except:
                                        pass
                        logging.info(f"[restore] Заказ {order_id} отменён (истёк срок selection)")

            elif status == "accepted":
                # stale-таймер
                remaining = STALE_SECONDS - elapsed
                if remaining > 0 and order_id not in stale_tasks:
                    task = asyncio.create_task(auto_cancel_stale_order_with_delay(order_id, remaining))
                    stale_tasks[order_id] = task
                    logging.info(f"[restore] Восстановлен stale-таймер для {order_id} (осталось {remaining:.1f} сек)")
                elif remaining <= 0:
                    await cancel_order_with_reason(order_id, "stale_timer")
                    try:
                        await bot.send_message(client_id, "⚠️ Заказ отменён из-за превышения времени выполнения.")
                    except:
                        pass
                    try:
                        await bot.send_message(driver_id, f"⚠️ Заказ №{order_id} отменён (превышено время).")
                    except:
                        pass
                    logging.info(f"[restore] Заказ {order_id} отменён (истёк срок stale)")

        except Exception as e:
            logging.error(f"[restore] Ошибка при восстановлении заказа {order_id}: {e}")

    logging.info("✅ Восстановление таймеров завершено.")


# --- Вспомогательные функции с задержкой ---
async def auto_cancel_unclaimed_order_with_delay(order_id: int, delay: float):
    await asyncio.sleep(delay)
    await cancel_order_with_reason(order_id, "unclaimed_timer")

async def auto_cancel_order_with_delay(order_id: int, delay: float):
    await asyncio.sleep(delay)
    await auto_cancel_order_logic(order_id)

async def auto_cancel_stale_order_with_delay(order_id: int, delay: float):
    await asyncio.sleep(delay)
    await auto_cancel_stale_order_logic(order_id)


# --- Логика отмены (без дублирования кода) ---
async def auto_cancel_unclaimed_order_logic(order_id: int):
    order = await get_order(order_id)
    if not order or order["status"] != "pending":
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM bids WHERE order_id = ? LIMIT 1", (order_id,)) as cursor:
            has_bids = await cursor.fetchone()
    if not has_bids:
        await cancel_order_with_reason(order_id, "unclaimed_timer")
        client_id = order["client_id"]
        try:
            await bot.send_message(client_id, "⏰ Никто из водителей не откликнулся. Заказ отменён.")
        except:
            pass
        logging.info(f"[unclaimed] Заказ {order_id} отменён (нет откликов).")
    unclaimed_tasks.pop(order_id, None)

async def auto_cancel_order_logic(order_id: int):
    order = await get_order(order_id)
    if not order or order["status"] != "pending":
        return
    await cancel_order_with_reason(order_id, "selection_timer")
    client_id = order["client_id"]
    try:
        await bot.send_message(client_id, "⏰ Время на выбор водителя истекло. Заказ отменён.")
    except:
        pass

    # === УДАЛЯЕМ СООБЩЕНИЯ У ВСЕХ ВОДИТЕЛЕЙ ===
    messages_to_delete = await get_driver_order_messages(order_id)
    for chat_id, msg_id, driver_id in messages_to_delete:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception as e:
            logging.warning(f"Не удалось удалить сообщение заказа у водителя {driver_id}: {e}")
    await delete_driver_order_messages(order_id)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT driver_id FROM bids WHERE order_id = ?", (order_id,)) as cursor:
            drivers = await cursor.fetchall()
            for (driver_id,) in drivers:
                try:
                    await bot.send_message(driver_id, f"⏰ Заказ №{order_id} отменён автоматически (клиент не выбрал водителя).")
                except:
                    pass
    logging.info(f"[selection] Заказ {order_id} отменён (клиент не выбрал).")
    selection_tasks.pop(order_id, None)

async def auto_cancel_stale_order_logic(order_id: int):
    order = await get_order(order_id)
    if not order or order["status"] != "accepted":
        return
    await cancel_order_with_reason(order_id, "stale_timer")
    client_id = order["client_id"]
    driver_id = order["driver_id"]
    try:
        await bot.send_message(client_id, "⚠️ Заказ отменён из-за превышения времени выполнения.")
    except:
        pass
    try:
        await bot.send_message(driver_id, f"⚠️ Заказ №{order_id} отменён автоматически (превышено время).")
    except:
        pass
    logging.info(f"[stale] Заказ {order_id} отменён (превышено время).")
    stale_tasks.pop(order_id, None)

@router.callback_query(F.data.startswith("send_location_"))
async def request_location_from_client(callback: CallbackQuery, state: FSMContext):
    order_id = int(callback.data.split("_")[2])
    client_id = callback.from_user.id

    order = await get_order(order_id)
    if not order or order["client_id"] != client_id or order["status"] != "accepted":
        await callback.answer("❌ Заказ не найден или уже завершён.", show_alert=True)
        return

    driver_id = order["driver_id"]

    # Клавиатура с кнопкой "Отмена"
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📍 Отправить мою геопозицию", request_location=True)],
            [KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

    await callback.message.answer(
        "📌 Нажмите «📍 Отправить мою геопозицию», чтобы поделиться местоположением.\n"
        "Или нажмите «❌ Отмена», чтобы вернуться в меню заказа.",
        reply_markup=cancel_kb
    )

    await state.set_state(ClientStates.sending_location)
    await state.update_data(order_id=order_id, driver_id=driver_id)
    await callback.answer()

@router.message(ClientStates.sending_location, F.text == "❌ Отмена")
async def cancel_location_sending(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("order_id")
    driver_id = data.get("driver_id")

    if not order_id or not driver_id:
        await message.answer("❌ Ошибка: заказ не найден.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    #Возвращаем меню заказа
    order = await get_order(order_id)
    await message.answer("🚖 Меню заказа:", reply_markup=ReplyKeyboardRemove())
    await message.answer(
        f"📍Откуда: {order['pickup_address']}\n🏁 Куда: {order['dropoff_address']}",
        reply_markup=client_order_menu(driver_id, order_id)
    )
    await state.clear()

@router.message(ClientStates.sending_location, F.location)
async def handle_client_location(message: Message, state: FSMContext):
    location = message.location
    data = await state.get_data()
    order_id = data.get("order_id")

    if not order_id:
        await message.answer("❌ Ошибка: не найден заказ.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    order = await get_order(order_id)
    if not order or order["client_id"] != message.from_user.id or order["status"] != "accepted":
        await message.answer("❌ Заказ недействителен.")
        await state.clear()
        return

    driver_id = order["driver_id"]
    if not driver_id:
        await message.answer("❌ Водитель не найден.")
        await state.clear()
        return

    # Отправляем геопозицию водителю
    try:
        await bot.send_message(driver_id, "📍 Клиент отправил свою геопозицию:")
        await bot.send_location(
            chat_id=driver_id,
            latitude=location.latitude,
            longitude=location.longitude
        )
        await message.answer("✅ Геопозиция отправлена водителю!", reply_markup=ReplyKeyboardRemove())
    except Exception as e:
        logging.error(f"Не удалось отправить геопозицию водителю {driver_id}: {e}")
        await message.answer("❌ Не удалось отправить геопозицию. Попробуйте позже.",
                                  reply_markup=ReplyKeyboardRemove()
        )

    await message.answer(
        f"📍Откуда: {order['pickup_address']}\n🏁 Куда: {order['dropoff_address']}",
        reply_markup=client_order_menu(driver_id, order_id)
    )
    await state.clear()

@router.message(ClientStates.sending_location)
async def handle_non_location(message: Message, state: FSMContext):
    await message.answer(
        "⚠️ Пожалуйста, отправьте геопозицию или нажмите «❌ Отмена».",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📍 Отправить мою геопозицию", request_location=True)],
                [KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )

@router.callback_query(F.data.startswith("arrived_"))
async def driver_arrived(callback: CallbackQuery):
    driver_id = callback.from_user.id
    order_id = int(callback.data.split("_")[1])
    order = await get_order(order_id)
    if not order or order["driver_id"] != driver_id or order["status"] != "accepted":
        await callback.answer("❌ Заказ не найден или уже завершён.", show_alert=True)
        return

    client_id = order["client_id"]
    pickup = order["pickup_address"]
    dropoff = order["dropoff_address"]
    comment_text = order["comment"].strip() if order["comment"] else ""
    comment_block = f"📝 Комментарий: {comment_text}\n" if comment_text else ""

    # Обновляем статус прибытия водителя в БД
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE orders SET driver_arrived = 1 WHERE id = ?",
            (order_id,)
        )
        await db.commit()

    # Отправляем уведомление клиенту
    order = await get_order(order_id)
    client_id = order['client_id']  # client_id

    source = order.get("source", "telegram")

    # Отправляем уведомление клиенту
    if source == "telegram":
        try:
            await bot.send_message(
                client_id,
            "🚕 <b>Водитель на месте!</b>\n"
                "У вас <b>5 минут бесплатного ожидания</b>.\n"
                "Пожалуйста, выходите к автомобилю.",
                parse_mode="HTML"
            )

        except Exception as e:
            logging.warning(f"Не удалось уведомить клиента {client_id}: {e}")
    # Обновляем сообщение у водителя с маршрутами и комментарием
    now = datetime.now().strftime("%H:%M")
    new_text = (
        f"📍 {pickup} → {dropoff}\n"
        f"{comment_block}"
        f"✅ Вы отметились как «на месте».\n"
        f"Клиент уведомлён — [{now}]"
    )

    try:
        await callback.message.edit_text(
            new_text,
            reply_markup=driver_order_menu(client_id, order_id)
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise

    await callback.answer("Клиент уведомлён!", show_alert=True)

@router.message(AdminStates.waiting_for_verification_date)
async def save_verification_date(message: Message, state: FSMContext):
    data = await state.get_data()
    driver_id = data["driver_id"]
    verify_message_id = data.get("verify_message_id")
    chat_id = data.get("chat_id")
    date_request_msg_id = data.get("date_request_message_id")

    text = message.text.strip()
    expires_date = None
    if text != "-":
        try:
            d = datetime.strptime(text, "%d.%m.%Y")
            expires_date = d.date().isoformat()
        except ValueError:
            await message.answer("❌ Неверный формат даты. Попробуйте снова: ДД.ММ.ГГГГ или «-».")
            return

    # Подтверждаем верификацию
    await set_driver_verification(driver_id, expires_date)

    # Уведомляем водителя
    try:
        if expires_date:
            await bot.send_message(
                driver_id,
                f"✅ Ваш аккаунт водителя подтверждён до {text}!\n"
                "Теперь вы можете открывать смену и принимать заказы."
            )
        else:
            await bot.send_message(
                driver_id,
                "✅ Ваш аккаунт водителя подтверждён бессрочно!\n"
                "Теперь вы можете открывать смену и принимать заказы."
            )
    except:
        pass

    # 🔥 Удаляем сообщения:
    # 1. Карточку водителя
    if verify_message_id and chat_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=verify_message_id)
        except Exception as e:
            logging.warning(f"Не удалось удалить карточку верификации: {e}")

    # 2. Сообщение с запросом даты
    if date_request_msg_id and chat_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=date_request_msg_id)
        except Exception as e:
            logging.warning(f"Не удалось удалить запрос даты: {e}")

    # 3. Сообщение пользователя с датой (текущее)
    try:
        await message.delete()
    except:
        pass

    await message.answer("✅ Верификация установлена!")
    await state.clear()

@router.message(AdminStates.waiting_for_new_car_info)
async def save_new_car_info(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await message.answer("❌ Изменение данных автомобиля отменено.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    try:
        parts = message.text.strip().split(maxsplit=1)
        if len(parts) < 2:
            raise ValueError
        brand, number = parts[0], parts[1]
    except:
        await message.answer("❌ Неверный формат. Пример: *Toyota A123BC*", parse_mode="Markdown")
        return

    data = await state.get_data()
    target_user_id = data["target_user_id"]

    # Сохраняем в БД
    await save_car_info(target_user_id, brand, number)

    # Уведомляем водителя
    try:
        await bot.send_message(
            target_user_id,
            f"🚗 Ваши данные автомобиля обновлены:\n{brand} {number}"
        )
    except:
        pass

    await message.answer(f"✅ Данные автомобиля пользователя {target_user_id} обновлены.", reply_markup=ReplyKeyboardRemove())
    await state.clear()

    # 🔥 Отправляем новое сообщение с обновлённой карточкой
    await message.answer("🔄 Обновлённая карточка пользователя:")
    await search_user_by_id_new_message(message, target_user_id)

@router.message(F.text == "🚗 Водители")
async def show_drivers_list(message: Message):
    if not is_admin(message.from_user.id):
        return
    await show_drivers_page(message, page=1)

@router.message(F.text == "📥 Список водителей (CSV)")
async def download_drivers_csv(message: Message):
    if not is_admin(message.from_user.id):
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT u.user_id, u.username, u.car_brand, u.car_number, u.is_verified, u.verification_expires, u.created_at,
                   (SELECT AVG(rating) FROM ratings WHERE target_id = u.user_id) as avg_rating,
                   (SELECT COUNT(*) FROM orders WHERE driver_id = u.user_id AND status = 'completed') as completed_count
            FROM users u
            WHERE u.role = 'driver'
            ORDER BY completed_count DESC
        """) as cursor:
            drivers = await cursor.fetchall()

    if not drivers:
        await message.answer("📭 Нет зарегистрированных водителей.")
        return

    # Создаём CSV в памяти
    output = StringIO()
    writer = csv.writer(output, delimiter=';')
    writer.writerow([
        "ID",
        "Username",
        "Марка авто",
        "Госномер",
        "Рейтинг",
        "Завершено заказов",
        "Верифицирован",
        "Действует до",
        "Дата регистрации"
    ])

    for row in drivers:
        uid, uname, brand, number, is_verified, expires, created_at, avg_rating, completed = row
        rating = round(avg_rating, 1) if avg_rating else 0.0
        verified_status = "✅" if is_verified else "❌"
        expires_text = expires if expires else "—"
        created_text = created_at if created_at else "—"
        writer.writerow([
            uid,
            uname or "—",
            brand or "—",
            number or "—",
            rating,
            completed,
            verified_status,
            expires_text,
            created_text
        ])

    output.seek(0)
    file_content = output.getvalue().encode('utf-8-sig')
    output.close()

    # Отправляем файл
    file = BufferedInputFile(
        file=file_content,
        filename="drivers_list.csv"
    )
    await message.answer_document(file, caption="📋 Список всех водителей.")

@router.message(F.text == "📥 Список всех пользователей (CSV)")
async def download_all_users_csv(message: Message):
    if not is_admin(message.from_user.id):
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT u.user_id, u.username, u.role, u.is_verified, u.verification_expires, u.created_at,
                   (SELECT AVG(rating) FROM ratings WHERE target_id = u.user_id) as avg_rating,
                   (SELECT COUNT(*) FROM orders WHERE (client_id = u.user_id OR driver_id = u.user_id) AND status = 'completed') as completed_count
            FROM users u
            ORDER BY u.created_at DESC
        """) as cursor:
            users = await cursor.fetchall()

    if not users:
        await message.answer("📭 Нет зарегистрированных пользователей.")
        return

    # Создаём CSV в памяти
    output = StringIO()
    writer = csv.writer(output, delimiter=';')
    writer.writerow([
        "#",                    # ← Новый столбец
        "ID",
        "Username",
        "Роль",
        "Рейтинг",
        "Завершено заказов",
        "Верифицирован",
        "Действует до",
        "Дата регистрации"
    ])

    # Заполняем строки с порядковым номером
    for idx, row in enumerate(users, start=1):  # ← начинаем с 1
        uid, uname, role, is_verified, expires, created_at, avg_rating, completed = row
        rating = round(avg_rating, 1) if avg_rating else 0.0
        verified_status = "✅" if is_verified else "❌" if is_verified is not None else "—"
        expires_text = expires if expires else "—"
        created_text = created_at if created_at else "—"
        role_text = "🚗 Водитель" if role == "driver" else "👤 Клиент"
        writer.writerow([
            idx,                 # ← Порядковый номер
            uid,
            uname or "—",
            role_text,
            rating,
            completed,
            verified_status,
            expires_text,
            created_text
        ])

    output.seek(0)
    file_content = output.getvalue().encode('utf-8-sig')
    output.close()

    # Отправляем файл
    file = BufferedInputFile(
        file=file_content,
        filename="all_users_list.csv"
    )
    await message.answer_document(file, caption="📋 Список всех пользователей.")

@router.message(F.text == "📥 История заказов (CSV)")
async def download_orders_history_csv(message: Message):
    if not is_admin(message.from_user.id):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT o.id, o.client_id, o.driver_id, o.pickup_address, o.dropoff_address,
                   o.comment, o.status, o.created_at, o.cancelled_by,
                   (SELECT username FROM users WHERE user_id = o.client_id) as client_username,
                   (SELECT username FROM users WHERE user_id = o.driver_id) as driver_username,
                   (SELECT rating FROM ratings WHERE order_id = o.id AND rater_id = o.client_id AND target_id = o.driver_id LIMIT 1) as driver_rating,
                   (SELECT rating FROM ratings WHERE order_id = o.id AND rater_id = o.driver_id AND target_id = o.client_id LIMIT 1) as client_rating
            FROM orders o
            WHERE o.status IN ('completed', 'cancelled')
            ORDER BY o.created_at DESC
        """) as cursor:
            orders = await cursor.fetchall()
    if not orders:
        await message.answer("📭 Нет завершённых или отменённых заказов.")
        return
    output = StringIO()
    writer = csv.writer(output, delimiter=';')
    writer.writerow([
        "#",
        "ID заказа",
        "ID клиента (@username)",
        "ID водителя (@username)",
        "Откуда",
        "Куда",
        "Комментарий",
        "Статус",
        "Дата создания",
        "Рейтинг клиента",
        "Рейтинг водителя"
    ])
    for idx, row in enumerate(orders, start=1):
        order_id, client_id, driver_id, pickup, dropoff, comment, status, created_at, cancelled_by, client_username, driver_username, driver_rating, client_rating = row

        # Формируем понятный статус
        display_status = status
        if status == "cancelled" and cancelled_by:
            if cancelled_by == "unclaimed_timer":
                display_status = "cancelled (таймер: никто не откликнулся)"
            elif cancelled_by == "selection_timer":
                display_status = "cancelled (таймер: клиент не выбрал)"
            elif cancelled_by == "stale_timer":
                display_status = "cancelled (таймер: превышено время)"
            elif cancelled_by.startswith("client_"):
                uid = cancelled_by.split("_", 1)[1]
                display_status = f"cancelled (клиент ID {uid})"
            elif cancelled_by.startswith("driver_"):
                uid = cancelled_by.split("_", 1)[1]
                display_status = f"cancelled (водитель ID {uid})"
            elif cancelled_by.startswith("admin_"):
                uid = cancelled_by.split("_", 1)[1]
                display_status = f"cancelled (админ ID {uid})"
            else:
                display_status = f"cancelled ({cancelled_by})"

        client_info = f"{client_id} (@{client_username})" if client_username else str(client_id)
        driver_info = f"{driver_id} (@{driver_username})" if driver_username else str(driver_id)
        writer.writerow([
            idx,
            order_id,
            client_info,
            driver_info,
            pickup,
            dropoff,
            comment if comment else "—",
            display_status,
            created_at,
            client_rating if client_rating else "—",
            driver_rating if driver_rating else "—"
        ])
    output.seek(0)
    file_content = output.getvalue().encode('utf-8-sig')
    output.close()
    file = BufferedInputFile(file=file_content, filename="orders_history.csv")
    await message.answer_document(file, caption="📋 История заказов (завершённые и отменённые).")

@router.message(F.text == "⚙️ Настройки сервиса")
async def service_settings(message: Message):
    if not is_admin(message.from_user.id):
        return
    driver_role_enabled = await get_setting("driver_role_enabled", "1") == "1"
    co_driver_enabled = await get_setting("co_driver_enabled", "1") == "1"
    auto_accept_enabled = await get_setting("auto_accept_on_first_bid", "0") == "1"

    role_status = "✅ Включён" if driver_role_enabled else "❌ Отключён"
    co_status = "✅ Включён" if co_driver_enabled else "❌ Отключён"
    auto_status = "✅ Включено" if auto_accept_enabled else "❌ Отключено"

    await message.answer(
        f"⚙️ <b>Настройки сервиса</b>\n"
        f"Роль «Водитель» при первом запуске: {role_status}\n"
        f"Возможность работать с штурманом: {co_status}\n"
        f"Авто-принятие при первом отклике: {auto_status}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Переключить роль водителя", callback_data="toggle_driver_role")],
            [InlineKeyboardButton(text="🔄 Переключить штурмана", callback_data="toggle_co_driver")],
            [InlineKeyboardButton(text="🔄 Переключить авто-принятие", callback_data="toggle_auto_accept")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
        ])
    )

@router.callback_query(F.data == "toggle_auto_accept")
async def toggle_auto_accept(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    current = await get_setting("auto_accept_on_first_bid", "0")
    new_value = "0" if current == "1" else "1"
    await set_setting("auto_accept_on_first_bid", new_value)

    # Обновляем сообщение (как в других toggle)
    driver_role_enabled = await get_setting("driver_role_enabled", "1") == "1"
    co_driver_enabled = await get_setting("co_driver_enabled", "1") == "1"
    auto_accept_enabled = new_value == "1"

    role_status = "✅ Включён" if driver_role_enabled else "❌ Отключён"
    co_status = "✅ Включён" if co_driver_enabled else "❌ Отключён"
    auto_status = "✅ Включено" if auto_accept_enabled else "❌ Отключено"

    await callback.message.edit_text(
        f"⚙️ <b>Настройки сервиса</b>\n"
        f"Роль «Водитель» при первом запуске: {role_status}\n"
        f"Возможность работать с штурманом: {co_status}\n"
        f"Авто-принятие при первом отклике: {auto_status}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Переключить роль водителя", callback_data="toggle_driver_role")],
            [InlineKeyboardButton(text="🔄 Переключить штурмана", callback_data="toggle_co_driver")],
            [InlineKeyboardButton(text="🔄 Переключить авто-принятие", callback_data="toggle_auto_accept")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
        ])
    )
    await callback.answer("Настройка «Авто-принятие» обновлена!")

@router.callback_query(F.data == "toggle_driver_role")
async def toggle_driver_role(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    current = await get_setting("driver_role_enabled", "1")
    new_value = "0" if current == "1" else "1"
    await set_setting("driver_role_enabled", new_value)

    # Получаем ВСЕ настройки
    driver_role_enabled = await get_setting("driver_role_enabled", "1") == "1"
    co_driver_enabled = await get_setting("co_driver_enabled", "1") == "1"
    auto_accept_enabled = await get_setting("auto_accept_on_first_bid", "0") == "1"

    role_status = "✅ Включён" if driver_role_enabled else "❌ Отключён"
    co_status = "✅ Включён" if co_driver_enabled else "❌ Отключён"
    auto_status = "✅ Включено" if auto_accept_enabled else "❌ Отключено"

    await callback.message.edit_text(
        f"⚙️ <b>Настройки сервиса</b>\n"
        f"Роль «Водитель» при первом запуске: {role_status}\n"
        f"Возможность работать с штурманом: {co_status}\n"
        f"Авто-принятие при первом отклике: {auto_status}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Переключить роль водителя", callback_data="toggle_driver_role")],
            [InlineKeyboardButton(text="🔄 Переключить штурмана", callback_data="toggle_co_driver")],
            [InlineKeyboardButton(text="🔄 Переключить авто-принятие", callback_data="toggle_auto_accept")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
        ])
    )
    await callback.answer("Настройка «Роль водителя» обновлена!")

@router.callback_query(F.data == "toggle_co_driver")
async def toggle_co_driver(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    current = await get_setting("co_driver_enabled", "1")
    new_value = "0" if current == "1" else "1"
    await set_setting("co_driver_enabled", new_value)

    # Получаем ВСЕ настройки
    driver_role_enabled = await get_setting("driver_role_enabled", "1") == "1"
    co_driver_enabled = new_value == "1"
    auto_accept_enabled = await get_setting("auto_accept_on_first_bid", "0") == "1"

    role_status = "✅ Включён" if driver_role_enabled else "❌ Отключён"
    co_status = "✅ Включён" if co_driver_enabled else "❌ Отключён"
    auto_status = "✅ Включено" if auto_accept_enabled else "❌ Отключено"

    await callback.message.edit_text(
        f"⚙️ <b>Настройки сервиса</b>\n"
        f"Роль «Водитель» при первом запуске: {role_status}\n"
        f"Возможность работать с штурманом: {co_status}\n"
        f"Авто-принятие при первом отклике: {auto_status}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Переключить роль водителя", callback_data="toggle_driver_role")],
            [InlineKeyboardButton(text="🔄 Переключить штурмана", callback_data="toggle_co_driver")],
            [InlineKeyboardButton(text="🔄 Переключить авто-принятие", callback_data="toggle_auto_accept")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
        ])
    )
    await callback.answer("Настройка «Штурман» обновлена!")

@router.message(F.text == "📋 Заказы")
async def show_active_orders(message: Message):
    if not is_admin(message.from_user.id):
        return
    await show_orders_page(message, page=1)

@router.message(F.text == "📊 Статистика")
async def show_statistics(message: Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        # Обычный пользователь — только личная статистика для водителя
        role = await get_user_role(user_id)
        if role == "driver":
            orders_count = await get_completed_orders_count(user_id, "driver")
            rating = await get_user_rating(user_id)
            hours = orders_count * 0.5
            # Получаем данные авто
            car_info = await get_driver_info(user_id)
            car_brand = car_info[0] if car_info and car_info[0] else "—"
            car_number = car_info[1] if car_info and car_info[1] else "—"
            await message.answer(
                f"📊 Ваша статистика: \n"
                f"🚘 Ваш автомобиль: {car_brand} {car_number} \n"
                f"✅ Выполнено заказов: {orders_count} \n"
                f"⭐ Рейтинг: {rating} \n"
                f"🕒 Рабочих часов: {hours:.1f}"
            )
        else:
            await message.answer("❌ Статистика доступна только водителям.")
        return

    # === АДМИНКА ===
    total = await get_total_orders_count()
    completed = await get_total_completed_orders()
    cancelled = await get_total_cancelled_orders()
    total_users = await get_total_users_count()
    monthly_regs = await get_monthly_registrations()

    # Новые метрики "За всё время"
    avg_pickup = await get_average_pickup_time()
    avg_driver_rating = await get_average_driver_rating()
    avg_client_rating = await get_average_client_rating()
    repeat_total, repeat_percent_total = await get_repeat_orders_stats()

    # За сегодня
    new_orders, completed_today, cancelled_today = await get_daily_stats()
    new_users_today, avg_pickup_today, repeat_today, repeat_percent_today = await get_today_stats_extended()
    active_drivers_now = await get_active_drivers_count()

    msg = (
        "📈 <b>Статистика «Такси БАРС»</b>\n\n"

        "🗃 <b>За всё время:</b>\n"
        f"🔢 Заказов: {total}\n"
        f"✅ Завершено: {completed}\n"
        f"❌ Отменено: {cancelled}\n"
        f"👥 Пользователей: {total_users}\n"
        f"🆕 Регистраций в этом месяце: {monthly_regs}\n"
        f"⏱ Среднее время подачи: {avg_pickup} мин\n"
        f"⭐ Средний рейтинг водителя: {avg_driver_rating}\n"
        f"👤 Средний рейтинг клиента: {avg_client_rating}\n"
        f"🔁 Повторных заказов: {repeat_total} ({repeat_percent_total}%)\n\n"

        "📆 <b>За сегодня:</b>\n"
        f"🆕 Новых заказов: {new_orders}\n"
        f"✅ Завершено: {completed_today}\n"
        f"❌ Отменено: {cancelled_today}\n"
        f"👥 Новых пользователей: {new_users_today}\n"
        f"⏱ Среднее время подачи: {avg_pickup_today} мин\n"
        f"🔁 Повторных заказов: {repeat_today} ({repeat_percent_today}%)\n\n"

        "🚕 <b>Сейчас:</b>\n"
        f"🟢 Активных водителей: {active_drivers_now}"
    )
    await message.answer(msg, parse_mode="HTML")

@router.message(Command("backup"))
async def manual_backup(message: Message):
    if not is_admin(message.from_user.id):
        return
    msg = await message.answer("💾 Создаю резервную копию...")
    path = await create_backup()
    if path:
        await msg.edit_text(f"✅ Резервная копия создана: `{path}`", parse_mode="Markdown")
    else:
        await msg.edit_text("❌ Ошибка создания резервной копии.")

@router.message(Command("backups"))
async def list_backups(message: Message):
    if not is_admin(message.from_user.id):
        return
    os.makedirs(BACKUP_DIR, exist_ok=True)
    backups = sorted(
        [f for f in os.listdir(BACKUP_DIR) if f.startswith("backup_") and f.endswith(".db")],
        key=lambda x: os.path.getmtime(os.path.join(BACKUP_DIR, x)),
        reverse=True
    )
    if not backups:
        await message.answer("📭 Нет резервных копий.")
        return

    msg = "💾 Последние резервные копии:\n\n"
    for f in backups:
        size = os.path.getsize(os.path.join(BACKUP_DIR, f))
        mtime = datetime.fromtimestamp(os.path.getmtime(os.path.join(BACKUP_DIR, f)))
        msg += f"📄 `{f}`\n"
        msg += f"   📅 {mtime.strftime('%d.%m.%Y %H:%M')}\n"
        msg += f"   📦 {size // 1024} KB\n\n"

    await message.answer(msg, parse_mode="Markdown")

def is_admin(user_id: int) -> bool:
    return user_id in ADMINS

async def get_active_orders(page: int = 1, limit: int = 10):
    """Возвращает список активных заказов (pending, accepted)."""
    offset = (page - 1) * limit
    async with aiosqlite.connect(DB_PATH) as db:
        # Запрос: заказы с информацией о клиентах и водителях
        async with db.execute("""
            SELECT o.id, o.client_id, o.driver_id, o.pickup_address, o.dropoff_address,
                   o.status, o.created_at,
                   (SELECT username FROM users WHERE user_id = o.client_id) as client_username,
                   (SELECT username FROM users WHERE user_id = o.driver_id) as driver_username
            FROM orders o
            WHERE o.status IN ('pending', 'accepted')
            ORDER BY o.created_at DESC
            LIMIT ? OFFSET ?
        """, (limit, offset)) as cursor:
            orders = await cursor.fetchall()

        # Общее количество активных заказов
        async with db.execute("SELECT COUNT(*) FROM orders WHERE status IN ('pending', 'accepted')") as cursor:
            total = (await cursor.fetchone())[0]

    return orders, total

async def show_orders_page(message: Message, page: int = 1, limit: int = 5):
    orders, total = await get_active_orders(page, limit)
    if not orders:
        await message.answer("📭 Нет активных заказов.")
        return

    total_pages = (total + limit - 1) // limit  # округление вверх
    msg = f"📋 Активные заказы (страница {page} из {total_pages}):\n\n"

    buttons = []

    for idx, (order_id, client_id, driver_id, pickup, dropoff, status, created_at, client_username, driver_username) in enumerate(orders, 1):
        client_info = f"{client_id} (@{client_username})" if client_username else str(client_id)
        driver_info = f"{driver_id} (@{driver_username})" if driver_username else  str(driver_id)
        mention_client = f'<a href="tg://user?id={client_id}">ПРОФИЛЬ</a>'
        mention_driver = f'<a href="tg://user?id={driver_id}">ПРОФИЛЬ</a>'
        msg += (
            f"{idx}. ID: {order_id}\n"
            f"   Клиент: {client_info} | {mention_client}\n"
            f"   Водитель: {driver_info} | {mention_driver}\n"
            f"   📍 {pickup} → 🏁 {dropoff}\n"
            f"   📊 Статус: {status}\n"
            f"   📅 {created_at}\n\n"
        )

        # Кнопки управления для каждого заказа
        order_buttons = []
        order_buttons.append(InlineKeyboardButton(text=f"👤 Профиль клиента #{idx}", callback_data=f"view_profile_{client_id}"))
        if driver_id:
            order_buttons.append(InlineKeyboardButton(text=f"👤 Профиль водителя #{idx}", callback_data=f"view_profile_{driver_id}"))
        order_buttons.append(InlineKeyboardButton(text=f"❌ Отменить заказ #{order_id}", callback_data=f"admin_cancel_order_{order_id}"))
        buttons.append(order_buttons)

    # Кнопки навигации
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"orders_page_{page - 1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"orders_page_{page + 1}"))

    # Кнопка "Назад в админку"
    nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back"))

    # Добавляем навигацию в конец
    buttons.append(nav_buttons)

    await message.answer(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

async def get_client_menu_with_rating_and_status(user_id: int) -> ReplyKeyboardMarkup:
    rating = await get_user_rating(user_id)
    rides = await get_monthly_rides(user_id)
    status_name, status_emoji = get_client_status(rides)
    order_btn_text = f"🚕 Сделать заказ\n⭐{rating}"
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=order_btn_text)],
            [
                KeyboardButton(text="📜 История заказов"),
                KeyboardButton(text=f"💎 Ваш статус \n     {status_name}{status_emoji}")
            ],
            [KeyboardButton(text="🛠 Техническая поддержка")]
        ],
        resize_keyboard=True
    )

async def get_drivers_list(page: int = 1, limit: int = 5):
    """Возвращает список водителей с информацией."""
    offset = (page - 1) * limit
    async with aiosqlite.connect(DB_PATH) as db:
        # Запрос: ID, username, car, rating, completed_orders, is_verified
        async with db.execute("""
            SELECT u.user_id, u.username, u.car_brand, u.car_number, u.is_verified,
                   (SELECT AVG(rating) FROM ratings WHERE target_id = u.user_id) as avg_rating,
                   (SELECT COUNT(*) FROM orders WHERE driver_id = u.user_id AND status = 'completed') as completed_count
            FROM users u
            WHERE u.role = 'driver'
            ORDER BY completed_count DESC
            LIMIT ? OFFSET ?
        """, (limit, offset)) as cursor:
            drivers = await cursor.fetchall()

        # Общее количество водителей
        async with db.execute("SELECT COUNT(*) FROM users WHERE role = 'driver'") as cursor:
            total = (await cursor.fetchone())[0]

    return drivers, total

async def show_drivers_page(message: Message, page: int = 1, limit: int = 5):
    drivers, total = await get_drivers_list(page, limit)
    if not drivers:
        await message.answer("📭 Нет зарегистрированных водителей.")
        return

    total_pages = (total + limit - 1) // limit  # округление вверх
    msg = f"🚗 Водители (страница {page} из {total_pages}):\n\n"

    buttons = []

    for idx, (uid, uname, brand, number, is_verified, avg_rating, completed_count) in enumerate(drivers, 1):
        rating = round(avg_rating, 1) if avg_rating else 0.0
        verified_status = "✅" if is_verified else "❌"
        car_info = f"{brand or '—'} {number or '—'}"
        msg += (
            f"#{idx}.🆔: {uid}\n"
            f"   👨‍💼: @{uname or '—'}\n"
            f"   🚘: {car_info}\n"
            f"   ⭐: {rating} ({completed_count} заказов)\n"
            f"   ✅ Статус верификации: {verified_status}\n\n"
        )

        # Кнопка "👤 Профиль" рядом с каждым водителем
        buttons.append([InlineKeyboardButton(text=f"👤 Профиль #{idx}", callback_data=f"view_profile_{uid}")])

    # Кнопки навигации (внизу)
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"drivers_page_{page - 1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"drivers_page_{page + 1}"))

    # Кнопка "Назад в админку"
    nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back"))

    # Разбиваем навигацию на строки (например, по 3 кнопки)
    nav_rows = [nav_buttons[i:i+3] for i in range(0, len(nav_buttons), 3)]

    # Объединяем кнопки: сначала "👤 Профиль" для каждого водителя, потом навигация
    full_buttons = buttons + nav_rows

    await message.answer(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=full_buttons))

async def search_user_by_id_new_message(message: Message, user_id: int):
    """Отправляет новое сообщение с карточкой пользователя (с рейтингом)."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT 
                u.user_id, u.username, u.role, u.car_brand, u.car_number, 
                u.is_verified, u.verification_expires, u.is_banned, u.created_at,
                (SELECT AVG(rating) FROM ratings WHERE target_id = u.user_id) as avg_rating
            FROM users u 
            WHERE user_id = ?
        """, (user_id,))
        row = await cursor.fetchone()
    if not row:
        await message.answer("❌ Пользователь не найден.")
        return

    uid, uname, role, brand, number, is_verified, expires, is_banned, created_at, avg_rating = row

    rating_text = f"{round(avg_rating, 2)}" if avg_rating is not None else "—"

    is_verification_active = False
    if is_verified:
        if expires is None:
            is_verification_active = True
        else:
            from datetime import date
            try:
                expire_date = date.fromisoformat(expires)
                is_verification_active = expire_date >= date.today()
            except:
                is_verification_active = False

    verified_status = "✅ Активна" if is_verification_active else ("❌ Истекла" if is_verified else "❌ Нет")
    expires_text = expires if expires else "Бессрочно"
    created_text = created_at if created_at else "—"

    msg = (
        f"👤 <b>ID:</b> {uid}\n"
        f"🔖 <b>Username:</b> @{uname if uname else '—'}\n"
        f"🎭 <b>Роль:</b> {'🚗 Водитель' if role == 'driver' else '👤 Клиент'}\n"
        f"⭐ <b>Рейтинг:</b> {rating_text}\n"
        f"✅ <b>Верификация:</b> {verified_status}\n"
        f"📅 <b>Дата регистрации:</b> {created_text}\n"
    )
    if role == "driver":
        msg += f"🚘 <b>Авто:</b> {brand or '—'} {number or '—'}\n"
        msg += f"🗓 <b>До:</b> {expires_text}\n"

    buttons = []
    if role == "client":
        buttons.append([InlineKeyboardButton(text="🔄 Сделать водителем", callback_data=f"change_role_{uid}_driver")])
    else:
        buttons.append([InlineKeyboardButton(text="🔄 Сделать клиентом", callback_data=f"change_role_{uid}_client")])
    if role == "driver":
        if is_verification_active:
            buttons.append([InlineKeyboardButton(text="🔁 Снять верификацию", callback_data=f"unverify_{uid}")])
        elif not is_verified or not is_verification_active:
            buttons.append([InlineKeyboardButton(text="✅ Верифицировать", callback_data=f"admin_verify_{uid}")])
        buttons.append([InlineKeyboardButton(text="✏️ Изменить авто", callback_data=f"change_car_{uid}")])
    if is_banned:
        buttons.append([InlineKeyboardButton(text="🔓 Разблокировать", callback_data=f"unban_{uid}")])
    else:
        buttons.append([InlineKeyboardButton(text="🚫 Заблокировать", callback_data=f"ban_{uid}")])
    # buttons.append([InlineKeyboardButton(text="⭐ Изменить рейтинг", callback_data=f"edit_rating_{uid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users_back")])

    await message.answer(
        msg,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )

async def get_monthly_registrations():
    """Возвращает количество регистраций в текущем месяце."""
    now = datetime.now()
    start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_str = start_of_month.strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM users WHERE created_at >= ?",
            (start_str,)
        ) as cursor:
            count = (await cursor.fetchone())[0]
    return count


async def get_average_pickup_time():
    """Среднее время подачи (в минутах) по всем завершённым заказам.
    Время = разница между created_at (заказ) и created_at (заявка со статусом 'accepted').
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT AVG(
                CAST((julianday(b.created_at) - julianday(o.created_at)) * 24 * 60 AS REAL)
            )
            FROM orders o
            JOIN bids b ON o.id = b.order_id
            WHERE o.status = 'completed'
              AND b.status = 'accepted'
        """) as cursor:
            avg = await cursor.fetchone()
    return round(avg[0], 1) if avg and avg[0] is not None else 0.0


async def get_average_driver_rating():
    """Средний рейтинг водителей по всем оценкам."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT AVG(rating)
            FROM ratings r
            JOIN users u ON r.target_id = u.user_id
            WHERE u.role = 'driver'
        """) as cursor:
            avg = await cursor.fetchone()
    return round(avg[0], 2) if avg and avg[0] is not None else 0.0


async def get_average_client_rating():
    """Средний рейтинг клиентов по всем оценкам."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT AVG(rating)
            FROM ratings r
            JOIN users u ON r.target_id = u.user_id
            WHERE u.role = 'client'
        """) as cursor:
            avg = await cursor.fetchone()
    return round(avg[0], 2) if avg and avg[0] is not None else 0.0


async def get_repeat_orders_stats():
    """Возвращает (всего повторных заказов, процент от всех завершённых)."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Всего завершённых заказов
        async with db.execute("SELECT COUNT(*) FROM orders WHERE status = 'completed'") as cursor:
            total = (await cursor.fetchone())[0]
        if total == 0:
            return 0, 0.0
        # Повторные заказы — те, у которых в comment есть упоминание повтора или order_id в repeat_order
        # Но проще: считаем заказы, созданные не в день регистрации клиента
        async with db.execute("""
            SELECT COUNT(*)
            FROM orders o
            JOIN users u ON o.client_id = u.user_id
            WHERE o.status = 'completed'
              AND date(o.created_at) > date(u.created_at)
        """) as cursor:
            repeat = (await cursor.fetchone())[0]
    percent = round(repeat / total * 100, 1) if total > 0 else 0.0
    return repeat, percent


async def get_today_stats_extended():
    """Расширенная статистика за сегодня."""
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

    async with aiosqlite.connect(DB_PATH) as db:
        # Новые пользователи
        async with db.execute("SELECT COUNT(*) FROM users WHERE created_at >= ?", (today_start,)) as cursor:
            new_users = (await cursor.fetchone())[0]

        # Завершённые заказы за сегодня
        async with db.execute("""
            SELECT COUNT(*)
            FROM orders
            WHERE status = 'completed' AND created_at >= ?
        """, (today_start,)) as cursor:
            total_today = (await cursor.fetchone())[0]

        repeat_today = 0
        avg_pickup_today = 0.0

        if total_today > 0:
            # Повторные заказы за сегодня
            async with db.execute("""
                SELECT COUNT(*)
                FROM orders o
                JOIN users u ON o.client_id = u.user_id
                WHERE o.status = 'completed'
                  AND o.created_at >= ?
                  AND date(o.created_at) > date(u.created_at)
            """, (today_start,)) as cursor:
                repeat_today = (await cursor.fetchone())[0]

            # Среднее время подачи за сегодня
            async with db.execute("""
                SELECT AVG(
                    CAST((julianday(b.created_at) - julianday(o.created_at)) * 24 * 60 AS REAL)
                )
                FROM orders o
                JOIN bids b ON o.id = b.order_id
                WHERE o.status = 'completed'
                  AND b.status = 'accepted'
                  AND o.created_at >= ?
            """, (today_start,)) as cursor:
                avg_row = await cursor.fetchone()
                avg_pickup_today = round(avg_row[0], 1) if avg_row and avg_row[0] is not None else 0.0

    repeat_percent = round(repeat_today / total_today * 100, 1) if total_today > 0 else 0.0
    return new_users, avg_pickup_today, repeat_today, repeat_percent


async def get_active_drivers_count():
    """Количество водителей со сменой 'открыта'."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users WHERE role = 'driver' AND shift_opened = 1") as cursor:
            count = (await cursor.fetchone())[0]
    return count

async def backup_scheduler():
    """Фоновая задача: создаёт резервные копии БД раз в сутки."""
    while True:
        now = datetime.now()
        # Следующий запуск — завтра в 03:00
        next_run = now.replace(hour=3, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run = next_run.replace(day=next_run.day + 1)

        sleep_seconds = (next_run - now).total_seconds()
        logging.info(f"🕒 Следующее резервное копирование через {sleep_seconds:.0f} секунд.")

        await asyncio.sleep(sleep_seconds)

        # Создаём копию
        await create_backup()


async def cleanup_old_data():
    """Удаляет старые записи из ad_stats и broadcasts."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # === 1. Очистка ad_stats (старше 90 дней) ===
            cutoff_ad = (datetime.now() - timedelta(days=90)).isoformat()
            cursor = await db.execute("DELETE FROM ad_stats WHERE timestamp < ?", (cutoff_ad,))
            deleted_ads = cursor.rowcount
            await db.commit()
            logging.info(f"🧹 Удалено {deleted_ads} старых записей из ad_stats")

            # === 2. Очистка broadcast_receipts и broadcasts (старше 60 дней) ===
            cutoff_broadcast = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")

            # Сначала удаляем подтверждения
            cursor = await db.execute("""
                DELETE FROM broadcast_receipts 
                WHERE broadcast_id IN (
                    SELECT id FROM broadcasts WHERE created_at < ?
                )
            """, (cutoff_broadcast,))
            deleted_receipts = cursor.rowcount

            # Затем сами рассылки
            cursor = await db.execute("DELETE FROM broadcasts WHERE created_at < ?", (cutoff_broadcast,))
            deleted_broadcasts = cursor.rowcount

            await db.commit()
            logging.info(f"🧹 Удалено {deleted_broadcasts} рассылок и {deleted_receipts} подтверждений")
    except Exception as e:
        logging.error(f"❌ Ошибка при очистке старых данных: {e}")

async def cleanup_scheduler():
    """Запускает очистку раз в сутки в 04:00."""
    while True:
        now = datetime.now()
        next_run = now.replace(hour=4, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        sleep_seconds = (next_run - now).total_seconds()
        logging.info(f"🕒 Следующая очистка старых данных через {sleep_seconds:.0f} секунд.")
        await asyncio.sleep(sleep_seconds)
        await cleanup_old_data()

async def finalize_rating_flow(message_or_callback, user_id: int, rating: int):
    """Завершает процесс оценки: благодарит и возвращает пользователя в главное меню."""
    # Определяем объект сообщения для редактирования/ответа
    if isinstance(message_or_callback, CallbackQuery):
        msg = message_or_callback.message
    else:
        msg = message_or_callback

    # Обновляем сообщение с благодарностью
    try:
        await msg.edit_text(f"⭐ Спасибо! Ваша оценка: {rating} ⭐")
    except Exception as e:
        logging.warning(f"Не удалось отредактировать сообщение после оценки: {e}")
        await msg.answer(f"⭐ Спасибо! Ваша оценка: {rating} ⭐")

    # Возвращаем пользователя в главное меню
    role = await get_user_role(user_id)
    if role == "driver":
        shift_opened = await is_shift_opened(user_id)
        menu = driver_menu(shift_opened)
    else:
        menu = await get_client_menu_with_rating_and_status(user_id)

    await msg.answer("📍 Главное меню:", reply_markup=menu)

def get_client_status(ride_count: int) -> tuple[str, str]:
    """Возвращает (статус, эмодзи) по количеству поездок за месяц."""
    if ride_count >= 30:
        return "Платина", "💎"
    elif ride_count >= 20:
        return "Золото", "🥇"
    elif ride_count >= 10:
        return "Серебро", "🥈"
    else:
        return "Стандарт", ""

async def ensure_client_menu_exists(order_id: int, client_id: int, driver_id: int):
    order = await get_order(order_id)
    if not order or order["status"] != "accepted":
        return
    pickup = order["pickup_address"]
    dropoff = order["dropoff_address"]
    comment = (order["comment"] or "").strip()
    comment_block = f"\n📝 Комментарий: {comment}" if comment else ""
    car_info = await get_driver_info(driver_id)
    car_text = f"{car_info[0]} {car_info[1]}" if car_info else "Не указано"
    rating = await get_driver_rating(driver_id)
    if order_id in client_order_messages:
        chat_id, msg_id = client_order_messages[order_id]
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=(
                    f"✅ Вы выбрали водителя!\n"
                    f"🚗 {car_text}\n"
                    f"⭐ {rating}\n\n"
                    f"📍 {pickup} → {dropoff}"
                    f"{comment_block}"
                ),
                reply_markup=client_order_menu(driver_id, order_id)
            )
            return
        except TelegramBadRequest as e:
            if "message to edit not found" in str(e):
                logging.warning(f"Меню клиента для заказа {order_id} утеряно. Переотправка...")
                client_order_messages.pop(order_id, None)
            else:
                raise
    try:
        new_msg = await bot.send_message(
            client_id,
            text=(
                f"✅ Вы выбрали водителя!\n"
                f"🚗 {car_text}\n"
                f"⭐ {rating}\n"
                f"📍 {pickup} → {dropoff}"
                f"{comment_block}"
            ),
            reply_markup=client_order_menu(driver_id, order_id)
        )
        client_order_messages[order_id] = (new_msg.chat.id, new_msg.message_id)
    except Exception as e:
        logging.error(f"Не удалось восстановить меню клиента {client_id} для заказа {order_id}: {e}")

async def ensure_driver_menu_exists(order_id: int, driver_id: int, client_id: int):
    """Проверяет и восстанавливает меню активного заказа у водителя."""
    order = await get_order(order_id)
    if not order or order["status"] != "accepted":
        return

    pickup = order["pickup_address"]
    dropoff = order["dropoff_address"]
    comment = (order["comment"] or "").strip()
    comment_block = f"\n📝 Комментарий: {comment}" if comment else ""

    if order_id in driver_order_messages:
        chat_id, msg_id = driver_order_messages[order_id]
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=f"✅ Клиент выбрал вас!\n📍 {pickup} → {dropoff}{comment_block}",
                reply_markup=driver_order_menu(client_id, order_id)
            )
            return
        except TelegramBadRequest as e:
            if "message to edit not found" in str(e):
                logging.warning(f"Меню водителя для заказа {order_id} утеряно. Переотправка...")
                driver_order_messages.pop(order_id, None)
            else:
                raise

    # Переотправляем
    try:
        new_msg = await bot.send_message(
            driver_id,
            f"✅ Клиент выбрал вас!\n📍 {pickup} → {dropoff}{comment_block}",
            reply_markup=driver_order_menu(client_id, order_id)
        )
        driver_order_messages[order_id] = (new_msg.chat.id, new_msg.message_id)
    except Exception as e:
        logging.error(f"Не удалось восстановить меню водителя {driver_id} для заказа {order_id}: {e}")

async def search_user_by_id(message: Message, user_id: int):
    """Редактирует существующее сообщение с карточкой пользователя (с рейтингом)."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT 
                u.user_id, u.username, u.role, u.car_brand, u.car_number, 
                u.is_verified, u.verification_expires, u.is_banned, u.created_at,
                (SELECT AVG(rating) FROM ratings WHERE target_id = u.user_id) as avg_rating
            FROM users u 
            WHERE user_id = ?
        """, (user_id,))
        row = await cursor.fetchone()
    if not row:
        await message.edit_text("❌ Пользователь не найден.")
        return
    uid, uname, role, brand, number, is_verified, expires, is_banned, created_at, avg_rating = row
    # Форматируем рейтинг
    rating_text = f"{round(avg_rating, 2)}" if avg_rating is not None else "—"
    # Проверка активности верификации
    is_verification_active = False
    if is_verified:
        if expires is None:
            is_verification_active = True
        else:
            from datetime import date
            try:
                expire_date = date.fromisoformat(expires)
                is_verification_active = expire_date >= date.today()
            except:
                is_verification_active = False
    verified_status = "✅ Активна" if is_verification_active else ("❌ Истекла" if is_verified else "❌ Нет")
    expires_text = expires if expires else "Бессрочно"
    created_text = created_at if created_at else "—"
    msg = (
        f"👤 <b>ID:</b> {uid}\n"
        f"🔖 <b>Username:</b> @{uname if uname else '—'}\n"
        f"🎭 <b>Роль:</b> {'🚗 Водитель' if role == 'driver' else '👤 Клиент'}\n"
        f"⭐ <b>Рейтинг:</b> {rating_text}\n"
        f"✅ <b>Верификация:</b> {verified_status}\n"
        f"📅 <b>Дата регистрации:</b> {created_text}\n"
    )
    if role == "driver":
        msg += f"🚘 <b>Авто:</b> {brand or '—'} {number or '—'}\n"
        msg += f"🗓 <b>До:</b> {expires_text}\n"
    # Кнопки
    buttons = []
    if role == "client":
        buttons.append([InlineKeyboardButton(text="🔄 Сделать водителем", callback_data=f"change_role_{uid}_driver")])
    else:
        buttons.append([InlineKeyboardButton(text="🔄 Сделать клиентом", callback_data=f"change_role_{uid}_client")])
    if role == "driver":
        if is_verification_active:
            buttons.append([InlineKeyboardButton(text="🔁 Снять верификацию", callback_data=f"unverify_{uid}")])
        elif not is_verified or not is_verification_active:
            buttons.append([InlineKeyboardButton(text="✅ Верифицировать", callback_data=f"admin_verify_{uid}")])
        buttons.append([InlineKeyboardButton(text="✏️ Изменить авто", callback_data=f"change_car_{uid}")])
    if is_banned:
        buttons.append([InlineKeyboardButton(text="🔓 Разблокировать", callback_data=f"unban_{uid}")])
    else:
        buttons.append([InlineKeyboardButton(text="🚫 Заблокировать", callback_data=f"ban_{uid}")])

    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users_back")])
    await message.edit_text(
        msg,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
# #===========================ЗАПУСК И ОСТАНОВКА WEBAPP PWA МОДУЛЯ=====================================
# def start_webapp():
#     """Запускает веб-приложение в отдельном процессе"""
#     logger.info("Запуск веб-приложения...")
#     try:
#         # Запускаем веб-приложение в фоновом режиме
#         return subprocess.Popen(
#             [sys.executable, "webapp.py"],
#             stdout=subprocess.PIPE,
#             stderr=subprocess.PIPE,
#             universal_newlines=True
#         )
#     except Exception as e:
#         logger.error(f"Ошибка при запуске веб-приложения: {e}")
#         return None
#
# def stop_webapp(webapp_process):
#     """Останавливает веб-приложение"""
#     if webapp_process:
#         logger.info("Остановка веб-приложения...")
#         webapp_process.terminate()
#         try:
#             webapp_process.wait(timeout=5)
#         except subprocess.TimeoutExpired:
#             webapp_process.kill()
#==============================================================================

# # --- ЗАПУСК --- ЛОГИКА ЗАПУСКА ДО WEBAPP
async def main():
    await init_db()
    await restore_active_timers()
    asyncio.create_task(broadcast_scheduler())
    asyncio.create_task(backup_scheduler())
    asyncio.create_task(cleanup_scheduler())
    dp.include_router(router)
    await dp.start_polling(bot)
#=============================================

# async def main():
#     await init_db()
#     await restore_active_timers()
#
#     # Запускаем веб-приложение
#     webapp_process = start_webapp()
#     if webapp_process:
#         logger.info("Веб-приложение успешно запущено")
#     else:
#         logger.warning("Не удалось запустить веб-приложение")
#
#     # Запускаем остальные задачи
#     asyncio.create_task(broadcast_scheduler())
#     asyncio.create_task(backup_scheduler())
#     asyncio.create_task(cleanup_scheduler())
#
#     dp.include_router(router)
#     try:
#         await dp.start_polling(bot)
#     finally:
#         # При завершении основного приложения останавливаем веб-приложение
#         stop_webapp(webapp_process)
#         logger.info("Веб-приложение остановлено")


if __name__ == "__main__":
    asyncio.run(main())
    start_webapp(bot)