import asyncio
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import aiosqlite
from pydantic import BaseModel
from typing import Optional
import uvicorn
import os
import aiohttp
import json
from datetime import datetime
try:
    from aiogram.types import InlineKeyboardMarkup
except ImportError:
    # aiogram may not be available in this context, but the hasattr check will still work
    pass


# === Настройки ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - webapp - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/var/log/taxi_api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_PATH = "/root/test/taxi_bot.db"
BOT_TOKEN = os.getenv("BOT_TOKEN", "8417867887:AAFzHQcBEYc3ZOE0KkURCN8zUWIh_tysscU")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

# === CORS ===
origins = [
    "https://taxibarsnz24.ru",
    "http://taxibarsnz24.ru",
    "https://www.taxibarsnz24.ru",
    "http://localhost:8000",
    "http://127.0.0.1",
]

app = FastAPI(title="Taxi Web API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ADMINS = 257681118, 805113718


def start_webapp(bot_instance):
    global bot
    bot = bot_instance
    # остальной код запуска веб-приложения

def send_telegram_message_direct(chat_id, text):
    # Импорт происходит ТОЛЬКО при вызове функции
    from main import bot
    return asyncio.run(bot.send_message(chat_id, text))

# === Модели ===
class CreateOrderRequest(BaseModel):
    client_id: int
    pickup_address: str
    dropoff_address: str
    comment: Optional[str] = ""
    passengers: int = 1
    price: float
    distance_km: float  # ← добавлено
    estimated_time_min: str  # ← добавлено (может быть "15 мин", "30 мин" и т.д.)
    pickup_lat: Optional[float] = None
    pickup_lon: Optional[float] = None
    dropoff_lat: Optional[float] = None
    dropoff_lon: Optional[float] = None

class AcceptDriverRequest(BaseModel):
    driver_id: int

class CancelOrderRequest(BaseModel):
    reason: str = "client_cancelled"

# === Временное хранилище ===
_TEMP_ORDER_DATA = {}  # order_id → dict
_ORDER_MESSAGES = {} # Хранения id сообщений для их удаления
CANCEL_TASKS = {} # Для хранения задачи таймеров

# === Вспомогательные функции ===
async def get_order(order_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
    return None

async def create_order(client_id, pickup, dropoff, comment):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO orders (client_id, pickup_address, dropoff_address, comment, status, source) VALUES (?, ?, ?, ?, 'pending', 'web')",
            (client_id, pickup, dropoff, comment)
        )
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cursor:
            return (await cursor.fetchone())[0]


async def send_telegram_message(chat_id: int, text: str, reply_markup=None):
    """Отправляет сообщение и возвращает ID сообщения для последующего удаления"""
    try:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        if reply_markup:
            # Convert InlineKeyboardMarkup to dictionary if needed
            if hasattr(reply_markup, 'to_dict'):
                # If it's an aiogram InlineKeyboardMarkup object, convert to dict
                reply_markup = reply_markup.to_dict()
            elif isinstance(reply_markup, dict):
                # If it's already a dict, use as is
                pass
            else:
                # If it's some other format, try to convert to dict
                try:
                    reply_markup = dict(reply_markup)
                except (TypeError, ValueError):
                    logger.warning(f"Could not convert reply_markup to dict: {type(reply_markup)}")
                    reply_markup = None
            
            if reply_markup:
                # Recursively convert any nested objects that might not be JSON serializable
                def convert_objects(obj):
                    if hasattr(obj, 'to_dict'):
                        return obj.to_dict()
                    elif isinstance(obj, dict):
                        return {key: convert_objects(value) for key, value in obj.items()}
                    elif isinstance(obj, list):
                        return [convert_objects(item) for item in obj]
                    else:
                        return obj
                
                reply_markup = convert_objects(reply_markup)
                payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

        logger.debug(f"📤 Отправка сообщения на {TELEGRAM_API_URL}/sendMessage")
        logger.debug(f"📦 Payload: {json.dumps(payload, ensure_ascii=False, indent=2)}")

        async with aiohttp.ClientSession() as session:
            async with session.post(f"{TELEGRAM_API_URL}/sendMessage", json=payload) as response:
                response_text = await response.text()
                response_data = json.loads(response_text)
                logger.debug(f"↩️ Ответ от Telegram API ({chat_id}): {response.status} {response_text}")

                if response.status == 200 and response_data.get("ok"):
                    message_id = response_data["result"]["message_id"]
                    logger.debug(f"✅ Сообщение успешно отправлено водителю {chat_id}, ID: {message_id}")
                    return message_id
                else:
                    logger.error(f"❌ Ошибка Telegram API ({chat_id}): {response.status} {response_text}")
                    return None

    except Exception as e:
        logger.exception(f"🚨 КРИТИЧЕСКАЯ ОШИБКА при отправке сообщения водителю {chat_id}: {e}")
        return None


# === Функция для удаления сообщений у водителей ===
async def delete_order_messages(order_id: int):
    """Удаляет сообщения о заказе у всех водителей, которым они были отправлены"""
    if order_id not in _ORDER_MESSAGES:
        return

    messages_to_delete = _ORDER_MESSAGES.pop(order_id)

    for driver_id, message_id in messages_to_delete:
        try:
            payload = {
                "chat_id": driver_id,
                "message_id": message_id
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(f"{TELEGRAM_API_URL}/deleteMessage", json=payload) as response:
                    if response.status == 200:
                        logger.debug(f"✅ Сообщение {message_id} удалено у водителя {driver_id}")
                    else:
                        logger.warning(
                            f"⚠️ Не удалось удалить сообщение {message_id} у водителя {driver_id}: {await response.text()}")
        except Exception as e:
            logger.error(f"❌ Ошибка при удалении сообщения {message_id} у водителя {driver_id}: {e}")

def get_client_status(ride_count: int) -> tuple[str, str]:
    if ride_count >= 30:
        return "Платина", "💎"
    elif ride_count >= 20:
        return "Золото", "🥇"
    elif ride_count >= 10:
        return "Серебро", "🥈"
    else:
        return "Стандарт", ""

# Добавьте эти функции перед определением эндпоинтов
async def has_user_rated(order_id: int, user_id: int) -> bool:
    """Проверяет, оценил ли пользователь этот заказ."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT 1 FROM ratings 
            WHERE order_id = ? AND rater_id = ?
        """, (order_id, user_id)) as cursor:
            return await cursor.fetchone() is not None

async def save_rating(order_id: int, rater_id: int, target_id: int, rating: int, comment: str = ""):
    """Сохраняет оценку в базу данных (без комментария)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO ratings (order_id, rater_id, target_id, rating)
            VALUES (?, ?, ?, ?)
        """, (order_id, rater_id, target_id, rating))
        await db.commit()

async def get_user_role(user_id: int) -> str:
    """Возвращает роль пользователя (client/driver)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT role FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else "client"  # По умолчанию client

async def get_user_username(user_id: int) -> str:
    """Возвращает username пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT username FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else f"ID_{user_id}"

# === Основная функция уведомления ===
async def notify_drivers_about_order(
    order_id: int,
    passengers: int = 1,
    price: float = 0.0,
    pickup_lat: Optional[float] = None,
    pickup_lon: Optional[float] = None,
    dropoff_lat: Optional[float] = None,
    dropoff_lon: Optional[float] = None,
):
    _TEMP_ORDER_DATA[order_id] = {
        "passengers": passengers,
        "price": price,
        "pickup_lat": pickup_lat,
        "pickup_lon": pickup_lon,
        "dropoff_lat": dropoff_lat,
        "dropoff_lon": dropoff_lon,
    }

    order = await get_order(order_id)
    if not order or order["status"] != "pending":
        return

    # Инициализируем список для хранения ID сообщений
    _ORDER_MESSAGES[order_id] = []

    client_id = order["client_id"]

    # 🔥 Рейтинг клиента
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT AVG(rating) FROM ratings WHERE target_id = ?", (client_id,)) as cursor:
            row = await cursor.fetchone()
            client_rating = round(row[0], 1) if row and row[0] is not None else 0.0

    # 🔥 Статус клиента (как в основном боте)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT ride_count FROM monthly_rides WHERE user_id = ? AND year_month = ?",
            (client_id, datetime.now().strftime("%Y-%m"))
        ) as cursor:
            row = await cursor.fetchone()
            rides = row[0] if row else 0
    status_name, status_emoji = get_client_status(rides)
    client_status_display = f"{status_emoji} {status_name}"

    # 🔥 Только водители со сменой = 1 И is_verified = 1
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id FROM users 
            WHERE role = 'driver' AND shift_opened = 1 AND is_verified = 1
        """) as cursor:
            drivers = [row[0] for row in await cursor.fetchall()]

    if not drivers:
        logger.warning(f"Нет активных водителей для заказа {order_id}")
        return

    # Формат пассажиров
    if passengers == 1:
        passenger_text = "1 пассажир"
    elif 2 <= passengers <= 4:
        passenger_text = f"{passengers} пассажира"
    else:
        passenger_text = f"{passengers} пассажиров"

    message_text = (
        f"🔥 <b>Новый заказ №{order_id}</b>\n\n"
        f"📍 <b>Откуда:</b> {order['pickup_address']}\n"
        f"🏁 <b>Куда:</b> {order['dropoff_address']}\n"
        f"📝 <b>Комментарий:</b> {order['comment'] or '—'}\n"
        f"👥 <b>Пассажиров:</b> {passenger_text}\n\n"
        f"💰 <b>Стоимость:</b> {price} руб.\n"
        f"⭐ <b>Рейтинг клиента:</b> {client_rating}\n"
        f"💎 <b>Статус клиента:</b> {client_status_display}\n\n"
        f"<i>Нажмите «✅ Принять заказ» для отклика</i>"
    )

    keyboard = {"inline_keyboard": [[{"text": "✅ Принять заказ", "callback_data": f"accept_{order_id}"}]]}

    for driver_id in drivers:
        # Используем новую функцию для получения ID сообщения
        message_id = await send_telegram_message(driver_id, message_text, reply_markup=keyboard)
        if message_id:
            _ORDER_MESSAGES[order_id].append((driver_id, message_id))
        else:
            logger.warning(f"Не удалось получить ID сообщения для водителя {driver_id}")

async def auto_cancel_order_if_no_bids(order_id: int, client_id: int):
    """Автоматически отменяет заказ если нет откликов через 180 секунд"""
    # Регистрируем задачу в глобальном словаре
    task = asyncio.current_task()
    CANCEL_TASKS[order_id] = task
    await asyncio.sleep(180)  # 3 минуты ожидания

    # Проверяем актуальный статус заказа
    order = await get_order(order_id)
    if not order or order["status"] != "pending":
        logger.debug(f"Заказ {order_id} уже обработан (статус: {order['status'] if order else 'unknown'})")
        return

    try:
        # Используем существующий механизм отмены заказов
        from database import cancel_order_with_reason
        await cancel_order_with_reason(order_id, "Никто не откликнулся")
        # === 🗑️ УДАЛЯЕМ СООБЩЕНИЯ У ВСЕХ ВОДИТЕЛЕЙ ===
        await delete_order_messages(order_id)
        # Очищаем временные данные
        _TEMP_ORDER_DATA.pop(order_id, None)

        # Уведомляем клиента
        await send_telegram_message(
            client_id,
            f"❌ Заказ №{order_id} отменён автоматически: не найдено водителей в течение 3 минут."
        )
        logger.info(f"✅ Заказ {order_id} автоматически отменён по таймауту")
    except Exception as e:
        logger.error(f"❌ Ошибка при автоматической отмене заказа {order_id}: {e}", exc_info=True)
    finally:
        # Всегда удаляем задачу из словаря при завершении
        CANCEL_TASKS.pop(order_id, None)

# === Эндпоинты ===
@app.post("/api/web/order/create")
async def create_web_order(order_data: CreateOrderRequest):
    try:
        order_id = await create_order(
            client_id=order_data.client_id,
            pickup=order_data.pickup_address,
            dropoff=order_data.dropoff_address,
            comment=order_data.comment
        )

        # Сохраняем ВСЕ данные заказа
        _TEMP_ORDER_DATA[order_id] = {
            "passengers": order_data.passengers,
            "price": order_data.price,
            "pickup_lat": order_data.pickup_lat,
            "pickup_lon": order_data.pickup_lon,
            "dropoff_lat": order_data.dropoff_lat,
            "dropoff_lon": order_data.dropoff_lon,
            "distance": order_data.distance_km,  # Добавлено поле расстояния
            "estimated_time": order_data.estimated_time_min,  # Добавлено поле времени
            "client_id": order_data.client_id,
            "pickup_address": order_data.pickup_address,
            "dropoff_address": order_data.dropoff_address,
            "comment": order_data.comment
        }

        # Начинаем уведомление водителей
        asyncio.create_task(notify_drivers_about_order(
            order_id=order_id,
            passengers=order_data.passengers,
            price=order_data.price,
            pickup_lat=order_data.pickup_lat,
            pickup_lon=order_data.pickup_lon,
            dropoff_lat=order_data.dropoff_lat,
            dropoff_lon=order_data.dropoff_lon,
        ))

        # Запускаем таймер автоматической отмены
        task = asyncio.create_task(auto_cancel_order_if_no_bids(order_id, order_data.client_id))
        CANCEL_TASKS[order_id] = task  # Сохраняем ссылку на задачу

        return {"success": True, "order_id": order_id}
    except Exception as e:
        logger.error(f"Ошибка создания заказа: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Ошибка при создании заказа")

@app.get("/api/web/order/{order_id}/bids")
async def get_order_bids(order_id: int):
    try:
        from database import get_bids_for_order, get_driver_rating

        bids = await get_bids_for_order(order_id)
        if not bids:
            return {"success": True, "bids": [], "count": 0}

        result = []
        for bid in bids:
            driver_id, car_brand, car_number, arrival_minutes, has_co_driver = bid
            rating = await get_driver_rating(driver_id)
            result.append({
                "driver_id": driver_id,
                "car_brand": car_brand or "Не указано",
                "car_number": car_number or "Не указан",
                "arrival_minutes": arrival_minutes or 5,
                "has_co_driver": bool(has_co_driver),
                "driver_name": f"Водитель #{driver_id}",
                "driver_rating": rating
            })

        # 🔥 ОТМЕНЯЕМ ТАЙМЕР АВТОМАТИЧЕСКОЙ ОТМЕНЫ
        if order_id in CANCEL_TASKS:
            task = CANCEL_TASKS.pop(order_id)
            if not task.done():  # Проверяем, не завершена ли задача уже
                task.cancel()
                logger.info(f"⏰ Таймер отмены заказа {order_id} успешно отменён")

        return {
            "success": True,
            "bids": result,
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"Ошибка при получении откликов для заказа {order_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/web/order/{order_id}/accept")
async def accept_driver(order_id: int, data: AcceptDriverRequest):
    try:
        # 🔥 ИСПРАВЛЕНО: используем ЛОКАЛЬНУЮ функцию get_order вместо импортированной
        order = await get_order(order_id)
        if not order:
            raise HTTPException(status_code=404, detail="Заказ не найден")

        # Проверяем, не занят ли водитель другим активным заказом
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT id FROM orders WHERE driver_id = ? AND status = 'accepted'", (data.driver_id,)) as cursor:
                active_orders = await cursor.fetchall()
                if active_orders:
                    raise HTTPException(status_code=400, detail="Водитель уже выполняет другой заказ. Пожалуйста выберите другого.")

        from database import accept_bid
        success = await accept_bid(order_id, data.driver_id)
        if not success:
            raise HTTPException(status_code=400, detail="Невозможно принять водителя")

        # === 🗑️ УДАЛЯЕМ СООБЩЕНИЯ У ВСЕХ ВОДИТЕЛЕЙ ===
        await delete_order_messages(order_id)

        # === 1. Отправляем активное меню водителю ===
        pickup = order["pickup_address"]
        dropoff = order["dropoff_address"]
        comment = order.get("comment") or ""
        comment_block = f"\n📝 Комментарий: {comment}" if comment else ""

        menu_text = (
            f"✅ Вы приняли заказ №{order_id}\n\n"
            f"📍 <b>Откуда:</b> {pickup}\n"
            f"🏁 <b>Куда:</b> {dropoff}"
            f"{comment_block}\n\n"
            f"<i>Используйте кнопки ниже для управления заказом</i>"
        )

        menu_keyboard = {
            "inline_keyboard": [
                [{"text": "✅ Прибыл на место", "callback_data": f"arrived_{order_id}"}],
                [{"text": "🏁 Завершить заказ", "callback_data": f"complete_{order_id}"}],
                [{"text": "❌ Отменить заказ", "callback_data": f"cancel_driver_{order_id}"}]
            ]
        }

        await send_telegram_message(data.driver_id, menu_text, reply_markup=menu_keyboard)

        # === 2. Отправляем кнопку «Маршрут» ===
        temp = _TEMP_ORDER_DATA.get(order_id, {})
        logger.info(f"🔍 Временные данные для заказа {order_id}: {temp}")

        pickup_lat = temp.get("pickup_lat")
        pickup_lon = temp.get("pickup_lon")

        if pickup_lat and pickup_lon:
            # 🔥 Правильный формат URL для Google Maps
            route_url = f"https://www.google.com/maps/dir/?api=1&destination={pickup_lat},{pickup_lon}"

            # 🔥 Правильный формат кнопки
            route_keyboard = {
                "inline_keyboard": [
                    [{"text": "🗺 Маршрут", "url": route_url}]
                ]
            }

            route_message = await send_telegram_message(
                data.driver_id,
                "✅ Вы приняли заказ. Откройте маршрут к клиенту:",
                reply_markup=route_keyboard
            )

            logger.info(
                f"✅ Кнопка маршрута отправлена водителю {data.driver_id} для заказа {order_id}. Результат: {route_message}")
        else:
            logger.error(f"❌ Координаты не найдены в _TEMP_ORDER_DATA для заказа {order_id}. Данные: {temp}")

        return {"success": True, "message": "Водитель принят"}
    except HTTPException:
        # Перебрасываем HTTP-исключения как есть
        raise
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА при принятии водителя: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/web/order/{order_id}")
async def get_order_details(order_id: int):
    try:
        order = await get_order(order_id)
        if not order:
            raise HTTPException(status_code=404, detail="Заказ не найден")

        # 🔥 Получаем статус прибытия водителя из БД
        is_arrived = False
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                    "SELECT driver_arrived FROM orders WHERE id = ?",
                    (order_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row and row[0] == 1:
                    is_arrived = True

        driver_info = None
        if order.get("driver_id"):
            from database import get_driver_info
            driver_data = await get_driver_info(order["driver_id"])
            if driver_data:  # 🔧 Исправлена синтаксическая ошибка
                car_brand, car_number = driver_data
                driver_info = {
                    "id": order["driver_id"],
                    "car_brand": car_brand,
                    "car_number": car_number
                }

        return {
            "success": True,
            "order": {
                "id": order["id"],
                "client_id": order["client_id"],
                "driver_id": order.get("driver_id"),
                "pickup_address": order["pickup_address"],
                "dropoff_address": order["dropoff_address"],
                "comment": order.get("comment", ""),
                "status": order["status"],
                "created_at": order["created_at"],
                "cancelled_by": order.get("cancelled_by"),
                "driver_arrived": is_arrived,  # 🔥 Теперь получаем из БД
                "driver": driver_info
            }
        }
    except Exception as e:
        logger.error(f"❌ Ошибка при получении деталей заказа {order_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/web/order/{order_id}/cancel")
async def cancel_order_api(order_id: int, cancel_data: CancelOrderRequest):
    try:
        # ✅ Отменяем таймер при ручной отмене
        if order_id in CANCEL_TASKS:
            task = CANCEL_TASKS.pop(order_id)
            if not task.done():
                task.cancel()
                logger.info(f"⏰ Таймер отменён при ручной отмене заказа {order_id}")
        await delete_order_messages(order_id)
        from database import cancel_order_with_reason
        await cancel_order_with_reason(order_id, cancel_data.reason)

        # Используем ЛОКАЛЬНУЮ функцию get_order
        order = await get_order(order_id)
        driver_id = order.get("driver_id") if order else None
        if driver_id:
            await send_telegram_message(
                driver_id,
                f"❌ Заказ №{order_id} отменён клиентом."
            )
             # === 🗑️ УДАЛЯЕМ СООБЩЕНИЯ У ВСЕХ ВОДИТЕЛЕЙ ===
            await delete_order_messages(order_id)

        return {"success": True, "message": "Заказ отменён"}
    except Exception as e:
        logger.error(f"❌ Ошибка отмены заказа: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/web/order/{order_id}/complete")
async def complete_order_api(order_id: int):
    try:
        from database import complete_order, has_user_rated
        from keyboards import rating_keyboard
        from main import bot
        await complete_order(order_id)
        
        # Используем ЛОКАЛЬНУЮ функцию get_order вместо импортированной
        order = await get_order(order_id)
        logger.info(f"Данные заказа после завершения: {order}")

        if order and order.get("driver_id"):
            driver_id = order["driver_id"]
            client_id = order["client_id"]
            
            # Уведомляем водителя об успешном завершении
            await send_telegram_message(
                driver_id,
                f"🎉 Заказ №{order_id} успешно завершен! Спасибо за работу."
            )
            logger.info(f"✅ Уведомление о завершении отправлено водителю {driver_id}")
            
            # Отправляем запрос на оценку водителя клиенту (аналогично Telegram-версии)
            already_client_rated = await has_user_rated(order_id, client_id)
            if not already_client_rated:
                try:
                    await bot.send_message(
                        client_id,
                        f"🏁 Заказ №{order_id} завершён! Пожалуйста, оцените водителя:",
                        reply_markup=rating_keyboard(driver_id, order_id)
                    )
                    logger.info(f"✅ Запрос на оценку водителя отправлен клиенту {client_id}")
                except Exception as e:
                    logger.warning(f"Не удалось отправить запрос на оценку клиенту {client_id}: {e}")
            
            # Отправляем запрос на оценку клиента водителю (если водитель еще не оценил)
            already_driver_rated = await has_user_rated(order_id, driver_id)
            if not already_driver_rated:
                try:
                    await send_telegram_message(
                        driver_id,
                        f"🏁 Заказ №{order_id} завершён. Оцените клиента:"
                    )
                    await send_telegram_message(
                        driver_id,
                        "Поставьте оценку клиенту от 1 до 5:",
                        reply_markup=rating_keyboard(client_id, order_id)
                    )
                    logger.info(f"✅ Запрос на оценку клиента отправлен водителю {driver_id}")
                except Exception as e:
                    logger.warning(f"Не удалось отправить запрос на оценку водителю {driver_id}: {e}")

        return {"success": True, "message": "Заказ завершён"}
    except Exception as e:
        logger.error(f"❌ Ошибка завершения заказа: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/web/rating/submit")
async def submit_rating(rating_data: dict):
    try:
        order_id = rating_data.get("order_id")
        rater_id = rating_data.get("rater_id")
        target_id = rating_data.get("target_id")
        rating = rating_data.get("rating")
        comment = rating_data.get("comment", "")

        # Валидация данных
        if not all([order_id, rater_id, target_id, rating]):
            raise HTTPException(status_code=400, detail="Необходимые данные отсутствуют")

        if not (1 <= rating <= 5):
            raise HTTPException(status_code=400, detail="Оценка должна быть от 1 до 5")

        # Проверяем, не оценивал ли пользователь уже этот заказ
        has_rated = await has_user_rated(order_id, rater_id)
        if has_rated:
            raise HTTPException(status_code=400, detail="Вы уже оценили этот заказ")

        # Сохраняем оценку
        await save_rating(order_id, rater_id, target_id, rating, comment)

        # Если оценка низкая и есть комментарий - отправляем админам
        if rating <= 3 and comment.strip():
            rater_role = await get_user_role(rater_id)
            target_role = await get_user_role(target_id)

            # Получаем username пользователей
            rater_username = await get_user_username(rater_id)
            target_username = await get_user_username(target_id)

            admin_msg = (
                f"⚠️ <b>Комментарий к низкой оценке</b>\n"
                f"Заказ: #{order_id}\n"
                f"Оценил: ID {rater_id} (@{rater_username}) — {rater_role}\n"
                f"Получил: ID {target_id} (@{target_username}) — {target_role}\n"
                f"Оценка: {rating}\n"
                f"Комментарий:\n{comment}"
            )

            # Отправляем сообщение всем администраторам
            for admin_id in ADMINS:
                try:
                    await send_telegram_message(admin_id, admin_msg)
                except Exception as e:
                    logging.error(f"Не удалось отправить комментарий админу {admin_id}: {e}")

            # Отправляем комментарий целевому пользователю
            target_msg = f"💬 Пользователь оставил комментарий к оценке {rating} за заказ №{order_id}:\n{comment}"
            try:
                await send_telegram_message(target_id, target_msg)
            except Exception as e:
                logging.warning(f"Не удалось отправить комментарий пользователю {target_id}: {e}")

        return {"success": True, "message": "Оценка успешно сохранена"}

    except Exception as e:
        logger.error(f"Ошибка при сохранении оценки: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/web/user/{user_id}/active-order")
async def get_active_order(user_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            query = """
            SELECT * FROM orders 
            WHERE client_id = ? 
            AND status = 'accepted' 
            ORDER BY created_at DESC 
            LIMIT 1
            """
            async with db.execute(query, (user_id,)) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return {"success": False, "message": "Нет активных заказов"}

                columns = [desc[0] for desc in cursor.description]
                order = dict(zip(columns, row))

                # Получаем данные из временного хранилища
                temp_data = _TEMP_ORDER_DATA.get(order["id"], {})

                # Получаем информацию о водителе
                driver_info = None
                if order.get("driver_id"):
                    async with db.execute("""
                        SELECT user_id, username, car_brand, car_number
                        FROM users
                        WHERE user_id = ?
                    """, (order["driver_id"],)) as driver_cursor:
                        driver_row = await driver_cursor.fetchone()
                        if driver_row:
                            driver_columns = [desc[0] for desc in driver_cursor.description]
                            driver_data = dict(zip(driver_columns, driver_row))

                            # Получаем рейтинг водителя
                            driver_rating = 4.8
                            async with db.execute("""
                                SELECT AVG(rating) FROM ratings WHERE target_id = ?
                            """, (order["driver_id"],)) as rating_cursor:
                                rating_row = await rating_cursor.fetchone()
                                if rating_row and rating_row[0] is not None:
                                    driver_rating = round(rating_row[0], 1)

                            driver_info = {
                                "driver_id": driver_data["user_id"],
                                "driver_name": driver_data.get("username", f"Водитель #{driver_data['user_id']}"),
                                "car_brand": driver_data.get("car_brand", "Автомобиль"),
                                "car_number": driver_data.get("car_number", ""),
                                "driver_rating": driver_rating
                            }

                # Формируем ответ с полными данными заказа
                order_data = {
                    "id": order["id"],
                    "status": order["status"],
                    "pickup_address": order["pickup_address"],
                    "dropoff_address": order["dropoff_address"],
                    "comment": order.get("comment", ""),
                    "created_at": order["created_at"],
                    "price": order.get("price", 0) or temp_data.get("price", 0),
                    "distance_km": temp_data.get("distance", 0.0),
                    "estimated_time_min": temp_data.get("estimated_time", "15 минут"),
                    "passengers": order.get("passengers", 1) or temp_data.get("passengers", 1),
                    "driver_id": order.get("driver_id"),
                    "pickup_coordinates": [
                        order.get("pickup_lat", 0) or temp_data.get("pickup_lat", 0),
                        order.get("pickup_lon", 0) or temp_data.get("pickup_lon", 0)
                    ] if (order.get("pickup_lat") or temp_data.get("pickup_lat")) and (
                                order.get("pickup_lon") or temp_data.get("pickup_lon")) else None,
                    "dropoff_coordinates": [
                        order.get("dropoff_lat", 0) or temp_data.get("dropoff_lat", 0),
                        order.get("dropoff_lon", 0) or temp_data.get("dropoff_lon", 0)
                    ] if (order.get("dropoff_lat") or temp_data.get("dropoff_lat")) and (
                                order.get("dropoff_lon") or temp_data.get("dropoff_lon")) else None
                }

                # Добавляем информацию о водителе
                if driver_info:
                    order_data.update({
                        "driver_name": driver_info["driver_name"],
                        "car_brand": driver_info["car_brand"],
                        "car_number": driver_info["car_number"],
                        "driver_rating": driver_info["driver_rating"]
                    })

                return {
                    "success": True,
                    "order": order_data
                }
    except Exception as e:
        logger.error(f"Ошибка при проверке активного заказа: {e}", exc_info=True)
        return {"success": False, "message": "Ошибка при проверке активных заказов"}

@app.get("/api/web/user/{user_id}")
async def get_user_profile(user_id: int):
    try:
        # Рейтинг
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT AVG(rating) FROM ratings WHERE target_id = ?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                avg_rating = round(row[0], 1) if row and row[0] is not None else 0.0

            # Общее количество завершенных поездок (за всё время)
            async with db.execute("SELECT COUNT(*) FROM orders WHERE client_id = ? AND status = 'completed'", (user_id,)) as cursor:
                row = await cursor.fetchone()
                total_ride_count = row[0] if row else 0

            # Количество поездок за текущий месяц (для определения статуса)
            current_month = datetime.now().strftime("%Y-%m")
            async with db.execute("SELECT ride_count FROM monthly_rides WHERE user_id = ? AND year_month = ?", (user_id, current_month)) as cursor:
                row = await cursor.fetchone()
                monthly_ride_count = row[0] if row else 0

        return {
            "success": True,
            "ride_count": total_ride_count,  # Общее количество поездок за всё время
            "monthly_ride_count": monthly_ride_count,  # Количество поездок за текущий месяц
            "rating": avg_rating
        }
    except Exception as e:
        logger.error(f"Ошибка профиля {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Ошибка загрузки профиля")

@app.get("/health")
async def health_check():
    return {"status": "ok"}

# === Запуск ===
if __name__ == "__main__":
    uvicorn.run("webapp:app", host="0.0.0.0", port=8004, log_level="info")