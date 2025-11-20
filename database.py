import aiosqlite
import logging
import shutil
import os
from datetime import datetime, date, timezone
from typing import Optional, Tuple, List

#РЕЗЕРВНОЕ КОПИРОВАНИЕ
DB_PATH = "taxi_bot.db"
BACKUP_DIR = "backups"
MAX_BACKUPS = 7

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ad_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ad_id INTEGER,
                user_id INTEGER,
                event_type TEXT,  -- 'impression' или 'click'
                timestamp TEXT
            )      
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS partner_ads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                partner_name TEXT NOT NULL,
                message_text TEXT NOT NULL,
                photo_file_id TEXT,
                url TEXT NOT NULL,               -- Обязательная партнёрская ссылка
                is_active BOOLEAN DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                role TEXT CHECK(role IN ('driver', 'client')),
                car_brand TEXT,
                car_number TEXT,
                is_verified BOOLEAN DEFAULT 0,
                verification_expires DATE,
                shift_opened BOOLEAN DEFAULT 0,
                is_banned BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER,
                driver_id INTEGER,
                pickup_address TEXT,
                dropoff_address TEXT,
                comment TEXT,
                status TEXT CHECK(status IN ('pending', 'accepted', 'completed', 'cancelled')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                cancelled_by TEXT,
                source TEXT DEFAULT 'telegram',
                driver_arrived INTEGER DEFAULT 0
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS bids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER,
                driver_id INTEGER,
                arrival_minutes INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT CHECK(status IN ('pending', 'accepted', 'rejected')) DEFAULT 'pending',
                FOREIGN KEY(order_id) REFERENCES orders(id),
                FOREIGN KEY(driver_id) REFERENCES users(user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS ratings (
                order_id INTEGER,
                rater_id INTEGER,
                target_id INTEGER,
                rating INTEGER CHECK(rating BETWEEN 1 AND 5),
                PRIMARY KEY (order_id, rater_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target TEXT NOT NULL,          -- 'all', 'drivers', 'clients'
                message_text TEXT,
                photo_file_id TEXT,
                document_file_id TEXT,
                caption TEXT,
                scheduled_at TIMESTAMP,        -- когда отправить
                is_sent BOOLEAN DEFAULT 0,
                total_recipients INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_receipts (
                broadcast_id INTEGER,
                user_id INTEGER,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (broadcast_id, user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS driver_order_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                driver_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
                FOREIGN KEY (driver_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        # По умолчанию выбор роли ВКЛЮЧЁН
        await db.execute("""
            INSERT OR IGNORE INTO settings (key, value) VALUES ('driver_role_enabled', '1')
        """)
        await db.commit()

        await db.execute("""
            INSERT OR IGNORE INTO settings (key, value) VALUES ('co_driver_enabled', '1')
        """)
        await db.commit()

        await db.execute("""
            CREATE TABLE IF NOT EXISTS monthly_rides (
                user_id INTEGER NOT NULL,
                year_month TEXT NOT NULL,  -- формат: '2025-10'
                ride_count INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, year_month)
            )
        """)

        await db.execute("""
            INSERT OR IGNORE INTO settings (key, value) VALUES ('auto_accept_on_first_bid', '0')
        """)

        # 2. Добавляем колонки, если их нет (для существующих баз)
        # Проверяем users
        cursor = await db.execute("PRAGMA table_info(users)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]

        if "is_banned" not in column_names:
            await db.execute("ALTER TABLE users ADD COLUMN is_banned BOOLEAN DEFAULT 0")

        cursor = await db.execute("PRAGMA table_info(broadcasts)")
        columns = await cursor.fetchall()
        colum_names = [col[1] for col in columns]

        if "total_recipients" not in colum_names:
            await db.execute("ALTER TABLE broadcasts ADD COLUMN total_recipients INTEGER DEFAULT 0")

        cursor = await db.execute("PRAGMA table_info(bids)")
        columns = await cursor.fetchall()
        colum_names = [col[1] for col in columns]

        if "arrival_minutes" not in colum_names:
            await db.execute("ALTER TABLE bids ADD COLUMN arrival_minutes INTEGER DEFAULT 0")


        cursor = await db.execute("PRAGMA table_info(users)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]

        if 'created_at' not in column_names:
            await db.execute("ALTER TABLE users ADD COLUMN created_at TIMESTAMP")
            await db.execute("UPDATE users SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL")

            # === Добавляем has_co_driver только если его нет ===
        async with db.execute("PRAGMA table_info(users)") as cursor:
            columns = await cursor.fetchall()
            column_names = {col[1] for col in columns}  # col[1] — имя столбца

        if "has_co_driver" not in column_names:
            await db.execute("ALTER TABLE users ADD COLUMN has_co_driver INTEGER DEFAULT 0")
            await db.commit()
            logging.info("✅ Добавлен столбец has_co_driver в таблицу users")
        # Проверяем другие таблицы при необходимости...

        # Добавляем cancelled_by, если его нет
        cursor = await db.execute("PRAGMA table_info(orders)")
        columns = await cursor.fetchall()
        column_names = {col[1] for col in columns}
        if "cancelled_by" not in column_names:
            await db.execute("ALTER TABLE orders ADD COLUMN cancelled_by TEXT")
            await db.commit()
            logging.info("✅ Добавлен столбец cancelled_by в таблицу orders")

        # Добавляем Source, если его нет
        cursor = await db.execute("PRAGMA table_info(orders)")
        columns = {col[1] for col in await cursor.fetchall()}
        if "source" not in columns:
            await db.execute("ALTER TABLE orders ADD COLUMN source TEXT DEFAULT 'telegram'")

        # Добавляем driver_arrived, если его нет
        cursor = await db.execute("PRAGMA table_info(orders)")
        columns = {col[1] for col in await cursor.fetchall()}
        if "driver_arrived" not in columns:
            await db.execute("ALTER TABLE orders ADD COLUMN driver_arrived INTEGER DEFAULT 0")

        await db.commit()

async def get_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def get_client_order_history(client_id: int, limit: int = 5):
    """Возвращает последние заказы клиента."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT id, pickup_address, dropoff_address, created_at
            FROM orders
            WHERE client_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (client_id, limit)) as cursor:
            return await cursor.fetchall()

async def save_user(user_id: int, role: str = None, username: str = None):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_PATH) as db:
        if role is not None:
            await db.execute("""
                INSERT INTO users (user_id, username, role, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET 
                    username = excluded.username,
                    role = excluded.role,
                    created_at = CASE 
                        WHEN created_at IS NULL THEN ? 
                        ELSE created_at 
                    END
            """, (user_id, username, role, now_str, now_str))
        else:
            await db.execute(
                "UPDATE users SET username = ? WHERE user_id = ?",
                (username, user_id)
            )
        await db.commit()

async def get_random_partner_ad():
    """Возвращает случайное активное партнёрское объявление."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT id, message_text, photo_file_id, url
            FROM partner_ads
            WHERE is_active = 1
            ORDER BY RANDOM()
            LIMIT 1
        """) as cursor:
            row = await cursor.fetchone()
    return row  # (id, message_text, photo_file_id, url)

async def save_car_info(user_id: int, brand: str, number: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET car_brand = ?, car_number = ? WHERE user_id = ?",
            (brand, number, user_id)
        )
        await db.commit()

async def set_shift(user_id: int, is_open: bool, has_co_driver: int = 0):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET shift_opened = ?, has_co_driver = ? WHERE user_id = ?",
            (1 if is_open else 0, has_co_driver, user_id)
        )
        await db.commit()

async def is_shift_opened(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT shift_opened FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return bool(row[0]) if row else False

async def create_order(client_id: int, pickup: str, dropoff: str, comment: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO orders (client_id, pickup_address, dropoff_address, comment, status) VALUES (?, ?, ?, ?, 'pending')",
            (client_id, pickup, dropoff, comment)
        )
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cursor:
            row = await cursor.fetchone()
            return row[0]

async def get_pending_orders():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT o.id, o.client_id, o.pickup_address, o.dropoff_address, o.comment 
            FROM orders o 
            WHERE o.status = 'pending'
        """) as cursor:
            return await cursor.fetchall()

async def get_drivers_with_open_shift():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users WHERE role = 'driver' AND shift_opened = 1") as cursor:
            return [row[0] for row in await cursor.fetchall()]

async def get_order(order_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)) as cursor:
            return await cursor.fetchone()

async def get_user_role(user_id: int) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT role FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row["role"] if row else None

async def get_driver_info(driver_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT car_brand, car_number FROM users WHERE user_id = ?", (driver_id,)
        ) as cursor:
            return await cursor.fetchone()

async def complete_order(order_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE orders SET status = 'completed' WHERE id = ?", (order_id,))
        await db.commit()

async def cancel_order(order_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE orders SET status = 'cancelled' WHERE id = ?", (order_id,))
        await db.commit()

async def save_rating(order_id: int, rater_id: int, target_id: int, rating: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO ratings (order_id, rater_id, target_id, rating) VALUES (?, ?, ?, ?)",
            (order_id, rater_id, target_id, rating)
        )
        await db.commit()


# Общее количество пользователей
async def get_total_users_count():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            return (await cursor.fetchone())[0]

# Статистика за сегодня
async def get_daily_stats():
    today = date.today().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        # Новые заказы
        async with db.execute(
                "SELECT COUNT(*) FROM orders WHERE DATE(created_at) = ?", (today,)
        ) as cursor:
            new_orders = (await cursor.fetchone())[0]

        # Завершённые
        async with db.execute(
                "SELECT COUNT(*) FROM orders WHERE status = 'completed' AND DATE(created_at) = ?", (today,)
        ) as cursor:
            completed = (await cursor.fetchone())[0]

        # Отменённые
        async with db.execute(
                "SELECT COUNT(*) FROM orders WHERE status = 'cancelled' AND DATE(created_at) = ?", (today,)
        ) as cursor:
            cancelled = (await cursor.fetchone())[0]

        return new_orders, completed, cancelled

async def get_user_rating(user_id: int) -> float:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT AVG(rating) FROM ratings WHERE target_id = ?",
            (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return round(row[0], 1) if row[0] else 0.0

async def get_completed_orders_count(user_id: int, role: str) -> int:
    field = "driver_id" if role == "driver" else "client_id"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            f"SELECT COUNT(*) FROM orders WHERE {field} = ? AND status = 'completed'", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

async def has_user_rated(order_id: int, rater_id: int) -> bool:
    """Проверяет, ставил ли пользователь (rater_id) оценку по заказу (order_id)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM ratings WHERE order_id = ? AND rater_id = ?",
            (order_id, rater_id)
        ) as cursor:
            return await cursor.fetchone() is not None

async def create_bid(order_id: int, driver_id: int, arrival_minutes: int = None):
    """Создаёт заявку водителя на заказ."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Проверяем, не делал ли уже заявку
        async with db.execute(
            "SELECT 1 FROM bids WHERE order_id = ? AND driver_id = ?", (order_id, driver_id)
        ) as cursor:
            if await cursor.fetchone():
                return False  # Уже заявлялся

        await db.execute(
            "INSERT INTO bids (order_id, driver_id, arrival_minutes) VALUES (?, ?, ?)",
            (order_id, driver_id, arrival_minutes)
        )
        await db.commit()
        return True

async def get_bids_for_order(order_id: int):
    """Получает список активных заявок по заказу."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT b.driver_id, u.car_brand, u.car_number, b.arrival_minutes, u.has_co_driver
            FROM bids b
            JOIN users u ON b.driver_id = u.user_id
            WHERE b.order_id = ? AND b.status = 'pending'
        """, (order_id,)) as cursor:
            return await cursor.fetchall()  # (driver_id, brand, number, arrival_minutes, has_co_driver)

async def accept_bid(order_id: int, driver_id: int):
    """Клиент выбирает водителя. Возвращает True, если успешно."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Проверяем, не выбран ли уже водитель на этот заказ
        async with db.execute("SELECT driver_id FROM orders WHERE id = ? AND status = 'pending'", (order_id,)) as cursor:
            row = await cursor.fetchone()
            if not row or row[0] is not None:
                return False

        # Принимаем заявку
        await db.execute("UPDATE bids SET status = 'accepted' WHERE order_id = ? AND driver_id = ?", (order_id, driver_id))
        # Отклоняем остальные
        await db.execute("UPDATE bids SET status = 'rejected' WHERE order_id = ? AND driver_id != ?", (order_id, driver_id))
        # Обновляем заказ
        await db.execute("UPDATE orders SET driver_id = ?, status = 'accepted' WHERE id = ?", (driver_id, order_id))
        await db.commit()
        return True

async def get_driver_rating(driver_id: int) -> float:
    """Получает рейтинг водителя (уже есть, но для ясности)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT AVG(rating) FROM ratings WHERE target_id = ?", (driver_id,)) as cursor:
            row = await cursor.fetchone()
            return round(row[0], 1) if row[0] else 0.0

async def set_driver_verification(user_id: int, expires_date: str = None):
    """
    Подтверждает верификацию водителя.
    expires_date: 'YYYY-MM-DD' или None (бессрочно)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET is_verified = 1, verification_expires = ? WHERE user_id = ?",
            (expires_date, user_id)
        )
        await db.commit()

async def is_driver_verified(user_id: int) -> bool:
    """Проверяет, верифицирован ли водитель и не истёк ли срок."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT is_verified, verification_expires 
            FROM users 
            WHERE user_id = ? AND role = 'driver'
        """, (user_id,)) as cursor:
            row = await cursor.fetchone()
            if not row or not row[0]:
                return False

            expires = row[1]
            if expires is None:
                return True  # бессрочно

            from datetime import date
            try:
                expire_date = date.fromisoformat(expires)
                return expire_date >= date.today()
            except:
                return False

# Статистика за всё время
async def get_total_orders_count():
    """Общее количество заказов за всё время."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM orders") as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0


async def get_total_completed_orders():
    """Количество завершённых заказов."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM orders WHERE status = 'completed'") as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0


async def get_total_cancelled_orders():
    """Количество отменённых заказов."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM orders WHERE status = 'cancelled'") as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0

async def ban_user(user_id: int):
    """Блокирует пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (user_id,))
        await db.commit()

async def unban_user(user_id: int):
    """Разблокирует пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_banned = 0 WHERE user_id = ?", (user_id,))
        await db.commit()

async def is_user_banned(user_id: int) -> bool:
    """Проверяет, заблокирован ли пользователь."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return bool(row[0]) if row else False

async def create_backup():
    """Создаёт резервную копию БД."""
    os.makedirs(BACKUP_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    backup_filename = f"backup_{timestamp}.db"
    backup_path = os.path.join(BACKUP_DIR, backup_filename)

    try:
        shutil.copy2(DB_PATH, backup_path)
        logging.info(f"✅ Резервная копия создана: {backup_path}")

        # Удаляем старые копии
        backups = sorted(
            [f for f in os.listdir(BACKUP_DIR) if f.startswith("backup_") and f.endswith(".db")],
            key=lambda x: os.path.getmtime(os.path.join(BACKUP_DIR, x))
        )

        # Если больше MAX_BACKUPS — удаляем старые
        while len(backups) > MAX_BACKUPS:
            oldest = backups.pop(0)
            os.remove(os.path.join(BACKUP_DIR, oldest))
            logging.info(f"🗑 Удалена старая копия: {oldest}")

        return backup_path
    except Exception as e:
        logging.error(f"❌ Ошибка создания резервной копии: {e}")
        return None

async def get_broadcast_recipients(target: str):
    """Возвращает список user_id для рассылки."""
    async with aiosqlite.connect(DB_PATH) as db:
        if target == "all":
            query = "SELECT user_id FROM users WHERE is_banned = 0"
        elif target == "drivers":
            query = "SELECT user_id FROM users WHERE role = 'driver' AND is_banned = 0"
        elif target == "clients":
            query = "SELECT user_id FROM users WHERE role = 'client' AND is_banned = 0"
        else:
            return []

        async with db.execute(query) as cursor:
            return [row[0] for row in await cursor.fetchall()]

async def save_driver_order_message(order_id: int, driver_id: int, chat_id: int, message_id: int):
    """Сохраняет ID сообщения заказа для водителя."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO driver_order_messages (order_id, driver_id, chat_id, message_id) VALUES (?, ?, ?, ?)",
            (order_id, driver_id, chat_id, message_id)
        )
        await db.commit()

async def get_driver_order_messages(order_id: int) -> list:
    """Возвращает список (chat_id, message_id, driver_id) для всех водителей по заказу."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT chat_id, message_id, driver_id
            FROM driver_order_messages
            WHERE order_id = ?
        """, (order_id,)) as cursor:
            return await cursor.fetchall()

async def delete_driver_order_messages(order_id: int):
    """Удаляет все записи сообщений по заказу."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM driver_order_messages WHERE order_id = ?", (order_id,))
        await db.commit()

async def get_setting(key: str, default: str = "1") -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else default

async def set_setting(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        await db.commit()

async def increment_monthly_rides(user_id: int):
    """Увеличивает счётчик поездок за текущий месяц."""
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO monthly_rides (user_id, year_month, ride_count)
            VALUES (?, ?, 1)
            ON CONFLICT(user_id, year_month) DO UPDATE SET ride_count = ride_count + 1
        """, (user_id, year_month))
        await db.commit()

async def get_monthly_rides(user_id: int) -> int:
    """Возвращает количество поездок за текущий месяц."""
    year_month = datetime.now().strftime("%Y-%m")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT ride_count FROM monthly_rides WHERE user_id = ? AND year_month = ?",
            (user_id, year_month)
        ) as cursor:
            row = await cursor.fetchone()
    return row[0] if row else 0

async def cancel_order_with_reason(order_id: int, reason: str):
    """Отменяет заказ и сохраняет причину."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE orders SET status = 'cancelled', cancelled_by = ? WHERE id = ?", (reason, order_id))
        await db.commit()