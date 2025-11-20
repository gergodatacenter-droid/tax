from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

def admin_broadcast_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Всем", callback_data="broadcast_all")],
        [InlineKeyboardButton(text="🚗 Водителям", callback_data="broadcast_drivers")],
        [InlineKeyboardButton(text="👤 Клиентам", callback_data="broadcast_clients")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
    ])

# Меню администратора
def admin_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="👥 Пользователи")],
            [KeyboardButton(text="🚗 Водители"), KeyboardButton(text="📋 Заказы")],
            [KeyboardButton(text="🔐 Верификация"), KeyboardButton(text="📤 Рассылка")],
            [KeyboardButton(text="📈 Статистика рассылок")],
            [KeyboardButton(text="📥 Список водителей (CSV)"), KeyboardButton(text="📥 Список всех пользователей (CSV)")],
            [KeyboardButton(text="📥 История заказов (CSV)")],
            [KeyboardButton(text="🤝 Партнёрская реклама"), KeyboardButton(text="⚙️ Настройки сервиса")]
        ],
        resize_keyboard=True
    )

# Главное меню выбора роли
def start_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚗 Водитель"), KeyboardButton(text="👤 Клиент")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

# Меню водителя
def driver_menu(shift_opened: bool):
    buttons = [
        [KeyboardButton(text="🔴 Закрыть смену" if shift_opened else "✅ Открыть смену")],
        [KeyboardButton(text="📜 История заказов"), KeyboardButton(text="🚕 Сделать заказ")],
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="🛠 Техническая поддержка")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

# # Меню клиента
# def client_menu():
#     return ReplyKeyboardMarkup(
#         keyboard=[[KeyboardButton(text="🚕 Сделать заказ")],
#                   [KeyboardButton(text="📜 История заказов")],
#                   [KeyboardButton(text="🛠 Техническая поддержка")]
#                   ],
#         resize_keyboard=True
#     )

# Поддержка
def support_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❓ Часто задаваемые вопросы FAQ", url="https://taxibarsnz24.ru/index.html#faq")],
        [InlineKeyboardButton(text="💰 Прейскурант на поездки", url="https://taxibarsnz24.ru/index.html#tariffs")],
        [InlineKeyboardButton(text="⚖️ Диспут / Репорт", callback_data="open_disput_from_support")],
        [InlineKeyboardButton(text="👨‍💼 Связь с администратором", url="https://t.me/azimut301")]
    ])

# Кнопка принять заказ
def accept_order_button(order_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять заказ", callback_data=f"accept_{order_id}")]
    ])

# Меню активного заказа — клиент
def client_order_menu(driver_id: int, order_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить заказ", callback_data=f"cancel_client_{order_id}")],
        [InlineKeyboardButton(text="📍 Отправить геопозицию", callback_data=f"send_location_{order_id}")],
        [InlineKeyboardButton(text="💬 Чат с водителем", url=f"tg://user?id={driver_id}")]
    ])

# Меню активного заказа — водитель
def driver_order_menu(client_id: int, order_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ На месте", callback_data=f"arrived_{order_id}")],
        [InlineKeyboardButton(text="❌ Отменить заказ", callback_data=f"cancel_driver_{order_id}")],
        [InlineKeyboardButton(text="💬 Чат с клиентом", url=f"tg://user?id={client_id}")],
        [InlineKeyboardButton(text="🏁 Завершить заказ", callback_data=f"complete_{order_id}")]
    ])

# Оценка (1–5)
def rating_keyboard(target_id: int, order_id: int):
    buttons = [
        [InlineKeyboardButton(text=str(i), callback_data=f"rate_{target_id}_{order_id}_{i}") for i in range(1, 6)]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def passengers_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1", callback_data="passengers_1"),
            InlineKeyboardButton(text="2", callback_data="passengers_2"),
            InlineKeyboardButton(text="3", callback_data="passengers_3"),
        ],
        [
            InlineKeyboardButton(text="4", callback_data="passengers_4"),
            InlineKeyboardButton(text="5", callback_data="passengers_5"),
        ]
    ])

def arrival_time_inline_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="2 мин", callback_data="arrival_time_2"),
            InlineKeyboardButton(text="5 мин", callback_data="arrival_time_5"),
            InlineKeyboardButton(text="7 мин", callback_data="arrival_time_7")
        ],
        [
            InlineKeyboardButton(text="10 мин", callback_data="arrival_time_10"),
            InlineKeyboardButton(text="15 мин", callback_data="arrival_time_15"),
            InlineKeyboardButton(text="30 мин", callback_data="arrival_time_30")
        ]
    ])