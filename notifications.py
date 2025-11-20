# notifications.py

from aiogram import Bot
import logging

# ID группового чата (можно получить через @RawDataBot или логируя обновления)
DRIVER_GROUP_CHAT_ID = -1002#360063823  # ← ЗАМЕНИТЕ НА РЕАЛЬНЫЙ ID ВАШЕЙ ГРУППЫ

async def notify_new_order_in_group(bot: Bot, order_id: int):
    """
    Отправляет уведомление о новом заказе в групповой чат водителей.
    """
    try:
        await bot.send_message(
            chat_id=DRIVER_GROUP_CHAT_ID,
            text="🚨 <b>ВОДИТЕЛИ</b> — новый заказ в анонимном боте @TaxiBarsBot!\n"
                 "🔥 <b>Приоритетная заявка!</b>",
            parse_mode="HTML"
        )
        logging.info(f"[group_notify] Уведомление о заказе {order_id} отправлено в группу.")
    except Exception as e:
        logging.error(f"[group_notify] Не удалось отправить уведомление в группу: {e}")