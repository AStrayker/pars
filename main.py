import asyncio
import os
import sys
import traceback
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telethon import TelegramClient, errors as telethon_errors
from telegram import error as telegram_error
from datetime import datetime, timedelta
from telegram import ReactionTypeEmoji
import json
import io
import pandas as pd
import requests
import vobject
import time
import firebase_admin
from firebase_admin import credentials, db

# Инициализация Firebase
firebase_config = {
    "apiKey": "AIzaSyC3TIHHwmTjY_Hu90uf9Ka988wE1kz5-Rk",
    "authDomain": "tgparser-f857c.firebaseapp.com",
    "databaseURL": "https://tgparser-f857c-default-rtdb.firebaseio.com",
    "projectId": "tgparser-f857c",
    "storageBucket": "tgparser-f857c.firebasestorage.app",
    "messagingSenderId": "531373856748",
    "appId": "1:531373856748:web:3a7120ec788e3c530152c9",
    "measurementId": "G-G52LLQ1B5J"
}

cred = credentials.Certificate({
    "type": "service_account",
    "project_id": "tgparser-f857c",
    "private_key_id": "566355ff398ae65c0df506c042ead8ff9a250b9a",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCw8L6gt7uW4T49\ns4MALhYZiODeNalBaT97YMH3tICWd/w7pNj0+eVqkMj2buMeDKLh/ZxoiHwh4Tsh\nX5s90K2MwBzq2wfV6PRBg66uu8GrAb0vg0JgGkTJbapaE/M3RFceSbhM1kEEGSVj\neaZWq9FgXNZVuNb/375lPrJmtZSlIDDOfUOLvDntv9a2t+4CGQrq+nkv0bRpwAvp\nGy0jyYhETweI7Wa6EX9mWNU4ufFmJFTYFLpODBKsf9AJilzxCcGJjEKcIp9DmBY9\n13015kM6BntFR/ykmlvb7FnGsmPXx8rrfLoeZvTO+zHw+6jPDCAHRj6XomdJEwe3\nSuiU9yxJAgMBAAECggEAEQttPvEuuRbt69FY62cFH6TqYy17JlRxAOhuDE0PMq97\nP57z0WF97UPmQABS5j2kYS27kpb3POeZTm/gdgDIqcLbO5p+3jx9xGPe1hvlyPHh\ncMFttEaY3WiFh6jdUzPE9U4iWrCBpyyUR/QGMdovpgnmEcWSXH1dYMZnNsgDejEp\noE1clSzV3xyNFmQdNZSU/CpBIwih7ONPQqh4NuWoCTSEIHYtPCy15Abm1Aw85y5V\nyXkMfz3gMrArHzLAWOmq9i7k25HGzLJk+CBmBK0teLcHmPAKFmvTkzz3ERUze8Zx\nVtMeVXPfOBOy9p6NmNHPl8d6u5GjS+8eSJ00J2Oz2wKBgQDqahJ+xI3uYgFMv/DR\nY+I50ejs2eETKWfLu2si2H75ZFdmFzNHTuYqDlBhr4c3a74pwg8DJEeGINxjJpw/\nwHiNNu2rDtyFgZDi5YJl0/Oawzbfdt7zY8X6PZBXrgfyFcxXSvW7/MmGXjkeJBC0\nC8nMTw9kczmKtqNsPpxD/ZY2uwKBgQDBO9HwkhxZia+mIN/Lx7ZRQOSDARWlqC8n\nYiMBT5aJz8yX7DCVA249DucGCECvOpZNevjNLUfP9tvDlNQxoIoEwbSu9SGCZ82J\nylg1NEfLPYAAsko7AojWJMZb4tFc8yASEqFl4ERLkAhJ0FttP8yWHelCReqDQeE2\nm2FRAAfyywKBgQCgiA2sdMzCEKnVNqkjrGSTtjXuZfNmXPexJONk1KB4CAh2aLL8\nRYMIEA9qJnvSL13mWPhQ7Xpx13U2DY35dsTX6GLwv1ezshxX1lbrhzAPr1qXxF9A\njPZavehos0zLs7Phn/sTRzV3aHVzN72cn2oOGaJv5xzj6tmV2nbHdRV22wKBgQCl\n9RiDwxkyFTyUM5vBys5czpzznpTW57FH49MopxAlYCDZQfMfqAifzBLCbYgQdwLr\nnHfez8rjY6KvQT1VOgoPt8XUlZeoBjrS4sQLPdGDTliHQJjcQXsAYCk0dYNWj0C2\nBOY1Nv2w4A0eSCKdm7O8IghZ8O5OuOASJDTaempZLQKBgQC6D2Ojg/257PrF/jnH\nE68JLGbaGsHZQuHnt7J0ZmH99Ks/TcziQjRXiVtzCoPeCPXnqQ/LxB7DH6MVoDqh\nBpaQYLZodb7piRWa+H/efLqqQUP4aSnxfStpGgOon5dPRwgO9r1fGfLfNai4O9dj\nzfpHDSHtUv/dTvBL+9lLdlrJWQ==\n-----END PRIVATE KEY-----\n",
    "client_email": "firebase-adminsdk-fbsvc@tgparser-f857c.iam.gserviceaccount.com",
    "client_id": "108084641389485350330",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-fbsvc%40tgparser-f857c.iam.gserviceaccount.com"
})

firebase_admin.initialize_app(cred, {
    'databaseURL': firebase_config["databaseURL"]
})

# Указываем переменные через код или переменные среды
API_ID = int(os.environ.get('API_ID', 25281388))
API_HASH = os.environ.get('API_HASH', 'a2e719f61f40ca912567c7724db5764e')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '7787290156:AAGfZlS6el5mkffVa6osGZ1sp_AXBAHTjSY')
LOG_CHANNEL_ID = -1002342891238
SUPPORT_USERNAME = '@alex_strayker'
TON_WALLET_ADDRESS = 'UQAP4wrP0Jviy03CTeniBjSnAL5UHvcMFtxyi1Ip1exl9pLu'
TON_API_KEY = os.environ.get('TON_API_KEY', 'YOUR_TON_API_KEY')
ADMIN_IDS = ['282198872']

# Путь к файлу общей сессии
SESSION_FILE = 'session_+380953804630.session'

# Создание клиента Telethon с общей сессией
client_telethon = TelegramClient(SESSION_FILE, API_ID, API_HASH)

# Функции для работы с Firebase
def load_users():
    ref = db.reference('users')
    users = ref.get()
    return users if users else {}

def save_users(users):
    ref = db.reference('users')
    ref.set(users)

# Языковые переводы для всех языков
LANGUAGES = {
    'Русский': {
        'welcome': 'Привет! Выбери язык общения:',
        'enter_phone': 'Введите номер телефона в формате +380639678038 для авторизации:',
        'enter_phone_invalid': 'Пожалуйста, введите номер в формате +380639678038:',
        'enter_code': 'Введите код подтверждения, который вы получили в SMS или Telegram:',
        'enter_password': 'Требуется пароль двухфакторной аутентификации. Введите пароль:',
        'auth_success': 'Авторизация успешно завершена!',
        'auth_error': 'Ошибка авторизации: {error}. Попробуйте снова с /start.',
        'choose_language': 'Выбери язык:',
        'subscribe': 'Подпишись на канал: https://t.me/tgparseruser\nПосле подписки нажми "Продолжить".',
        'subscribed': 'Продолжить',
        'you_chose': 'Вы выбрали: {button}',
        'skip': 'Пропустить',
        'start_menu': 'Вы: {name}\nВаш ID: {user_id}\nВыбран язык: {lang} /language\nТип подписки: {sub_type}\nВаша подписка закончится через: {sub_time}\nВсего запросов сделано: {requests}\nОсталось запросов сегодня: {limit}',
        'identifiers': 'Отправь мне @username, ссылку в любом формате или перешли сообщение из канала, чтобы узнать ID.',
        'parser': 'Выбери, что хочешь спарсить:',
        'subscribe_button': 'Оформить подписку',
        'support': 'Поддержка: {support}',
        'requisites': 'Возможности оплаты:\n1. [Метод 1]\n2. [Метод 2]\nСвяжитесь с {support} для оплаты.',
        'logs_channel': 'Канал с логами: t.me/YourLogChannel',
        'link_group': 'Отправь мне ссылку на группу или канал, например: https://t.me/group_name, @group_name или group_name\nМожно указать несколько ссылок через Enter.',
        'link_post': 'Отправь мне ссылку на пост, например: https://t.me/channel_name/12345\nИли перешли пост. Можно указать несколько ссылок через Enter.',
        'limit': 'Сколько пользователей парсить? Выбери или укажи число (макс. 5000 авторов/15000 участников/10000 комментаторов поста для платных подписок, 150 для бесплатной).',
        'invalid_limit': 'Укажи число от 1 до {max_limit}!',
        'invalid_number': 'Пожалуйста, укажи число!',
        'invalid_link': 'Пожалуйста, отправь корректную ссылку на пост/чат, например: https://t.me/channel_name/12345, @channel_name или channel_name\nИли несколько ссылок через Enter.',
        'fix_link': 'Если ты ошибся, могу помочь исправить ссылку.',
        'suggest_link': 'Ты имел в виду: {link}?',
        'retry_link': 'Отправь ссылку заново:',
        'no_access': 'Ошибка: у меня нет доступа к {link}. Убедись, что я добавлен в чат или он публичный.',
        'flood_error': 'Ошибка: {e}',
        'rpc_error': 'Ошибка: {e}',
        'new_user': 'Новый пользователь: {name} (@{username})',
        'language_cmd': 'Выбери новый язык:',
        'caption_commentators': 'Вот ваш файл с комментаторами.',
        'caption_participants': 'Вот ваш файл с участниками.',
        'caption_post_commentators': 'Вот ваш файл с комментаторами поста.',
        'limit_reached': 'Ты исчерпал дневной лимит ({limit} запросов). Попробуй снова через {hours} часов.',
        'id_result': 'ID: {id}',
        'close': 'Закрыть',
        'continue_id': 'Продолжить',
        'subscription_1h': 'Подписка на 1 час - 2 USDT (TON)',
        'subscription_3d': 'Подписка на 3 дня - 5 USDT (TON)',
        'subscription_7d': 'Подписка на 7 дней - 7 USDT (TON)',
        'subscription_bank_card': 'Оплата через банковскую карту',
        'payment_wallet': 'Переведите {amount} USDT на кошелёк TON:\n{address}\nПосле оплаты нажмите "Я оплатил".',
        'payment_bank_card': 'Оплата на карту ПриватБанку.\nПолучатель: Демченко Владислав.\nКарта: <code>5169 3600 2736 1258</code>.\n1 час (70 грн)\n3 дня (200 грн)\n7 дней (250 грн)\nКонтакт: @Alex_Strayker',
        'payment_cancel': 'Отменить',
        'payment_paid': 'Я оплатил',
        'payment_hash': 'Отправьте хеш транзакции/или скрин если оплата на карту:',
        'payment_pending': 'Транзакция отправлена в обработку',
        'payment_update': 'Обновить',
        'payment_success': 'Подписка успешно оформлена!\nВаша подписка активирована до {end_time}.',
        'payment_error': 'Ваша транзакция была не удачной!\nПодать аппеляцию можно написав пользователю @astrajker_cb_id.',
        'entity_error': 'Не удалось получить информацию о пользователе. Пользователь может быть приватным или недоступным.',
        'no_filter': 'Не применять фильтр',
        'phone_contacts': 'Сбор номеров телефонов и ФИО',
        'auth_access': 'Авторизация для закрытых чатов',
        'caption_phones': 'Вот ваш файл с номерами телефонов и ФИО (Excel и VCF).',
        'auth_request': 'Для доступа к закрытым чатам добавьте бота в чат как администратора или отправьте ссылку на закрытый чат.',
        'auth_success': 'Доступ к закрытому чату успешно предоставлен!',
        'auth_error': 'Не удалось получить доступ. Убедитесь, что бот добавлен как администратор или чат публичный.',
        'note_cmd': 'Заметка успешно сохранена (бот не будет реагировать).',
        'home_cmd': 'Вы вернулись в главное меню.',
        'info_cmd': 'Это бот для парсинга Telegram.\nВозможности:\n- Сбор ID\n- Парсинг участников\n- Парсинг комментаторов\n- Сбор контактов\nДля поддержки: {support}',
        'working_message': 'Всё нормально, мы ещё работаем...',
        'loading_message': 'Подождите...',
        'invalid_link_suggestion': 'Ссылка "{link}" неверная. Возможно, вы имели в виду что-то вроде https://t.me/group_name или https://t.me/channel_name/12345? Попробуйте снова.',
        'rate_parsing': 'Оцените качество парсинга:',
        'info_identifiers': 'Функция "Идентификаторы" позволяет узнать ID пользователей, групп или постов.\nОтправьте @username, ссылку в любом формате или перешлите сообщение из канала.',
        'info_parser': 'Функция "Парсер" позволяет собирать данные:\n- Авторы сообщений (чат должен быть открыт)\n- Участники чату/канала (только с открытыми списками участников)\n- Комментаторы поста (открытые каналы)\n- Номера телефонов\n- Доступ к закрытым чатам',
        'info_subscribe': 'Оформите подписку для расширенных лимитов:\n- 1 час: 2 USDT\n- 3 дня: 5 USDT\n- 7 дней: 7 USDT',
        'info_requisites': 'Оплата через TON кошелёк:\n1. Переведите USDT на адрес\n2. Укажите хеш транзакции\nСвяжитесь с поддержкой для деталей.',
        'info_logs': 'Канал с логами доступен только администраторам. Логи содержат информацию о действиях пользователей.',
        'info_parse_authors': 'Сбор авторов сообщений из чата или канала.',
        'info_parse_participants': 'Сбор всех участников чата или канала.',
        'info_parse_post_commentators': 'Сбор комментаторов конкретного поста.',
        'info_parse_phone_contacts': 'Сбор номеров телефонов и ФИО участников.',
        'info_parse_auth_access': 'Предоставление доступа к закрытым чатам.',
        'problem_message': 'Привет. У вас возникла проблема при работе с ботом?',
        'yes': 'Да',
        'no': 'Нет',
        'maybe': 'Возможно',
        'subscribe_offer': 'Не хотели бы вы оформить Платную подписку чтобы воспользоваться всеми возможностями, и избавиться от установленных ограничений?',
        'send_message': 'Отправить сообщение',
        'select_message': 'Выберите тип сообщения для отправки:',
        'bank_card_info': 'Для оплаты через банковскую карту свяжитесь с {support} для получения реквизитов.',
        'parse_authors': 'Авторы сообщений',
        'parse_participants': 'Участники чата',
        'parse_post_commentators': 'Комментаторы поста',
        'no_access_to_logs': 'У вас нет доступа к логам.',
        'admin_message_type_1': 'Тип 1 (Проблема)',
        'admin_message_type_2': 'Тип 2 (Подписка)',
        'admin_reject_transaction': 'Транзакция пользователя {user_id} отклонена.',
        'admin_invalid_user_id': 'Пожалуйста, укажите корректный ID пользователя.',
        'admin_user_not_found': 'Пользователь не найден.',
        'admin_no_permission': 'У вас нет прав для этой команды.',
        'admin_invalid_set_plan': 'Неверный тип подписки. Используйте "1h", "3d", "7d" или "permanent".',
        'admin_set_plan_usage': 'Использование: /set_plan <user_id> <type> <duration>',
        'admin_remove_plan_usage': 'Использование: /remove_plan <user_id>',
        'admin_subscription_updated': 'Подписка для пользователя {user_id} ({username}) обновлена до {end_time}.',
        'admin_subscription_removed': 'Платная подписка для пользователя {user_id} ({username}) удалена, установлен бесплатный план.',
        'admin_message_sent': 'Сообщение типа {type} отправлено пользователю {user_id}.',
        'admin_select_message': 'Выберите тип сообщения для отправки:',
        'admin_transaction_rejected': 'Администратор отклонил транзакцию пользователя {user_id}',
        'admin_subscription_set': 'Администратор установил подписку для пользователя {user_id} ({username}): {sub_type}, до {end_time}',
        'admin_subscription_removed_log': 'Администратор удалил платную подписку для пользователя {user_id} ({username})',
        'admin_message_type_selected': 'Админ выбрал тип сообщения: {type}',
        'admin_menu_opened': 'Админ открыл меню отправки сообщения',
        'admin_enter_target_user_id': 'Введите ID пользователя для отправки сообщения:',
        'problem_response_yes': 'Пожалуйста, свяжитесь с {support} для решения проблемы.',
        'rating_thanks': 'Спасибо за оценку: {rating} ⭐️',
        'info_not_available': 'Информация недоступна',
        'parsing_start': 'Начинаем парсинг...',
        'action_forbidden': 'Действие запрещено пока запущен парсинг',
        'abort_parsing': 'Парсинг прерван.'
    },
    'Украинский': {
        'welcome': 'Привіт! Обери мову спілкування:',
        'enter_phone': 'Введи номер телефону у форматі +380639678038 для авторизації:',
        'enter_phone_invalid': 'Будь ласка, введи номер у форматі +380639678038:',
        'enter_code': 'Введи код підтвердження, який ти отримав у SMS або Telegram:',
        'enter_password': 'Потрібен пароль двофакторної аутентифікації. Введи пароль:',
        'auth_success': 'Авторизація успішно завершена!',
        'auth_error': 'Помилка авторизації: {error}. Спробуй знову з /start.',
        'choose_language': 'Обери мову:',
        'subscribe': 'Підпишись на канал: https://t.me/tgparseruser\nПісля підписки натисни "Продовжити".',
        'subscribed': 'Продовжити',
        'you_chose': 'Ви обрали: {button}',
        'skip': 'Пропустити',
        'start_menu': 'Ви: {name}\nВаш ID: {user_id}\nОбрана мова: {lang} /language\nТип підписки: {sub_type}\nВаша підписка закінчиться через: {sub_time}\nВсього запитів зроблено: {requests}\nЗалишилось запитів сьогодні: {limit}',
        'identifiers': 'Надішли мені @username, посилання у будь-якому форматі або перешли повідомлення з каналу, щоб дізнатися ID.',
        'parser': 'Обери, що хочеш спарсити:',
        'subscribe_button': 'Оформити підписку',
        'support': 'Підтримка: {support}',
        'requisites': 'Можливості оплати:\n1. [Метод 1]\n2. [Метод 2]\nЗв’яжіться з {support} для оплати.',
        'logs_channel': 'Канал з логами: t.me/YourLogChannel',
        'link_group': 'Надішли мені посилання на групу або канал, наприклад: https://t.me/group_name, @group_name або group_name\nМожна вказати кілька посилань через Enter.',
        'link_post': 'Надішли мені посилання на пост, наприклад: https://t.me/channel_name/12345\nАбо перешли пост. Можна вказати кілька посилань через Enter.',
        'limit': 'Скільки користувачів парсить? Обери або вкажи число (макс. 5000 авторів/15000 учасників/10000 коментаторів поста для платних підписок, 150 для безкоштовної).',
        'invalid_limit': 'Вкажи число від 1 до {max_limit}!',
        'invalid_number': 'Будь ласка, вкажи число!',
        'invalid_link': 'Будь ласка, надішли коректне посилання на пост/чат, наприклад: https://t.me/channel_name/12345, @channel_name або channel_name\nАбо кілька посилань через Enter.',
        'fix_link': 'Якщо ти помилився, можу допомогти виправити посилання.',
        'suggest_link': 'Ти мав на увазі: {link}?',
        'retry_link': 'Надішли посилання заново:',
        'no_access': 'Помилка: у мене немає доступу до {link}. Переконайся, що я доданий до чату або він публічний.',
        'flood_error': 'Помилка: {e}',
        'rpc_error': 'Помилка: {e}',
        'new_user': 'Новий користувач: {name} (@{username})',
        'language_cmd': 'Обери нову мову:',
        'caption_commentators': 'Ось ваш файл з коментаторами.',
        'caption_participants': 'Ось ваш файл з учасниками.',
        'caption_post_commentators': 'Ось ваш файл з комментаторами поста.',
        'limit_reached': 'Ти вичерпав денний ліміт ({limit} запитів). Спробуй знову через {hours} годин.',
        'id_result': 'ID: {id}',
        'close': 'Закрити',
        'continue_id': 'Продовжити',
        'subscription_1h': 'Підписка на 1 годину - 2 USDT (TON)',
        'subscription_3d': 'Підписка на 3 дні - 5 USDT (TON)',
        'subscription_7d': 'Підписка на 7 днів - 7 USDT (TON)',
        'subscription_bank_card': 'Оплата через банківську карту',
        'payment_wallet': 'Переведіть {amount} USDT на гаманець TON:\n{address}\nПісля оплати натисніть "Я оплатив".',
        'payment_bank_card': 'Оплата на карту ПриватБанку.\nОтримувач: Демченко Владислав.\nКарта: <code>5169 3600 2736 1258</code>.\n1 година (70 грн)\n3 дні (200 грн)\n7 днів (250 грн)\nКонтакт: @Alex_Strayker',
        'payment_cancel': 'Скасувати',
        'payment_paid': 'Я оплатив',
        'payment_hash': 'Надішліть хеш транзакції/або скрін якщо оплата на карту:',
        'payment_pending': 'Транзакція відправлена в обробку',
        'payment_update': 'Оновити',
        'payment_success': 'Підписка успішно оформлена!\nВаша підписка активована до {end_time}.',
        'payment_error': 'Ваша транзакція була не вдалою!\nПодати апеляцію можна написавши користувачу @astrajker_cb_id.',
        'entity_error': 'Не вдалося отримати інформацію про користувача. Користувач може бути приватним або недоступним.',
        'no_filter': 'Не застосовувати фільтр',
        'phone_contacts': 'Збір номерів телефонів та ПІБ',
        'auth_access': 'Авторизація для закритих чатів',
        'caption_phones': 'Ось ваш файл з номерами телефонів та ПІБ (Excel і VCF).',
        'auth_request': 'Для доступу до закритих чатів додайте бота в чат як адміністратора або надішліть посилання на закритий чат.',
        'auth_success': 'Доступ до закритого чату успішно надано!',
        'auth_error': 'Не вдалося отримати доступ. Переконайтесь, що бот доданий як адміністратор або чат публічний.',
        'note_cmd': 'Примітка успішно збережено (бот не реагуватиме).',
        'home_cmd': 'Ви повернулися до головного меню.',
        'info_cmd': 'Це бот для парсинга Telegram.\nМожливості:\n- Збір ID\n- Парсинг учасників\n- Парсинг комментаторов\n- Збір контактів\nДля підтримки: {support}',
        'working_message': 'Все нормально, ми ще працюємо...',
        'loading_message': 'Зачекайте...',
        'invalid_link_suggestion': 'Посилання "{link}" неправильне. Можливо, ви мали на увазі щось на кшталт https://t.me/group_name або https://t.me/channel_name/12345? Спробуйте ще раз.',
        'rate_parsing': 'Оцініть якість парсингу:',
        'info_identifiers': 'Функція "Ідентифікатори" дозволяє дізнатися ID користувачів, груп або постів.\nНадішліть @username, посилання у будь-якому форматі або перешліть повідомлення з каналу.',
        'info_parser': 'Функція "Парсер" дозволяє збирати дані:\n- Автори повідомлень\n- Учасники чату\n- Коментатори посту\n- Номера телефонів\n- Доступ до закритих чатів',
        'info_subscribe': 'Оформіть підписку для розширених лімітів:\n- 1 година: 2 USDT\n- 3 дні: 5 USDT\n- 7 днів: 7 USDT',
        'info_requisites': 'Оплата через TON гаманець:\n1. Переведіть USDT на адресу\n2. Вкажіть хеш транзакції\nЗв’яжіться з підтримкой для деталей.',
        'info_logs': 'Канал з логами доступний тільки адміністраторам. Логи містять інформацію про дії користувачів.',
        'info_parse_authors': 'Збір авторів повідомлень з чату або каналу.',
        'info_parse_participants': 'Збір усіх учасників чату або каналу.',
        'info_parse_post_commentators': 'Збір коментаторів конкретного посту.',
        'info_parse_phone_contacts': 'Збір номерів телефонів та ПІБ учасників.',
        'info_parse_auth_access': 'Надання доступу до закритих чатів.',
        'problem_message': 'Привіт. У вас виникла проблема при роботі з ботом?',
        'yes': 'Так',
        'no': 'Ні',
        'maybe': 'Можливо',
        'subscribe_offer': 'Не хотіли б ви оформити Платну підписку, щоб скористатися всіся можливостями та позбутися встановлених обмежень?',
        'send_message': 'Надіслати повідомлення',
        'select_message': 'Виберіть тип повідомлення для надсилання:',
        'bank_card_info': 'Для оплаты через банківську карту зверніться до {support} для отримання реквізитов.',
        'parse_authors': 'Автори повідомлень',
        'parse_participants': 'Учасники чату',
        'parse_post_commentators': 'Коментатори поста',
        'no_access_to_logs': 'У вас немає доступу до логів.',
        'admin_message_type_1': 'Тип 1 (Проблема)',
        'admin_message_type_2': 'Тип 2 (Підписка)',
        'admin_reject_transaction': 'Транзакція користувача {user_id} відхилена.',
        'admin_invalid_user_id': 'Будь ласка, вкажіть коректний ID користувача.',
        'admin_user_not_found': 'Користувач не знайдений.',
        'admin_no_permission': 'У вас немає прав для цієї команди.',
        'admin_invalid_set_plan': 'Невірний тип підписки. Використовуйте "1h", "3d", "7d" або "permanent".',
        'admin_set_plan_usage': 'Використання: /set_plan <user_id> <type> <duration>',
        'admin_remove_plan_usage': 'Використання: /remove_plan <user_id>',
        'admin_subscription_updated': 'Підписка для користувача {user_id} ({username}) оновлена до {end_time}.',
        'admin_subscription_removed': 'Платна підписка для користувача {user_id} ({username}) видалена, встановлено безкоштовний план.',
        'admin_message_sent': 'Повідомлення типу {type} надіслано користувачу {user_id}.',
        'admin_select_message': 'Виберіть тип повідомлення для надсилання:',
        'admin_transaction_rejected': 'Адміністратор відхилив транзакцію користувача {user_id}',
        'admin_subscription_set': 'Адміністратор встановив підписку для користувача {user_id} ({username}): {sub_type}, до {end_time}',
        'admin_subscription_removed_log': 'Адміністратор видалив платну підписку для користувача {user_id} ({username})',
        'admin_message_type | edit | copy'
        'admin_message_type_selected': 'Адмін вибрав тип повідомлення: {type}',
        'admin_menu_opened': 'Адмін відкрив меню надсилання повідомлення',
        'admin_enter_target_user_id': 'Введіть ID користувача для надсилання повідомлення:',
        'problem_response_yes': 'Будь ласка, зверніться до {support} для вирішення проблеми.',
        'rating_thanks': 'Дякуємо за оцінку: {rating} ⭐️',
        'info_not_available': 'Інформація недоступна',
        'parsing_start': 'Починаємо парсинг...',
        'action_forbidden': 'Дія заборонена, поки запущено парсинг'
    },
    'English': {
        'welcome': 'Hello! Choose your language:',
        'enter_phone': 'Enter your phone number in the format +380639678038 for authorization:',
        'enter_phone_invalid': 'Please enter the number in the format +380639678038:',
        'enter_code': 'Enter the confirmation code you received via SMS or Telegram:',
        'enter_password': 'Two-factor authentication password required. Enter your password:',
        'auth_success': 'Authorization completed successfully!',
        'auth_error': 'Authorization error: {error}. Try again with /start.',
        'choose_language': 'Choose language:',
        'subscribe': 'Subscribe to the channel: https://t.me/tgparseruser\nAfter subscribing, press "Continue".',
        'subscribed': 'Continue',
        'you_chose': 'You chose: {button}',
        'skip': 'Skip',
        'start_menu': 'You: {name}\nYour ID: {user_id}\nSelected language: {lang} /language\nSubscription type: {sub_type}\nYour subscription ends in: {sub_time}\nTotal requests made: {requests}\nRequests left today: {limit}',
        'identifiers': 'Send me @username, a link in any format, or forward a message from a channel to find out the ID.',
        'parser': 'Choose what you want to parse:',
        'subscribe_button': 'Subscribe',
        'support': 'Support: {support}',
        'requisites': 'Payment options:\n1. [Method 1]\n2. [Method 2]\nContact {support} for payment.',
        'logs_channel': 'Logs channel: t.me/YourLogChannel',
        'link_group': 'Send me a link to a group or channel, e.g.: https://t.me/group_name, @group_name or group_name\nYou can specify multiple links via Enter.',
        'link_post': 'Send me a link to a post, e.g.: https://t.me/channel_name/12345\nOr forward a post. You can specify multiple links via Enter.',
        'limit': 'How many users to parse? Choose or enter your number (max 5,000 authors/15,000 participants/10,000 post commentators for paid subscriptions, 150 for free).',
        'invalid_limit': 'Enter a number from 1 to {max_limit}!',
        'invalid_number': 'Please enter a number!',
        'invalid_link': 'Please send a valid post/chat link, e.g.: https://t.me/channel_name/12345, @channel_name or group_name\nOr multiple links via Enter.',
        'fix_link': 'If you made a mistake, I can help fix the link.',
        'suggest_link': 'Did you mean: {link}?',
        'retry_link': 'Send the link again:',
        'no_access': 'Error: I don’t have access to {link}. Make sure I’m added to the chat or it’s public.',
        'flood_error': 'Error: {e}',
        'rpc_error': 'Error: {e}',
        'new_user': 'New user: {name} (@{username})',
        'language_cmd': 'Choose a new language:',
        'caption_commentators': 'Here is your file with commentators.',
        'caption_participants': 'Here is your file with participants.',
        'caption_post_commentators': 'Here is your file with post commentators.',
        'limit_reached': 'You’ve reached the daily limit ({limit} requests). Try again in {hours} hours.',
        'id_result': 'ID: {id}',
        'close': 'Close',
        'continue_id': 'Continue',
        'subscription_1h': '1 Hour Subscription - 2 USDT (TON)',
        'subscription_3d': '3 Days Subscription - 5 USDT (TON)',
        'subscription_7d': '7 Days Subscription - 7 USDT (TON)',
        'subscription_bank_card': 'Payment via Bank Card',
        'payment_wallet': 'Transfer {amount} USDT to the TON wallet:\n{address}\nAfter payment, press "I Paid".',
        'payment_bank_card': 'Contact: @Alex_Strayker',
        'payment_cancel': 'Cancel',
        'payment_paid': 'I Paid',
        'payment_hash': 'Send the transaction hash/or screenshot if payment via card:',
        'payment_pending': 'Transaction sent for processing',
        'payment_update': 'Update',
        'payment_success': 'Subscription successfully activated!\nYour subscription is active until {end_time}.',
        'payment_error': 'Your transaction was unsuccessful!\nYou can appeal by contacting @astrajker_cb_id.',
        'entity_error': 'Could not retrieve user information. The user may be private or inaccessible.',
        'no_filter': 'Do not apply filter',
        'phone_contacts': 'Collect phone numbers and full names',
        'auth_access': 'Authorize for private chats',
        'caption_phones': 'Here is your file with phone numbers and full names (Excel and VCF).',
        'auth_request': 'To access private chats, add the bot as an admin or send a link to a private chat.',
        'auth_success': 'Access to the private chat successfully granted!',
        'auth_error': 'Could not gain access. Ensure the bot is added as an admin or the chat is public.',
        'note_cmd': 'Note successfully saved (bot will not respond).',
        'home_cmd': 'You returned to the main menu.',
        'info_cmd': 'This is a Telegram parsing bot.\nFeatures:\n- ID collection\n- Participants parsing\n- Commentators parsing\n- Contact collection\nFor support: {support}',
        'working_message': 'Everything is fine, we are still working...',
        'loading_message': 'Please wait...',
        'invalid_link_suggestion': 'The link "{link}" is invalid. Did you mean something like https://t.me/group_name or https://t.me/channel_name/12345? Try again.',
        'rate_parsing': 'Rate the parsing quality:',
        'info_identifiers': 'The "Identifiers" function allows you to find out the ID of users, groups, or posts.\nSend @username, a link in any format, or forward a message from a channel.',
        'info_parser': 'The "Parser" function allows you to collect data:\n- Message authors\n- Chat participants\n- Post commentators\n- Phone numbers\n- Access to private chats',
        'info_subscribe': 'Subscribe for extended limits:\n- 1 hour: 2 USDT\n- 3 days: 5 USDT\n- 7 days: 7 USDT',
        'info_requisites': 'Payment via TON wallet:\n1. Transfer USDT to the address\n2. Provide the transaction hash\nContact support for details.',
        'info_logs': 'The logs channel is available only to administrators. Logs contain information about user actions.',
        'info_parse_authors': 'Collecting message authors from a chat or channel.',
        'info_parse_participants': 'Collecting all participants of a chat or channel.',
        'info_parse_post_commentators': 'Collecting commentators of a specific post.',
        'info_parse_phone_contacts': 'Collecting phone numbers and full names of participants.',
        'info_parse_auth_access': 'Granting access to private chats.',
        'problem_message': 'Hello. Did you encounter a problem while using the bot?',
        'yes': 'Yes',
        'no': 'No',
        'maybe': 'Maybe',
        'subscribe_offer': 'Would you like to subscribe to a paid plan to access all features and remove restrictions?',
        'send_message': 'Send Message',
        'select_message': 'Select the type of message to send:',
        'bank_card_info': 'To pay via bank card, contact {support} to receive payment details.',
        'parse_authors': 'Message Authors',
        'parse_participants': 'Chat Participants',
        'parse_post_commentators': 'Post Commentators',
        'no_access_to_logs': 'You do not have access to the logs.',
        'admin_message_type_1': 'Type 1 (Problem)',
        'admin_message_type_2': 'Type 2 (Subscription)',
        'admin_reject_transaction': 'Transaction of user {user_id} has been rejected.',
        'admin_invalid_user_id': 'Please provide a valid user ID.',
        'admin_user_not_found': 'User not found.',
        'admin_no_permission': 'You do not have permission for this command.',
        'admin_invalid_set_plan': 'Invalid subscription type. Use "1h", "3d", "7d", or "permanent".',
        'admin_set_plan_usage': 'Usage: /set_plan <user_id> <type> <duration>',
        'admin_remove_plan_usage': 'Usage: /remove_plan <user_id>',
        'admin_subscription_updated': 'Subscription for user {user_id} ({username}) updated until {end_time}.',
        'admin_subscription_removed': 'Paid subscription for user {user_id} ({username}) removed, free plan set.',
        'admin_message_sent': 'Message type {type} sent to user {user_id}.',
        'admin_select_message': 'Select the type of message to send:',
        'admin_transaction_rejected': 'Administrator rejected the transaction of user {user_id}',
        'admin_subscription_set': 'Administrator set subscription for user {user_id} ({username}): {sub_type}, until {end_time}',
        'admin_subscription_removed_log': 'Administrator removed paid subscription for user {user_id} ({username})',
        'admin_message_type_selected': 'Admin selected message type: {type}',
        'admin_menu_opened': 'Admin opened the message sending menu',
        'admin_enter_target_user_id': 'Enter the user ID to send the message to:',
        'problem_response_yes': 'Please contact {support} to resolve the issue.',
        'rating_thanks': 'Thank you for your rating: {rating} ⭐️',
        'info_not_available': 'Information not available',
        'parsing_start': 'Starting parsing...',
        'action_forbidden': 'Action forbidden while parsing is running'
    },
    'Deutsch': {
        'welcome': 'Hallo! Wähle deine Sprache:',
        'enter_phone': 'Gib deine Telefonnummer im Format +380639678038 für die Autorisierung ein:',
        'enter_phone_invalid': 'Bitte gib die Nummer im Format +380639678038 ein:',
        'enter_code': 'Gib den Bestätigungscode ein, den du per SMS oder Telegram erhalten hast:',
        'enter_password': 'Passwort für die Zwei-Faktor-Authentifizierung erforderlich. Gib dein Passwort ein:',
        'auth_success': 'Autorisierung erfolgreich abgeschlossen!',
        'auth_error': 'Autorisierungsfehler: {error}. Versuche es erneut mit /start.',
        'choose_language': 'Wähle eine Sprache:',
        'subscribe': 'Abonniere den Kanal: https://t.me/tgparseruser\nDrücke nach dem Abonnieren "Fortfahren".',
        'subscribed': 'Fortfahren',
        'you_chose': 'Du hast gewählt: {button}',
        'skip': 'Überspringen',
        'start_menu': 'Du: {name}\nDeine ID: {user_id}\nGewählte Sprache: {lang} /language\nAbonnementtyp: {sub_type}\nDein Abonnement endet in: {sub_time}\nGesamte Anfragen: {requests}\nVerbleibende Anfragen heute: {limit}',
        'identifiers': 'Sende mir @username, einen Link in beliebigem Format oder leite eine Nachricht aus einem Kanal weiter, um die ID herauszufinden.',
        'parser': 'Wähle, was du parsen möchtest:',
        'subscribe_button': 'Abonnement abschließen',
        'support': 'Support: {support}',
        'requisites': 'Zahlungsmöglichkeiten:\n1. [Methode 1]\n2. [Methode 2]\nKontaktiere {support} für die Zahlung.',
        'logs_channel': 'Log-Kanal: t.me/YourLogChannel',
        'link_group': 'Sende mir einen Link zu einer Gruppe oder einem Kanal, z.B.: https://t.me/group_name, @group_name oder group_name\nDu kannst mehrere Links mit Enter angeben.',
        'link_post': 'Sende mir einen Link zu einem Beitrag, z.B.: https://t.me/channel_name/12345\nOder leite einen Beitrag weiter. Du kannst mehrere Links mit Enter angeben.',
        'limit': 'Wie viele Benutzer sollen geparst werden? Wähle oder gib eine Zahl ein (max. 5.000 Autoren/15.000 Teilnehmer/10.000 Beitragskommentatoren für bezahlte Abonnements, 150 für kostenlos).',
        'invalid_limit': 'Gib eine Zahl von 1 bis {max_limit} ein!',
        'invalid_number': 'Bitte gib eine Zahl ein!',
        'invalid_link': 'Bitte sende einen gültigen Beitrag/Chat-Link, z.B.: https://t.me/channel_name/12345, @channel_name oder group_name\nOder mehrere Links mit Enter.',
        'fix_link': 'Wenn du einen Fehler gemacht hast, kann ich den Link korrigieren.',
        'suggest_link': 'Meintest du: {link}?',
        'retry_link': 'Sende den Link erneut:',
        'no_access': 'Fehler: Ich habe keinen Zugriff auf {link}. Stelle sicher, dass ich zum Chat hinzugefügt bin oder er öffentlich ist.',
        'flood_error': 'Fehler: {e}',
        'rpc_error': 'Fehler: {e}',
        'new_user': 'Neuer Benutzer: {name} (@{username})',
        'language_cmd': 'Wähle eine neue Sprache:',
        'caption_commentators': 'Hier ist deine Datei mit Kommentatoren.',
        'caption_participants': 'Hier ist deine Datei mit Teilnehmern.',
        'caption_post_commentators': 'Hier ist deine Datei mit Beitragskommentatoren.',
        'limit_reached': 'Du hast das tägliche Limit ({limit} Anfragen) erreicht. Versuche es in {hours} Stunden erneut.',
        'id_result': 'ID: {id}',
        'close': 'Schließen',
        'continue_id': 'Fortfahren',
        'subscription_1h': '1 Stunde Abonnement - 2 USDT (TON)',
        'subscription_3d': '3 Tage Abonnement - 5 USDT (TON)',
        'subscription_7d': '7 Tage Abonnement - 7 USDT (TON)',
        'subscription_bank_card': 'Zahlung per Bankkarte',
        'payment_wallet': 'Überweise {amount} USDT auf den TON-Wallet:\n{address}\nNach der Zahlung drücke "Ich habe bezahlt".',
        'payment_bank_card': 'Kontakt: @Alex_Strayker',
        'payment_cancel': 'Abbrechen',
        'payment_paid': 'Ich habe bezahlt',
        'payment_hash': 'Sende den Transaktionshash/oder Screenshot, falls per Karte bezahlt:',
        'payment_pending': 'Transaktion zur Verarbeitung gesendet',
        'payment_update': 'Aktualisieren',
        'payment_success': 'Abonnement erfolgreich aktiviert!\nDein Abonnement ist aktiv bis {end_time}.',
        'payment_error': 'Deine Transaktion war nicht erfolgreich!\nDu kannst einen Einspruch bei @astrajker_cb_id einreichen.',
        'entity_error': 'Konnte keine Benutzerinformationen abrufen. Der Benutzer könnte privat oder nicht zugänglich sein.',
        'no_filter': 'Keinen Filter anwenden',
        'phone_contacts': 'Telefonnummern und vollständige Namen sammeln',
        'auth_access': 'Autorisierung für private Chats',
        'caption_phones': 'Hier ist deine Datei mit Telefonnummern und vollständigen Namen (Excel und VCF).',
        'auth_request': 'Um auf private Chats zuzugreifen, füge den Bot als Administrator hinzu oder sende einen Link zu einem privaten Chat.',
        'auth_success': 'Zugang zum privaten Chat erfolgreich gewährt!',
        'auth_error': 'Konnte keinen Zugriff erhalten. Stelle sicher, dass der Bot als Administrator hinzugefügt wurde oder der Chat öffentlich ist.',
        'note_cmd': 'Notiz erfolgreich gespeichert (der Bot wird nicht reagieren).',
        'home_cmd': 'Du bist zum Hauptmenü zurückgekehrt.',
        'info_cmd': 'Dies ist ein Telegram-Parsing-Bot.\nFunktionen:\n- ID-Sammlung\n- Teilnehmer-Parsing\n- Kommentatoren-Parsing\n- Kontaktsammlung\nFür Support: {support}',
        'working_message': 'Alles ist gut, wir arbeiten noch...',
        'loading_message': 'Bitte warten...',
        'invalid_link_suggestion': 'Der Link "{link}" ist ungültig. Meintest du vielleicht etwas wie https://t.me/group_name oder https://t.me/channel_name/12345? Versuche es erneut.',
        'rate_parsing': 'Bewerte die Parsing-Qualität:',
        'info_identifiers': 'Die Funktion "Identifikatoren" ermöglicht es, die ID von Benutzern, Gruppen oder Beiträgen herauszufinden.\nSende @username, einen Link in beliebigem Format oder leite eine Nachricht aus einem Kanal weiter.',
        'info_parser': 'Die Funktion "Parser" ermöglicht das Sammeln von Daten:\n- Nachrichtenautoren\n- Chat-Teilnehmer\n- Beitragskommentatoren\n- Telefonnummern\n- Zugriff auf private Chats',
        'info_subscribe': 'Schließe ein Abonnement für erweiterte Limits ab:\n- 1 Stunde: 2 USDT\n- 3 Tage: 5 USDT\n- 7 Tage: 7 USDT',
        'info_requisites': 'Zahlung über TON-Wallet:\n1. Überweise USDT an die Adresse\n2. Gib den Transaktionshash an\nKontaktiere den Support für Details.',
        'info_logs': 'Der Log-Kanal ist nur für Administratoren verfügbar. Logs enthalten Informationen über Benutzeraktionen.',
        'info_parse_authors': 'Sammeln von Nachrichtenautoren aus einem Chat oder Kanal.',
        'info_parse_participants': 'Sammeln aller Teilnehmer eines Chats oder Kanals.',
        'info_parse_post_commentators': 'Sammeln von Kommentatoren eines bestimmten Beitrags.',
        'info_parse_phone_contacts': 'Sammeln von Telefonnummern und vollständigen Namen der Teilnehmer.',
        'info_parse_auth_access': 'Gewährung von Zugriff auf private Chats.',
        'problem_message': 'Hallo. Haben Sie ein Problem bei der Nutzung des Bots?',
        'yes': 'Ja',
        'no': 'Nein',
        'maybe': 'Vielleicht',
        'subscribe_offer': 'Möchten Sie ein kostenpflichtiges Abonnement abschließen, um alle Funktionen nutzen zu können und Einschränkungen zu beseitigen?',
        'send_message': 'Nachricht senden',
        'select_message': 'Wählen Sie den Nachrichtentyp zum Senden:',
        'bank_card_info': 'Um per Bankkarte zu zahlen, kontaktieren Sie {support}, um Zahlungsdetails zu erhalten.',
        'parse_authors': 'Nachrichtenautoren',
        'parse_participants': 'Chat-Teilnehmer',
        'parse_post_commentators': 'Beitragskommentatoren',
        'no_access_to_logs': 'Sie haben keinen Zugriff auf die Logs.',
        'admin_message_type_1': 'Typ 1 (Problem)',
        'admin_message_type_2': 'Typ 2 (Abonnement)',
        'admin_reject_transaction': 'Transaktion des Benutzers {user_id} wurde abgelehnt.',
        'admin_invalid_user_id': 'Bitte geben Sie eine gültige Benutzer-ID an.',
        'admin_user_not_found': 'Benutzer nicht gefunden.',
        'admin_no_permission': 'Sie haben keine Berechtigung für diesen Befehl.',
        'admin_invalid_set_plan': 'Ungültiger Abonnementtyp. Verwenden Sie "1h", "3d", "7d" oder "permanent".',
        'admin_set_plan_usage': 'Verwendung: /set_plan <user_id> <type> <duration>',
        'admin_remove_plan_usage': 'Verwendung: /remove_plan <user_id>',
        'admin_subscription_updated': 'Abonnement für Benutzer {user_id} ({username}) aktualisiert bis {end_time}.',
        'admin_subscription_removed': 'Bezahlte Abonnement für Benutzer {user_id} ({username}) entfernt, kostenloser Plan gesetzt.',
        'admin_message_sent': 'Nachrichtentyp {type} an Benutzer {user_id} gesendet.',
        'admin_select_message': 'Wählen Sie den Nachrichtentyp zum Senden:',
        'admin_transaction_rejected': 'Administrator hat die Transaktion des Benutzers {user_id} abgelehnt',
        'admin_subscription_set': 'Administrator hat Abonnement für Benutzer {user_id} ({username}) gesetzt: {sub_type}, bis {end_time}',
        'admin_subscription_removed_log': 'Administrator hat das bezahlte Abonnement für Benutzer {user_id} ({username}) entfernt',
        'admin_message_type_selected': 'Admin hat Nachrichtentyp gewählt: {type}',
        'admin_menu_opened': 'Admin hat das Menü zum Senden von Nachrichten geöffnet',
        'admin_enter_target_user_id': 'Geben Sie die Benutzer-ID ein, an die die Nachricht gesendet werden soll:',
        'problem_response_yes': 'Bitte kontaktieren Sie {support}, um das Problem zu lösen.',
        'rating_thanks': 'Vielen Dank für Ihre Bewertung: {rating} ⭐️',
        'info_not_available': 'Information nicht verfügbar',
        'parsing_start': 'Parsing wird gestartet...',
        'action_forbidden': 'Aktion verboten, während das Parsing läuft'
    }
}

# Логирование в канал с поддержкой отправки файлов
async def log_to_channel(context, message, username=None, file=None):
    try:
        user = context.user_data.get('user', {})
        name = user.get('name', 'Неизвестно')
        log_message = f"Пользователь {name} (@{username}): {message}" if username else message
        if file:
            file.seek(0)
            await context.bot.send_document(
                chat_id=LOG_CHANNEL_ID,
                document=file,
                filename=file.name if hasattr(file, 'name') else f"file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                caption=log_message
            )
        else:
            await context.bot.send_message(chat_id=LOG_CHANNEL_ID, text=log_message)
    except telegram_error.BadRequest as e:
        print(f"Ошибка при отправке лога в канал: {e}")

# Обновление пользовательских данных
def update_user_data(user_id, name, context, lang=None, subscription=None, requests=0):
    users = load_users()
    user_id_str = str(user_id)
    now = datetime.now()
    if user_id_str not in users:
        users[user_id_str] = {
            'name': name,
            'language': lang or 'Русский',
            'subscription': subscription or {'type': 'Бесплатная', 'end': None},
            'requests': 0,
            'daily_requests': {'count': 0, 'last_reset': now.isoformat()},
            'is_parsing': False
        }
    user = users[user_id_str]
    if 'daily_requests' not in user or not isinstance(user['daily_requests'], dict):
        user['daily_requests'] = {'count': 0, 'last_reset': now.isoformat()}
    if lang:
        user['language'] = lang
    if subscription:
        user['subscription'] = subscription
    last_reset = datetime.fromisoformat(user['daily_requests']['last_reset'])
    if (now - last_reset).days >= 1:
        user['daily_requests'] = {'count': 0, 'last_reset': now.isoformat()}
    user['requests'] = user.get('requests', 0) + requests
    user['daily_requests']['count'] = user['daily_requests'].get('count', 0) + requests
    user['name'] = name
    context.user_data['user'] = user
    save_users(users)
    return user

# Проверка лимита запросов
def check_request_limit(user_id):
    users = load_users()
    user_id_str = str(user_id)
    user = users.get(user_id_str, {})
    now = datetime.now()
    if 'daily_requests' not in user or not isinstance(user.get('daily_requests'), dict):
        user['daily_requests'] = {'count': 0, 'last_reset': now.isoformat()}
        save_users(users)
    daily_requests = user['daily_requests']
    last_reset = datetime.fromisoformat(daily_requests['last_reset'])
    if (now - last_reset).days >= 1:
        daily_requests = {'count': 0, 'last_reset': now.isoformat()}
        users[user_id_str]['daily_requests'] = daily_requests
        save_users(users)
    subscription = user.get('subscription', {'type': 'Бесплатная', 'end': None})
    max_requests = 5 if subscription['type'] == 'Бесплатная' else 90
    return daily_requests['count'] < max_requests, 24 - (now - last_reset).seconds // 3600

# Проверка лимита парсинга
def check_parse_limit(user_id, limit, parse_type):
    users = load_users()
    user_id_str = str(user_id)
    user = users.get(user_id_str, {})
    subscription = user.get('subscription', {'type': 'Бесплатная', 'end': None})
    now = datetime.now()
    if subscription['type'].startswith('Платная') and subscription['end']:
        if datetime.fromisoformat(subscription['end']) < now:
            subscription = {'type': 'Бесплатная', 'end': None}
            users[user_id_str]['subscription'] = subscription
            save_users(users)
    if subscription['type'] == 'Бесплатная':
        return min(limit, 1000)
    elif parse_type == 'parse_authors':
        return min(limit, 5000)
    elif parse_type == 'parse_participants':
        return min(limit, 15000)
    elif parse_type == 'parse_post_commentators':
        return min(limit, 10000)
    else:
        return min(limit, 15000)

# Проверка статуса подписки
def has_paid_subscription(user_id):
    users = load_users()
    user_id_str = str(user_id)
    user = users.get(user_id_str, {})
    subscription = user.get('subscription', {'type': 'Бесплатная', 'end': None})
    now = datetime.now()
    if subscription['type'].startswith('Платная') and subscription['end']:
        if datetime.fromisoformat(subscription['end']) >= now:
            return True
    return False

# Создание файла Excel
async def create_excel_in_memory(data):
    df = pd.DataFrame(data, columns=['ID', 'Username', 'First Name', 'Last Name', 'Bot'])
    excel_file = io.BytesIO()
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    excel_file.seek(0)
    return excel_file

# Создание VCF файла
async def create_vcf_in_memory(data):
    vcf_content = ""
    for entry in data:
        vcard = vobject.vCard()
        vcard.add('fn').value = f"{entry['first_name']} {entry['last_name']}".strip()
        vcard.add('tel').value = entry['phone']
        vcf_content += vcard.serialize()
    vcf_file = io.StringIO(vcf_content)
    vcf_file.name = f"phones_{datetime.now().strftime('%Y%m%d_%H%M%S')}.vcf"
    return vcf_file

# Подсчёт статистики
def get_statistics(data):
    total = len(data)
    with_username = sum(1 for row in data if row[1])
    bots = sum(1 for row in data if row[4])
    without_name = sum(1 for row in data if not row[2])
    return f"Всего: {total}\nС username: {with_username}\nБотов: {bots}\nБез имени: {without_name}"

# Получение главного меню
def get_main_menu(user_id, context):
    users = load_users()
    user_id_str = str(user_id)
    user_data = users.get(user_id_str, {})
    lang = user_data.get('language', 'Русский')
    texts = LANGUAGES[lang]
    sub_type = user_data.get('subscription', {}).get('type', 'Бесплатная')
    sub_end = user_data.get('subscription', {}).get('end')
    sub_time = '—' if sub_type == 'Бесплатная' else (
        'бессрочно' if sub_end is None else 
        f"{(datetime.fromisoformat(sub_end) - datetime.now()).days * 24 + (datetime.fromisoformat(sub_end) - datetime.now()).seconds // 3600} часов"
    )
    requests = user_data.get('requests', 0)
    name = user_data.get('name', 'Неизвестно')
    limit_left, _ = check_request_limit(user_id)
    limit_display = 5 - user_data.get('daily_requests', {}).get('count', 0) if sub_type == 'Бесплатная' else 90 - user_data.get('daily_requests', {}).get('count', 0)
    
    is_admin = user_id_str in ADMIN_IDS
    
    buttons = [
        [InlineKeyboardButton(texts['identifiers'], callback_data=f'identifiers_{user_id}'), 
         InlineKeyboardButton("(!)", callback_data=f'info_identifiers_{user_id}')],
        [InlineKeyboardButton(texts['parser'], callback_data=f'parser_{user_id}'), 
         InlineKeyboardButton("(!)", callback_data=f'info_parser_{user_id}')],
        [InlineKeyboardButton(texts['subscribe_button'], callback_data=f'subscribe_{user_id}'), 
         InlineKeyboardButton("(!)", callback_data=f'info_subscribe_{user_id}')],
        [InlineKeyboardButton(f"{texts['support'].format(support=SUPPORT_USERNAME)}", url=f"https://t.me/{SUPPORT_USERNAME[1:]}")],
        [InlineKeyboardButton(texts['requisites'], callback_data=f'requisites_{user_id}'), 
         InlineKeyboardButton("(!)", callback_data=f'info_requisites_{user_id}')]
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton(texts['logs_channel'], callback_data=f'logs_{user_id}'), 
                        InlineKeyboardButton("(!)", callback_data=f'info_logs_{user_id}')])
        buttons.append([InlineKeyboardButton(texts['send_message'], callback_data=f'send_message_{user_id}')])
    
    return texts['start_menu'].format(
        name=name, user_id=user_id, lang=lang, sub_type=sub_type, sub_time=sub_time, requests=requests, limit=limit_display
    ), InlineKeyboardMarkup(buttons)

# Обработчик команды /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()

    if users.get(str(user_id), {}).get('is_parsing', False):
        await update.message.reply_text(LANGUAGES['Русский']['action_forbidden'])
        return

    try:
        await client_telethon.connect()
        if not await client_telethon.is_user_authorized():
            await update.message.reply_text(LANGUAGES['Русский']['enter_phone'])
            context.user_data['waiting_for_phone'] = True
            await log_to_channel(context, LANGUAGES['Русский']['new_user'].format(name=name, username=username), username)
            return

        if str(user_id) not in users:
            await log_to_channel(context, LANGUAGES['Русский']['new_user'].format(name=name, username=username), username)
            keyboard = [
                [InlineKeyboardButton("Русский", callback_data=f'lang_Русский_{user_id}')],
                [InlineKeyboardButton("Украинский", callback_data=f'lang_Украинский_{user_id}')],
                [InlineKeyboardButton("English", callback_data=f'lang_English_{user_id}')],
                [InlineKeyboardButton("Deutsch", callback_data=f'lang_Deutsch_{user_id}')]
            ]
            await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            lang = users[str(user_id)]['language']
            texts = LANGUAGES[lang]
            await update.message.reply_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data=f'subscribed_{user_id}')]]))
            update_user_data(user_id, name, context)

    except telethon_errors.RPCError as e:
        await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка подключения/авторизации для {name} (@{username}): {str(e)}", username)
    finally:
        if client_telethon.is_connected():
            await client_telethon.disconnect()

# Обработчик команды /home
async def home(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    if users.get(str(user_id), {}).get('is_parsing', False):
        await update.message.reply_text(texts['action_forbidden'])
        return

    menu_text, menu_keyboard = get_main_menu(user_id, context)
    await update.message.reply_text(texts['home_cmd'], reply_markup=menu_keyboard)
    await log_to_channel(context, f"Пользователь {name} (@{username}) вернулся в главное меню", username)

# Обработчик команды /info
async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    if users.get(str(user_id), {}).get('is_parsing', False):
        await update.message.reply_text(texts['action_forbidden'])
        return

    await update.message.reply_text(texts['info_cmd'].format(support=SUPPORT_USERNAME))
    await log_to_channel(context, f"Пользователь {name} (@{username}) запросил информацию о боте", username)

# Обработчик команды /language
async def language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    if users.get(str(user_id), {}).get('is_parsing', False):
        await update.message.reply_text(texts['action_forbidden'])
        return

    keyboard = [
        [InlineKeyboardButton("Русский", callback_data=f'lang_Русский_{user_id}')],
        [InlineKeyboardButton("Украинский", callback_data=f'lang_Украинский_{user_id}')],
        [InlineKeyboardButton("English", callback_data=f'lang_English_{user_id}')],
        [InlineKeyboardButton("Deutsch", callback_data=f'lang_Deutsch_{user_id}')]
    ]
    await update.message.reply_text(texts['language_cmd'], reply_markup=InlineKeyboardMarkup(keyboard))
    await log_to_channel(context, f"Пользователь {name} (@{username}) запросил смену языка", username)

# Обработчик команды /set_plan
async def set_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    if users.get(str(user_id), {}).get('is_parsing', False):
        await update.message.reply_text(texts['action_forbidden'])
        return

    if str(user_id) not in ADMIN_IDS:
        await update.message.reply_text(texts['admin_no_permission'])
        return
    
    args = context.args
    if len(args) != 3:
        await update.message.reply_text(texts['admin_set_plan_usage'])
        return
    
    target_user_id, sub_type, duration = args[0], args[1], args[2]
    now = datetime.now()
    
    try:
        duration = int(duration)
    except ValueError:
        await update.message.reply_text(texts['admin_set_plan_usage'])
        return
    
    if sub_type == '1h':
        end_time = now + timedelta(hours=duration)
    elif sub_type == '3d':
        end_time = now + timedelta(days=duration)
    elif sub_type == '7d':
        end_time = now + timedelta(days=duration)
    elif sub_type == 'permanent':
        end_time = None
    else:
        await update.message.reply_text(texts['admin_invalid_set_plan'])
        return
    
    subscription_type = f'Платная ({sub_type})' if sub_type in ['1h', '3d', '7d'] else 'Платная (бессрочная)'
    update_user_data(target_user_id, "Имя пользователя", context, subscription={'type': subscription_type, 'end': end_time.isoformat() if end_time else None})
    
    username = load_users().get(str(target_user_id), {}).get('name', 'Неизвестно')
    lang = load_users().get(str(target_user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    notification = texts['payment_success'].format(end_time=end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'бессрочно')
    await context.bot.send_message(chat_id=target_user_id, text=f"🎉 {notification}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_update'], callback_data=f'update_menu_{target_user_id}')]]))
    
    admin_lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    admin_texts = LANGUAGES[admin_lang]
    await update.message.reply_text(admin_texts['admin_subscription_updated'].format(
        user_id=target_user_id, username=username, end_time=end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'бессрочно'
    ))
    await log_to_channel(context, admin_texts['admin_subscription_set'].format(
        user_id=target_user_id, username=username, sub_type=subscription_type, end_time=end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'бессрочно'
    ))

# Обработчик команды /remove_plan
async def remove_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    if users.get(str(user_id), {}).get('is_parsing', False):
        await update.message.reply_text(texts['action_forbidden'])
        return

    if str(user_id) not in ADMIN_IDS:
        await update.message.reply_text(texts['admin_no_permission'])
        return
    
    args = context.args
    if len(args) != 1:
        await update.message.reply_text(texts['admin_remove_plan_usage'])
        return
    
    target_user_id = args[0]
    users = load_users()
    if target_user_id not in users:
        await update.message.reply_text(texts['admin_user_not_found'])
        return
    
    update_user_data(target_user_id, users[target_user_id]['name'], context, subscription={'type': 'Бесплатная', 'end': None})
    
    username = users[target_user_id]['name']
    lang = users[target_user_id]['language']
    texts = LANGUAGES[lang]
    
    await context.bot.send_message(chat_id=target_user_id, text=texts['admin_subscription_removed'].format(user_id=target_user_id, username=username))
    
    admin_lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    admin_texts = LANGUAGES[admin_lang]
    await update.message.reply_text(admin_texts['admin_subscription_removed'].format(user_id=target_user_id, username=username))
    await log_to_channel(context, admin_texts['admin_subscription_removed_log'].format(user_id=target_user_id, username=username))

# Обработчик команды /abort
async def abort_parsing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    if not users.get(str(user_id), {}).get('is_parsing', False):
        await update.message.reply_text("Парсинг не запущен.")
        return
    
    user = users.get(str(user_id), {})
    user['is_parsing'] = False
    save_users(users)
    
    menu_text, menu_keyboard = get_main_menu(user_id, context)
    await update.message.reply_text(texts['abort_parsing'], reply_markup=menu_keyboard)
    await log_to_channel(context, f"Пользователь {name} (@{username}) прервал парсинг", username)

# Обработчик ввода номера телефона
async def handle_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('waiting_for_phone'):
        return
    
    user_id = update.effective_user.id
    username = update.effective_user.username
    phone = update.message.text.strip()
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    if users.get(str(user_id), {}).get('is_parsing', False):
        await update.message.reply_text(texts['action_forbidden'])
        return

    if not phone.startswith('+') or not phone[1:].isdigit():
        await update.message.reply_text(texts['enter_phone_invalid'])
        return
    
    try:
        await client_telethon.connect()
        await client_telethon.send_code_request(phone)
        context.user_data['phone'] = phone
        context.user_data['waiting_for_phone'] = False
        context.user_data['waiting_for_code'] = True
        await update.message.reply_text(texts['enter_code'])
        await log_to_channel(context, f"Пользователь {update.effective_user.full_name} (@{username}) ввёл номер телефона: {phone}", username)
    except telethon_errors.RPCError as e:
        await update.message.reply_text(texts['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка отправки кода для {update.effective_user.full_name} (@{username}): {str(e)}", username)
    finally:
        if client_telethon.is_connected():
            await client_telethon.disconnect()

# Обработчик ввода кода подтверждения
async def handle_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('waiting_for_code'):
        return
    
    user_id = update.effective_user.id
    username = update.effective_user.username
    code = update.message.text.strip()
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    if users.get(str(user_id), {}).get('is_parsing', False):
        await update.message.reply_text(texts['action_forbidden'])
        return

    try:
        await client_telethon.connect()
        phone = context.user_data['phone']
        await client_telethon.sign_in(phone, code)
        context.user_data['waiting_for_code'] = False
        await update.message.reply_text(texts['auth_success'])
        await update.message.reply_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data=f'subscribed_{user_id}')]]))
        await log_to_channel(context, f"Пользователь {update.effective_user.full_name} (@{username}) успешно авторизовался", username)
    except telethon_errors.SessionPasswordNeededError:
        context.user_data['waiting_for_code'] = False
        context.user_data['waiting_for_password'] = True
        await update.message.reply_text(texts['enter_password'])
        await log_to_channel(context, f"Пользователю {update.effective_user.full_name} (@{username}) требуется пароль 2FA", username)
    except telethon_errors.RPCError as e:
        await update.message.reply_text(texts['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка ввода кода для {update.effective_user.full_name} (@{username}): {str(e)}", username)
    finally:
        if client_telethon.is_connected():
            await client_telethon.disconnect()

# Обработчик ввода пароля 2FA
async def handle_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('waiting_for_password'):
        return
    
    user_id = update.effective_user.id
    username = update.effective_user.username
    password = update.message.text.strip()
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    if users.get(str(user_id), {}).get('is_parsing', False):
        await update.message.reply_text(texts['action_forbidden'])
        return

    try:
        await client_telethon.connect()
        phone = context.user_data['phone']
        await client_telethon.sign_in(password=password)
        context.user_data['waiting_for_password'] = False
        await update.message.reply_text(texts['auth_success'])
        await update.message.reply_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data=f'subscribed_{user_id}')]]))
        await log_to_channel(context, f"Пользователь {update.effective_user.full_name} (@{username}) успешно ввёл пароль 2FA", username)
    except telethon_errors.RPCError as e:
        await update.message.reply_text(texts['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка ввода пароля для {update.effective_user.full_name} (@{username}): {str(e)}", username)
    finally:
        if client_telethon.is_connected():
            await client_telethon.disconnect()

# Обработчик callback-запросов
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    username = query.from_user.username
    name = query.from_user.full_name or "Без имени"
    data = query.data
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    if users.get(str(user_id), {}).get('is_parsing', False) and not data.startswith('rate_') and not data.startswith('close_'):
        await query.message.reply_text(texts['action_forbidden'])
        await query.answer()
        return
    
    await query.answer()
    
    try:
        if data.startswith('lang_'):
            selected_lang = data.split('_')[1]
            update_user_data(user_id, name, context, lang=selected_lang)
            lang = selected_lang
            texts = LANGUAGES[lang]
            await query.message.reply_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data=f'subscribed_{user_id}')]]))
            await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал язык: {selected_lang}", username)
        
        elif data.startswith('subscribed_'):
            menu_text, menu_keyboard = get_main_menu(user_id, context)
            await query.message.reply_text(menu_text, reply_markup=menu_keyboard)
            await log_to_channel(context, f"Пользователь {name} (@{username}) подтвердил подписку и открыл главное меню", username)
        
        elif data.startswith('identifiers_'):
            await query.message.reply_text(texts['identifiers'], reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
            ]))
            context.user_data['state'] = 'waiting_for_identifier'
            await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал функцию 'Идентификаторы'", username)
        
        elif data.startswith('parser_'):
            buttons = [
                [InlineKeyboardButton(texts['parse_authors'], callback_data=f'parse_authors_{user_id}'), InlineKeyboardButton("(!)", callback_data=f'info_parse_authors_{user_id}')],
                [InlineKeyboardButton(texts['parse_participants'], callback_data=f'parse_participants_{user_id}'), InlineKeyboardButton("(!)", callback_data=f'info_parse_participants_{user_id}')],
                [InlineKeyboardButton(texts['parse_post_commentators'], callback_data=f'parse_post_commentators_{user_id}'), InlineKeyboardButton("(!)", callback_data=f'info_parse_post_commentators_{user_id}')],
                [InlineKeyboardButton(texts['phone_contacts'], callback_data=f'parse_phone_contacts_{user_id}'), InlineKeyboardButton("(!)", callback_data=f'info_parse_phone_contacts_{user_id}')],
                [InlineKeyboardButton(texts['auth_access'], callback_data=f'parse_auth_access_{user_id}'), InlineKeyboardButton("(!)", callback_data=f'info_parse_auth_access_{user_id}')],
                [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
            ]
            await query.message.reply_text(texts['parser'], reply_markup=InlineKeyboardMarkup(buttons))
            await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал функцию 'Парсер'", username)
        
        elif data.startswith('parse_authors_'):
            await query.message.reply_text(texts['link_group'], reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
            ]))
            context.user_data['state'] = 'waiting_for_group_link'
            context.user_data['parse_type'] = 'parse_authors'
            await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал парсинг авторов сообщений", username)
        
        elif data.startswith('parse_participants_'):
            await query.message.reply_text(texts['link_group'], reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
            ]))
            context.user_data['state'] = 'waiting_for_group_link'
            context.user_data['parse_type'] = 'parse_participants'
            await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал парсинг участников чата", username)
        
        elif data.startswith('parse_post_commentators_'):
            await query.message.reply_text(texts['link_post'], reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
            ]))
            context.user_data['state'] = 'waiting_for_post_link'
            context.user_data['parse_type'] = 'parse_post_commentators'
            await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал парсинг комментаторов поста", username)
        
        elif data.startswith('parse_phone_contacts_'):
            await query.message.reply_text(texts['link_group'], reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
            ]))
            context.user_data['state'] = 'waiting_for_group_link'
            context.user_data['parse_type'] = 'parse_phone_contacts'
            await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал сбор номеров телефонов", username)
        
        elif data.startswith('parse_auth_access_'):
            await query.message.reply_text(texts['auth_request'], reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
            ]))
            context.user_data['state'] = 'waiting_for_auth_access'
            context.user_data['parse_type'] = 'parse_auth_access'
            await log_to_channel(context, f"Пользователь {name} (@{username}) запросил доступ к закрытым чатам", username)
        
        elif data.startswith('subscribe_'):
            buttons = [
                [InlineKeyboardButton(texts['subscription_1h'], callback_data=f'sub_1h_{user_id}')],
                [InlineKeyboardButton(texts['subscription_3d'], callback_data=f'sub_3d_{user_id}')],
                [InlineKeyboardButton(texts['subscription_7d'], callback_data=f'sub_7d_{user_id}')],
                [InlineKeyboardButton(texts['subscription_bank_card'], callback_data=f'sub_bank_card_{user_id}')],
                [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
            ]
            await query.message.reply_text(texts['subscribe_button'], reply_markup=InlineKeyboardMarkup(buttons))
            await log_to_channel(context, f"Пользователь {name} (@{username}) открыл меню подписки", username)
        
        elif data.startswith('sub_'):
            sub_type = data.split('_')[1]
            amounts = {'1h': 2, '3d': 5, '7d': 7}
            amount = amounts.get(sub_type, 0)
            buttons = [
                [InlineKeyboardButton(texts['payment_paid'], callback_data=f'paid_{sub_type}_{user_id}')],
                [InlineKeyboardButton(texts['payment_cancel'], callback_data=f'close_{user_id}')]
            ]
            await query.message.reply_text(
                texts['payment_wallet'].format(amount=amount, address=TON_WALLET_ADDRESS),
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            context.user_data['subscription_type'] = sub_type
            await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал подписку: {sub_type}", username)
        
        elif data.startswith('sub_bank_card_'):
            await query.message.reply_text(texts['payment_bank_card'], parse_mode='HTML')
            context.user_data['subscription_type'] = 'bank_card'
            context.user_data['state'] = 'waiting_for_payment_hash'
            await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал оплату банковской картой", username)
        
        elif data.startswith('paid_'):
            sub_type = data.split('_')[1]
            context.user_data['subscription_type'] = sub_type
            context.user_data['state'] = 'waiting_for_payment_hash'
            await query.message.reply_text(texts['payment_hash'])
            await log_to_channel(context, f"Пользователь {name} (@{username}) подтвердил оплату для подписки: {sub_type}", username)
        
        elif data.startswith('limit_'):
            limit = int(data.split('_')[1])
            context.user_data['limit'] = limit
            parse_type = context.user_data.get('parse_type')
            max_limit = check_parse_limit(user_id, limit, parse_type)
            if limit > max_limit:
                await query.message.reply_text(texts['invalid_limit'].format(max_limit=max_limit))
                return
            message = await query.message.reply_text(texts['parsing_start'])
            for i in range(3):
                await asyncio.sleep(1)
                await message.edit_text(texts['parsing_start'] + '.' * (i + 1))
            for i in range(2, -1, -1):
                await asyncio.sleep(1)
                await message.edit_text(texts['parsing_start'] + '.' * i)
            await parse_data(update, context, limit=limit)
            await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал лимит парсинга: {limit}", username)
        
        elif data.startswith('close_'):
            menu_text, menu_keyboard = get_main_menu(user_id, context)
            await query.message.reply_text(menu_text, reply_markup=menu_keyboard)
            context.user_data.clear()
            await log_to_channel(context, f"Пользователь {name} (@{username}) закрыл меню", username)
        
        elif data.startswith('update_menu_'):
            menu_text, menu_keyboard = get_main_menu(user_id, context)
            await query.message.reply_text(menu_text, reply_markup=menu_keyboard)
            await log_to_channel(context, f"Пользователь {name} (@{username}) обновил главное меню", username)
        
        elif data.startswith('info_'):
            info_type = data.split('_')[1]
            await query.message.reply_text(texts[f'info_{info_type}'].format(support=SUPPORT_USERNAME))
            await log_to_channel(context, f"Пользователь {name} (@{username}) запросил информацию о: {info_type}", username)
        
        elif data.startswith('send_message_'):
            if str(user_id) not in ADMIN_IDS:
                await query.message.reply_text(texts['admin_no_permission'])
                return
            buttons = [
                [InlineKeyboardButton(texts['admin_message_type_1'], callback_data=f'admin_message_1_{user_id}')],
                [InlineKeyboardButton(texts['admin_message_type_2'], callback_data=f'admin_message_2_{user_id}')],
                [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
            ]
            await query.message.reply_text(texts['admin_select_message'], reply_markup=InlineKeyboardMarkup(buttons))
            await log_to_channel(context, texts['admin_menu_opened'], username)
        
        elif data.startswith('admin_message_'):
            message_type = data.split('_')[2]
            context.user_data['admin_message_type'] = message_type
            await query.message.reply_text(texts['admin_enter_target_user_id'])
            context.user_data['state'] = 'waiting_for_admin_user_id'
            await log_to_channel(context, texts['admin_message_type_selected'].format(type=message_type), username)
        
        elif data.startswith('rate_'):
            rating = int(data.split('_')[1])
            await query.message.reply_text(texts['rating_thanks'].format(rating=rating))
            menu_text, menu_keyboard = get_main_menu(user_id, context)
            await query.message.reply_text(menu_text, reply_markup=menu_keyboard)
            await log_to_channel(context, f"Пользователь {name} (@{username}) оценил парсинг: {rating} ⭐️ и вернулся в главное меню", username)
            user = users.get(str(user_id), {})
            user['is_parsing'] = False
            save_users(users)

    except Exception as e:
        await query.message.reply_text(texts['rpc_error'].format(e=str(e)))
        await log_to_channel(context, f"Ошибка callback для {name} (@{username}): {str(e)}", username)

# Обработчик ввода сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    state = context.user_data.get('state')
    
    if users.get(str(user_id), {}).get('is_parsing', False):
        await update.message.reply_text(texts['action_forbidden'])
        return
    
    message_text = update.message.text.strip()
    await log_to_channel(context, f"Сообщение от {name} (@{username}): {message_text}", username)
    
    if state == 'waiting_for_identifier':
        identifier = message_text
        try:
            await client_telethon.connect()
            entity = await client_telethon.get_entity(identifier)
            entity_id = entity.id
            buttons = [
                [InlineKeyboardButton(texts['continue_id'], callback_data=f'identifiers_{user_id}')],
                [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
            ]
            await update.message.reply_text(texts['id_result'].format(id=entity_id), reply_markup=InlineKeyboardMarkup(buttons))
            await log_to_channel(context, f"Пользователь {name} (@{username}) получил ID для {identifier}: {entity_id}", username)
            context.user_data['state'] = None
        except telethon_errors.RPCError as e:
            await update.message.reply_text(texts['entity_error'])
            await log_to_channel(context, f"Ошибка получения ID для {name} (@{username}): {str(e)}", username)
        finally:
            if client_telethon.is_connected():
                await client_telethon.disconnect()
    
    elif state == 'waiting_for_group_link':
        links = message_text.split('\n')
        context.user_data['links'] = links
        has_paid = has_paid_subscription(user_id)
        buttons = [
            [InlineKeyboardButton(f"150 {'✅' if has_paid else '✅'}", callback_data=f'limit_150_{user_id}'),
             InlineKeyboardButton(f"500 {'✅' if has_paid else '✅'}", callback_data=f'limit_500_{user_id}')],
            [InlineKeyboardButton(f"1000 {'✅' if has_paid else '✅'}", callback_data=f'limit_1000_{user_id}')],
            [InlineKeyboardButton(f"5000 {'✅' if has_paid else '❌'}", callback_data=f'limit_5000_{user_id}' if has_paid else 'no_access'),
             InlineKeyboardButton(f"10000 {'✅' if has_paid else '❌'}", callback_data=f'limit_10000_{user_id}' if has_paid else 'no_access')],
            [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
        ]
        await update.message.reply_text(texts['limit'], reply_markup=InlineKeyboardMarkup(buttons))
        await log_to_channel(context, f"Пользователь {name} (@{username}) ввёл ссылки для парсинга: {links}", username)
    
    elif state == 'waiting_for_post_link':
        links = message_text.split('\n')
        context.user_data['links'] = links
        has_paid = has_paid_subscription(user_id)
        buttons = [
            [InlineKeyboardButton(f"150 {'✅' if has_paid else '✅'}", callback_data=f'limit_150_{user_id}'),
             InlineKeyboardButton(f"500 {'✅' if has_paid else '✅'}", callback_data=f'limit_500_{user_id}')],
            [InlineKeyboardButton(f"1000 {'✅' if has_paid else '✅'}", callback_data=f'limit_1000_{user_id}')],
            [InlineKeyboardButton(f"5000 {'✅' if has_paid else '❌'}", callback_data=f'limit_5000_{user_id}' if has_paid else 'no_access'),
             InlineKeyboardButton(f"10000 {'✅' if has_paid else '❌'}", callback_data=f'limit_10000_{user_id}' if has_paid else 'no_access')],
            [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
        ]
        await update.message.reply_text(texts['limit'], reply_markup=InlineKeyboardMarkup(buttons))
        await log_to_channel(context, f"Пользователь {name} (@{username}) ввёл ссылки на посты: {links}", username)
    
    elif state == 'waiting_for_payment_hash':
        hash_or_screenshot = message_text
        context.user_data['payment_hash'] = hash_or_screenshot
        await update.message.reply_text(texts['payment_pending'])
        await log_to_channel(context, f"Пользователь {name} (@{username}) отправил хеш/скрин транзакции: {hash_or_screenshot}", username)
        sub_type = context.user_data.get('subscription_type')
        now = datetime.now()
        if sub_type == '1h':
            end_time = now + timedelta(hours=1)
        elif sub_type == '3d':
            end_time = now + timedelta(days=3)
        elif sub_type == '7d':
            end_time = now + timedelta(days=7)
        else:
            end_time = now + timedelta(hours=1)
        update_user_data(user_id, name, context, subscription={'type': f'Платная ({sub_type})', 'end': end_time.isoformat()})
        await update.message.reply_text(
            texts['payment_success'].format(end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_update'], callback_data=f'update_menu_{user_id}')]])
        )
        await log_to_channel(context, f"Подписка для {name} (@{username}) успешно оформлена до {end_time}", username)
        context.user_data['state'] = None
    
    elif state == 'waiting_for_admin_user_id':
        if str(user_id) not in ADMIN_IDS:
            await update.message.reply_text(texts['admin_no_permission'])
            return
        target_user_id = message_text
        if not target_user_id.isdigit():
            await update.message.reply_text(texts['admin_invalid_user_id'])
            return
        target_user_id = int(target_user_id)
        users = load_users()
        if str(target_user_id) not in users:
            await update.message.reply_text(texts['admin_user_not_found'])
            return
        message_type = context.user_data.get('admin_message_type')
        target_lang = users[str(target_user_id)]['language']
        target_texts = LANGUAGES[target_lang]
        if message_type == '1':
            await context.bot.send_message(target_user_id, target_texts['problem_message'], reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(target_texts['yes'], callback_data=f'problem_yes_{target_user_id}')],
                [InlineKeyboardButton(target_texts['no'], callback_data=f'problem_no_{target_user_id}')],
                [InlineKeyboardButton(target_texts['maybe'], callback_data=f'problem_maybe_{target_user_id}')]
            ]))
        elif message_type == '2':
            await context.bot.send_message(target_user_id, target_texts['subscribe_offer'], reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(target_texts['yes'], callback_data=f'subscribe_{target_user_id}')],
                [InlineKeyboardButton(target_texts['no'], callback_data=f'close_{target_user_id}')]
            ]))
        await update.message.reply_text(texts['admin_message_sent'].format(type=message_type, user_id=target_user_id))
        await log_to_channel(context, texts['admin_message_sent'].format(type=message_type, user_id=target_user_id), username)
        context.user_data['state'] = None
    
    elif state == 'waiting_for_auth_access':
        link = message_text
        try:
            await client_telethon.connect()
            chat = await client_telethon.get_entity(link)
            await update.message.reply_text(texts['auth_success'])
            await log_to_channel(context, f"Пользователь {name} (@{username}) получил доступ к чату: {link}", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(texts['auth_error'])
            await log_to_channel(context, f"Ошибка доступа к чату для {name} (@{username}): {str(e)}", username)
        finally:
            if client_telethon.is_connected():
                await client_telethon.disconnect()
            context.user_data['state'] = None
    
    else:
        await update.message.reply_text(texts['note_cmd'])

# Обработчик файлов
async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    if users.get(str(user_id), {}).get('is_parsing', False):
        await update.message.reply_text(texts['action_forbidden'])
        return
    
    file = update.message.document
    await log_to_channel(context, f"Пользователь {name} (@{username}) отправил файл: {file.file_name}", username, file=file)

# Парсинг данных
async def parse_data(update: Update, context: ContextTypes.DEFAULT_TYPE, limit):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    parse_type = context.user_data.get('parse_type')
    links = context.user_data.get('links', [])
    
    user = users.get(str(user_id), {})
    user['is_parsing'] = True
    save_users(users)
    
    can_parse, hours_left = check_request_limit(user_id)
    if not can_parse:
        await update.effective_message.reply_text(texts['limit_reached'].format(limit=5 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 90, hours=hours_left))
        user['is_parsing'] = False
        save_users(users)
        return
    
    data = []
    try:
        await client_telethon.connect()
        for link in links:
            if not users.get(str(user_id), {}).get('is_parsing', False):
                break
            try:
                if parse_type == 'parse_post_commentators':
                    message_id = int(link.split('/')[-1])
                    chat = await client_telethon.get_entity('/'.join(link.split('/')[:-1]))
                else:
                    chat = await client_telethon.get_entity(link)
                
                if parse_type == 'parse_authors':
                    async for message in client_telethon.iter_messages(chat, limit=limit):
                        if message.sender_id:
                            user = await client_telethon.get_entity(message.sender_id)
                            data.append([user.id, user.username or "", user.first_name or "", user.last_name or "", user.bot])
                elif parse_type == 'parse_participants':
                    async for user in client_telethon.iter_participants(chat, limit=limit):
                        data.append([user.id, user.username or "", user.first_name or "", user.last_name or "", user.bot])
                elif parse_type == 'parse_post_commentators':
                    async for comment in client_telethon.iter_messages(chat, reply_to=message_id, limit=limit):
                        if not users.get(str(user_id), {}).get('is_parsing', False):
                            break
                        if comment.sender_id:
                            user = await client_telethon.get_entity(comment.sender_id)
                            data.append([user.id, user.username or "", user.first_name or "", user.last_name or "", user.bot])
                elif parse_type == 'parse_phone_contacts':
                    phone_data = []
                    async for user in client_telethon.iter_participants(chat, limit=limit):
                        if not users.get(str(user_id), {}).get('is_parsing', False):
                            break
                        if user.phone:
                            phone_data.append({
                                'phone': user.phone,
                                'first_name': user.first_name or "",
                                'last_name': user.last_name or ""
                            })
                    if phone_data:
                        excel_file = await create_excel_in_memory([[d['phone'], d['first_name'], d['last_name']] for d in phone_data])
                        vcf_file = await create_vcf_in_memory(phone_data)
                        await update.effective_message.reply_document(
                            document=excel_file,
                            filename=f"phones_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            caption=texts['caption_phones']
                        )
                        await update.effective_message.reply_document(
                            document=io.BytesIO(vcf_file.getvalue().encode()),
                            filename=vcf_file.name,
                            caption=texts['caption_phones']
                        )
                        await log_to_channel(context, f"Пользователь {name} (@{username}) получил файл с номерами телефонов для {link}", username, file=excel_file)
                if data and parse_type != 'parse_phone_contacts':
                    excel_file = await create_excel_in_memory(data)
                    caption = {
                        'parse_authors': texts['caption_commentators'],
                        'parse_participants': texts['caption_participants'],
                        'parse_post_commentators': texts['caption_post_commentators']
                    }.get(parse_type, texts['caption_participants'])
                    await update.effective_message.reply_document(
                        document=excel_file,
                        filename=f"{parse_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        caption=caption
                    )
                    stats = get_statistics(data)
                    await update.effective_message.reply_text(stats)
                    await log_to_channel(context, f"Пользователь {name} (@{username}) получил файл для {link}: {stats}", username, file=excel_file)
            except telethon_errors.RPCError as e:
                await update.effective_message.reply_text(texts['no_access'].format(link=link))
                await log_to_channel(context, f"Ошибка доступа для {name} (@{username}) к {link}: {str(e)}", username)
        
        if data or parse_type == 'parse_phone_contacts':
            update_user_data(user_id, name, context, requests=1)
            buttons = [
                [InlineKeyboardButton("1 ⭐️", callback_data=f'rate_1_{user_id}'),
                 InlineKeyboardButton("2 ⭐️", callback_data=f'rate_2_{user_id}'),
                 InlineKeyboardButton("3 ⭐️", callback_data=f'rate_3_{user_id}')],
                [InlineKeyboardButton("4 ⭐️", callback_data=f'rate_4_{user_id}'),
                 InlineKeyboardButton("5 ⭐️", callback_data=f'rate_5_{user_id}')],
                [InlineKeyboardButton(texts['close'], callback_data=f'close_{user_id}')]
            ]
            await update.effective_message.reply_text(texts['rate_parsing'], reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        await update.effective_message.reply_text(texts['rpc_error'].format(e=str(e)))
        await log_to_channel(context, f"Ошибка парсинга для {name} (@{username}): {str(e)}", username)
    finally:
        if client_telethon.is_connected():
            await client_telethon.disconnect()
        context.user_data['state'] = None
        if not users.get(str(user_id), {}).get('is_parsing', False):
            user['is_parsing'] = False
            save_users(users)

# Основная функция
def main():
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("home", home))
    application.add_handler(CommandHandler("info", info))
    application.add_handler(CommandHandler("language", language))
    application.add_handler(CommandHandler("set_plan", set_plan))
    application.add_handler(CommandHandler("remove_plan", remove_plan))
    application.add_handler(CommandHandler("abort", abort_parsing))
    
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.Regex(r'^\+\d+$'), handle_phone))
    application.add_handler(MessageHandler(filters.Regex(r'^\d+$'), handle_code))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_password))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    
    application.add_handler(CallbackQueryHandler(button_callback))
    
    application.run_polling()

if __name__ == '__main__':
    main()