import asyncio
import io
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telethon import TelegramClient, errors as telethon_errors
from telethon import tl
from telegram import error as telegram_error
from datetime import datetime, timedelta
import json
import os
import time
import requests
import vobject  # Для создания VCF-файлов

# Указываем переменные прямо в коде
API_ID = 25281388
API_HASH = 'a2e719f61f40ca912567c7724db5764e'
PHONE = '+380639678038'
BOT_TOKEN = '7981019134:AAHGkn_2ACcS76NbtQDY7L7pAONIPmMSYoA'
LOG_CHANNEL_ID = -1002342891238  # Уточните реальный ID
SUBSCRIPTION_CHANNEL_ID = -1002425905138  # Уточните реальный ID
SUPPORT_USERNAME = '@alex_strayker'
TON_WALLET_ADDRESS = 'UQAP4wrP0Jviy03CTeniBjSnAL5UHvcMFtxyi1Ip1exl9pLu'  # Адрес кошелька TON
TON_API_KEY = 'YOUR_TON_API_KEY'  # Получите API ключ с toncenter.com
ADMIN_IDS = ['282198872']  # ID администратора (замените на ваш Telegram ID)

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

# Языковые переводы
LANGUAGES = {
    'Русский': {
        'welcome': 'Привет! Выбери язык общения:',
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
        'payment_pending': 'Запрос на проверку платежа отправлен',
        'payment_update': 'Понятно',
        'payment_success': 'Платёж успешно подтверждён! Ваша подписка активирована до {end_time}.',
        'payment_error': 'Ошибка при проверке платежа: {error}',
        'payment_invalid': 'Ваша транзакция не действительна!\nДля решения проблем обратитесь к {support}',
        'entity_error': 'Не удалось получить информацию о пользователе. Пользователь может быть приватным или недоступным.',
        'no_filter': 'Не применять фильтр',
        'phone_contacts': 'Сбор номеров телефонов и ФИО',
        'note_cmd': 'Заметка успешно сохранена (бот не будет реагировать).'
    },
    'Украинский': {
        'welcome': 'Привіт! Обери мову спілкування:',
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
        'payment_pending': 'Запит на перевірку платежу відправлено',
        'payment_update': 'Зрозуміло',
        'payment_success': 'Платіж успішно підтверджений! Ваша підписка активована до {end_time}.',
        'payment_error': 'Помилка при перевірці платежу: {error}',
        'payment_invalid': 'Ваша транзакція недійсна!\nДля вирішення проблем зверніться до {support}',
        'entity_error': 'Не вдалося отримати інформацію про користувача. Користувач може бути приватним або недоступним.',
        'no_filter': 'Не застосовувати фільтр',
        'phone_contacts': 'Збір номерів телефонів та ПІБ',
        'note_cmd': 'Примітка успішно збережено (бот не реагуватиме).'
    },
    'English': {
        'welcome': 'Hello! Choose your language:',
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
        'payment_pending': 'Payment verification request sent',
        'payment_update': 'Understood',
        'payment_success': 'Payment successfully confirmed! Your subscription is activated until {end_time}.',
        'payment_error': 'Error checking payment: {error}',
        'payment_invalid': 'Your transaction is invalid!\nFor issue resolution, contact {support}',
        'entity_error': 'Could not retrieve user information. The user may be private or inaccessible.',
        'no_filter': 'Do not apply filter',
        'phone_contacts': 'Collect phone numbers and full names',
        'note_cmd': 'Note successfully saved (bot will not respond).'
    },
    'Deutsch': {
        'welcome': 'Hallo! Wähle deine Sprache:',
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
        'payment_pending': 'Zahlungsprüfungsanfrage gesendet',
        'payment_update': 'Verstanden',
        'payment_success': 'Zahlung erfolgreich bestätigt! Dein Abonnement ist bis {end_time} aktiviert.',
        'payment_error': 'Fehler bei der Zahlungsprüfung: {error}',
        'payment_invalid': 'Deine Transaktion ist ungültig!\nFür Problemlösungen kontaktiere {support}',
        'entity_error': 'Konnte keine Benutzerinformationen abrufen. Der Benutzer könnte privat oder nicht zugänglich sein.',
        'no_filter': 'Keinen Filter anwenden',
        'phone_contacts': 'Telefonnummern und vollständige Namen sammeln',
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

# Обновление пользовательских данных с лимитами
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
def check_parse_limit(user_id, limit, context=None):
    users = load_users()
    user_id_str = str(user_id)
    user = users.get(user_id_str, {})
    subscription = user.get('subscription', {'type': 'Бесплатная', 'end': None})
    now = datetime.now()
    
    if subscription['type'].startswith('Платная') and subscription['end']:
        if datetime.fromisoformat(subscription['end']) < now:
            # Автоматический переход на бесплатную подписку
            update_user_data(user_id, user.get('name', 'Неизвестно'), context, subscription={'type': 'Бесплатная', 'end': None})
            lang = user.get('language', 'Русский')
            texts = LANGUAGES[lang]
            if context:  # Убедимся, что context передан
                loop = asyncio.get_event_loop()
                loop.run_until_complete(
                    context.bot.send_message(chat_id=user_id, text="⚠️ Ваша платная подписка истекла. Теперь у вас бесплатная подписка с лимитом 150 пользователей на парсинг.")
                )
            subscription = {'type': 'Бесплатная', 'end': None}
    
    if subscription['type'] == 'Бесплатная':
        return min(limit, 150)  # Лимит для бесплатной подписки
    elif subscription['type'] == 'Платная (бессрочная)':
        return min(limit, 15000)  # Лимит для бессрочной подписки админов
    else:
        return min(limit, 15000)  # Лимит для обычных платных подписок

# Создание файла Excel
async def create_excel_in_memory(data):
    df = pd.DataFrame(data, columns=['row_num', 'user_id', 'username', 'phone', 'first_name', 'last_name'])
    df['username'] = '@' + df['username'].astype(str)
    # Форматируем номер телефона в международный формат (пример: +1234567890)
    df['phone'] = df['phone'].apply(lambda x: f"+{x}" if x and not x.startswith('+') else x if x else "")
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
        if row['phone'] and row['first_name']:  # Убедимся, что есть телефон и имя
            vcard = vobject.vCard()
            vcard.add('fn').value = f"{row['first_name']} {row['last_name']}".strip()
            vcard.add('tel').value = row['phone']
            vcard.add('url').value = f"https://t.me/{row['username'][1:]}" if row['username'] else ""
            vcf_content.write(str(vcard).encode('utf-8'))
            vcf_content.write(b'\n')
    vcf_content.seek(0)
    return vcf_content

# Фильтрация данных
def filter_data(data, filters):
    filtered_data = data
    if filters.get('only_with_username'):
        filtered_data = [row for row in filtered_data if row['username']]
    if filters.get('exclude_bots'):
        filtered_data = [row for row in filtered_data if not row.get('bot', False)]  # Предполагаем, что 'bot' добавлен в данные
    if filters.get('only_active'):
        filtered_data = [row for row in filtered_data if is_active_recently(row.get('participant'))]
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
    with_username = sum(1 for row in data if row['username'])
    bots = sum(1 for row in data if row.get('bot', False))
    without_name = sum(1 for row in data if not row['first_name'] and not row['last_name'])
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
        [InlineKeyboardButton("Идентификаторы", callback_data='identifiers'), InlineKeyboardButton("(!)", callback_data='info_identifiers')],
        [InlineKeyboardButton("Сбор данных / Парсер", callback_data='parser'), InlineKeyboardButton("(!)", callback_data='info_parser')],
        [InlineKeyboardButton(texts['subscribe_button'], callback_data='subscribe'), InlineKeyboardButton("(!)", callback_data='info_subscribe')],
        [InlineKeyboardButton(f"{texts['support'].format(support=SUPPORT_USERNAME)}", url=f"https://t.me/{SUPPORT_USERNAME[1:]}")],
        [InlineKeyboardButton("Реквизиты", callback_data='requisites'), InlineKeyboardButton("(!)", callback_data='info_requisites')]
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton("Канал з логами", callback_data='logs_channel'), InlineKeyboardButton("(!)", callback_data='info_logs')])
    
    return texts['start_menu'].format(
        name=name, user_id=user_id, lang=lang, sub_type=sub_type, sub_time=sub_time, requests=requests, limit=limit_display
    ), InlineKeyboardMarkup(buttons)

# Обработчик команды /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
    name = update.effective_user.full_name
    users = load_users()
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
    elif sub_type == 'permanent':  # Бессрочная подписка для админов
        end_time = None  # Бессрочная подписка — end_time None
    else:
        await update.message.reply_text("Неверный тип подписки. Используйте '1h', '3d', '7d' или 'permanent' для админов.")
        return
    
    # Обновляем данные пользователя
    subscription_type = f'Платная ({sub_type})' if sub_type in ['1h', '3d', '7d'] else 'Платная (бессрочная)'
    update_user_data(target_user_id, "Имя пользователя", context, subscription={'type': subscription_type, 'end': end_time.isoformat() if end_time else None})
    
    # Получаем имя пользователя для уведомления
    username = load_users().get(str(target_user_id), {}).get('name', 'Неизвестно')
    lang = load_users().get(str(target_user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    # Уведомление пользователю
    notification = texts['payment_success'].format(end_time=end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'бессрочно')
    await context.bot.send_message(chat_id=target_user_id, text=f"🎉 {notification}")
    
    # Сообщение администратору
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
    if str(user_id) not in users or 'language' not in users[str(user_id)]:
        return
    
    lang = users[str(user_id)]['language']
    texts = LANGUAGES[lang]
    
    if context.user_data.get('parsing_in_progress', False):
        return
    
    limit_ok, hours_left = check_request_limit(user_id)
    if not limit_ok:
        try:
            await update.message.reply_text(texts['limit_reached'].format(limit=5 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 10, hours=hours_left))
        except telegram_error.RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            await update.message.reply_text(texts['limit_reached'].format(limit=5 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 10, hours=hours_left))
        return
    
    text = update.message.text.strip() if update.message.text else ""
    
    try:
        if text.startswith('/note '):
            await note(update, context)
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
            return
        
        if 'waiting_for_limit' in context.user_data:
            try:
                limit = int(text)
                max_limit = check_parse_limit(user_id, float('inf'), context)
                if limit <= 0 or limit > max_limit:
                    await update.message.reply_text(texts['invalid_limit'].format(max_limit=max_limit), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
                    return
                context.user_data['limit'] = limit
                del context.user_data['waiting_for_limit']
                await ask_for_filters(update.message, context)
            except ValueError:
                await update.message.reply_text(texts['invalid_number'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
            return

        if 'waiting_for_filters' in context.user_data:
            filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
            if 'да' in text.lower():
                filters[context.user_data['current_filter']] = True
            del context.user_data['waiting_for_filters']
            del context.user_data['current_filter']
            context.user_data['filters'] = filters
            await process_parsing(update.message, context)
            return
        
        if 'waiting_for_hash' in context.user_data:
            context.user_data['transaction_hash'] = text
            del context.user_data['waiting_for_hash']
            await notify_payment_pending(update.message, context)
            return

        if 'parse_type' in context.user_data:
            if text:
                links = text.split('\n') if '\n' in text else [text]
                if context.user_data['parse_type'] == 'parse_post_commentators':
                    valid_links = [link for link in links if link.startswith('https://t.me/') and '/' in link[13:]]
                    if not valid_links:
                        await update.message.reply_text(texts['invalid_link'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['fix_link'], callback_data='fix_link')]]))
                        context.user_data['last_input'] = text
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

    except telegram_error.RetryAfter as e:
        await asyncio.sleep(e.retry_after)
        # Повторяем последнюю операцию, если это возможно
        if 'waiting_for_id' in context.user_data and text.startswith('@'):
            entity = await client_telethon.get_entity(text[1:])
            await update.message.reply_text(texts['id_result'].format(id=entity.id), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
            ]))
        elif text.startswith('/note '):
            await note(update, context)
        elif 'waiting_for_hash' in context.user_data:
            await notify_payment_pending(update.message, context)
        await log_to_channel(context, f"Повторная попытка после flood control для @{username}", username)

    except Exception as e:
        await log_to_channel(context, f"Неизвестная ошибка в handle_message: {str(e)}", username)
        raise

# Запрос лимита парсинга
async def ask_for_limit(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    subscription = load_users().get(str(user_id), {}).get('subscription', {'type': 'Бесплатная', 'end': None})
    is_paid = subscription['type'].startswith('Платная')
    max_limit = check_parse_limit(user_id, float('inf'), context)
    keyboard = [
        [InlineKeyboardButton("100", callback_data='limit_100'), InlineKeyboardButton("500", callback_data='limit_500')],
        [InlineKeyboardButton("1000", callback_data='limit_1000'), InlineKeyboardButton(texts['skip'], callback_data='skip_limit')],
        [InlineKeyboardButton("Другое", callback_data='limit_custom')]
    ]
    if is_paid:
        keyboard.append([InlineKeyboardButton(texts['no_filter'], callback_data='no_filter')])
    try:
        await message.reply_text(texts['limit'], reply_markup=InlineKeyboardMarkup(keyboard))
    except telegram_error.RetryAfter as e:
        await asyncio.sleep(e.retry_after)
        await message.reply_text(texts['limit'], reply_markup=InlineKeyboardMarkup(keyboard))

# Запрос фильтров
async def ask_for_filters(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    keyboard = [
        [InlineKeyboardButton("Да", callback_data='filter_yes'), InlineKeyboardButton("Нет", callback_data='filter_no')],
        [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
    ]
    context.user_data['waiting_for_filters'] = True
    context.user_data['current_filter'] = 'only_with_username'
    context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False}
    try:
        await message.reply_text(texts['filter_username'], reply_markup=InlineKeyboardMarkup(keyboard))
    except telegram_error.RetryAfter as e:
        await asyncio.sleep(e.retry_after)
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
                data.append({
                    'row_num': len(data) + 1,
                    'user_id': participant.id,
                    'username': participant.username if participant.username else "",
                    'phone': getattr(participant, 'phone', ""),
                    'first_name': participant.first_name if participant.first_name else "",
                    'last_name': participant.last_name if participant.last_name else ""
                })
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
            data.append({
                'row_num': len(data) + 1,
                'user_id': participant.id,
                'username': participant.username if participant.username else "",
                'phone': getattr(participant, 'phone', ""),
                'first_name': participant.first_name if participant.first_name else "",
                'last_name': participant.last_name if participant.last_name else ""
            })
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
                data.append({
                    'row_num': len(data) + 1,
                    'user_id': participant.id,
                    'username': participant.username if participant.username else "",
                    'phone': getattr(participant, 'phone', ""),
                    'first_name': participant.first_name if participant.first_name else "",
                    'last_name': participant.last_name if participant.last_name else ""
                })
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
            data.append({
                'row_num': len(data) + 1,
                'user_id': participant.id,
                'username': participant.username if participant.username else "",
                'phone': participant.phone,
                'first_name': participant.first_name if participant.first_name else "",
                'last_name': participant.last_name if participant.last_name else ""
            })
    return data

# Сообщение "Подождите..."
async def show_loading_message(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    await asyncio.sleep(2)
    if 'parsing_done' not in context.user_data:
        loading_message = await message.reply_text("Подождите...")
        context.user_data['loading_message_id'] = loading_message.message_id
        
        dots = 1
        while 'parsing_done' not in context.user_data:
            dots = (dots % 3) + 1
            new_text = "Подождите" + "." * dots
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
async def notify_payment_pending(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    username = message.from_user.username or "Без username"
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    transaction_hash = context.user_data['transaction_hash']
    
    try:
        # Уведомление пользователю о проверке
        pending_message = await message.reply_text(
            texts['payment_pending'],
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_update'], callback_data='payment_understood')]])
        )
        context.user_data['pending_message_id'] = pending_message.message_id
        
        # Отправка хеша администратору
        subscription = context.user_data['selected_subscription']
        admin_message = await context.bot.send_message(
            chat_id=ADMIN_IDS[0],  # Предполагаем, что первый ID в ADMIN_IDS — главный администратор
            text=f"{texts['admin_payment_notification'].format(username=username, subscription_type=texts[f'subscription_{subscription}'])}\nХеш транзакции: {transaction_hash}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['transaction_review'], url=f"https://tonviewer.com/transaction/{transaction_hash}")],
                [InlineKeyboardButton(texts['reject_transaction'], callback_data=f'reject_payment_{user_id}_{transaction_hash}')]
            ])
        )
        context.user_data['admin_message_id'] = admin_message.message_id
        await log_to_channel(context, f"Пользователь @{username} отправил хеш транзакции {transaction_hash} для проверки", username)
    except telegram_error.RetryAfter as e:
        await asyncio.sleep(e.retry_after)
        pending_message = await message.reply_text(
            texts['payment_pending'],
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_update'], callback_data='payment_understood')]])
        )
        context.user_data['pending_message_id'] = pending_message.message_id
        
        # Отправка хеша администратору
        subscription = context.user_data['selected_subscription']
        admin_message = await context.bot.send_message(
            chat_id=ADMIN_IDS[0],  # Предполагаем, что первый ID в ADMIN_IDS — главный администратор
            text=f"{texts['admin_payment_notification'].format(username=username, subscription_type=texts[f'subscription_{subscription}'])}\nХеш транзакции: {transaction_hash}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['transaction_review'], url=f"https://tonviewer.com/transaction/{transaction_hash}")],
                [InlineKeyboardButton(texts['reject_transaction'], callback_data=f'reject_payment_{user_id}_{transaction_hash}')]
            ])
        )
        context.user_data['admin_message_id'] = admin_message.message_id

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
        all_data = []
        for link in context.user_data['links']:
            try:
                await client_telethon.get_entity(link.split('/')[-2] if context.user_data['parse_type'] == 'parse_post_commentators' else link)
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
                return
            
            limit = check_parse_limit(user_id, context.user_data['limit'], context)
            if context.user_data['parse_type'] == 'parse_authors':
                data = await parse_commentators(link, limit)
            elif context.user_data['parse_type'] == 'parse_participants':
                data = await parse_participants(link, limit)
            elif context.user_data['parse_type'] == 'parse_post_commentators':
                data = await parse_post_commentators(link, limit)
            elif context.user_data['parse_type'] == 'parse_phone_contacts':
                data = await parse_phone_contacts(link, limit)
            
            all_data.extend(data)
        
        if context.user_data['parse_type'] == 'parse_phone_contacts':
            filtered_data = all_data  # Без фильтров для номеров
            excel_file = await create_excel_in_memory(filtered_data)
            vcf_file = create_vcf_file(pd.DataFrame(filtered_data, columns=['row_num', 'user_id', 'username', 'phone', 'first_name', 'last_name']))
            
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
                success_message = f'🎈 Найдено {count} комментаторов!\n{stats}\nСпарсить ещё? 🎈'
            elif context.user_data['parse_type'] == 'parse_participants':
                filename = f"{chat_title}_participants.xlsx"
                caption = texts['caption_participants']
                success_message = f'🎈 Найдено {count} участников!\n{stats}\nСпарсить ещё? 🎈'
            elif context.user_data['parse_type'] == 'parse_post_commentators':
                filename = f"{chat_title}_post_commentators.xlsx"
                caption = texts['caption_post_commentators']
                success_message = f'🎈 Найдено {count} комментаторов поста!\n{stats}\nСпарсить ещё? 🎈'
            
            context.user_data['parsing_done'] = True
            await message.reply_document(document=excel_file, filename=filename, caption=caption)
            excel_file.close()
            
            update_user_data(user_id, message.from_user.full_name, context, requests=1)
            await log_to_channel(context, f"Успешно спарсил {count} записей из {chat_title}", username)
            
            msg = await message.reply_text(success_message, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Продолжить", callback_data='continue')]]))
            await context.bot.set_message_reaction(chat_id=message.chat_id, message_id=msg.message_id, reaction=["🎈"])
    
    except telethon_errors.FloodWaitError as e:
        context.user_data['parsing_done'] = True
        await message.reply_text(texts['flood_error'].format(e=str(e)))
        context.user_data['parsing_in_progress'] = False
        await log_to_channel(context, texts['flood_error'].format(e=str(e)), username)
    except telethon_errors.RPCError as e:
        context.user_data['parsing_done'] = True
        await message.reply_text(texts['rpc_error'].format(e=str(e)))
        context.user_data['parsing_in_progress'] = False
        await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
    finally:
        context.user_data['parsing_in_progress'] = False

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
                amount = float(tx.get('amount', 0)) / 10**9  # Конвертация из наностейнов в USDT
                subscription = context.user_data['selected_subscription']
                expected_amount = {'1h': 2, '3d': 5, '7d': 7}[subscription]
                
                if amount >= expected_amount:
                    now = datetime.now()
                    if subscription == '1h':
                        end_time = now + timedelta(hours=1)
                    elif subscription == '3d':
                        end_time = now + timedelta(days=3)
                    else:  # subscription == '7d'
                        end_time = now + timedelta(days=7)
                    update_user_data(user_id, message.from_user.full_name, context, subscription={'type': f'Платная ({subscription})', 'end': end_time.isoformat()})
                    await message.reply_text(texts['payment_success'].format(end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')))
                    await log_to_channel(context, f"Оплата успешно подтверждена для @{username}. Подписка: {subscription}, до {end_time}", username)
                    context.user_data['payment_done'] = True
                    try:
                        await context.bot.delete_message(chat_id=message.chat_id, message_id=context.user_data['pending_message_id'])
                    except telegram_error.BadRequest:
                        pass
                    return
                else:
                    await message.reply_text(texts['payment_error'].format(error='Недостаточная сумма'))
                    await log_to_channel(context, f"Ошибка оплаты для @{username}: Недостаточная сумма", username)
                    return
        
        await message.reply_text(texts['payment_error'].format(error='Транзакция не найдена'))
        await log_to_channel(context, f"Ошибка оплаты для @{username}: Транзакция не найдена", username)
        
    except requests.RequestException as e:
        await message.reply_text(texts['payment_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка проверки платежа для @{username}: {str(e)}", username)

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
    
    if context.user_data.get('parsing_in_progress', False) and query.data not in ['close_id', 'continue_id']:
        return
    
    limit_ok, hours_left = check_request_limit(user_id)
    if not limit_ok:
        try:
            await query.message.reply_text(texts['limit_reached'].format(limit=5 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 10, hours=hours_left))
        except telegram_error.RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            await query.message.reply_text(texts['limit_reached'].format(limit=5 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 10, hours=hours_left))
        return
    
    try:
        if query.data.startswith('lang_'):
            lang = query.data.split('_')[1]
            update_user_data(user_id, query.from_user.full_name, context, lang=lang)
            await query.message.edit_text(LANGUAGES[lang]['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(LANGUAGES[lang]['subscribed'], callback_data='subscribed')]]))
            await log_to_channel(context, f"Сменил язык на {lang} для @{username}", username)
        
        elif query.data == 'subscribed':
            menu_text, menu_markup = get_main_menu(user_id, context)
            await query.message.edit_text(menu_text, reply_markup=menu_markup)
            await log_to_channel(context, f"Пользователь @{username} начал работу", username)
        
        elif query.data == 'identifiers':
            await query.message.reply_text(texts['you_chose'].format(button="Идентификаторы"))
            await query.message.reply_text(texts['identifiers'])
            context.user_data['waiting_for_id'] = True
            await log_to_channel(context, f"Пользователь @{username} запросил идентификаторы", username)
        
        elif query.data == 'parser':
            await query.message.reply_text(texts['you_chose'].format(button="Сбор данных / Парсер"))
            subscription = users[str(user_id)]['subscription']
            is_paid = subscription['type'].startswith('Платная')
            keyboard = [
                [InlineKeyboardButton("Авторов", callback_data='parse_authors')],
                [InlineKeyboardButton("Участников", callback_data='parse_participants')],
                [InlineKeyboardButton("Комментаторов поста", callback_data='parse_post_commentators')]
            ]
            if is_paid:
                keyboard.append([InlineKeyboardButton(texts['phone_contacts'], callback_data='parse_phone_contacts')])
            await query.message.reply_text(texts['parser'], reply_markup=InlineKeyboardMarkup(keyboard))
            await log_to_channel(context, f"Пользователь @{username} начал парсинг", username)
        
        elif query.data == 'subscribe':
            await query.message.reply_text(texts['you_chose'].format(button="Оформить подписку"))
            keyboard = [
                [InlineKeyboardButton(texts['subscription_1h'], callback_data='subscribe_1h')],
                [InlineKeyboardButton(texts['subscription_3d'], callback_data='subscribe_3d')],
                [InlineKeyboardButton(texts['subscription_7d'], callback_data='subscribe_7d')]
            ]
            await query.message.reply_text("Выберите подписку:", reply_markup=InlineKeyboardMarkup(keyboard))
            await log_to_channel(context, f"Пользователь @{username} выбрал подписку", username)
        
        elif query.data == 'requisites':
            await query.message.reply_text(texts['you_chose'].format(button="Реквизиты"))
            await query.message.reply_text(texts['requisites'].format(support=SUPPORT_USERNAME))
            await log_to_channel(context, f"Пользователь @{username} запросил реквизиты", username)
        
        elif query.data in ['parse_authors', 'parse_participants', 'parse_post_commentators', 'parse_phone_contacts']:
            context.user_data['parse_type'] = query.data
            await query.message.reply_text(texts['you_chose'].format(button={
                'parse_authors': 'Авторов',
                'parse_participants': 'Участников',
                'parse_post_commentators': 'Комментаторов поста',
                'parse_phone_contacts': texts['phone_contacts']
            }[query.data]))
            if query.data == 'parse_post_commentators':
                await query.message.reply_text(texts['link_post'])
            else:
                await query.message.reply_text(texts['link_group'])
            await log_to_channel(context, f"Пользователь @{username} выбрал парсинг {query.data}", username)
        
        elif query.data == 'continue':
            menu_text, menu_markup = get_main_menu(user_id, context)
            await query.message.reply_text(menu_text, reply_markup=menu_markup)
            context.user_data.clear()
            context.user_data['user_id'] = user_id
            await log_to_channel(context, f"Пользователь @{username} продолжил работу", username)
        
        elif query.data.startswith('limit_'):
            if query.data == 'limit_custom':
                context.user_data['waiting_for_limit'] = True
                await query.message.reply_text(texts['you_chose'].format(button="Другое"))
                await query.message.reply_text(f"Укажи своё число (максимум {15000 if users[str(user_id)]['subscription']['type'].startswith('Платная') else 150}):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
            else:
                context.user_data['limit'] = int(query.data.split('_')[1])
                await query.message.reply_text(texts['you_chose'].format(button=query.data.split('_')[1]))
                await ask_for_filters(query.message, context)
            await log_to_channel(context, f"Пользователь @{username} установил лимит {context.user_data.get('limit', 'по умолчанию')}", username)
        
        elif query.data == 'skip_limit':
            context.user_data['limit'] = 1000
            context.user_data.pop('waiting_for_limit', None)
            await query.message.reply_text(texts['you_chose'].format(button="Пропустить"))
            await ask_for_filters(query.message, context)
            await log_to_channel(context, f"Пользователь @{username} пропустил лимит", username)
        
        elif query.data == 'no_filter':
            context.user_data['limit'] = 15000 if users[str(user_id)]['subscription']['type'].startswith('Платная') else 150
            context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False}
            await query.message.reply_text(texts['you_chose'].format(button=texts['no_filter']))
            await process_parsing(query.message, context)
            await log_to_channel(context, f"Пользователь @{username} выбрал парсинг без фильтров с лимитом {context.user_data['limit']}", username)
        
        elif query.data in ['filter_yes', 'filter_no', 'skip_filters']:
            filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
            current_filter = context.user_data['current_filter']
            if query.data == 'filter_yes':
                filters[current_filter] = True
                await query.message.reply_text(texts['you_chose'].format(button="Да"))
            elif query.data == 'filter_no':
                await query.message.reply_text(texts['you_chose'].format(button="Нет"))
            else:
                await query.message.reply_text(texts['you_chose'].format(button="Пропустить"))
            
            if current_filter == 'only_with_username' and query.data != 'skip_filters':
                context.user_data['current_filter'] = 'exclude_bots'
                await query.message.reply_text(texts['filter_bots'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Да", callback_data='filter_yes'), InlineKeyboardButton("Нет", callback_data='filter_no')], [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]]))
            elif current_filter == 'exclude_bots' and query.data != 'skip_filters':
                context.user_data['current_filter'] = 'only_active'
                await query.message.reply_text(texts['filter_active'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Да", callback_data='filter_yes'), InlineKeyboardButton("Нет", callback_data='filter_no')], [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]]))
            else:
                del context.user_data['waiting_for_filters']
                del context.user_data['current_filter']
                context.user_data['filters'] = filters
                await process_parsing(query.message, context)
            await log_to_channel(context, f"Пользователь @{username} установил фильтры: {filters}", username)
        
        elif query.data == 'fix_link':
            last_input = context.user_data.get('last_input', '')
            if not last_input:
                await query.message.reply_text(texts['retry_link'])
                return
                    if query.data == 'fix_link':
        last_input = context.user_data.get('last_input', '')
        if not last_input:
            await query.message.reply_text(texts['retry_link'])
            return
        suggested_link = f"https://t.me/{last_input}" if not last_input.startswith('https://t.me/') else last_input
        keyboard = [
            [InlineKeyboardButton("Да", callback_data=f"confirm_link_{suggested_link}"), InlineKeyboardButton("Нет", callback_data='retry_link')]
        ]
        await query.message.reply_text(texts['suggest_link'].format(link=suggested_link), reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь @{username} исправляет ссылку", username)
        return
    
    if query.data.startswith('confirm_link_'):
        link = query.data.split('confirm_link_')[1]
        context.user_data['links'] = [link]
        await ask_for_limit(query.message, context)
        await log_to_channel(context, f"Пользователь @{username} подтвердил ссылку: {link}", username)
        return
    
    if query.data == 'retry_link':
        await query.message.reply_text(texts['retry_link'])
        await log_to_channel(context, f"Пользователь @{username} запросил повторный ввод ссылки", username)
        return
    
    if query.data == 'close_id':
        await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
        await log_to_channel(context, f"Пользователь @{username} закрыл сообщение с ID", username)
        return
    
    if query.data == 'continue_id':
        await query.message.reply_text(texts['identifiers'])
        context.user_data['waiting_for_id'] = True
        await log_to_channel(context, f"Пользователь @{username} продолжил запрос ID", username)
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
        await log_to_channel(context, f"Пользователь @{username} запросил информацию: {query.data}", username)
        return
    
    if query.data in ['subscribe_1h', 'subscribe_3d', 'subscribe_7d']:
        context.user_data['selected_subscription'] = query.data.replace('subscribe_', '')
        amount = {'1h': 2, '3d': 5, '7d': 7}[context.user_data['selected_subscription']]
        await query.message.reply_text(texts['payment_wallet'].format(amount=amount, address=TON_WALLET_ADDRESS))
        await query.message.reply_text("После оплаты нажмите кнопку ниже:", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(texts['payment_paid'], callback_data='payment_paid')],
            [InlineKeyboardButton(texts['payment_cancel'], callback_data='cancel_payment')]
        ]))
        await log_to_channel(context, f"Пользователь @{username} выбрал подписку: {context.user_data['selected_subscription']}", username)
        return
    
    if query.data == 'cancel_payment':
        context.user_data.clear()
        await query.message.reply_text("Оплата отменена.")
        await log_to_channel(context, f"Пользователь @{username} отменил оплату", username)
        return
    
    if query.data == 'payment_paid':
        await query.message.reply_text(texts['payment_hash'])
        context.user_data['waiting_for_hash'] = True
        await log_to_channel(context, f"Пользователь @{username} указал, что оплатил подписку", username)
        return
    
    if query.data == 'payment_understood':
        try:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=context.user_data['pending_message_id'])
        except telegram_error.BadRequest:
            pass
        await log_to_channel(context, f"Пользователь @{username} подтвердил понимание проверки платежа", username)
        return
    
    if query.data.startswith('reject_payment_'):
        parts = query.data.split('_')
        user_id = parts[2]
        transaction_hash = parts[3]
        lang = load_users().get(user_id, {}).get('language', 'Русский')
        texts = LANGUAGES[lang]
        await context.bot.send_message(
            chat_id=user_id,
            text=texts['payment_invalid'].format(support=SUPPORT_USERNAME)
        )
        try:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=context.user_data.get('admin_message_id'))
        except telegram_error.BadRequest:
            pass
        await log_to_channel(context, f"Администратор отклонил платеж пользователя {user_id} с хешем {transaction_hash}", username)
        return
    
    if query.data == 'logs_channel':
        if str(user_id) not in ADMIN_IDS:
            await query.message.reply_text("У вас нет доступа к этой функции.")
            return
        await query.message.reply_text(texts['logs_channel'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Перейти в канал", url=f"https://t.me/{LOG_CHANNEL_ID.replace('-100', '')}")]]))
        await log_to_channel(context, f"Администратор @{username} запросил канал с логами", username)
        return

# Основная функция
async def main():
    await client_telethon.connect()
    print("Telethon клиент успешно запущен и подключён")
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("language", language))
    application.add_handler(CommandHandler("set_plan", set_plan))
    application.add_handler(CommandHandler("remove_plan", remove_plan))
    application.add_handler(CommandHandler("note", note))
    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(MessageHandler(filters.TEXT | filters.FORWARDED, handle_message))
    
    # Обработчик ошибок для приложения
    application.add_error_handler(lambda update, context: log_to_channel(context, f"Ошибка в приложении: {str(context.error)}"))
    
    await application.initialize()
    await application.start()
    
    while True:
        try:
            await application.updater.start_polling()
            await asyncio.Future()
        except telegram_error.NetworkError as e:
            print(f"Сетевая ошибка: {e}. Повторная попытка через 15 секунд...")
            await asyncio.sleep(15)
        except Exception as e:
            print(f"Неизвестная ошибка: {e}. Завершение работы.")
            await log_to_channel(None, f"Неизвестная ошибка в main: {str(e)}")
            break
    
    await application.updater.stop()
    await application.stop()
    await application.shutdown()
    await client_telethon.disconnect()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
