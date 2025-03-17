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
import firebase_admin
from firebase_admin import credentials, db
import vobject

# Инициализация Firebase
cred = credentials.Certificate({
    "type": "service_account",
    "project_id": "tgparser-f857c",
    "private_key_id": "YOUR_PRIVATE_KEY_ID",
    "private_key": "-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY\n-----END PRIVATE KEY-----\n",
    "client_email": "firebase-adminsdk@tgparser-f857c.iam.gserviceaccount.com",
    "client_id": "YOUR_CLIENT_ID",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk%40tgparser-f857c.iam.gserviceaccount.com"
})
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://tgparser-f857c-default-rtdb.firebaseio.com'
})

# Переменные
API_ID = int(os.environ.get('API_ID', 25281388))
API_HASH = os.environ.get('API_HASH', 'a2e719f61f40ca912567c7724db5764e')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '7981019134:AAEARQ__XD1Ki60avGlWL1wDKDVcUKh6ny8')
LOG_CHANNEL_ID = -1002342891238
SUPPORT_USERNAME = '@alex_strayker'
TON_WALLET_ADDRESS = 'UQAP4wrP0Jviy03CTeniBjSnAL5UHvcMFtxyi1Ip1exl9pLu'
TON_API_KEY = os.environ.get('TON_API_KEY', 'YOUR_TON_API_KEY')
ADMIN_IDS = ['282198872']
SESSION_FILE = 'shared_session.session'

client_telethon = TelegramClient(SESSION_FILE, API_ID, API_HASH)

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

# Firebase функции
def save_subscription_to_firebase(user_id, subscription):
    ref = db.reference(f'users/{user_id}/subscription')
    ref.set(subscription)

def get_subscription_from_firebase(user_id):
    ref = db.reference(f'users/{user_id}/subscription')
    return ref.get() or {'type': 'Бесплатная', 'end': None}

def check_subscription_expiry(user_id, context):
    subscription = get_subscription_from_firebase(user_id)
    now = datetime.now()
    if subscription['end'] and datetime.fromisoformat(subscription['end']) < now:
        subscription = {'type': 'Бесплатная', 'end': None}
        save_subscription_to_firebase(user_id, subscription)
        lang = load_users().get(str(user_id), {}).get('language', 'Русский')
        texts = LANGUAGES[lang]
        asyncio.create_task(context.bot.send_message(
            chat_id=user_id,
            text=texts['subscription_expired']
        ))
    return subscription

# Полные переводы для всех языков
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
        'rpc_error': 'Error: {e}',
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
        'phone_contacts': 'Сбор номеров телефонов',
        'auth_access': 'Авторизация для закрытых чатов',
        'caption_phones': 'Вот ваш файл с номерами телефонов (Excel и VCF).',
        'auth_request': 'Для доступа к закрытым чатам добавьте бота в чат как администратора или отправьте ссылку на закрытый чат.',
        'auth_success': 'Доступ к закрытому чату успешно предоставлен!',
        'auth_error': 'Не удалось получить доступ. Убедитесь, что бот добавлен как администратор или чат публичный.',
        'note_cmd': 'Заметка успешно сохранена (бот не будет реагировать).',
        'info_cmd': 'Информация о боте:\n- Парсинг пользователей из групп и постов\n- Поддержка фильтров\n- Экспорт в Excel',
        'home_cmd': 'Вернуться в главное меню',
        'parsing_checklist': 'Чек-лист парсинга:\n- Исключены "Удалённые аккаунты"\n- Только пользователи с username\n- Количество: {count}',
        'rate_parsing': 'Оцените пожалуйста работу TGParser:\n(1)(2)(3)(4)(5)\nили нажмите "Пропустить"',
        'thanks': 'Спасибо!',
        'subscription_expired': 'Ваша подписка истекла! Теперь у вас бесплатная подписка.'
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
        'phone_contacts': 'Збір номерів телефонів',
        'auth_access': 'Авторизація для закритих чатів',
        'caption_phones': 'Ось ваш файл з номерами телефонів (Excel і VCF).',
        'auth_request': 'Для доступу до закритих чатів додайте бота в чат як адміністратора або надішліть посилання на закритий чат.',
        'auth_success': 'Доступ до закритого чату успішно надано!',
        'auth_error': 'Не вдалося отримати доступ. Переконайтесь, що бот доданий як адміністратор або чат публічний.',
        'note_cmd': 'Примітка успішно збережено (бот не реагуватиме).',
        'info_cmd': 'Інформація про бота:\n- Парсинг користувачів із груп та постів\n- Підтримка фільтрів\n- Експорт в Excel',
        'home_cmd': 'Повернутися до головного меню',
        'parsing_checklist': 'Чек-лист парсингу:\n- Виключено "Видалені акаунти"\n- Тільки користувачі з username\n- Кількість: {count}',
        'rate_parsing': 'Оцініть будь ласка роботу TGParser:\n(1)(2)(3)(4)(5)\nабо натисніть "Пропустити"',
        'thanks': 'Дякую!',
        'subscription_expired': 'Ваша підписка закінчилася! Тепер у вас безкоштовна підписка.'
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
        'phone_contacts': 'Collect phone numbers',
        'auth_access': 'Authorize for private chats',
        'caption_phones': 'Here is your file with phone numbers (Excel and VCF).',
        'auth_request': 'To access private chats, add the bot as an admin or send a link to a private chat.',
        'auth_success': 'Access to the private chat successfully granted!',
        'auth_error': 'Could not gain access. Ensure the bot is added as an admin or the chat is public.',
        'note_cmd': 'Note successfully saved (bot will not respond).',
        'info_cmd': 'Bot info:\n- Parsing users from groups and posts\n- Filter support\n- Export to Excel',
        'home_cmd': 'Return to main menu',
        'parsing_checklist': 'Parsing checklist:\n- Excluded "Deleted accounts"\n- Only users with username\n- Count: {count}',
        'rate_parsing': 'Please rate TGParser:\n(1)(2)(3)(4)(5)\nOr press "Skip"',
        'thanks': 'Thank you!',
        'subscription_expired': 'Your subscription has expired! You now have a free subscription.'
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
        'phone_contacts': 'Telefonnummern sammeln',
        'auth_access': 'Autorisierung für private Chats',
        'caption_phones': 'Hier ist deine Datei mit Telefonnummern (Excel und VCF).',
        'auth_request': 'Um auf private Chats zuzugreifen, füge den Bot als Administrator hinzu oder sende einen Link zu einem privaten Chat.',
        'auth_success': 'Zugang zum privaten Chat erfolgreich gewährt!',
        'auth_error': 'Konnte keinen Zugriff erhalten. Stelle sicher, dass der Bot als Administrator hinzugefügt wurde oder der Chat öffentlich ist.',
        'note_cmd': 'Notiz erfolgreich gespeichert (der Bot wird nicht reagieren).',
        'info_cmd': 'Bot-Info:\n- Parsen von Benutzern aus Gruppen und Beiträgen\n- Filterunterstützung\n- Export nach Excel',
        'home_cmd': 'Zum Hauptmenü zurückkehren',
        'parsing_checklist': 'Parsing-Checkliste:\n- Ausgeschlossene "Gelöschte Konten"\n- Nur Benutzer mit Username\n- Anzahl: {count}',
        'rate_parsing': 'Bitte bewerte TGParser:\n(1)(2)(3)(4)(5)\nOder drücke "Überspringen"',
        'thanks': 'Danke!',
        'subscription_expired': 'Dein Abonnement ist abgelaufen! Du hast jetzt ein kostenloses Abonnement.'
    }
}

# Логирование в канал
async def log_to_channel(context, message, username=None, file=None):
    try:
        user = context.user_data.get('user', {})
        name = user.get('name', username or 'Unknown')
        log_message = f"{name} (@{username}): {message}" if username else f"{message}"
        if file:
            file.seek(0)
            await context.bot.send_document(chat_id=LOG_CHANNEL_ID, document=file, caption=log_message)
        else:
            await context.bot.send_message(chat_id=LOG_CHANNEL_ID, text=log_message)
    except telegram_error.BadRequest as e:
        print(f"Error sending log to channel: {e}")

# Обновление пользовательских данных
def update_user_data(user_id, name, context, lang=None, subscription=None, requests=0):
    users = load_users()
    user_id_str = str(user_id)
    now = datetime.now()
    if user_id_str not in users:
        users[user_id_str] = {
            'name': name,
            'language': lang or 'Русский',
            'requests': 0,
            'daily_requests': {'count': 0, 'last_reset': now.isoformat()}
        }
    user = users[user_id_str]
    if 'daily_requests' not in user:
        user['daily_requests'] = {'count': 0, 'last_reset': now.isoformat()}
    if lang:
        user['language'] = lang
    if subscription:
        save_subscription_to_firebase(user_id, subscription)
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
    if 'daily_requests' not in user:
        user['daily_requests'] = {'count': 0, 'last_reset': now.isoformat()}
        save_users(users)
    daily_requests = user['daily_requests']
    last_reset = datetime.fromisoformat(daily_requests['last_reset'])
    if (now - last_reset).days >= 1:
        daily_requests = {'count': 0, 'last_reset': now.isoformat()}
        users[user_id_str]['daily_requests'] = daily_requests
        save_users(users)
    subscription = check_subscription_expiry(user_id, None)
    max_requests = 5 if subscription['type'] == 'Бесплатная' else 10
    return daily_requests['count'] < max_requests, 24 - (now - last_reset).seconds // 3600

# Проверка лимита парсинга
def check_parse_limit(user_id, limit, parse_type):
    subscription = check_subscription_expiry(user_id, None)
    if subscription['type'] == 'Бесплатная':
        return min(limit, 150)
    elif parse_type == 'parse_authors':
        return min(limit, 5000)
    elif parse_type == 'parse_participants':
        return min(limit, 15000)
    elif parse_type == 'parse_post_commentators':
        return limit
    elif parse_type == 'parse_phone_contacts':
        return min(limit, 15000)
    else:
        return min(limit, 15000)

# Создание файла Excel
async def create_excel_in_memory(data):
    df = pd.DataFrame(data, columns=['ID', 'Username', 'First Name', 'Last Name', 'Country', 'Age', 'User'])
    df['Nickname'] = '@' + df['Username'].astype(str)
    excel_file = io.BytesIO()
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Users')
    excel_file.seek(0)
    return excel_file

# Создание VCF файла
def create_vcf_file(data):
    vcf_content = io.BytesIO()
    for _, row in data.iterrows():
        if row['Phone']:
            vcard = vobject.vCard()
            vcard.add('fn').value = f"{row['First Name']} {row['Last Name']}".strip()
            vcard.add('tel').value = row['Phone']
            vcard.add('url').value = f"https://t.me/{row['Username']}" if row['Username'] else ""
            vcf_content.write(str(vcard).encode('utf-8'))
            vcf_content.write(b'\n')
    vcf_content.seek(0)
    return vcf_content

# Фильтрация данных
def filter_data(data, filters, parse_type):
    filtered_data = [row for row in data if not row[1] == "Удалённый аккаунт" and row[1]]
    if filters.get('only_with_username'):
        filtered_data = [row for row in filtered_data if row[1]]
    if filters.get('exclude_bots'):
        filtered_data = [row for row in filtered_data if not row[4]]
    if filters.get('only_active'):
        filtered_data = [row for row in filtered_data if is_active_recently(row[6])]
    if parse_type == 'parse_phone_contacts':
        filtered_data = [row for row in filtered_data if row[5]]
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
    return f"Total: {total}\nWith username: {with_username}\nBots: {bots}\nWithout name: {without_name}"

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

# Главное меню
def get_main_menu(user_id, context):
    users = load_users()
    user_id_str = str(user_id)
    user_data = users.get(user_id_str, {})
    lang = user_data.get('language', 'Русский')
    texts = LANGUAGES[lang]
    subscription = check_subscription_expiry(user_id, context)
    sub_type = subscription.get('type', 'Бесплатная')
    sub_end = subscription.get('end')
    sub_time = '—' if sub_type == 'Бесплатная' else (
        'permanently' if sub_end is None else 
        f"{(datetime.fromisoformat(sub_end) - datetime.now()).days * 24 + (datetime.fromisoformat(sub_end) - datetime.now()).seconds // 3600} hours"
    )
    requests = user_data.get('requests', 0)
    name = user_data.get('name', 'Unknown')
    limit_left, _ = check_request_limit(user_id)
    limit_display = 5 - user_data.get('daily_requests', {}).get('count', 0) if sub_type == 'Бесплатная' else 10 - user_data.get('daily_requests', {}).get('count', 0)
    
    buttons = [
        [InlineKeyboardButton("Identifiers", callback_data='identifiers'), InlineKeyboardButton("(!)", callback_data='info_identifiers')],
        [InlineKeyboardButton("Parser", callback_data='parser'), InlineKeyboardButton("(!)", callback_data='info_parser')],
        [InlineKeyboardButton(texts['subscribe_button'], callback_data='subscribe'), InlineKeyboardButton("(!)", callback_data='info_subscribe')],
        [InlineKeyboardButton(f"{texts['support'].format(support=SUPPORT_USERNAME)}", url=f"https://t.me/{SUPPORT_USERNAME[1:]}")],
        [InlineKeyboardButton("Requisites", callback_data='requisites'), InlineKeyboardButton("(!)", callback_data='info_requisites')]
    ]
    if user_id_str in ADMIN_IDS:
        buttons.append([InlineKeyboardButton("Logs", callback_data='logs_channel'), InlineKeyboardButton("(!)", callback_data='info_logs')])
    
    return texts['start_menu'].format(
        name=name, user_id=user_id, lang=lang, sub_type=sub_type, sub_time=sub_time, requests=requests, limit=limit_display
    ), InlineKeyboardMarkup(buttons)

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "No name"
    users = load_users()

    await log_to_channel(context, "User started bot", username)
    try:
        await client_telethon.connect()
        if not await client_telethon.is_user_authorized():
            await update.message.reply_text(LANGUAGES['Русский']['enter_phone'])
            context.user_data['waiting_for_phone'] = True
            await log_to_channel(context, f"Requested phone number from {name}", username)
            return

        if str(user_id) not in users:
            await log_to_channel(context, LANGUAGES['Русский']['new_user'].format(name=name, user_id=user_id), username)
            keyboard = [[InlineKeyboardButton(lang, callback_data=f'lang_{lang}')] for lang in LANGUAGES.keys()]
            await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            lang = users[str(user_id)]['language']
            await update.message.reply_text(LANGUAGES[lang]['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(LANGUAGES[lang]['subscribed'], callback_data='subscribed')]]))
            update_user_data(user_id, name, context)

    except Exception as e:
        await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Error: {str(e)} | Ошибка: {str(e)}", username)
    finally:
        if client_telethon.is_connected():
            await client_telethon.disconnect()

# Команда /info
async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    await update.message.reply_text(texts['info_cmd'])
    await log_to_channel(context, "User requested /info", update.effective_user.username)

# Команда /home
async def home(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    menu_text, menu_keyboard = get_main_menu(user_id, context)
    await update.message.reply_text(menu_text, reply_markup=menu_keyboard)
    await log_to_channel(context, "User requested /home", update.effective_user.username)

# Обработчик текстовых сообщений (оригинальная логика)
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data['user_id'] = user_id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    text = update.message.text.strip() if update.message.text else ""

    await log_to_channel(context, f"Received message: {text}", username)
    try:
        await client_telethon.connect()
    except telethon_errors.RPCError as e:
        await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Connection error: {str(e)} | Ошибка подключения: {str(e)}", username)
        print(f"Telethon connection error: {str(e)}\n{traceback.format_exc()}")
        return
    except Exception as e:
        print(f"Unknown Telethon connection error: {str(e)}\n{traceback.format_exc()}")
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
            await log_to_channel(context, f"Phone number from {name}: {text}", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Phone input error: {str(e)} | Ошибка ввода номера: {str(e)}", username)
            print(f"Code request error: {str(e)}\n{traceback.format_exc()}")
        finally:
            await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_code'):
        try:
            await client_telethon.sign_in(context.user_data['phone'], text)
            await update.message.reply_text(LANGUAGES['Русский']['auth_success'])
            del context.user_data['waiting_for_code']
            await log_to_channel(context, f"Successful authorization for {name}", username)
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
            await log_to_channel(context, f"2FA password requested for {name}", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Code input error: {str(e)} | Ошибка ввода кода: {str(e)}", username)
            print(f"Code entry error: {str(e)}\n{traceback.format_exc()}")
        finally:
            await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_password'):
        try:
            await client_telethon.sign_in(password=text)
            await update.message.reply_text(LANGUAGES['Русский']['auth_success'])
            del context.user_data['waiting_for_password']
            await log_to_channel(context, f"Successful 2FA authorization for {name}", username)
            keyboard = [
                [InlineKeyboardButton("Русский", callback_data='lang_Русский')],
                [InlineKeyboardButton("Украинский", callback_data='lang_Украинский')],
                [InlineKeyboardButton("English", callback_data='lang_English')],
                [InlineKeyboardButton("Deutsch", callback_data='lang_Deutsch')]
            ]
            await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        except telethon_errors.RPCError as e:
            await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"2FA password error: {str(e)} | Ошибка ввода пароля 2FA: {str(e)}", username)
            print(f"2FA password error: {str(e)}\n{traceback.format_exc()}")
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
                    text=f"User {name} (@{username}) (ID: {user_id}) sent transaction hash:\n{text}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Reject", callback_data=f'reject_{user_id}')]])
                )
            except telegram_error.BadRequest as e:
                print(f"Error sending hash to admin {admin_id}: {e}")
        await log_to_channel(context, f"Transaction hash from {name}: {text}", username)
        await update.message.reply_text(texts['payment_pending'])
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
            await log_to_channel(context, f"Chat ID obtained: {chat_id}", username)
        elif update.message.forward_origin and hasattr(update.message.forward_origin, 'sender_user'):
            user_id_forward = update.message.forward_origin.sender_user.id
            msg = await update.message.reply_text(texts['id_result'].format(id=user_id_forward), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
            ]))
            await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=["🎉"])
            await log_to_channel(context, f"User ID obtained: {user_id_forward}", username)
        elif text.startswith(('https://t.me/', '@')) or not text.startswith('http'):
            try:
                if text.startswith('@'):
                    normalized_link = f"https://t.me/{text[1:]}"
                elif not text.startswith('http'):
                    normalized_link = f"https://t.me/{text}"
                else:
                    normalized_link = text
                
                parts = normalized_link.split('/')
                if len(parts) > 3:
                    chat_id = parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}'
                else:
                    chat_id = f'@{parts[-1]}' if not parts[-1].startswith('+') else parts[-1]
                
                entity = await client_telethon.get_entity(chat_id)
                msg = await update.message.reply_text(texts['id_result'].format(id=entity.id), reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
                ]))
                await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=["🎉"])
                await log_to_channel(context, f"ID obtained via link: {entity.id}", username)
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
            normalized_links = []
            for link in links:
                if link.startswith('https://t.me/'):
                    normalized_links.append(link)
                elif link.startswith('@'):
                    normalized_links.append(f"https://t.me/{link[1:]}")
                elif not link.startswith('http'):
                    normalized_links.append(f"https://t.me/{link}")
            
            if context.user_data['parse_type'] == 'parse_post_commentators':
                valid_links = [link for link in normalized_links if '/'.join(link.split('/')[3:]).strip()]
                if not valid_links:
                    await update.message.reply_text(texts['invalid_link'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['fix_link'], callback_data='fix_link')]]))
                    context.user_data['last_input'] = text
                    await client_telethon.disconnect()
                    return
                context.user_data['links'] = valid_links
            else:
                context.user_data['links'] = normalized_links
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
    subscription = check_subscription_expiry(user_id, context)
    is_paid = subscription['type'] != 'Бесплатная'
    max_limit = 15000 if is_paid else 150
    keyboard = [
        [InlineKeyboardButton("100", callback_data='limit_100'), InlineKeyboardButton("500", callback_data='limit_500')],
        [InlineKeyboardButton("1000", callback_data='limit_1000'), InlineKeyboardButton(texts['skip'], callback_data='skip_limit')],
        [InlineKeyboardButton("Custom" if lang == 'English' else "Другое" if lang == 'Русский' else "Інше" if lang == 'Украинский' else "Andere", callback_data='limit_custom')]
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
        [InlineKeyboardButton("Yes" if lang == 'English' else "Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Ja", callback_data='filter_yes'),
         InlineKeyboardButton("No" if lang == 'English' else "Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "Nein", callback_data='filter_no')],
        [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
    ]
    context.user_data['waiting_for_filters'] = True
    context.user_data['current_filter'] = 'only_with_username'
    context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False}
    await message.reply_text(texts['filter_username'], reply_markup=InlineKeyboardMarkup(keyboard))

# Функции парсинга
async def parse_commentators(group_link, limit):
    entity = await client_telethon.get_entity(group_link)
    data = []
    async for message in client_telethon.iter_messages(entity, limit=limit):
        if message.sender_id:
            user = await client_telethon.get_entity(message.sender_id)
            if isinstance(user, tl.types.User) and not user.deleted:
                data.append([user.id, user.username or "", user.first_name or "", user.last_name or "", "🇬🇧", 29, user])
    return data

async def parse_participants(group_link, limit):
    entity = await client_telethon.get_entity(group_link)
    data = []
    async for user in client_telethon.iter_participants(entity, limit=limit):
        if isinstance(user, tl.types.User) and not user.deleted:
            data.append([user.id, user.username or "", user.first_name or "", user.last_name or "", "🇬🇧", 29, user])
    return data

async def parse_post_commentators(link, limit):
    parts = link.split('/')
    chat_id = f"@{parts[-2]}"
    message_id = int(parts[-1])
    entity = await client_telethon.get_entity(chat_id)
    data = []
    async for reply in client_telethon.iter_messages(entity, limit=limit, reply_to=message_id):
        if reply.sender_id:
            user = await client_telethon.get_entity(reply.sender_id)
            if isinstance(user, tl.types.User) and not user.deleted:
                data.append([user.id, user.username or "", user.first_name or "", user.last_name or "", "🇬🇧", 29, user])
    return data

async def parse_phone_contacts(group_link, limit):
    entity = await client_telethon.get_entity(group_link)
    data = []
    async for user in client_telethon.iter_participants(entity, limit=limit):
        if isinstance(user, tl.types.User) and not user.deleted and user.phone:
            data.append([user.id, user.username or "", user.first_name or "", user.last_name or "", user.phone, user])
    return data

async def parse_auth_access(link, context):
    user_id = context.user_data.get('user_id')
    username = context.user_data.get('username', 'No username')
    name = load_users().get(str(user_id), {}).get('name', 'Unknown')
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    try:
        parts = link.split('/')
        chat_id = parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}'
        entity = await client_telethon.get_entity(chat_id)
        if hasattr(entity, 'participants_count'):
            await context.bot.send_message(chat_id=user_id, text=texts['auth_success'])
            await log_to_channel(context, f"Access to private chat {chat_id} granted for {name}", username)
        else:
            await context.bot.send_message(chat_id=user_id, text=texts['auth_error'])
            await log_to_channel(context, f"Access error to private chat {chat_id} for {name}", username)
    except telethon_errors.RPCError as e:
        await context.bot.send_message(chat_id=user_id, text=texts['auth_error'])
        await log_to_channel(context, f"Authorization error for {name}: {str(e)} | Ошибка авторизации: {str(e)}", username)

# Обработка парсинга
async def process_parsing(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    username = message.from_user.username or "No username"
    name = message.from_user.full_name or "No name"
    users = load_users()
    lang = users[str(user_id)]['language']
    texts = LANGUAGES[lang]
    parse_type = context.user_data['parse_type']

    context.user_data['parsing_in_progress'] = True
    asyncio.create_task(show_loading_message(message, context))
    
    try:
        await client_telethon.connect()
        all_data = []
        for link in context.user_data['links']:
            try:
                if link.startswith('@'):
                    normalized_link = f"https://t.me/{link[1:]}"
                elif not link.startswith('http'):
                    normalized_link = f"https://t.me/{link}"
                else:
                    normalized_link = link
                
                await client_telethon.get_entity(normalized_link.split('/')[-2] if parse_type in ['parse_post_commentators', 'parse_auth_access'] else normalized_link)
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
                await log_to_channel(context, f"RPC error: {str(e)} | Ошибка RPC: {str(e)}", username)
                return
            
            limit = check_parse_limit(user_id, context.user_data['limit'], parse_type)
            if parse_type == 'parse_authors':
                data = await parse_commentators(normalized_link, limit)
            elif parse_type == 'parse_participants':
                data = await parse_participants(normalized_link, limit)
            elif parse_type == 'parse_post_commentators':
                data = await parse_post_commentators(normalized_link, limit)
            elif parse_type == 'parse_phone_contacts':
                data = await parse_phone_contacts(normalized_link, limit)
            elif parse_type == 'parse_auth_access':
                await parse_auth_access(normalized_link, context)
                context.user_data['parsing_done'] = True
                context.user_data['parsing_in_progress'] = False
                return
            
            all_data.extend(data)
        
        filtered_data = filter_data(all_data, context.user_data.get('filters', {}), parse_type)
        count = len(filtered_data)
        excel_file = await create_excel_in_memory(filtered_data)
        
        context.user_data['parsing_done'] = True
        checklist = texts['parsing_checklist'].format(count=count)
        await message.reply_text(checklist)
        
        if parse_type == 'parse_phone_contacts':
            vcf_file = create_vcf_file(pd.DataFrame(filtered_data, columns=['ID', 'Username', 'First Name', 'Last Name', 'Phone', 'User']))
            await message.reply_document(document=excel_file, filename="phones.xlsx", caption=texts['caption_phones'])
            await message.reply_document(document=vcf_file, filename="phones.vcf")
            await log_to_channel(context, "Parsed phone numbers", username, excel_file)
        else:
            caption = texts['caption_' + parse_type.split('_')[1]]
            await message.reply_document(document=excel_file, filename=f"{parse_type}.xlsx", caption=caption)
            await log_to_channel(context, f"Parsed {parse_type}: {count}", username, excel_file)

        update_user_data(user_id, name, context, requests=1)
        keyboard = [
            [InlineKeyboardButton(str(i), callback_data=f'rate_{i}') for i in range(1, 6)],
            [InlineKeyboardButton(texts['skip'], callback_data='skip_rating')]
        ]
        await message.reply_text(texts['rate_parsing'], reply_markup=InlineKeyboardMarkup(keyboard))

    except telethon_errors.FloodWaitError as e:
        context.user_data['parsing_done'] = True
        await message.reply_text(texts['flood_error'].format(e=str(e)))
        context.user_data['parsing_in_progress'] = False
        await log_to_channel(context, f"Flood error: {str(e)} | Ошибка Flood: {str(e)}", username)
    except Exception as e:
        context.user_data['parsing_done'] = True
        await message.reply_text(texts['rpc_error'].format(e=str(e)))
        context.user_data['parsing_in_progress'] = False
        await log_to_channel(context, f"Error: {str(e)} | Ошибка: {str(e)}", username)
    finally:
        context.user_data['parsing_in_progress'] = False
        if client_telethon.is_connected():
            await client_telethon.disconnect()

# Обработчик кнопок
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    username = query.from_user.username or "No username"
    name = query.from_user.full_name or "No name"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    await query.answer()
    await log_to_channel(context, f"Button pressed: {query.data}", username)

    if query.data.startswith('lang_'):
        lang = query.data.split('_')[1]
        update_user_data(user_id, name, context, lang=lang)
        await query.edit_message_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data='subscribed')]]))
        return

    if query.data == 'subscribed':
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
        return

    if query.data == 'parser':
        keyboard = [
            [InlineKeyboardButton("Group Commentators" if lang == 'English' else "Комментаторы группы" if lang == 'Русский' else "Коментатори групи" if lang == 'Украинский' else "Gruppenkommentatoren", callback_data='parse_authors')],
            [InlineKeyboardButton("Group Participants" if lang == 'English' else "Участники группы" if lang == 'Русский' else "Учасники групи" if lang == 'Украинский' else "Gruppenteilnehmer", callback_data='parse_participants')],
            [InlineKeyboardButton("Post Commentators" if lang == 'English' else "Комментаторы поста" if lang == 'Русский' else "Коментатори поста" if lang == 'Украинский' else "Beitragskommentatoren", callback_data='parse_post_commentators')],
            [InlineKeyboardButton(texts['phone_contacts'], callback_data='parse_phone_contacts')],
            [InlineKeyboardButton(texts['auth_access'], callback_data='parse_auth_access')]
        ]
        await query.edit_message_text(texts['parser'], reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if query.data.startswith('parse_'):
        context.user_data['parse_type'] = query.data
        if query.data in ['parse_authors', 'parse_participants', 'parse_phone_contacts', 'parse_auth_access']:
            await query.edit_message_text(texts['link_group'])
        elif query.data == 'parse_post_commentators':
            await query.edit_message_text(texts['link_post'])
        return

    if query.data.startswith('limit_'):
        if query.data == 'limit_custom':
            context.user_data['waiting_for_limit'] = True
            await query.edit_message_text("Enter number:")
        else:
            context.user_data['limit'] = int(query.data.split('_')[1])
            await ask_for_filters(query.message, context)
        return

    if query.data == 'skip_limit':
        context.user_data['limit'] = 150 if check_subscription_expiry(user_id, context)['type'] == 'Бесплатная' else 5000
        await ask_for_filters(query.message, context)
        return

    if query.data == 'no_filter':
        context.user_data['limit'] = 150 if check_subscription_expiry(user_id, context)['type'] == 'Бесплатная' else 5000
        context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False}
        await process_parsing(query.message, context)
        return

    if query.data == 'filter_yes':
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        filters[context.user_data['current_filter']] = True
        context.user_data['filters'] = filters
        next_filter = {'only_with_username': 'exclude_bots', 'exclude_bots': 'only_active', 'only_active': None}
        if next_filter[context.user_data['current_filter']]:
            context.user_data['current_filter'] = next_filter[context.user_data['current_filter']]
            context.user_data['waiting_for_filters'] = True
            if context.user_data['current_filter'] == 'exclude_bots':
                await query.edit_message_text(texts['filter_bots'], reply_markup=query.message.reply_markup)
            elif context.user_data['current_filter'] == 'only_active':
                await query.edit_message_text(texts['filter_active'], reply_markup=query.message.reply_markup)
        else:
            await process_parsing(query.message, context)
        return

    if query.data == 'filter_no':
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        filters[context.user_data['current_filter']] = False
        context.user_data['filters'] = filters
        next_filter = {'only_with_username': 'exclude_bots', 'exclude_bots': 'only_active', 'only_active': None}
        if next_filter[context.user_data['current_filter']]:
            context.user_data['current_filter'] = next_filter[context.user_data['current_filter']]
            context.user_data['waiting_for_filters'] = True
            if context.user_data['current_filter'] == 'exclude_bots':
                await query.edit_message_text(texts['filter_bots'], reply_markup=query.message.reply_markup)
            elif context.user_data['current_filter'] == 'only_active':
                await query.edit_message_text(texts['filter_active'], reply_markup=query.message.reply_markup)
        else:
            await process_parsing(query.message, context)
        return

    if query.data == 'skip_filters':
        del context.user_data['waiting_for_filters']
        del context.user_data['current_filter']
        await process_parsing(query.message, context)
        return

    if query.data.startswith('rate_'):
        await query.message.reply_text(texts['thanks'])
        await asyncio.sleep(1)
        await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id + 1)
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.message.reply_text(menu_text, reply_markup=menu_keyboard)
        await log_to_channel(context, f"Rating: {query.data.split('_')[1]}", username)
        return

    if query.data == 'skip_rating':
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.message.reply_text(menu_text, reply_markup=menu_keyboard)
        return

    if query.data == 'subscribe':
        keyboard = [
            [InlineKeyboardButton(texts['subscription_1h'], callback_data='subscribe_1h')],
            [InlineKeyboardButton(texts['subscription_3d'], callback_data='subscribe_3d')],
            [InlineKeyboardButton(texts['subscription_7d'], callback_data='subscribe_7d')]
        ]
        await query.edit_message_text("Choose subscription:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if query.data.startswith('subscribe_'):
        sub_type = query.data.split('_')[1]
        amount = {'1h': 2, '3d': 5, '7d': 7}[sub_type]
        keyboard = [
            [InlineKeyboardButton(texts['payment_paid'], callback_data=f'paid_{sub_type}')],
            [InlineKeyboardButton(texts['payment_cancel'], callback_data='cancel_payment')]
        ]
        await query.edit_message_text(texts['payment_wallet'].format(amount=amount, address=TON_WALLET_ADDRESS), reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if query.data.startswith('paid_'):
        sub_type = query.data.split('_')[1]
        context.user_data['waiting_for_hash'] = True
        context.user_data['sub_type'] = sub_type
        await query.edit_message_text(texts['payment_hash'])
        return

    if query.data == 'cancel_payment':
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard
async def note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "No username"
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    note_text = update.message.text.replace('/note ', '').strip()
    if note_text:
        await log_to_channel(context, f"Note saved: {note_text}", username)
        await update.message.reply_text(texts['note_cmd'])
    else:
        await update.message.reply_text("Пожалуйста, укажи текст заметки после /note!")

# Обработчик кнопок (продолжение)
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... (предыдущая часть обработчика осталась выше)

    if query.data == 'identifiers':
        context.user_data['waiting_for_id'] = True
        await query.edit_message_text(texts['identifiers'])
        return

    if query.data == 'close_id':
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
        return

    if query.data == 'continue_id':
        context.user_data['waiting_for_id'] = True
        await query.edit_message_text(texts['identifiers'])
        return

    if query.data == 'fix_link':
        last_input = context.user_data.get('last_input', '')
        if last_input:
            suggested_link = f"https://t.me/{last_input.strip('@')}"
            keyboard = [
                [InlineKeyboardButton(texts['suggest_link'].format(link=suggested_link), callback_data=f"use_link_{suggested_link}")],
                [InlineKeyboardButton(texts['retry_link'], callback_data='retry_link')]
            ]
            await query.edit_message_text(texts['fix_link'], reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await query.edit_message_text(texts['retry_link'])
        return

    if query.data.startswith('use_link_'):
        link = query.data.split('use_link_')[1]
        context.user_data['links'] = [link]
        await ask_for_limit(query.message, context)
        return

    if query.data == 'retry_link':
        await query.edit_message_text(texts['retry_link'])
        return

    if query.data == 'requisites':
        await query.edit_message_text(texts['requisites'].format(support=SUPPORT_USERNAME))
        return

    if query.data == 'logs_channel' and str(user_id) in ADMIN_IDS:
        await query.edit_message_text(texts['logs_channel'], reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Go to Logs", url=f"https://t.me/c/{str(LOG_CHANNEL_ID)[4:]}")]
        ]))
        return

    if query.data.startswith('reject_') and str(user_id) in ADMIN_IDS:
        rejected_user_id = query.data.split('_')[1]
        rejected_user = load_users().get(rejected_user_id, {})
        rejected_lang = rejected_user.get('language', 'Русский')
        rejected_texts = LANGUAGES[rejected_lang]
        await context.bot.send_message(
            chat_id=rejected_user_id,
            text=rejected_texts['payment_error']
        )
        await query.edit_message_text("Transaction rejected")
        await log_to_channel(context, f"Transaction rejected for user {rejected_user_id}", username)
        return

    if query.data.startswith('info_'):
        info_type = query.data.split('_')[1]
        info_texts = {
            'identifiers': "Get user or chat IDs by sending @username, a link, or forwarding a message.",
            'parser': "Parse users from groups, posts, or collect phone numbers with filters.",
            'subscribe': "Unlock higher limits and more features with a paid subscription.",
            'requisites': "Payment details for subscriptions.",
            'logs': "View bot logs (admin only)."
        }
        await query.edit_message_text(info_texts.get(info_type, "No info available"), reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(texts['close'], callback_data='back_to_menu')]
        ]))
        return

    if query.data == 'back_to_menu':
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
        return

# Главная функция
def main():
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("info", info))
    application.add_handler(CommandHandler("home", home))
    application.add_handler(CommandHandler("note", note))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(button))
    
    print("Bot is starting...")
    application.run_polling()

if __name__ == '__main__':
    main()
