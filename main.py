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
BOT_TOKEN = os.environ.get('BOT_TOKEN', '7981019134:AAHV39jxmzvYWW0jiNf6vRW-pB2MSa7QGVU')
LOG_CHANNEL_ID = -1002342891238
SUBSCRIPTION_CHANNEL_ID = -1002425905138  # Оставляем для справки, но не используем
SUPPORT_USERNAME = '@alex_strayker'
TON_WALLET_ADDRESS = 'UQAP4wrP0Jviy03CTeniBjSnAL5UHvcMFtxyi1Ip1exl9pLu'
TON_API_KEY = os.environ.get('TON_API_KEY', 'YOUR_TON_API_KEY')
ADMIN_IDS = ['282198872']

# Путь к файлу общей сессии
SESSION_FILE = 'shared_session.session'

# Создание клиента Telethon с общей сессией
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
        'limit': 'Сколько пользователей парсить? Выбери или укажи своё число (макс. 5000 авторов/15000 участников/10000 комментаторов поста для платных подписок, 150 для бесплатной).',
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
        'new_user': 'Новый пользователь: {name} (@{username})',
        'language_cmd': 'Выбери новый язык:',
        'caption_commentators': 'Вот ваш файл с комментаторами.',
        'caption_participants': 'Вот ваш файл с участниками.',
        'caption_post_commentators': 'Вот ваш файл с комментаторами поста.',
        'limit_reached': 'Ты исчерпал дневной ліміт ({limit} запросов). Попробуй снова через {hours} часов.',
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
        'note_cmd': 'Заметка успешно сохранена (бот не будет реагировать).',
        'home_cmd': 'Вы вернулись в главное меню.',
        'info_cmd': 'Это бот для парсинга Telegram.\nВозможности:\n- Сбор ID\n- Парсинг участников\n- Парсинг комментаторов\n- Сбор контактов\nДля поддержки: {support}',
        'working_message': 'Всё нормально, мы ещё работаем...',
        'invalid_link_suggestion': 'Ссылка "{link}" неверная. Возможно, вы имели в виду что-то вроде https://t.me/group_name или https://t.me/channel_name/12345? Попробуйте снова.',
        'rate_parsing': 'Оцените качество парсинга:',
        'info_identifiers': 'Функция "Идентификаторы" позволяет узнать ID пользователей, групп или постов.\nОтправьте @username, ссылку в любом формате или перешлите сообщение из канала.',
        'info_parser': 'Функция "Парсер" позволяет собирать данные:\n- Авторы сообщений\n- Участники чата\n- Комментаторы поста\n- Номера телефонов\n- Доступ к закрытым чатам',
        'info_subscribe': 'Оформите подписку для расширенных лимитов:\n- 1 час: 2 USDT\n- 3 дня: 5 USDT\n- 7 дней: 7 USDT',
        'info_requisites': 'Оплата через TON кошелёк:\n1. Переведите USDT на адрес\n2. Укажите хеш транзакции\nСвяжитесь с поддержкой для деталей.',
        'info_logs': 'Канал с логами доступен только администраторам. Логи содержат информацию о действиях пользователей.',
        'info_parse_authors': 'Сбор авторов сообщений из чата или канала.',
        'info_parse_participants': 'Сбор всех участников чата или канала.',
        'info_parse_post_commentators': 'Сбор комментаторов конкретного поста.',
        'info_parse_phone_contacts': 'Сбор номеров телефонов и ФИО участников.',
        'info_parse_auth_access': 'Предоставление доступа к закрытым чатам.'
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
        'identifiers': 'Надішли мені @username, посилання у будь-якому форматі або перешли повідомлення з каналу, щоб дізнатися ID.',
        'parser': 'Обери, що хочеш спарсити:',
        'subscribe_button': 'Оформити підписку',
        'support': 'Підтримка: {support}',
        'requisites': 'Можливості оплати:\n1. [Метод 1]\n2. [Метод 2]\nЗв’яжіться з {support} для оплати.',
        'logs_channel': 'Канал з логами: t.me/YourLogChannel',
        'link_group': 'Надішли мені посилання на групу або канал, наприклад: https://t.me/group_name, @group_name або group_name\nМожна вказати кілька посилань через Enter.',
        'link_post': 'Надішли мені посилання на пост, наприклад: https://t.me/channel_name/12345\nАбо перешли пост. Можна вказати кілька посилань через Enter.',
        'limit': 'Скільки користувачів парсити? Обери або вкажи своє число (макс. 5000 авторів/15000 учасників/10000 коментаторів посту для платних підписок, 150 для безкоштовної).',
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
        'new_user': 'Новий користувач: {name} (@{username})',
        'language_cmd': 'Обери нову мову:',
        'caption_commentators': 'Ось ваш файл з коментаторами.',
        'caption_participants': 'Ось ваш файл з учасниками.',
        'caption_post_commentators': 'Ось ваш файл з коментаторами поста.',
        'limit_reached': 'Ти вичерпав денний ліміт ({limit} запитів). Спробуй знову через {hours} годин.',
        'id_result': 'ID: {id}',
        'close': 'Закрыть',
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
        'note_cmd': 'Примітка успішно збережено (бот не реагуватиме).',
        'home_cmd': 'Ви повернулися до головного меню.',
        'info_cmd': 'Це бот для парсингу Telegram.\nМожливості:\n- Збір ID\n- Парсинг учасників\n- Парсинг коментаторів\n- Збір контактів\nДля підтримки: {support}',
        'working_message': 'Всё нормально, мы ещё работаем...',
        'invalid_link_suggestion': 'Посилання "{link}" неправильне. Можливо, ви мали на увазі щось на кшталт https://t.me/group_name або https://t.me/channel_name/12345? Спробуйте ще раз.',
        'rate_parsing': 'Оцініть якість парсингу:',
        'info_identifiers': 'Функція "Ідентифікатори" дозволяє дізнатися ID користувачів, груп або постів.\nНадішліть @username, посилання у будь-якому форматі або перешліть повідомлення з каналу.',
        'info_parser': 'Функція "Парсер" дозволяє збирати дані:\n- Автори повідомлень\n- Учасники чату\n- Коментатори посту\n- Номери телефонів\n- Доступ до закритих чатів',
        'info_subscribe': 'Оформіть підписку для розширених лімітів:\n- 1 година: 2 USDT\n- 3 дні: 5 USDT\n- 7 днів: 7 USDT',
        'info_requisites': 'Оплата через TON гаманець:\n1. Переведіть USDT на адресу\n2. Вкажіть хеш транзакції\nЗв’яжіться з підтримкою для деталей.',
        'info_logs': 'Канал з логами доступний тільки адміністраторам. Логи містять інформацію про дії користувачів.',
        'info_parse_authors': 'Збір авторів повідомлень з чату або каналу.',
        'info_parse_participants': 'Збір усіх учасників чату або каналу.',
        'info_parse_post_commentators': 'Збір коментаторів конкретного посту.',
        'info_parse_phone_contacts': 'Збір номерів телефонів та ПІБ учасників.',
        'info_parse_auth_access': 'Надання доступу до закритих чатів.'
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
        'identifiers': 'Send me @username, a link in any format, or forward a message from a channel to find out the ID.',
        'parser': 'Choose what you want to parse:',
        'subscribe_button': 'Subscribe',
        'support': 'Support: {support}',
        'requisites': 'Payment options:\n1. [Method 1]\n2. [Method 2]\nContact {support} for payment.',
        'logs_channel': 'Logs channel: t.me/YourLogChannel',
        'link_group': 'Send me a link to a group or channel, e.g.: https://t.me/group_name, @group_name or group_name\nYou can specify multiple links via Enter.',
        'link_post': 'Send me a link to a post, e.g.: https://t.me/channel_name/12345\nOr forward a post. You can specify multiple links via Enter.',
        'limit': 'How many users to parse? Choose or enter your number (max 5,000 authors/15,000 participants/10,000 post commentators for paid subscriptions, 150 for free).',
        'filter_username': 'Filter only users with username?',
        'filter_bots': 'Exclude bots?',
        'filter_active': 'Only recently active (within 30 days)?',
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
        'note_cmd': 'Note successfully saved (bot will not respond).',
        'home_cmd': 'You returned to the main menu.',
        'info_cmd': 'This is a Telegram parsing bot.\nFeatures:\n- ID collection\n- Participants parsing\n- Commentators parsing\n- Contact collection\nFor support: {support}',
        'working_message': 'Everything is fine, we are still working...',
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
        'info_parse_auth_access': 'Granting access to private chats.'
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
        'identifiers': 'Sende mir @username, einen Link in beliebigem Format oder leite eine Nachricht aus einem Kanal weiter, um die ID herauszufinden.',
        'parser': 'Wähle, was du parsen möchtest:',
        'subscribe_button': 'Abonnement abschließen',
        'support': 'Support: {support}',
        'requisites': 'Zahlungsmöglichkeiten:\n1. [Methode 1]\n2. [Methode 2]\nKontaktiere {support} für die Zahlung.',
        'logs_channel': 'Log-Kanal: t.me/YourLogChannel',
        'link_group': 'Sende mir einen Link zu einer Gruppe oder einem Kanal, z.B.: https://t.me/group_name, @group_name oder group_name\nDu kannst mehrere Links mit Enter angeben.',
        'link_post': 'Sende mir einen Link zu einem Beitrag, z.B.: https://t.me/channel_name/12345\nOder leite einen Beitrag weiter. Du kannst mehrere Links mit Enter angeben.',
        'limit': 'Wie viele Benutzer sollen geparst werden? Wähle oder gib eine Zahl ein (max. 5.000 Autoren/15.000 Teilnehmer/10.000 Beitragskommentatoren für bezahlte Abonnements, 150 für kostenlos).',
        'filter_username': 'Nur Benutzer mit Username filtern?',
        'filter_bots': 'Bots ausschließen?',
        'filter_active': 'Nur kürzlich aktive (innerhalb von 30 Tagen)?',
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
        'note_cmd': 'Notiz erfolgreich gespeichert (der Bot wird nicht reagieren).',
        'home_cmd': 'Du bist zum Hauptmenü zurückgekehrt.',
        'info_cmd': 'Dies ist ein Telegram-Parsing-Bot.\nFunktionen:\n- ID-Sammlung\n- Teilnehmer-Parsing\n- Kommentatoren-Parsing\n- Kontaktsammlung\nFür Support: {support}',
        'working_message': 'Alles ist gut, wir arbeiten noch...',
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
        'info_parse_auth_access': 'Gewährung von Zugriff auf private Chats.'
    }
}

# Логирование в канал
async def log_to_channel(context, message, username=None):
    try:
        user = context.user_data.get('user', {})
        name = user.get('name', 'Неизвестно')
        log_message = f"Пользователь {name} (@{username}): {message}" if username else message
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
    user['daily_requests']['count'] = user['daily_requests'].get('count', 0) + requests
    user['name'] = name  # Имя пользователя обновляется только при явном вызове
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
    max_requests = 5 if subscription['type'] == 'Бесплатная' else 10
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
        return min(limit, 150)
    elif parse_type == 'parse_authors':
        return min(limit, 5000)
    elif parse_type == 'parse_participants':
        return min(limit, 15000)
    elif parse_type == 'parse_post_commentators':
        return min(limit, 10000)
    else:
        return min(limit, 15000)

# Создание файла Excel
async def create_excel_in_memory(data, chat_name=""):
    df = pd.DataFrame(data, columns=['ID', 'Username', 'First Name', 'Nickname'] if len(data[0]) == 4 else ['ID', 'Username', 'First Name', 'Last Name', 'Phone'])
    df['Nickname'] = '@' + df['Username'].astype(str)  # Форматируем никнейм как @username
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
        if pd.notna(row['Phone']) and pd.notna(row['First Name']):
            vcard = vobject.vCard()
            vcard.add('fn').value = f"{row['First Name']} {row['Last Name'] or ''}".strip()
            vcard.add('tel').value = str(row['Phone'])
            vcard.add('url').value = f"https://t.me/{row['Username']}" if pd.notna(row['Username']) else ""
            vcf_content.write(vcard.serialize().encode('utf-8'))
            vcf_content.write(b'\n')
    vcf_content.seek(0)
    return vcf_content

# Фильтрация данных
def filter_data(data, filters):
    filtered_data = data
    if filters.get('only_with_username'):
        filtered_data = [row for row in filtered_data if row[1]]
    if filters.get('exclude_bots'):
        filtered_data = [row for row in filtered_data if not row[4]] if len(data[0]) > 4 else filtered_data
    if filters.get('only_active'):
        filtered_data = [row for row in filtered_data if is_active_recently(row[5] if len(row) > 5 else None)]
    return filtered_data[:filters.get('limit', len(filtered_data))]  # Ограничение по лимиту

def is_active_recently(user):
    if not user or not hasattr(user, 'status') or not user.status:
        return True
    if hasattr(user.status, 'was_online'):
        return (datetime.now() - user.status.was_online.replace(tzinfo=None)).days < 30
    return True

# Подсчёт статистики
def get_statistics(data):
    total = len(data)
    with_username = sum(1 for row in data if row[1])
    bots = sum(1 for row in data if len(row) > 4 and row[4]) if len(data) > 0 and len(data[0]) > 4 else 0
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
    name = user_data.get('name', 'Неизвестно')  # Используем сохранённое имя
    limit_left, _ = check_request_limit(user_id)
    limit_display = 5 - user_data.get('daily_requests', {}).get('count', 0) if sub_type == 'Бесплатная' else 10 - user_data.get('daily_requests', {}).get('count', 0)
    
    is_admin = user_id_str in ADMIN_IDS
    
    buttons = [
        [InlineKeyboardButton("Идентификаторы" if lang == 'Русский' else "Ідентифікатори" if lang == 'Украинский' else "Identifiers" if lang == 'English' else "Identifikatoren", callback_data=f'identifiers_{user_id}_{datetime.now().timestamp()}'), 
         InlineKeyboardButton("(!)", callback_data=f'info_identifiers_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton("Сбор данных / Парсер" if lang == 'Русский' else "Збір даних / Парсер" if lang == 'Украинский' else "Data collection / Parser" if lang == 'English' else "Datensammlung / Parser", callback_data=f'parser_{user_id}_{datetime.now().timestamp()}'), 
         InlineKeyboardButton("(!)", callback_data=f'info_parser_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton(texts['subscribe_button'], callback_data=f'subscribe_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton("(!)", callback_data=f'info_subscribe_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton(f"{texts['support'].format(support=SUPPORT_USERNAME)}", url=f"https://t.me/{SUPPORT_USERNAME[1:]}")],
        [InlineKeyboardButton("Реквизиты" if lang == 'Русский' else "Реквізити" if lang == 'Украинский' else "Requisites" if lang == 'English' else "Zahlungsdaten", callback_data=f'requisites_{user_id}_{datetime.now().timestamp()}'), 
         InlineKeyboardButton("(!)", callback_data=f'info_requisites_{user_id}_{datetime.now().timestamp()}')]
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton("Канал с логами" if lang == 'Русский' else "Канал з логами" if lang == 'Украинский' else "Logs channel" if lang == 'English' else "Log-Kanal", callback_data=f'logs_channel_{user_id}_{datetime.now().timestamp()}'), 
                        InlineKeyboardButton("(!)", callback_data=f'info_logs_{user_id}_{datetime.now().timestamp()}')])
    
    return texts['start_menu'].format(
        name=name, user_id=user_id, lang=lang, sub_type=sub_type, sub_time=sub_time, requests=requests, limit=limit_display
    ), InlineKeyboardMarkup(buttons)

# Обработчик команды /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()

    try:
        await client_telethon.connect()
        if not await client_telethon.is_user_authorized():
            await update.message.reply_text(LANGUAGES['Русский']['enter_phone'])
            context.user_data['waiting_for_phone'] = True
            await log_to_channel(context, f"Новый пользователь: {name} (@{username})", username)
            return

        if str(user_id) not in users:
            await log_to_channel(context, f"Новый пользователь: {name} (@{username})", username)
            keyboard = [
                [InlineKeyboardButton("Русский", callback_data=f'lang_Русский_{user_id}_{datetime.now().timestamp()}')],
                [InlineKeyboardButton("Украинский", callback_data=f'lang_Украинский_{user_id}_{datetime.now().timestamp()}')],
                [InlineKeyboardButton("English", callback_data=f'lang_English_{user_id}_{datetime.now().timestamp()}')],
                [InlineKeyboardButton("Deutsch", callback_data=f'lang_Deutsch_{user_id}_{datetime.now().timestamp()}')]
            ]
            await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            lang = users[str(user_id)]['language']
            await update.message.reply_text(LANGUAGES[lang]['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(LANGUAGES[lang]['subscribed'], callback_data=f'subscribed_{user_id}_{datetime.now().timestamp()}')]]))
            update_user_data(user_id, name, context)

    except telethon_errors.RPCError as e:
        await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка подключения/авторизации для {name} (@{username}): {str(e)}", username)
    except Exception as e:
        print(f"Ошибка в /start: {str(e)}\n{traceback.format_exc()}")
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
    
    await update.message.reply_text(texts['info_cmd'].format(support=SUPPORT_USERNAME))
    await log_to_channel(context, f"Пользователь {name} (@{username}) запросил информацию о боте", username)

# Обработчик команды /language
async def language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    keyboard = [
        [InlineKeyboardButton("Русский", callback_data=f'lang_Русский_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton("Украинский", callback_data=f'lang_Украинский_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton("English", callback_data=f'lang_English_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton("Deutsch", callback_data=f'lang_Deutsch_{user_id}_{datetime.now().timestamp()}')]
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
        await update.message.reply_text("Неверный тип подписки. Используйте '1h', '3d', '7d' или 'permanent'.")
        return
    
    subscription_type = f'Платная ({sub_type})' if sub_type in ['1h', '3d', '7d'] else 'Платная (бессрочная)'
    update_user_data(target_user_id, "Имя пользователя", context, subscription={'type': subscription_type, 'end': end_time.isoformat() if end_time else None})
    
    username = load_users().get(str(target_user_id), {}).get('name', 'Неизвестно')
    lang = load_users().get(str(target_user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    notification = texts['payment_success'].format(end_time=end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'бессрочно')
    await context.bot.send_message(chat_id=target_user_id, text=f"🎉 {notification}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_update'], callback_data=f'update_menu_{target_user_id}_{datetime.now().timestamp()}')]]))
    
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

# Сообщение "Подождите..." и "Всё нормально, мы ещё работаем..."
async def show_loading_message(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    loading_msg = "Подождите..." if lang == 'Русский' else "Зачекайте..." if lang == 'Украинский' else "Please wait..." if lang == 'English' else "Bitte warten..."
    working_msg = texts['working_message']
    
    loading_message = await message.reply_text(loading_msg)
    context.user_data['loading_message_id'] = loading_message.message_id
    context.user_data['working_message_id'] = None
    
    dots = 1
    elapsed_time = 0
    working_sent = False
    while 'parsing_done' not in context.user_data:
        dots = (dots % 3) + 1
        new_text = loading_msg + "." * dots
        try:
            await context.bot.edit_message_text(
                chat_id=message.chat_id,
                message_id=loading_message.message_id,
                text=new_text
            )
        except telegram_error.BadRequest:
            break
        await asyncio.sleep(1)
        elapsed_time += 1
        if elapsed_time >= 15 and not working_sent:
            working_message = await message.reply_text(working_msg)
            context.user_data['working_message_id'] = working_message.message_id
            working_sent = True
    
    if 'parsing_done' in context.user_data:
        try:
            await context.bot.delete_message(
                chat_id=message.chat_id,
                message_id=loading_message.message_id
            )
            if working_sent and context.user_data.get('working_message_id'):
                await context.bot.delete_message(
                    chat_id=message.chat_id,
                    message_id=context.user_data['working_message_id']
                )
        except telegram_error.BadRequest:
            pass

# Обработчик текстовых сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data['user_id'] = user_id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    text = update.message.text.strip() if update.message.text else ""

    try:
        await client_telethon.connect()
    except telethon_errors.RPCError as e:
        await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка подключения для {name} (@{username}): {str(e)}", username)
        return
    except Exception as e:
        print(f"Неизвестная ошибка подключения Telethon: {str(e)}\n{traceback.format_exc()}")
        return
    finally:
        if not client_telethon.is_connected():
            await client_telethon.connect()

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
            await log_to_channel(context, f"Пользователь {name} (@{username}) ввёл номер телефона: {text}", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода номера для {name} (@{username}): {str(e)}", username)
        finally:
            await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_code'):
        try:
            await client_telethon.sign_in(context.user_data['phone'], text)
            await update.message.reply_text(LANGUAGES['Русский']['auth_success'])
            del context.user_data['waiting_for_code']
            await log_to_channel(context, f"Пользователь {name} (@{username}) успешно авторизовался", username)
            keyboard = [
                [InlineKeyboardButton("Русский", callback_data=f'lang_Русский_{user_id}_{datetime.now().timestamp()}')],
                [InlineKeyboardButton("Украинский", callback_data=f'lang_Украинский_{user_id}_{datetime.now().timestamp()}')],
                [InlineKeyboardButton("English", callback_data=f'lang_English_{user_id}_{datetime.now().timestamp()}')],
                [InlineKeyboardButton("Deutsch", callback_data=f'lang_Deutsch_{user_id}_{datetime.now().timestamp()}')]
            ]
            await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        except telethon_errors.SessionPasswordNeededError:
            await update.message.reply_text(LANGUAGES['Русский']['enter_password'])
            context.user_data['waiting_for_password'] = True
            del context.user_data['waiting_for_code']
            await log_to_channel(context, f"Пользователь {name} (@{username}) запросил пароль 2FA", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода кода для {name} (@{username}): {str(e)}", username)
        finally:
            await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_password'):
        try:
            await client_telethon.sign_in(password=text)
            await update.message.reply_text(LANGUAGES['Русский']['auth_success'])
            del context.user_data['waiting_for_password']
            await log_to_channel(context, f"Пользователь {name} (@{username}) успешно авторизовался с 2FA", username)
            keyboard = [
                [InlineKeyboardButton("Русский", callback_data=f'lang_Русский_{user_id}_{datetime.now().timestamp()}')],
                [InlineKeyboardButton("Украинский", callback_data=f'lang_Украинский_{user_id}_{datetime.now().timestamp()}')],
                [InlineKeyboardButton("English", callback_data=f'lang_English_{user_id}_{datetime.now().timestamp()}')],
                [InlineKeyboardButton("Deutsch", callback_data=f'lang_Deutsch_{user_id}_{datetime.now().timestamp()}')]
            ]
            await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        except telethon_errors.RPCError as e:
            await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода пароля 2FA для {name} (@{username}): {str(e)}", username)
        finally:
            await client_telethon.disconnect()
        return

    if str(user_id) not in users or 'language' not in users[str(user_id)]:
        await client_telethon.disconnect()
        return
    
    lang = users[str(user_id)]['language']
    texts = LANGUAGES[lang]
    
    if context.user_data.get('parsing_in_progress', False):
        await update.message.reply_text(texts['working_message'])
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
                    text=f"Пользователь {name} (@{username}) (ID: {user_id}) отправил хэш транзакции:\n{text}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отклонить", callback_data=f'reject_{user_id}_{datetime.now().timestamp()}')]])
                )
            except telegram_error.BadRequest as e:
                print(f"Ошибка отправки хэша администратору {admin_id}: {e}")
        await log_to_channel(context, f"Пользователь {name} (@{username}) отправил хэш транзакции: {text}", username)
        await update.message.reply_text(texts['payment_pending'])
        await client_telethon.disconnect()
        return

    if 'waiting_for_id' in context.user_data:
        if text:
            try:
                normalized_link = text
                if text.startswith('@'):
                    normalized_link = f"https://t.me/{text[1:]}"
                elif not text.startswith('http'):
                    normalized_link = f"https://t.me/{text}"
                
                parts = normalized_link.split('/')
                if len(parts) > 4 and parts[-1].isdigit():
                    chat_id = parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}'
                else:
                    chat_id = f'@{parts[-1]}' if not parts[-1].startswith('+') else parts[-1]
                
                entity = await client_telethon.get_entity(chat_id)
                msg = await update.message.reply_text(texts['id_result'].format(id=entity.id), reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(texts['close'], callback_data=f'close_id_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton(texts['continue_id'], callback_data=f'continue_id_{user_id}_{datetime.now().timestamp()}')]
                ]))
                await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=[{"type": "emoji", "emoji": "🎉"}])
                await log_to_channel(context, f"Пользователь {name} (@{username}) получил ID через ссылку: {entity.id}", username)
            except telethon_errors.RPCError as e:
                await update.message.reply_text(texts['rpc_error'].format(e=str(e)))
                await log_to_channel(context, f"Ошибка получения ID для {name} (@{username}): {str(e)}", username)
        elif update.message.forward_origin and hasattr(update.message.forward_origin, 'chat'):
            chat_id = update.message.forward_origin.chat.id
            msg = await update.message.reply_text(texts['id_result'].format(id=chat_id), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data=f'close_id_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton(texts['continue_id'], callback_data=f'continue_id_{user_id}_{datetime.now().timestamp()}')]
            ]))
            await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=[{"type": "emoji", "emoji": "🎉"}])
            await log_to_channel(context, f"Пользователь {name} (@{username}) получил ID чата: {chat_id}", username)
        del context.user_data['waiting_for_id']
        await client_telethon.disconnect()
        return
    
    if 'waiting_for_limit' in context.user_data:
        try:
            limit = int(text)
            max_limit = 15000 if users[str(user_id)]['subscription']['type'].startswith('Платная') else 150
            if context.user_data['parse_type'] == 'parse_post_commentators' and users[str(user_id)]['subscription']['type'].startswith('Платная'):
                max_limit = 10000
            elif context.user_data['parse_type'] == 'parse_authors' and users[str(user_id)]['subscription']['type'].startswith('Платная'):
                max_limit = 5000
            if limit <= 0 or limit > max_limit:
                await update.message.reply_text(texts['invalid_limit'].format(max_limit=max_limit), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data=f'skip_limit_{user_id}_{datetime.now().timestamp()}')]]))
                await client_telethon.disconnect()
                return
            context.user_data['limit'] = limit
            del context.user_data['waiting_for_limit']
            await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал количество **{limit}**", username)
            await ask_for_filters(update.message, context)
        except ValueError:
            await update.message.reply_text(texts['invalid_number'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data=f'skip_limit_{user_id}_{datetime.now().timestamp()}')]]))
            await client_telethon.disconnect()
        return

    if 'waiting_for_filters' in context.user_data:
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False, 'limit': context.user_data['limit']})
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
                else:
                    await update.message.reply_text(texts['invalid_link_suggestion'].format(link=link), 
                                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['fix_link'], callback_data=f'fix_link_{user_id}_{datetime.now().timestamp()}')]]))
                    context.user_data['last_input'] = text
                    await client_telethon.disconnect()
                    return
            
            if context.user_data['parse_type'] == 'parse_post_commentators':
                valid_links = [link for link in normalized_links if len(link.split('/')) > 4 and link.split('/')[-1].isdigit()]
                if not valid_links:
                    await update.message.reply_text(texts['invalid_link_suggestion'].format(link=text), 
                                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['fix_link'], callback_data=f'fix_link_{user_id}_{datetime.now().timestamp()}')]]))
                    context.user_data['last_input'] = text
                    await client_telethon.disconnect()
                    return
                context.user_data['links'] = valid_links
            else:
                context.user_data['links'] = normalized_links
            await ask_for_limit(update.message, context)
        elif update.message.forward_origin and hasattr(update.message.forward_origin, 'chat') and context.user_data['parse_type'] == 'parse_post_commentators':
            if update.message.forward_origin.chat.username:
                context.user_data['links'] = [f"https://t.me/{update.message.forward_origin.chat.username}/{update.message.forward_origin.message_id}"]
            else:
                context.user_data['links'] = [f"https://t.me/c/{str(update.message.forward_origin.chat.id).replace('-100', '')}/{update.message.forward_origin.message_id}"]
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
    if context.user_data['parse_type'] == 'parse_post_commentators' and is_paid:
        max_limit = 10000
    elif context.user_data['parse_type'] == 'parse_authors' and is_paid:
        max_limit = 5000
    keyboard = [
        [InlineKeyboardButton("100", callback_data=f'limit_100_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton("500", callback_data=f'limit_500_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton("1000", callback_data=f'limit_1000_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton(texts['skip'], callback_data=f'skip_limit_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton("Другое" if lang == 'Русский' else "Інше" if lang == 'Украинский' else "Other" if lang == 'English' else "Andere", callback_data=f'limit_custom_{user_id}_{datetime.now().timestamp()}')]
    ]
    if is_paid:
        keyboard.append([InlineKeyboardButton(texts['no_filter'], callback_data=f'no_filter_{user_id}_{datetime.now().timestamp()}')])
    await message.reply_text(texts['limit'].format(max_limit=max_limit), reply_markup=InlineKeyboardMarkup(keyboard))

# Запрос фильтров
async def ask_for_filters(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    keyboard = [
        [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data=f'filter_yes_{user_id}_{datetime.now().timestamp()}'),
         InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data=f'filter_no_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton(texts['skip'], callback_data=f'skip_filters_{user_id}_{datetime.now().timestamp()}')]
    ]
    context.user_data['waiting_for_filters'] = True
    context.user_data['current_filter'] = 'only_with_username'
    context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False, 'limit': context.user_data['limit']}
    await message.reply_text(texts['filter_username'], reply_markup=InlineKeyboardMarkup(keyboard))

# Функции парсинга
async def parse_commentators(group_link, limit):
    try:
        entity = await client_telethon.get_entity(group_link)
        commentators = set()
        async for message in client_telethon.iter_messages(entity, limit=limit * 2):  # Увеличиваем лимит для получения нужного количества уникальных пользователей
            if hasattr(message, 'sender_id') and message.sender_id:
                commentators.add(message.sender_id)
                if len(commentators) >= limit:
                    break
        
        data = []
        for commentator_id in list(commentators)[:limit]:  # Ограничиваем до указанного лимита
            try:
                participant = await client_telethon.get_entity(commentator_id)
                if isinstance(participant, tl.types.User):
                    data.append([
                        participant.id,
                        participant.username if participant.username else "",
                        participant.first_name if participant.first_name else "",
                        f"@{participant.username}" if participant.username else ""
                    ])
            except (telethon_errors.RPCError, ValueError) as e:
                print(f"Ошибка получения сущности для ID {commentator_id}: {str(e)}")
                continue
        return data
    except Exception as e:
        print(f"Ошибка в parse_commentators: {str(e)}")
        return []

async def parse_participants(group_link, limit):
    try:
        entity = await client_telethon.get_entity(group_link)
        participants = await client_telethon.get_participants(entity, limit=limit)  # Указываем точный лимит
        data = []
        for participant in participants[:limit]:  # Ограничиваем до указанного лимита
            if isinstance(participant, tl.types.User):
                data.append([
                    participant.id,
                    participant.username if participant.username else "",
                    participant.first_name if participant.first_name else "",
                    f"@{participant.username}" if participant.username else ""
                ])
        return data
    except Exception as e:
        print(f"Ошибка в parse_participants: {str(e)}")
        return []

async def parse_post_commentators(link, limit):
    try:
        parts = link.split('/')
        if len(parts) < 5 or not parts[-1].isdigit():
            return []
        chat_id = parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}'
        message_id = int(parts[-1])
        entity = await client_telethon.get_entity(chat_id)
        comments = await client_telethon(tl.functions.messages.GetRepliesRequest(
            peer=entity,
            msg_id=message_id,
            offset_id=0,
            offset_date=None,
            add_offset=0,
            limit=limit,  # Указываем точный лимит
            max_id=0,
            min_id=0,
            hash=0
        ))
        
        data = []
        for comment in comments.messages[:limit]:  # Ограничиваем до указанного лимита
            if hasattr(comment, 'from_id') and comment.from_id and isinstance(comment.from_id, tl.types.PeerUser):
                user_id = comment.from_id.user_id
                try:
                    user = await client_telethon.get_entity(user_id)
                    if isinstance(user, tl.types.User):
                        data.append([
                            user.id,
                            user.username if user.username else "",
                            user.first_name if user.first_name else "",
                            f"@{user.username}" if user.username else ""
                        ])
                except (telethon_errors.RPCError, ValueError) as e:
                    print(f"Ошибка получения сущности для ID {user_id}: {str(e)}")
                    continue
        return data
    except Exception as e:
        print(f"Ошибка в parse_post_commentators: {str(e)}")
        return []

async def parse_phone_contacts(group_link, limit):
    try:
        entity = await client_telethon.get_entity(group_link)
        participants = await client_telethon.get_participants(entity, limit=limit * 2)  # Увеличиваем лимит для фильтрации
        data = []
        for participant in participants:
            if isinstance(participant, tl.types.User) and participant.phone:
                data.append([
                    participant.id,
                    participant.username if participant.username else "",
                    participant.first_name if participant.first_name else "",
                    participant.phone
                ])
                if len(data) >= limit:
                    break
        return data[:limit]  # Ограничиваем до указанного лимита
    except Exception as e:
        print(f"Ошибка в parse_phone_contacts: {str(e)}")
        return []

async def parse_auth_access(link, context):
    user_id = context.user_data.get('user_id')
    username = context.user_data.get('username', 'Без username')
    name = load_users().get(str(user_id), {}).get('name', 'Неизвестно')
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    try:
        parts = link.split('/')
        chat_id = parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}'
        entity = await client_telethon.get_entity(chat_id)
        if hasattr(entity, 'participants_count'):
            await context.bot.send_message(chat_id=user_id, text=texts['auth_success'])
            await log_to_channel(context, f"Пользователь {name} (@{username}) успешно получил доступ к закрытому чату {chat_id}", username)
        else:
            await context.bot.send_message(chat_id=user_id, text=texts['auth_error'])
            await log_to_channel(context, f"Пользователь {name} (@{username}) не смог получить доступ к закрытому чату {chat_id}", username)
    except telethon_errors.RPCError as e:
        await context.bot.send_message(chat_id=user_id, text=texts['auth_error'])
        await log_to_channel(context, f"Ошибка авторизации для {name} (@{username}): {str(e)}", username)

async def process_parsing(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    username = message.from_user.username or "Без username"
    name = message.from_user.full_name or "Без имени"
    users = load_users()
    lang = users[str(user_id)]['language']
    texts = LANGUAGES[lang]
    subscription = users[str(user_id)]['subscription']
    
    if subscription['type'].startswith('Платная') and subscription['end']:
        if datetime.fromisoformat(subscription['end']) < datetime.now():
            update_user_data(user_id, name, context, subscription={'type': 'Бесплатная', 'end': None})
            await message.reply_text(
                "⚠️ Ваша платная подписка истекла. Теперь у вас бесплатная подписка с лимитом 150 пользователей на парсинг." if lang == 'Русский' else 
                "⚠️ Ваша платна підписка закінчилася. Тепер у вас безкоштовна підписка з лімітом 150 користувачів на парсинг." if lang == 'Украинский' else 
                "⚠️ Your paid subscription has expired. You now have a free subscription with a limit of 150 users for parsing." if lang == 'English' else 
                "⚠️ Dein bezahltes Abonnement ist abgelaufen. Du hast jetzt ein kostenloses Abonnement mit einem Limit von 150 Benutzern zum Parsen."
            )
            subscription = users[str(user_id)]['subscription']
    
    context.user_data['parsing_in_progress'] = True
    context.user_data['last_message_id'] = message.message_id
    
    loading_task = asyncio.create_task(show_loading_message(message, context))
    
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
                
                entity = await client_telethon.get_entity(normalized_link.split('/')[-2] if context.user_data['parse_type'] in ['parse_post_commentators', 'parse_auth_access'] else normalized_link)
                chat_name = entity.username if hasattr(entity, 'username') and entity.username else entity.title if hasattr(entity, 'title') else "unknown"
            except telethon_errors.ChannelPrivateError:
                context.user_data['parsing_done'] = True
                await message.reply_text(texts['no_access'].format(link=link))
                context.user_data['parsing_in_progress'] = False
                await log_to_channel(context, f"Пользователь {name} (@{username}) не имеет доступа к {link}", username)
                return
            except telethon_errors.RPCError as e:
                context.user_data['parsing_done'] = True
                await message.reply_text(texts['rpc_error'].format(e=str(e)))
                context.user_data['parsing_in_progress'] = False
                await log_to_channel(context, f"Ошибка RPC для {name} (@{username}): {str(e)}", username)
                return
            
            limit = check_parse_limit(user_id, context.user_data['limit'], context.user_data['parse_type'])
            if context.user_data['parse_type'] == 'parse_authors':
                data = await parse_commentators(normalized_link, limit)
            elif context.user_data['parse_type'] == 'parse_participants':
                data = await parse_participants(normalized_link, limit)
            elif context.user_data['parse_type'] == 'parse_post_commentators':
                data = await parse_post_commentators(normalized_link, limit)
            elif context.user_data['parse_type'] == 'parse_phone_contacts':
                data = await parse_phone_contacts(normalized_link, limit)
            elif context.user_data['parse_type'] == 'parse_auth_access':
                await parse_auth_access(normalized_link, context)
                context.user_data['parsing_done'] = True
                context.user_data['parsing_in_progress'] = False
                return
            
            all_data.extend(data)
        
        context.user_data['parsing_done'] = True
        
        if not all_data:
            await message.reply_text("Не удалось собрать данные. Возможно, чат пустой или доступ ограничен.")
            context.user_data['parsing_in_progress'] = False
            await log_to_channel(context, f"Пользователь {name} (@{username}) не собрал данные для {context.user_data['parse_type']} по ссылке {','.join(context.user_data['links'])}", username)
            return
        
        filtered_data = filter_data(all_data, context.user_data.get('filters', {'limit': limit}))
        stats = get_statistics(filtered_data)
        
        if context.user_data['parse_type'] == 'parse_phone_contacts':
            df = pd.DataFrame(filtered_data, columns=['ID', 'Username', 'First Name', 'Phone'])
            excel_file = await create_excel_in_memory(df, chat_name)
            vcf_file = create_vcf_file(df)
            
            excel_msg = await message.reply_document(
                document=excel_file,
                filename=f'phone_contacts_{chat_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx',
                caption=f"{texts['caption_phones']}\n\n{stats}"
            )
            vcf_msg = await message.reply_document(
                document=vcf_file,
                filename=f'phone_contacts_{chat_name}.vcf',
                caption=f"{texts['caption_phones']}\n\n{stats}"
            )
            # Отправка файлов в канал с логами
            await context.bot.send_document(
                chat_id=LOG_CHANNEL_ID,
                document=excel_file,
                filename=f'phone_contacts_{chat_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx',
                caption=f"Пользователь {name} (@{username}) получил файл с номерами телефонов: {chat_name}"
            )
            await context.bot.send_document(
                chat_id=LOG_CHANNEL_ID,
                document=vcf_file,
                filename=f'phone_contacts_{chat_name}.vcf',
                caption=f"Пользователь {name} (@{username}) получил VCF файл: {chat_name}"
            )
        else:
            excel_file = await create_excel_in_memory(filtered_data, chat_name)
            caption = (
                texts['caption_commentators'] if context.user_data['parse_type'] == 'parse_authors' else
                texts['caption_participants'] if context.user_data['parse_type'] == 'parse_participants' else
                texts['caption_post_commentators']
            )
            excel_msg = await message.reply_document(
                document=excel_file,
                filename=f"{context.user_data['parse_type']}_{chat_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                caption=f"{caption}\n\n{stats}"
            )
            # Отправка файла в канал с логами
            await context.bot.send_document(
                chat_id=LOG_CHANNEL_ID,
                document=excel_file,
                filename=f"{context.user_data['parse_type']}_{chat_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                caption=f"Пользователь {name} (@{username}) получил файл: {context.user_data['parse_type']} для {chat_name}"
            )
        
        update_user_data(user_id, name, context, requests=1)
        await log_to_channel(context, f"Пользователь {name} (@{username}) успешно завершил парсинг {context.user_data['parse_type']} для {chat_name}: {stats}", username)
        
        # Добавляем оценку парсинга
        keyboard = [
            [InlineKeyboardButton("⭐", callback_data=f'rate_1_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton("⭐⭐", callback_data=f'rate_2_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton("⭐⭐⭐", callback_data=f'rate_3_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton("⭐⭐⭐⭐", callback_data=f'rate_4_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton("⭐⭐⭐⭐⭐", callback_data=f'rate_5_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['close'], callback_data=f'close_rate_{user_id}_{datetime.now().timestamp()}')]
        ]
        await message.reply_text(texts['rate_parsing'], reply_markup=InlineKeyboardMarkup(keyboard))
    
    except telethon_errors.FloodWaitError as e:
        context.user_data['parsing_done'] = True
        await message.reply_text(texts['flood_error'].format(e=f"Слишком много запросов. Подождите {e.seconds} секунд"))
        await log_to_channel(context, f"Пользователь {name} (@{username}) получил ошибку FloodWait: {e.seconds} секунд", username)
    except Exception as e:
        context.user_data['parsing_done'] = True
        await message.reply_text(texts['rpc_error'].format(e=str(e)))
        await log_to_channel(context, f"Ошибка парсинга для {name} (@{username}): {str(e)}\n{traceback.format_exc()}", username)
    finally:
        context.user_data['parsing_in_progress'] = False
        if client_telethon.is_connected():
            await client_telethon.disconnect()
        loading_task.cancel()

# Обработчик кнопок
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    username = query.from_user.username or "Без username"
    name = query.from_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    # Проверка актуальности кнопки
    data_parts = query.data.split('_')
    if len(data_parts) < 3:
        await query.answer("Эта кнопка больше не активна.", show_alert=True)
        return
    
    action, user_id_from_data, timestamp = data_parts[0], data_parts[-2], float(data_parts[-1])
    if user_id_from_data != str(user_id) or (datetime.now().timestamp() - timestamp > 3600):  # Кнопка устаревает через 1 час
        await query.answer("Эта кнопка больше не активна.", show_alert=True)
        return
    
    if query.data.startswith('lang_'):
        lang = data_parts[1]
        update_user_data(user_id, name, context, lang=lang)
        await query.edit_message_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data=f'subscribed_{user_id}_{datetime.now().timestamp()}')]]))
        await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал язык **{lang}**", username)
    
    elif query.data.startswith('subscribed_'):
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
    
    elif query.data.startswith('identifiers_'):
        await query.edit_message_text(texts['identifiers'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
        context.user_data['waiting_for_id'] = True
        await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал **Идентификаторы**", username)
    
    elif query.data.startswith('parser_'):
        keyboard = [
            [InlineKeyboardButton("Авторы сообщений" if lang == 'Русский' else "Автори повідомлень" if lang == 'Украинский' else "Message authors" if lang == 'English' else "Nachrichtenautoren", callback_data=f'parse_authors_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton("(!)", callback_data=f'info_parse_authors_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton("Участники чата" if lang == 'Русский' else "Учасники чату" if lang == 'Украинский' else "Chat participants" if lang == 'English' else "Chat-Teilnehmer", callback_data=f'parse_participants_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton("(!)", callback_data=f'info_parse_participants_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton("Комментаторы поста" if lang == 'Русский' else "Коментатори посту" if lang == 'Украинский' else "Post commentators" if lang == 'English' else "Beitragskommentatoren", callback_data=f'parse_post_commentators_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton("(!)", callback_data=f'info_parse_post_commentators_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['phone_contacts'], callback_data=f'parse_phone_contacts_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton("(!)", callback_data=f'info_parse_phone_contacts_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['auth_access'], callback_data=f'parse_auth_access_{user_id}_{datetime.now().timestamp()}'), InlineKeyboardButton("(!)", callback_data=f'info_parse_auth_access_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]
        ]
        await query.edit_message_text(texts['parser'], reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал **Сбор данных / Парсер**", username)
    
    elif query.data.startswith('parse_'):
        context.user_data['parse_type'] = '_'.join(data_parts[:-2])
        parse_type_text = {
            'parse_authors': 'Авторы сообщений',
            'parse_participants': 'Участники чата',
            'parse_post_commentators': 'Комментаторы поста',
            'parse_phone_contacts': 'Сбор номеров телефонов и ФИО',
            'parse_auth_access': 'Авторизация для закрытых чатов'
        }.get(context.user_data['parse_type'], 'Неизвестно')
        if context.user_data['parse_type'] in ['parse_authors', 'parse_participants', 'parse_phone_contacts', 'parse_auth_access']:
            await query.edit_message_text(texts['link_group'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
        elif context.user_data['parse_type'] == 'parse_post_commentators':
            await query.edit_message_text(texts['link_post'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
        await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал **{parse_type_text}**", username)
    
    elif query.data.startswith('limit_'):
        if data_parts[1] == 'custom':
            context.user_data['waiting_for_limit'] = True
            await query.edit_message_text(texts['limit'].format(max_limit=15000 if users[str(user_id)]['subscription']['type'].startswith('Платная') else 150), 
                                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data=f'skip_limit_{user_id}_{datetime.now().timestamp()}')]]))
        else:
            context.user_data['limit'] = int(data_parts[1])
            await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал количество **{context.user_data['limit']}**", username)
            await ask_for_filters(query.message, context)
    
    elif query.data.startswith('skip_limit_'):
        context.user_data['limit'] = 150 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 1000
        if 'waiting_for_limit' in context.user_data:
            del context.user_data['waiting_for_limit']
        await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал количество **{context.user_data['limit']}** (по умолчанию)", username)
        await ask_for_filters(query.message, context)
    
    elif query.data.startswith('filter_'):
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False, 'limit': context.user_data['limit']})
        current_filter = context.user_data.get('current_filter')
        if data_parts[1] == 'yes':
            filters[current_filter] = True
        if current_filter == 'only_with_username':
            context.user_data['current_filter'] = 'exclude_bots'
            keyboard = [[InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data=f'filter_yes_{user_id}_{datetime.now().timestamp()}'),
                        InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data=f'filter_no_{user_id}_{datetime.now().timestamp()}')],
                        [InlineKeyboardButton(texts['skip'], callback_data=f'skip_filters_{user_id}_{datetime.now().timestamp()}')]]
            await query.edit_message_text(texts['filter_bots'], reply_markup=InlineKeyboardMarkup(keyboard))
        elif current_filter == 'exclude_bots':
            context.user_data['current_filter'] = 'only_active'
            keyboard = [[InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data=f'filter_yes_{user_id}_{datetime.now().timestamp()}'),
                        InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data=f'filter_no_{user_id}_{datetime.now().timestamp()}')],
                        [InlineKeyboardButton(texts['skip'], callback_data=f'skip_filters_{user_id}_{datetime.now().timestamp()}')]]
            await query.edit_message_text(texts['filter_active'], reply_markup=InlineKeyboardMarkup(keyboard))
        elif current_filter == 'only_active':
            del context.user_data['current_filter']
            context.user_data['filters'] = filters
            await process_parsing(query.message, context)
    
    elif query.data.startswith('skip_filters_'):
        del context.user_data['current_filter']
        await process_parsing(query.message, context)
    
    elif query.data.startswith('no_filter_'):
        context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False, 'limit': context.user_data['limit']}
        await process_parsing(query.message, context)
    
    elif query.data.startswith('subscribe_'):
        keyboard = [
            [InlineKeyboardButton(texts['subscription_1h'], callback_data=f'sub_1h_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['subscription_3d'], callback_data=f'sub_3d_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['subscription_7d'], callback_data=f'sub_7d_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]
        ]
        await query.edit_message_text("Выберите тип подписки:", reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал **Оформить подписку**", username)
    
    elif query.data.startswith('sub_'):
        sub_type = data_parts[1]
        amount = {'1h': 2, '3d': 5, '7d': 7}[sub_type]
        keyboard = [
            [InlineKeyboardButton(texts['payment_paid'], callback_data=f'paid_{sub_type}_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['payment_cancel'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]
        ]
        await query.edit_message_text(texts['payment_wallet'].format(amount=amount, address=TON_WALLET_ADDRESS), reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал подписку **{sub_type}**", username)
    
    elif query.data.startswith('paid_'):
        sub_type = data_parts[1]
        context.user_data['waiting_for_hash'] = True
        context.user_data['sub_type'] = sub_type
        await query.edit_message_text(texts['payment_hash'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_cancel'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
    
    elif query.data.startswith('close_id_'):
        del context.user_data['waiting_for_id']
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
    
    elif query.data.startswith('continue_id_'):
        context.user_data['waiting_for_id'] = True
        await query.edit_message_text(texts['identifiers'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
    
    elif query.data.startswith('close_menu_') or query.data.startswith('payment_cancel_'):
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
    
    elif query.data.startswith('update_menu_'):
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
    
    elif query.data.startswith('requisites_'):
        await query.edit_message_text(texts['requisites'].format(support=SUPPORT_USERNAME), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
        await log_to_channel(context, f"Пользователь {name} (@{username}) запросил **Реквизиты**", username)
    
    elif query.data.startswith('logs_channel_') and str(user_id) in ADMIN_IDS:
        await query.edit_message_text(texts['logs_channel'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
        await log_to_channel(context, f"Пользователь {name} (@{username}) запросил **Канал с логами**", username)
    
    elif query.data.startswith('reject_') and str(user_id) in ADMIN_IDS:
        rejected_user_id = data_parts[1]
        rejected_username = load_users().get(str(rejected_user_id), {}).get('name', 'Неизвестно')
        rejected_lang = load_users().get(str(rejected_user_id), {}).get('language', 'Русский')
        await context.bot.send_message(chat_id=rejected_user_id, text=LANGUAGES[rejected_lang]['payment_error'])
        await query.edit_message_text(f"Транзакция пользователя {rejected_username} (ID: {rejected_user_id}) отклонена.", reply_markup=InlineKeyboardMarkup([]))
        await log_to_channel(context, f"Администратор {name} (@{username}) отклонил транзакцию пользователя {rejected_username} (ID: {rejected_user_id})", username)
    
    elif query.data.startswith('rate_'):
        rating = int(data_parts[1])
        await log_to_channel(context, f"Пользователь {name} (@{username}) оценил парсинг: **{rating} звёзд**", username)
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.message.reply_text(texts['home_cmd'], reply_markup=menu_keyboard)
        await query.message.delete()
    
    elif query.data.startswith('close_rate_'):
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.message.reply_text(texts['home_cmd'], reply_markup=menu_keyboard)
        await query.message.delete()
    
    elif query.data.startswith('info_'):
        info_type = '_'.join(data_parts[1:-2])
        info_text = texts[f'info_{info_type}']
        await query.message.reply_text(info_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
        await log_to_channel(context, f"Пользователь {name} (@{username}) запросил информацию о **{info_type.replace('parse_', '').replace('_', ' ')}**", username)

# Главная функция
def main():
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("home", home))
    application.add_handler(CommandHandler("info", info))
    application.add_handler(CommandHandler("language", language))
    application.add_handler(CommandHandler("set_plan", set_plan))
    application.add_handler(CommandHandler("remove_plan", remove_plan))
    application.add_handler(CommandHandler("note", note))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(button))
    
    application.run_polling()

if __name__ == '__main__':
    main()
