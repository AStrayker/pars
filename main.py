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
        'limit': 'Сколько пользователей парсить? Укажи число (доступно: {max_limit}).',
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
        'caption_commentators': 'Вот ваш файл с авторами сообщений.',
        'caption_participants': 'Вот ваш файл с участниками.',
        'caption_post_commentators': 'Вот ваш файл с комментаторами поста.',
        'caption_phones': 'Вот ваш файл с номерами телефонов и ФИО (Excel и VCF).',
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
        'no_filter': 'Не применять',
        'phone_contacts': 'Сбор номеров телефонов и ФИО',
        'phone_authors': 'Парсинг телефонов среди Авторов',
        'phone_participants': 'Парсинг телефонов среди Участников',
        'phone_commentators': 'Парсинг телефонов среди Комментаторов',
        'auth_access': 'Авторизация для закрытых чатов',
        'auth_request': 'Для доступа к закрытым чатам добавьте бота в чат как администратора или отправьте ссылку на закрытый чат.',
        'auth_success': 'Доступ к закрытому чату успешно предоставлен!',
        'auth_error': 'Не удалось получить доступ. Убедитесь, что бот добавлен как администратор или чат публичный.',
        'note_cmd': 'Заметка успешно сохранена (бот не будет реагировать).',
        'home_cmd': 'Вы вернулись в главное меню.',
        'info_cmd': 'Это бот для парсинга Telegram.\nВозможности:\n- Сбор ID\n- Парсинг участников\n- Парсинг комментаторов\n- Сбор контактов\nДля поддержки: {support}',
        'working_message': 'Всё нормально, мы ещё работаем...',
        'invalid_link_suggestion': 'Ссылка "{link}" неверная. Возможно, вы имели в виду что-то вроде https://t.me/group_name или https://t.me/channel_name/12345? Попробуйте снова.',
        'rate_parsing': 'Оцените качество парсинга:',
        'parsing_error': 'Ошибка: неверный тип ссылки для выбранного парсинга. Попробуйте другой раздел:\n',
        'chat_closed': 'Ошибка: чат {link} закрыт или недоступен.',
        'participants_closed': 'Ошибка: список участников в {link} закрыт.',
    },
    # Добавьте переводы для других языков по аналогии, если нужно
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
        'limit': 'How many users to parse? Enter a number (available: {max_limit}).',
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
        'caption_commentators': 'Here is your file with message authors.',
        'caption_participants': 'Here is your file with participants.',
        'caption_post_commentators': 'Here is your file with post commentators.',
        'caption_phones': 'Here is your file with phone numbers and full names (Excel and VCF).',
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
        'no_filter': 'Do not apply',
        'phone_contacts': 'Collect phone numbers and full names',
        'phone_authors': 'Parsing phones among Authors',
        'phone_participants': 'Parsing phones among Participants',
        'phone_commentators': 'Parsing phones among Commentators',
        'auth_access': 'Authorize for private chats',
        'auth_request': 'To access private chats, add the bot as an admin or send a link to a private chat.',
        'auth_success': 'Access to the private chat successfully granted!',
        'auth_error': 'Could not gain access. Ensure the bot is added as an admin or the chat is public.',
        'note_cmd': 'Note successfully saved (bot will not respond).',
        'home_cmd': 'You returned to the main menu.',
        'info_cmd': 'This is a Telegram parsing bot.\nFeatures:\n- ID collection\n- Participants parsing\n- Commentators parsing\n- Contact collection\nFor support: {support}',
        'working_message': 'Everything is fine, we are still working...',
        'invalid_link_suggestion': 'The link "{link}" is invalid. Did you mean something like https://t.me/group_name or https://t.me/channel_name/12345? Try again.',
        'rate_parsing': 'Rate the parsing quality:',
        'parsing_error': 'Error: invalid link type for the selected parsing. Try another section:\n',
        'chat_closed': 'Error: chat {link} is closed or inaccessible.',
        'participants_closed': 'Error: participant list in {link} is closed.',
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
    max_requests = 5 if subscription['type'] == 'Бесплатная' else 10
    return daily_requests['count'] < max_requests, 24 - (now - last_reset).seconds // 3600

# Проверка лимита парсинга
def check_parse_limit(user_id, parse_type):
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
        return 150
    elif parse_type == 'parse_authors' or parse_type == 'parse_phone_authors':
        return 5000
    elif parse_type == 'parse_participants' or parse_type == 'parse_phone_participants':
        return 15000
    elif parse_type == 'parse_post_commentators' or parse_type == 'parse_phone_commentators':
        return 10000
    else:
        return 15000

# Создание файла Excel
async def create_excel_in_memory(data, chat_name=""):
    df = pd.DataFrame(data, columns=['ID', 'Username', 'First Name', 'Nickname'] if len(data[0]) == 4 else ['ID', 'Username', 'First Name', 'Phone'])
    df['Nickname'] = '@' + df['Username'].astype(str)
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
            vcard.add('fn').value = f"{row['First Name']}"
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
    return filtered_data[:filters.get('limit', len(filtered_data))]

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
    name = user_data.get('name', 'Неизвестно')
    limit_left, _ = check_request_limit(user_id)
    limit_display = 5 - user_data.get('daily_requests', {}).get('count', 0) if sub_type == 'Бесплатная' else 10 - user_data.get('daily_requests', {}).get('count', 0)
    
    is_admin = user_id_str in ADMIN_IDS
    
    buttons = [
        [InlineKeyboardButton("Идентификаторы" if lang == 'Русский' else "Identifiers" if lang == 'English' else "Идентификаторы", callback_data=f'identifiers_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton("Сбор данных / Парсер" if lang == 'Русский' else "Data collection / Parser" if lang == 'English' else "Сбор данных / Парсер", callback_data=f'parser_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton(texts['subscribe_button'], callback_data=f'subscribe_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton(f"{texts['support'].format(support=SUPPORT_USERNAME)}", url=f"https://t.me/{SUPPORT_USERNAME[1:]}")],
        [InlineKeyboardButton("Реквизиты" if lang == 'Русский' else "Requisites" if lang == 'English' else "Реквизиты", callback_data=f'requisites_{user_id}_{datetime.now().timestamp()}')]
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton("Канал с логами" if lang == 'Русский' else "Logs channel" if lang == 'English' else "Канал с логами", callback_data=f'logs_channel_{user_id}_{datetime.now().timestamp()}')])
    
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
                [InlineKeyboardButton("English", callback_data=f'lang_English_{user_id}_{datetime.now().timestamp()}')]
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
        [InlineKeyboardButton("English", callback_data=f'lang_English_{user_id}_{datetime.now().timestamp()}')]
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
    loading_msg = "Подождите..." if lang == 'Русский' else "Please wait..." if lang == 'English' else "Подождите..."
    working_msg = texts['working_message']
    
    loading_message = await message.reply_text(loading_msg)
    context.user_data['loading_message_id'] = loading_message.message_id
    context.user_data['working_message_id'] = None
    
    dots = 1
    elapsed_time = 0
    working_sent = False
    start_time = datetime.now()
    
    while 'parsing_done' not in context.user_data or elapsed_time < 2:
        if 'parsing_done' in context.user_data and (datetime.now() - start_time).total_seconds() >= 2:
            break
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
        await asyncio.sleep(0.5)
        elapsed_time = (datetime.now() - start_time).total_seconds()
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

# Проверка валидности ссылки для парсинга
async def validate_link_for_parsing(link, parse_type):
    try:
        await client_telethon.connect()
        if parse_type in ['parse_post_commentators', 'parse_phone_commentators']:
            parts = link.split('/')
            if len(parts) < 5 or not parts[-1].isdigit():
                return False, "post_link_invalid"
            entity = await client_telethon.get_entity(parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}')
        else:
            entity = await client_telethon.get_entity(link.split('/')[-1] if link.startswith('https://t.me/') else link[1:] if link.startswith('@') else link)
        
        if not entity:
            return False, "chat_closed"
        
        if parse_type in ['parse_participants', 'parse_phone_participants']:
            participants = await client_telethon.get_participants(entity, limit=1)
            if not participants:
                return False, "participants_closed"
        
        return True, None
    except telethon_errors.ChannelPrivateError:
        return False, "chat_closed"
    except telethon_errors.RPCError as e:
        return False, f"rpc_error: {str(e)}"
    finally:
        if client_telethon.is_connected():
            await client_telethon.disconnect()

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
                [InlineKeyboardButton("English", callback_data=f'lang_English_{user_id}_{datetime.now().timestamp()}')]
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
                [InlineKeyboardButton("English", callback_data=f'lang_English_{user_id}_{datetime.now().timestamp()}')]
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
            max_limit = check_parse_limit(user_id, context.user_data['parse_type'])
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
        if 'да' in text.lower() or 'yes' in text.lower():
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
                    normalized_link = link
                elif link.startswith('@'):
                    normalized_link = f"https://t.me/{link[1:]}"
                elif not link.startswith('http'):
                    normalized_link = f"https://t.me/{link}"
                else:
                    await update.message.reply_text(texts['invalid_link_suggestion'].format(link=link), 
                                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['fix_link'], callback_data=f'fix_link_{user_id}_{datetime.now().timestamp()}')]]))
                    context.user_data['last_input'] = text
                    await client_telethon.disconnect()
                    return
                is_valid, error = await validate_link_for_parsing(normalized_link, context.user_data['parse_type'])
                if not is_valid:
                    if error == "post_link_invalid":
                        error_msg = texts['parsing_error'] + "\n- Авторы: /parse_authors\n- Участники: /parse_participants\n- Телефоны: /parse_phone_contacts"
                        keyboard = [
                            [InlineKeyboardButton("Авторы", callback_data=f'parse_authors_{user_id}_{datetime.now().timestamp()}')],
                            [InlineKeyboardButton("Участники", callback_data=f'parse_participants_{user_id}_{datetime.now().timestamp()}')],
                            [InlineKeyboardButton("Телефоны", callback_data=f'parse_phone_contacts_{user_id}_{datetime.now().timestamp()}')],
                            [InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]
                        ]
                        await update.message.reply_text(error_msg, reply_markup=InlineKeyboardMarkup(keyboard))
                    elif error == "chat_closed":
                        await update.message.reply_text(texts['chat_closed'].format(link=normalized_link))
                    elif error == "participants_closed":
                        await update.message.reply_text(texts['participants_closed'].format(link=normalized_link))
                    else:
                        await update.message.reply_text(texts['rpc_error'].format(e=error))
                    await client_telethon.disconnect()
                    return
                normalized_links.append(normalized_link)
            
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
    max_limit = check_parse_limit(user_id, context.user_data['parse_type'])
    keyboard = [
        [InlineKeyboardButton(texts['skip'], callback_data=f'skip_limit_{user_id}_{datetime.now().timestamp()}')],
        [InlineKeyboardButton(texts['no_filter'], callback_data=f'no_filter_{user_id}_{datetime.now().timestamp()}')]
    ]
    await message.reply_text(texts['limit'].format(max_limit=max_limit), reply_markup=InlineKeyboardMarkup(keyboard))
    context.user_data['waiting_for_limit'] = True

# Запрос фильтров
async def ask_for_filters(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    keyboard = [
        [InlineKeyboardButton("Да" if lang == 'Русский' else "Yes" if lang == 'English' else "Да", callback_data=f'filter_yes_{user_id}_{datetime.now().timestamp()}'),
         InlineKeyboardButton("Нет" if lang == 'Русский' else "No" if lang == 'English' else "Нет", callback_data=f'filter_no_{user_id}_{datetime.now().timestamp()}')],
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
        async for message in client_telethon.iter_messages(entity, limit=limit * 2):
            if hasattr(message, 'sender_id') and message.sender_id:
                commentators.add(message.sender_id)
                if len(commentators) >= limit:
                    break
        
        data = []
        for commentator_id in list(commentators)[:limit]:
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
        participants = await client_telethon.get_participants(entity, limit=limit)
        data = []
        for participant in participants[:limit]:
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
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))
        
        data = []
        for comment in comments.messages[:limit]:
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

async def parse_phone_contacts(group_link, limit, parse_type):
    try:
        entity = await client_telethon.get_entity(group_link)
        if parse_type == 'parse_phone_authors':
            commentators = set()
            async for message in client_telethon.iter_messages(entity, limit=limit * 2):
                if hasattr(message, 'sender_id') and message.sender_id:
                    commentators.add(message.sender_id)
                    if len(commentators) >= limit:
                        break
            users_to_check = list(commentators)[:limit]
        elif parse_type == 'parse_phone_participants':
            participants = await client_telethon.get_participants(entity, limit=limit * 2)
            users_to_check = [p.id for p in participants[:limit]]
        elif parse_type == 'parse_phone_commentators':
            parts = group_link.split('/')
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
                limit=limit * 2,
                max_id=0,
                min_id=0,
                hash=0
            ))
            users_to_check = [c.from_id.user_id for c in comments.messages if hasattr(c, 'from_id') and c.from_id and isinstance(c.from_id, tl.types.PeerUser)][:limit]
        else:
            return []

        data = []
        for user_id in users_to_check:
            try:
                user = await client_telethon.get_entity(user_id)
                if isinstance(user, tl.types.User) and user.phone:
                    data.append([
                        user.id,
                        user.username if user.username else "",
                        user.first_name if user.first_name else "",
                        user.phone
                    ])
                    if len(data) >= limit:
                        break
            except (telethon_errors.RPCError, ValueError):
                continue
        return data[:limit]
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
                "⚠️ Your paid subscription has expired. You now have a free subscription with a limit of 150 users for parsing." if lang == 'English' else 
                "⚠️ Ваша платная подписка истекла."
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
                
                entity = await client_telethon.get_entity(normalized_link.split('/')[-2] if context.user_data['parse_type'] in ['parse_post_commentators', 'parse_phone_commentators', 'parse_auth_access'] else normalized_link)
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
            
            limit = context.user_data['limit']
            if context.user_data['parse_type'] == 'parse_authors':
                data = await parse_commentators(normalized_link, limit)
            elif context.user_data['parse_type'] == 'parse_participants':
                data = await parse_participants(normalized_link, limit)
            elif context.user_data['parse_type'] == 'parse_post_commentators':
                data = await parse_post_commentators(normalized_link, limit)
            elif context.user_data['parse_type'] in ['parse_phone_authors', 'parse_phone_participants', 'parse_phone_commentators']:
                data = await parse_phone_contacts(normalized_link, limit, context.user_data['parse_type'])
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
        
        if context.user_data['parse_type'].startswith('parse_phone_'):
            df = pd.DataFrame(filtered_data, columns=['ID', 'Username', 'First Name', 'Phone'])
            excel_file = await create_excel_in_memory(df, chat_name)
            excel_file.seek(0)
            vcf_file = create_vcf_file(df)
            vcf_file.seek(0)
            
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
            
            excel_file.seek(0)
            if excel_file.getbuffer().nbytes > 0:
                await context.bot.send_document(
                    chat_id=LOG_CHANNEL_ID,
                    document=excel_file,
                    filename=f'phone_contacts_{chat_name}_{datetime.now

().strftime("%Y%m%d_%H%M%S")}.xlsx',
                    caption=f"Пользователь {name} (@{username}) получил файл с номерами телефонов: {chat_name}"
                )
            vcf_file.seek(0)
            if vcf_file.getbuffer().nbytes > 0:
                await context.bot.send_document(
                    chat_id=LOG_CHANNEL_ID,
                    document=vcf_file,
                    filename=f'phone_contacts_{chat_name}.vcf',
                    caption=f"Пользователь {name} (@{username}) получил VCF файл: {chat_name}"
                )
        else:
            excel_file = await create_excel_in_memory(filtered_data, chat_name)
            excel_file.seek(0)
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
            
            excel_file.seek(0)
            if excel_file.getbuffer().nbytes > 0:
                await context.bot.send_document(
                    chat_id=LOG_CHANNEL_ID,
                    document=excel_file,
                    filename=f"{context.user_data['parse_type']}_{chat_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    caption=f"Пользователь {name} (@{username}) получил файл: {context.user_data['parse_type']} для {chat_name}"
                )
        
        update_user_data(user_id, name, context, requests=1)
        await log_to_channel(context, f"Пользователь {name} (@{username}) успешно завершил парсинг {context.user_data['parse_type']} для {chat_name}: {stats}", username)
        
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
    
    data_parts = query.data.split('_')
    if len(data_parts) < 3:
        await query.answer("Эта кнопка больше не активна.", show_alert=True)
        return
    
    action, user_id_from_data, timestamp = data_parts[0], data_parts[-2], float(data_parts[-1])
    if user_id_from_data != str(user_id) or (datetime.now().timestamp() - timestamp > 3600):
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
            [InlineKeyboardButton("Авторы сообщений" if lang == 'Русский' else "Message authors" if lang == 'English' else "Авторы сообщений", callback_data=f'parse_authors_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton("Участники чата" if lang == 'Русский' else "Chat participants" if lang == 'English' else "Участники чата", callback_data=f'parse_participants_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton("Комментаторы поста" if lang == 'Русский' else "Post commentators" if lang == 'English' else "Комментаторы поста", callback_data=f'parse_post_commentators_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['phone_contacts'], callback_data=f'parse_phone_contacts_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['auth_access'], callback_data=f'parse_auth_access_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]
        ]
        await query.edit_message_text(texts['parser'], reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал **Сбор данных / Парсер**", username)
    
    elif query.data.startswith('parse_phone_contacts_'):
        keyboard = [
            [InlineKeyboardButton(texts['phone_authors'], callback_data=f'parse_phone_authors_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['phone_participants'], callback_data=f'parse_phone_participants_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['phone_commentators'], callback_data=f'parse_phone_commentators_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]
        ]
        await query.edit_message_text(texts['phone_contacts'], reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал **Сбор номеров телефонов и ФИО**", username)
    
    elif query.data.startswith('parse_'):
        context.user_data['parse_type'] = '_'.join(data_parts[:-2])
        parse_type_text = {
            'parse_authors': 'Авторы сообщений',
            'parse_participants': 'Участники чата',
            'parse_post_commentators': 'Комментаторы поста',
            'parse_phone_authors': 'Парсинг телефонов среди Авторов',
            'parse_phone_participants': 'Парсинг телефонов среди Участников',
            'parse_phone_commentators': 'Парсинг телефонов среди Комментаторов',
            'parse_auth_access': 'Авторизация для закрытых чатов'
        }.get(context.user_data['parse_type'], 'Неизвестно')
        if context.user_data['parse_type'] in ['parse_authors', 'parse_participants', 'parse_phone_authors', 'parse_phone_participants', 'parse_auth_access']:
            await query.edit_message_text(texts['link_group'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
        elif context.user_data['parse_type'] in ['parse_post_commentators', 'parse_phone_commentators']:
            await query.edit_message_text(texts['link_post'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
        await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал **{parse_type_text}**", username)
    
    elif query.data.startswith('skip_limit_'):
        context.user_data['limit'] = check_parse_limit(user_id, context.user_data['parse_type'])
        del context.user_data['waiting_for_limit']
        await ask_for_filters(query.message, context)
        await query.delete_message()
    
    elif query.data.startswith('no_filter_'):
        context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False, 'limit': context.user_data['limit']}
        del context.user_data['waiting_for_limit']
        await process_parsing(query.message, context)
        await query.delete_message()
    
    elif query.data.startswith('filter_yes_'):
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False, 'limit': context.user_data['limit']})
        filters[context.user_data['current_filter']] = True
        context.user_data['filters'] = filters
        del context.user_data['waiting_for_filters']
        next_filter = {'only_with_username': 'exclude_bots', 'exclude_bots': 'only_active', 'only_active': None}
        if next_filter[context.user_data['current_filter']]:
            context.user_data['current_filter'] = next_filter[context.user_data['current_filter']]
            context.user_data['waiting_for_filters'] = True
            keyboard = [
                [InlineKeyboardButton("Да" if lang == 'Русский' else "Yes" if lang == 'English' else "Да", callback_data=f'filter_yes_{user_id}_{datetime.now().timestamp()}'),
                 InlineKeyboardButton("Нет" if lang == 'Русский' else "No" if lang == 'English' else "Нет", callback_data=f'filter_no_{user_id}_{datetime.now().timestamp()}')],
                [InlineKeyboardButton(texts['skip'], callback_data=f'skip_filters_{user_id}_{datetime.now().timestamp()}')]
            ]
            if context.user_data['current_filter'] == 'exclude_bots':
                await query.edit_message_text(texts['filter_bots'], reply_markup=InlineKeyboardMarkup(keyboard))
            elif context.user_data['current_filter'] == 'only_active':
                await query.edit_message_text(texts['filter_active'], reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await process_parsing(query.message, context)
            await query.delete_message()
    
    elif query.data.startswith('filter_no_'):
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False, 'limit': context.user_data['limit']})
        filters[context.user_data['current_filter']] = False
        context.user_data['filters'] = filters
        del context.user_data['waiting_for_filters']
        next_filter = {'only_with_username': 'exclude_bots', 'exclude_bots': 'only_active', 'only_active': None}
        if next_filter[context.user_data['current_filter']]:
            context.user_data['current_filter'] = next_filter[context.user_data['current_filter']]
            context.user_data['waiting_for_filters'] = True
            keyboard = [
                [InlineKeyboardButton("Да" if lang == 'Русский' else "Yes" if lang == 'English' else "Да", callback_data=f'filter_yes_{user_id}_{datetime.now().timestamp()}'),
                 InlineKeyboardButton("Нет" if lang == 'Русский' else "No" if lang == 'English' else "Нет", callback_data=f'filter_no_{user_id}_{datetime.now().timestamp()}')],
                [InlineKeyboardButton(texts['skip'], callback_data=f'skip_filters_{user_id}_{datetime.now().timestamp()}')]
            ]
            if context.user_data['current_filter'] == 'exclude_bots':
                await query.edit_message_text(texts['filter_bots'], reply_markup=InlineKeyboardMarkup(keyboard))
            elif context.user_data['current_filter'] == 'only_active':
                await query.edit_message_text(texts['filter_active'], reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await process_parsing(query.message, context)
            await query.delete_message()
    
    elif query.data.startswith('skip_filters_'):
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False, 'limit': context.user_data['limit']})
        filters[context.user_data['current_filter']] = False
        context.user_data['filters'] = filters
        del context.user_data['waiting_for_filters']
        del context.user_data['current_filter']
        await process_parsing(query.message, context)
        await query.delete_message()
    
    elif query.data.startswith('subscribe_'):
        keyboard = [
            [InlineKeyboardButton(texts['subscription_1h'], callback_data=f'sub_1h_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['subscription_3d'], callback_data=f'sub_3d_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['subscription_7d'], callback_data=f'sub_7d_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]
        ]
        await query.edit_message_text(texts['subscribe_button'], reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал **Оформить подписку**", username)
    
    elif query.data.startswith('sub_'):
        sub_type = data_parts[1]
        amount = {'1h': 2, '3d': 5, '7d': 7}[sub_type]
        context.user_data['sub_type'] = sub_type
        keyboard = [
            [InlineKeyboardButton(texts['payment_paid'], callback_data=f'paid_{user_id}_{datetime.now().timestamp()}')],
            [InlineKeyboardButton(texts['payment_cancel'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]
        ]
        await query.edit_message_text(texts['payment_wallet'].format(amount=amount, address=TON_WALLET_ADDRESS), reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Пользователь {name} (@{username}) выбрал подписку **{sub_type}**", username)
    
    elif query.data.startswith('paid_'):
        context.user_data['waiting_for_hash'] = True
        await query.edit_message_text(texts['payment_hash'])
    
    elif query.data.startswith('requisites_'):
        await query.edit_message_text(texts['requisites'].format(support=SUPPORT_USERNAME), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
        await log_to_channel(context, f"Пользователь {name} (@{username}) запросил **Реквизиты**", username)
    
    elif query.data.startswith('logs_channel_'):
        if str(user_id) in ADMIN_IDS:
            await query.edit_message_text(texts['logs_channel'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
            await log_to_channel(context, f"Администратор {name} (@{username}) запросил **Канал с логами**", username)
        else:
            await query.answer("Доступно только администраторам.", show_alert=True)
    
    elif query.data.startswith('close_menu_'):
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
    
    elif query.data.startswith('close_id_'):
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
    
    elif query.data.startswith('continue_id_'):
        await query.edit_message_text(texts['identifiers'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))
        context.user_data['waiting_for_id'] = True
    
    elif query.data.startswith('update_menu_'):
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
    
    elif query.data.startswith('rate_'):
        rating = int(data_parts[1])
        await log_to_channel(context, f"Пользователь {name} (@{username}) оценил парсинг: {rating} звёзд", username)
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
    
    elif query.data.startswith('close_rate_'):
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
    
    elif query.data.startswith('reject_'):
        if str(user_id) not in ADMIN_IDS:
            await query.answer("Доступно только администраторам.", show_alert=True)
            return
        rejected_user_id = data_parts[1]
        await context.bot.send_message(chat_id=rejected_user_id, text=texts['payment_error'])
        await log_to_channel(context, f"Администратор {name} (@{username}) отклонил транзакцию пользователя ID {rejected_user_id}", username)
        await query.delete_message()
    
    elif query.data.startswith('fix_link_'):
        await query.edit_message_text(texts['retry_link'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data=f'close_menu_{user_id}_{datetime.now().timestamp()}')]]))

# Основная функция запуска бота
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
