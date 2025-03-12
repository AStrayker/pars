import asyncio
import os
import sys
import traceback
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telethon import TelegramClient, errors as telethon_errors
from telethon import tl
from telethon.sessions import StringSession
from telethon.network import ConnectionTcpFull
from telegram import error as telegram_error
from datetime import datetime, timedelta
import json
import io
import pandas as pd
import requests
import vobject
from firebase_admin import credentials, initialize_app, db
from faker import Faker
import random

# Переменные
API_ID = int(os.environ.get('API_ID', 25281388))
API_HASH = os.environ.get('API_HASH', 'a2e719f61f40ca912567c7724db5764e')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '7981019134:AAEARQ__XD1Ki60avGlWL1wDKDVcUKh6ny8')
LOG_CHANNEL_ID = -1002342891238
SUBSCRIPTION_CHANNEL_ID = -1002425905138
SUPPORT_USERNAME = '@alex_strayker'
TON_WALLET_ADDRESS = 'UQAP4wrP0Jviy03CTeniBjSnAL5UHvcMFtxyi1Ip1exl9pLu'
TON_API_KEY = os.environ.get('TON_API_KEY', 'YOUR_TON_API_KEY')
ADMIN_IDS = ['282198872']

# Инициализация Firebase
SERVICE_ACCOUNT_FILE = 'serviceAccountKey.json'
if not os.path.exists(SERVICE_ACCOUNT_FILE):
    raise FileNotFoundError(f"Файл {SERVICE_ACCOUNT_FILE} не найден.")
firebase_cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
initialize_app(firebase_cred, {
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

# Глобальный клиент
client = TelegramClient('my_session.session', API_ID, API_HASH)

# Инициализация клиента
async def initialize_client():
    await client.start()
    print("Клиент успешно авторизован для общего использования")
    # Сохраняем сессию (выполните один раз при авторизации)
    # session_data = client.session.save()
    # with open('my_session.session', 'w') as f:
    #     f.write(session_data)

# Создание клиента Telethon (используем глобальный клиент)
async def get_telethon_client(user_id):
    if not await client.is_user_authorized():
        raise Exception("Глобальный клиент не авторизован. Проверьте сессию.")
    print(f"Используется глобальный клиент для {user_id}")
    return client

# Языковые переводы
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
        'note_cmd': 'Заметка успешно сохранена (бот не будет реагировать).'
    },
    'Украинский': {},  # Оставлено пустым для заполнения
    'English': {},     # Оставлено пустым для заполнения
    'Deutsch': {}      # Оставлено пустым для заполнения
}

# Логирование в канал
async def log_to_channel(context, message, username=None):
    print(f"Попытка отправки лога в канал {LOG_CHANNEL_ID}")
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

# Создание VCF файла
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

# Подсчет статистики
def get_statistics(data):
    total = len(data)
    with_username = sum(1 for row in data if row[1])
    bots = sum(1 for row in data if row[4])
    without_name = sum(1 for row in data if not row[2] and not row[3])
    return f"Всего: {total}\nС username: {with_username}\nБотов: {bots}\nБез имени: {without_name}"

# Главное меню
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
    limit_display = 5 if sub_type == 'Бесплатная' else 10 - user_data.get('daily_requests', {}).get('count', 0)
    
    is_admin = user_id_str in ADMIN_IDS
    
    buttons = [
        [InlineKeyboardButton(texts['identifiers'], callback_data='identifiers'), 
         InlineKeyboardButton("(!)", callback_data='info_identifiers')],
        [InlineKeyboardButton(texts['parser'], callback_data='parser'), 
         InlineKeyboardButton("(!)", callback_data='info_parser')],
        [InlineKeyboardButton(texts['subscribe_button'], callback_data='subscribe'), 
         InlineKeyboardButton("(!)", callback_data='info_subscribe')],
        [InlineKeyboardButton(texts['support'].format(support=SUPPORT_USERNAME), url=f"https://t.me/{SUPPORT_USERNAME[1:]}")],
        [InlineKeyboardButton(texts['requisites'], callback_data='requisites'), 
         InlineKeyboardButton("(!)", callback_data='info_requisites')]
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton(texts['logs_channel'], callback_data='logs_channel'), 
                       InlineKeyboardButton("(!)", callback_data='info_logs')])
    
    return texts['start_menu'].format(
        name=name, user_id=user_id, lang=lang, sub_type=sub_type, sub_time=sub_time, requests=requests, limit=limit_display
    ), InlineKeyboardMarkup(buttons)

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()

    if str(user_id) not in users:
        print(LANGUAGES['Русский']['new_user'].format(name=name, user_id=user_id))
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

    await log_to_channel(context, LANGUAGES['Русский']['new_user'].format(name=name, user_id=user_id), username)

# Обработчик сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data['user_id'] = user_id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    text = update.message.text.strip() if update.message.text else ""

    if str(user_id) not in users or 'language' not in users[str(user_id)]:
        return
    
    lang = users[str(user_id)]['language']
    texts = LANGUAGES[lang]
    
    if context.user_data.get('parsing_in_progress', False):
        return
    
    limit_ok, hours_left = check_request_limit(user_id)
    if not limit_ok:
        await update.message.reply_text(texts['limit_reached'].format(limit=5 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 10, hours=hours_left))
        return
    
    if text.startswith('/note '):
        await note(update, context)
        return
    
    if 'waiting_for_hash' in context.user_data:
        context.user_data['transaction_hash'] = text
        del context.user_data['waiting_for_hash']
        for admin_id in ADMIN_IDS:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"Пользователь {name} (@{username}) (ID: {user_id}) отправил хэш транзакции:\n{text}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отклонить", callback_data=f'reject_{user_id}')]])
            )
        await log_to_channel(context, f"Хэш транзакции от {name} (@{username}): {text}", username)
        await update.message.reply_text(texts['payment_pending'])
        return

    if 'waiting_for_id' in context.user_data:
        if text.startswith('@'):
            try:
                user = await context.bot.get_chat(text[1:])
                await update.message.reply_text(texts['id_result'].format(id=user.id), reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
                ]))
            except Exception as e:
                await update.message.reply_text(texts['rpc_error'].format(e=str(e)))
        del context.user_data['waiting_for_id']
    
    if 'waiting_for_limit' in context.user_data:
        try:
            limit = int(text)
            max_limit = 15000 if users[str(user_id)]['subscription']['type'].startswith('Платная') else 150
            if limit <= 0 or limit > max_limit:
                await update.message.reply_text(texts['invalid_limit'].format(max_limit=max_limit), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
                return
            context.user_data['limit'] = limit
            del context.user_data['waiting_for_limit']
            await ask_for_filters(update.message, context)
        except ValueError:
            await update.message.reply_text(texts['invalid_number'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
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

def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(initialize_client())
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("language", language))
    application.add_handler(CommandHandler("set_plan", set_plan))
    application.add_handler(CommandHandler("remove_plan", remove_plan))
    application.add_handler(CommandHandler("note", note))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_error_handler(error_handler)
    
    print("Бот запущен...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
