import asyncio
import os
import sys
import traceback
from io import BytesIO
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

from telethon.tl.types import ChannelParticipantsSearch
from telethon.errors import ChannelPrivateError

# Функция для парсинга участников чата
async def parse_participants(link, limit):
    try:
        entity = await client_telethon.get_entity(link)
        all_participants = []

        async for user in client_telethon.iter_participants(entity, limit=limit):
            if user.username or user.phone or user.first_name:
                participant_data = [
                    user.id,
                    user.username or "",
                    user.first_name or "",
                    user.phone or "",
                    "active" if user.status else "inactive"
                ]
                all_participants.append(participant_data)

        return all_participants

    except ChannelPrivateError:
        return []
    except Exception as e:
        print(f"Ошибка в parse_participants: {str(e)}")
        return []

# Аналогичные функции для других типов парсинга
async def parse_commentators(link, limit):
    return []

async def parse_post_commentators(link, limit):
    return []

async def parse_phone_contacts(link, limit):
    return []

async def parse_auth_access(link, context):
    return

# Асинхронная отправка сообщения о загрузке
async def send_loading_message(message, context):
    loading_symbols = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
    i = 0
    loading_msg = await message.reply_text("Загрузка " + loading_symbols[i])
    while context.user_data.get('parsing_in_progress', False):
        i = (i + 1) % len(loading_symbols)
        try:
            await loading_msg.edit_text("Загрузка " + loading_symbols[i])
        except telegram_error.BadRequest:
            pass
        await asyncio.sleep(0.1)
    try:
        await loading_msg.delete()
    except telegram_error.BadRequest:
        pass

# Переменные
API_ID = int(os.environ.get('API_ID', 25281388))
API_HASH = os.environ.get('API_HASH', 'a2e719f61f40ca912567c7724db5764e')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '7981019134:AAEARQ__XD1Ki60avGlWL1wDKDVcUKh6ny8')
LOG_CHANNEL_ID = -1002342891238
SUBSCRIPTION_CHANNEL_ID = -1002342891238
SUPPORT_USERNAME = '@alex_strayker'
TON_WALLET_ADDRESS = 'UQAP4wrP0Jviy03CTeniBjSnAL5UHvcMFtxyi1Ip1exl9pLu'
TON_API_KEY = os.environ.get('TON_API_KEY', 'YOUR_TON_API_KEY')
ADMIN_IDS = ['282198872']

SESSION_FILE = 'shared_session.session'
client_telethon = TelegramClient(SESSION_FILE, API_ID, API_HASH)

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
        'limit': 'Сколько пользователей парсить? Выбери или укажи своё число (макс. зависит от подписки).',
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
        'entity_error': 'Не удалось получить информацию о пользователе/чате. Сущность может быть приватной или недоступной.',
        'no_filter': 'Не применять фильтр',
        'phone_contacts': 'Сбор номеров телефонов и ФИО',
        'auth_access': 'Авторизация для закрытых чатов',
        'caption_phones': 'Вот ваш файл с номерами телефонов и ФИО (Excel и VCF).',
        'auth_request': 'Для доступа к закрытым чатам добавьте бота в чат как администратора или отправьте ссылку на закрытый чат.',
        'auth_success': 'Доступ к закрытому чату успешно предоставлен!',
        'auth_error': 'Не удалось получить доступ. Убедитесь, что бот добавлен как администратор или чат публичный.',
        'note_cmd': 'Заметка успешно сохранена (бот не будет реагировать)',
        'info_cmd': 'Информация о боте:\n- Версия: 1.0\n- Разработчик: @alex_strayker\n- Описание: Бот для парсинга Telegram',
        'home_cmd': 'Вернуться в главное меню',
        'parsing_checklist': 'Чек-лист парсинга:\n- Исключены удалённые аккаунты: ✓\n- Только с username: {username_filter}\n- Исключены боты: {bots_filter}\n- Только активные: {active_filter}',
        'rate_parsing': 'Оцените пожалуйста работу TGParser.\n(1)(2)(3)(4)(5)',
        'thanks': 'Спасибо'
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
        'limit': 'Скільки користувачів парсити? Обери або вкажи своє число (макс. залежить від підписки).',
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
        'entity_error': 'Не вдалося отримати інформацію про користувача/чат. Сутність може бути приватною або недоступною.',
        'no_filter': 'Не застосовувати фільтр',
        'phone_contacts': 'Збір номерів телефонів та ПІБ',
        'auth_access': 'Авторизація для закритих чатів',
        'caption_phones': 'Ось ваш файл з номерами телефонів та ПІБ (Excel і VCF).',
        'auth_request': 'Для доступу до закритих чатів додайте бота в чат як адміністратора або надішліть посилання на закритий чат.',
        'auth_success': 'Доступ до закритого чату успішно надано!',
        'auth_error': 'Не вдалося отримати доступ. Переконайтесь, що бот доданий як адміністратор або чат публічний.',
        'note_cmd': 'Примітка успішно збережено (бот не реагуватиме)',
        'info_cmd': 'Інформація про бота:\n- Версія: 1.0\n- Розробник: @alex_strayker\n- Опис: Бот для парсингу Telegram',
        'home_cmd': 'Повернутися до головного меню',
        'parsing_checklist': 'Чек-лист парсингу:\n- Виключено видалені акаунти: ✓\n- Тільки з username: {username_filter}\n- Виключено ботів: {bots_filter}\n- Тільки активні: {active_filter}',
        'rate_parsing': 'Оцініть будь ласка роботу TGParser.\n(1)(2)(3)(4)(5)',
        'thanks': 'Дякую'
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
        'limit': 'How many users to parse? Choose or enter your number (max depends on subscription).',
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
        'entity_error': 'Could not retrieve user/chat information. The entity may be private or inaccessible.',
        'no_filter': 'Do not apply filter',
        'phone_contacts': 'Collect phone numbers and full names',
        'auth_access': 'Authorize for private chats',
        'caption_phones': 'Here is your file with phone numbers and full names (Excel and VCF).',
        'auth_request': 'To access private chats, add the bot as an admin or send a link to a private chat.',
        'auth_success': 'Access to the private chat successfully granted!',
        'auth_error': 'Could not gain access. Ensure the bot is added as an admin or the chat is public.',
        'note_cmd': 'Note successfully saved (bot will not respond)',
        'info_cmd': 'Bot information:\n- Version: 1.0\n- Developer: @alex_strayker\n- Description: Telegram parsing bot',
        'home_cmd': 'Return to main menu',
        'parsing_checklist': 'Parsing checklist:\n- Excluded deleted accounts: ✓\n- Only with username: {username_filter}\n- Excluded bots: {bots_filter}\n- Only active: {active_filter}',
        'rate_parsing': 'Please rate TGParser’s performance.\n(1)(2)(3)(4)(5)',
        'thanks': 'Thank you'
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
        'limit': 'Wie viele Benutzer sollen geparst werden? Wähle oder gib eine Zahl ein (max. abhängig vom Abonnement).',
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
        'entity_error': 'Konnte keine Benutzer-/Chat-Informationen abrufen. Die Entität könnte privat oder nicht zugänglich sein.',
        'no_filter': 'Keinen Filter anwenden',
        'phone_contacts': 'Telefonnummern und vollständige Namen sammeln',
        'auth_access': 'Autorisierung für private Chats',
        'caption_phones': 'Hier ist deine Datei mit Telefonnummern und vollständigen Namen (Excel und VCF).',
        'auth_request': 'Um auf private Chats zuzugreifen, füge den Bot als Administrator hinzu oder sende einen Link zu einem privaten Chat.',
        'auth_success': 'Zugang zum privaten Chat erfolgreich gewährt!',
        'auth_error': 'Konnte keinen Zugriff erhalten. Stelle sicher, dass der Bot als Administrator hinzugefügt wurde oder der Chat öffentlich ist.',
        'note_cmd': 'Notiz erfolgreich gespeichert (der Bot wird nicht reagieren)',
        'info_cmd': 'Bot-Informationen:\n- Version: 1.0\n- Entwickler: @alex_strayker\n- Beschreibung: Telegram-Parsing-Bot',
        'home_cmd': 'Zum Hauptmenü zurückkehren',
        'parsing_checklist': 'Parsing-Checkliste:\n- Gelöschte Konten ausgeschlossen: ✓\n- Nur mit Username: {username_filter}\n- Bots ausgeschlossen: {bots_filter}\n- Nur aktive: {active_filter}',
        'rate_parsing': 'Bitte bewerte die Leistung von TGParser.\n(1)(2)(3)(4)(5)',
        'thanks': 'Danke'
    }
}

# Логирование в канал
async def log_to_channel(context, message, username=None, file=None):
    try:
        user = context.user_data.get('user', {})
        name = user.get('name', username or 'Неизвестно')
        log_message = f"{message}"
        if username:
            log_message = f"{name} (@{username}): {message}"
        if file:
            file.seek(0)
            await context.bot.send_document(chat_id=LOG_CHANNEL_ID, document=file, caption=log_message)
        else:
            await context.bot.send_message(chat_id=LOG_CHANNEL_ID, text=log_message)
    except telegram_error.BadRequest as e:
        print(f"Ошибка при отправке лога в канал: {e}")

# Обновление пользовательских данных
def update_user_data(user_id, name, context, lang=None, subscription=None, requests=0):
    users = load_users()
    user_id_str = str(user_id)
    now = datetime.now()
    username = context.user_data.get('username', name) or name
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
    user['name'] = name or user.get('name', username)
    context.user_data['user'] = user
    context.user_data['username'] = username
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
def check_parse_limit(user_id, limit):
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
    elif subscription['type'] == 'Платная (1h)':
        return min(limit, 200)
    elif subscription['type'] == 'Платная (3d)':
        return min(limit, 5000)
    elif subscription['type'] == 'Платная (7d)':
        return min(limit, 10000)
    return limit

# Создание файла Excel
async def create_excel_in_memory(data):
    if not data:
        return BytesIO()

    num_columns = len(data[0])
    default_columns = ['ID', 'Username', 'First Name', 'Phone', 'Status']
    columns = default_columns[:num_columns]
    
    df = pd.DataFrame(data, columns=columns)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        worksheet = writer.sheets['Sheet1']
        worksheet.set_column(f'A:{chr(65 + num_columns - 1)}', 20)
    output.seek(0)
    return output

# Создание VCF файла
def create_vcf_file(data):
    vcf_content = io.StringIO()
    for entry in data:
        if len(entry) < 4 or not entry[3]:
            continue
        vcard = vobject.vCard()
        vcard.add('fn').value = f"{entry[2] or ''}".strip() or "Unknown"
        vcard.add('tel').value = entry[3]
        if entry[1]:
            vcard.add('url').value = f"https://t.me/{entry[1]}"
        vcf_content.write(vcard.serialize())
    vcf_data = vcf_content.getvalue().encode('utf-8')
    vcf_content.close()
    return io.BytesIO(vcf_data)

# Фильтрация данных
def filter_data(data, filters):
    filtered_data = [row for row in data if not row[1] == "Удалённый аккаунт"]
    if filters.get('only_with_username'):
        filtered_data = [row for row in filtered_data if row[1]]
    if filters.get('exclude_bots'):
        filtered_data = [row for row in filtered_data if not row[4]]
    if filters.get('only_active'):
        filtered_data = [row for row in filtered_data if row[4] == "active"]
    return filtered_data

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
    name = user_data.get('name', 'Без имени')
    limit_left, _ = check_request_limit(user_id)
    limit_display = 5 - user_data.get('daily_requests', {}).get('count', 0) if sub_type == 'Бесплатная' else 10 - user_data.get('daily_requests', {}).get('count', 0)
    
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
    name = update.effective_user.full_name or "Без имени"
    context.user_data['username'] = username
    users = load_users()

    try:
        await client_telethon.connect()
        if not await client_telethon.is_user_authorized():
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
        if client_telethon.is_connected():
            await client_telethon.disconnect()

# Обработчик команды /language
async def language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
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
    await log_to_channel(context, f"Команда /language вызвана пользователем {name}", username)

# Обработчик команды /info
async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    await update.message.reply_text(LANGUAGES[lang]['info_cmd'])
    await log_to_channel(context, f"Команда /info вызвана пользователем {name}", username)

# Обработчик команды /home
async def home(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
    name = update.effective_user.full_name or "Без имени"
    menu_text, menu_keyboard = get_main_menu(user_id, context)
    await update.message.reply_text(menu_text, reply_markup=menu_keyboard)
    await log_to_channel(context, f"Команда /home вызвана пользователем {name}", username)

# Обработчик команды /set_plan
async def set_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
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
    
    target_username = load_users().get(str(target_user_id), {}).get('name', 'Неизвестно')
    lang = load_users().get(str(target_user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    notification = texts['payment_success'].format(end_time=end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'бессрочно')
    await context.bot.send_message(chat_id=target_user_id, text=f"🎉 {notification}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_update'], callback_data='update_menu')]]))
    
    await update.message.reply_text(f"Подписка для пользователя {target_user_id} ({target_username}) обновлена до {end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'бессрочно'}.")
    await log_to_channel(context, f"Администратор установил подписку для пользователя {target_user_id} ({target_username}): {sub_type}, до {end_time if end_time else 'бессрочно'}", username)

# Обработчик команды /remove_plan
async def remove_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
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
    target_username = load_users().get(str(target_user_id), {}).get('name', 'Неизвестно')
    await update.message.reply_text(f"Платная подписка для пользователя {target_user_id} ({target_username}) удалена, установлен бесплатный план.")
    await log_to_channel(context, f"Администратор удалил платную подписку для пользователя {target_user_id} ({target_username})", username)

# Обработчик команды /note
async def note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
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
    context.user_data['user_id'] = user_id
    username = update.effective_user.username or "Без username"
    name = update.effective_user.full_name or "Без имени"
    context.user_data['username'] = username
    users = load_users()
    text = update.message.text.strip() if update.message.text else ""
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]

    await log_to_channel(context, f"Сообщение от пользователя: {text}", username)

    try:
        await client_telethon.connect()
    except telethon_errors.RPCError as e:
        await update.message.reply_text(texts['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка подключения для {name} (@{username}): {str(e)}", username)
        return
    finally:
        if client_telethon.is_connected():
            await client_telethon.disconnect()

    if context.user_data.get('waiting_for_phone'):
        if not text.startswith('+'):
            await update.message.reply_text("Пожалуйста, введите номер в формате +380639678038:")
            return
        context.user_data['phone'] = text
        try:
            await client_telethon.connect()
            sent_code = await client_telethon.send_code_request(text)
            context.user_data['phone_code_hash'] = sent_code.phone_code_hash
            await update.message.reply_text(texts['enter_code'])
            context.user_data['waiting_for_code'] = True
            del context.user_data['waiting_for_phone']
            await log_to_channel(context, f"Номер телефона: {text}", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(texts['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода номера: {str(e)}", username)
        finally:
            if client_telethon.is_connected():
                await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_code'):
        try:
            await client_telethon.connect()
            await client_telethon.sign_in(context.user_data['phone'], text, phone_code_hash=context.user_data['phone_code_hash'])
            await update.message.reply_text(texts['auth_success'])
            del context.user_data['waiting_for_code']
            await log_to_channel(context, f"Успешная авторизация", username)
            keyboard = [
                [InlineKeyboardButton("Русский", callback_data='lang_Русский')],
                [InlineKeyboardButton("Украинский", callback_data='lang_Украинский')],
                [InlineKeyboardButton("English", callback_data='lang_English')],
                [InlineKeyboardButton("Deutsch", callback_data='lang_Deutsch')]
            ]
            await update.message.reply_text(texts['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        except telethon_errors.SessionPasswordNeededError:
            await update.message.reply_text(texts['enter_password'])
            context.user_data['waiting_for_password'] = True
            del context.user_data['waiting_for_code']
            await log_to_channel(context, f"Запрос пароля 2FA", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(texts['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода кода: {str(e)}", username)
        finally:
            if client_telethon.is_connected():
                await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_password'):
        try:
            await client_telethon.connect()
            await client_telethon.sign_in(password=text)
            await update.message.reply_text(texts['auth_success'])
            del context.user_data['waiting_for_password']
            await log_to_channel(context, f"Успешная авторизация с 2FA", username)
            keyboard = [
                [InlineKeyboardButton("Русский", callback_data='lang_Русский')],
                [InlineKeyboardButton("Украинский", callback_data='lang_Украинский')],
                [InlineKeyboardButton("English", callback_data='lang_English')],
                [InlineKeyboardButton("Deutsch", callback_data='lang_Deutsch')]
            ]
            await update.message.reply_text(texts['welcome'], reply_markup=InlineKeyboardMarkup(keyboard))
        except telethon_errors.RPCError as e:
            await update.message.reply_text(texts['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода пароля 2FA: {str(e)}", username)
        finally:
            if client_telethon.is_connected():
                await client_telethon.disconnect()
        return

    if str(user_id) not in users or 'language' not in users[str(user_id)]:
        return
    
    limit_ok, hours_left = check_request_limit(user_id)
    if not limit_ok:
        await update.message.reply_text(texts['limit_reached'].format(limit=5 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 10, hours=hours_left))
        return

    if text.startswith('/note'):
        await note(update, context)
        return

    if context.user_data.get('waiting_for_hash'):
        context.user_data['transaction_hash'] = text
        del context.user_data['waiting_for_hash']
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"Пользователь {name} (@{username}) (ID: {user_id}) отправил хэш транзакции:\n{text}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отклонить", callback_data=f'reject_{user_id}')]])
                )
            except telegram_error.BadRequest as e:
                print(f"Ошибка отправки хэша администратору {admin_id}: {e}")
        await log_to_channel(context, f"Хэш транзакции: {text}", username)
        await update.message.reply_text(texts['payment_pending'])
        return

    if context.user_data.get('waiting_for_id'):
        try:
            await client_telethon.connect()
            entity_id = None
            entity_type = None

            if update.message.forward_from_chat:
                entity_id = update.message.forward_from_chat.id
                entity_type = "Chat/Channel"
            elif update.message.forward_from:
                entity_id = update.message.forward_from.id
                entity_type = "User"
            elif text:
                if text.startswith('@'):
                    entity_name = text[1:]
                elif text.startswith('https://t.me/'):
                    parts = text.split('/')
                    if len(parts) >= 5 and parts[4].isdigit():
                        entity_name = parts[3]
                        post_id = int(parts[4])
                        entity = await client_telethon.get_entity(entity_name)
                        entity_id = entity.id
                        entity_type = "Post in Chat/Channel"
                    else:
                        entity_name = parts[3]
                else:
                    entity_name = text

                try:
                    entity = await client_telethon.get_entity(entity_name)
                    entity_id = entity.id
                    entity_type = "User" if isinstance(entity, tl.types.User) else "Chat/Channel"
                except telethon_errors.RPCError as e:
                    await update.message.reply_text(texts['entity_error'])
                    await log_to_channel(context, f"Ошибка получения ID: {str(e)} для {text}", username)
                    return
            else:
                await update.message.reply_text(texts['invalid_link'])
                await log_to_channel(context, f"Некорректный ввод для ID: {text}", username)
                return

            await update.message.reply_text(
                texts['id_result'].format(id=entity_id),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(texts['close'], callback_data='home')]
                ])
            )
            await context.bot.set_message_reaction(
                chat_id=update.message.chat_id,
                message_id=update.message.message_id + 1,
                reaction=[{"type": "emoji", "emoji": "🎉"}]
            )
            await log_to_channel(context, f"ID найден: {entity_id} ({entity_type})", username)
            context.user_data['waiting_for_id'] = False

        except telethon_errors.RPCError as e:
            await update.message.reply_text(texts['entity_error'])
            await log_to_channel(context, f"Ошибка получения ID: {str(e)}", username)
            context.user_data['waiting_for_id'] = False
        finally:
            if client_telethon.is_connected():
                await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_limit'):
        try:
            limit = int(text)
            max_limit = check_parse_limit(user_id, float('inf'))
            if limit <= 0 or limit > max_limit:
                await update.message.reply_text(texts['invalid_limit'].format(max_limit=max_limit), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
                return
            context.user_data['limit'] = limit
            del context.user_data['waiting_for_limit']
            await ask_for_filters(update.message, context)
        except ValueError:
            await update.message.reply_text(texts['invalid_number'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
        return

    if context.user_data.get('waiting_for_filters'):
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        if 'да' in text.lower() or 'yes' in text.lower() or 'ja' in text.lower():
            filters[context.user_data['current_filter']] = True
        del context.user_data['waiting_for_filters']
        del context.user_data['current_filter']
        context.user_data['filters'] = filters
        await process_parsing(update.message, context)
        return

    if context.user_data.get('parse_type') in ['parse_authors', 'parse_participants', 'parse_phone_contacts', 'parse_auth_access', 'parse_post_commentators']:
        if text:
            links = text.split('\n') if '\n' in text else [text]
            normalized_links = []
            for link in links:
                link = link.strip()
                if link.startswith('https://t.me/'):
                    normalized_links.append(link)
                elif link.startswith('@'):
                    normalized_links.append(f"https://t.me/{link[1:]}")
                elif not link.startswith('http'):
                    normalized_links.append(f"https://t.me/{link}")
                else:
                    normalized_links.append(link)
            
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
        elif update.message.forward_from_chat and context.user_data['parse_type'] == 'parse_post_commentators':
            chat_username = update.message.forward_from_chat.username
            message_id = update.message.forward_from_message_id
            if chat_username:
                context.user_data['links'] = [f"https://t.me/{chat_username}/{message_id}"]
                context.user_data['chat_id'] = update.message.forward_from_chat.id
                context.user_data['post'] = message_id
                await ask_for_limit(update.message, context)
            else:
                await update.message.reply_text(texts['invalid_link'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['fix_link'], callback_data='fix_link')]]))
                context.user_data['last_input'] = str(update.message.forward_from_chat.id)
        return

# Запрос лимита парсинга
async def ask_for_filters(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False}
    context.user_data['current_filter'] = 'only_with_username'
    keyboard = [
        [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
         InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
        [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
    ]
    await message.reply_text(texts['filter_username'], reply_markup=InlineKeyboardMarkup(keyboard))

async def ask_for_limit(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    subscription = load_users().get(str(user_id), {}).get('subscription', {'type': 'Бесплатная', 'end': None})
    max_limit = check_parse_limit(user_id, float('inf'))
    keyboard = [
        [InlineKeyboardButton("200", callback_data='limit_200'), InlineKeyboardButton("500", callback_data='limit_500')],
        [InlineKeyboardButton("1000", callback_data='limit_1000'), InlineKeyboardButton("5000", callback_data='limit_5000')],
        [InlineKeyboardButton("10000", callback_data='limit_10000'), InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]
    ]
    await message.reply_text(texts['limit'], reply_markup=InlineKeyboardMarkup(keyboard))
    await log_to_channel(context, "Запрос лимита парсинга", context.user_data.get('username', 'Без username'))

# Обработка парсинга
async def process_parsing(message, context):
    user_id = message.from_user.id if message.from_user else context.user_data.get('user_id')
    if not user_id:
        await message.reply_text("Ошибка: не удалось определить ваш ID. Попробуйте снова с /start.")
        return
    
    username = message.from_user.username or "Без username"
    name = message.from_user.full_name or "Без имени"
    context.user_data['username'] = username
    users = load_users()
    
    if str(user_id) not in users:
        users[str(user_id)] = {
            'name': name,
            'language': 'Русский',
            'subscription': {'type': 'Бесплатная', 'end': None},
            'requests': 0,
            'daily_requests': {'count': 0, 'last_reset': datetime.now().isoformat()}
        }
        save_users(users)
    
    lang = users[str(user_id)]['language']
    texts = LANGUAGES[lang]
    subscription = users[str(user_id)]['subscription']
    
    if 'limit' not in context.user_data:
        context.user_data['limit'] = check_parse_limit(user_id, 150)
    
    context.user_data['parsing_in_progress'] = True
    asyncio.create_task(send_loading_message(message, context))
    
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
                channel_name = getattr(entity, 'title', str(entity.id))
                context.user_data['channel_name'] = channel_name
            except telethon_errors.ChannelPrivateError:
                context.user_data['parsing_in_progress'] = False
                await message.reply_text(texts['no_access'].format(link=link))
                await log_to_channel(context, texts['no_access'].format(link=link), username)
                return
            except telethon_errors.RPCError as e:
                context.user_data['parsing_in_progress'] = False
                await message.reply_text(texts['rpc_error'].format(e=str(e)))
                await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
                return
            
            limit = check_parse_limit(user_id, context.user_data['limit'])
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
                context.user_data['parsing_in_progress'] = False
                return
            else:
                context.user_data['parsing_in_progress'] = False
                await message.reply_text("Неизвестный тип парсинга.")
                await log_to_channel(context, "Неизвестный тип парсинга", username)
                return

            all_data.extend(data)

        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        filtered_data = filter_data(all_data, filters)

        update_user_data(user_id, name, context, requests=1)

        if not filtered_data:
            context.user_data['parsing_in_progress'] = False
            await message.reply_text("Данные не найдены после применения фильтров.")
            await log_to_channel(context, "Данные не найдены после применения фильтров", username)
            return

        excel_file = await create_excel_in_memory(filtered_data)

        username_filter = "✓" if filters['only_with_username'] else "✗"
        bots_filter = "✓" if filters['exclude_bots'] else "✗"
        active_filter = "✓" if filters['only_active'] else "✗"
        checklist = texts['parsing_checklist'].format(
            username_filter=username_filter,
            bots_filter=bots_filter,
            active_filter=active_filter
        )
        total_rows = len(filtered_data)
        rows_without_username = sum(1 for row in filtered_data if not row[1])
        stats = f"\nСтатистика:\nОбщее количество строк: {total_rows}\nСтрок без username: {rows_without_username}"

        channel_name = context.user_data.get('channel_name', 'unknown_channel')
        if context.user_data['parse_type'] == 'parse_authors':
            caption = texts['caption_commentators']
            file_name = f"{channel_name}_commentators.xlsx"
        elif context.user_data['parse_type'] == 'parse_participants':
            caption = texts['caption_participants']
            file_name = f"{channel_name}_participants.xlsx"
        elif context.user_data['parse_type'] == 'parse_post_commentators':
            caption = texts['caption_post_commentators']
            file_name = f"{channel_name}_post_commentators.xlsx"
        elif context.user_data['parse_type'] == 'parse_phone_contacts':
            caption = texts['caption_phones']
            file_name = f"{channel_name}_phone_contacts.xlsx"
        else:
            caption = "Результаты парсинга"
            file_name = f"{channel_name}_results.xlsx"

        await message.reply_document(
            document=excel_file,
            filename=file_name,
            caption=f"{caption}\n\n{checklist}{stats}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data='home')]
            ])
        )

        if context.user_data['parse_type'] == 'parse_phone_contacts':
            vcf_file = create_vcf_file(filtered_data)
            await message.reply_document(
                document=vcf_file,
                filename=f"{channel_name}_phone_contacts.vcf",
                caption=texts['caption_phones'],
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(texts['close'], callback_data='home')]
                ])
            )

        stats = get_statistics(filtered_data)
        await log_to_channel(context, f"Парсинг завершён:\n{stats}", username, file=excel_file)

    except telethon_errors.FloodWaitError as e:
        context.user_data['parsing_in_progress'] = False
        await message.reply_text(texts['flood_error'].format(e=str(e)))
        await log_to_channel(context, texts['flood_error'].format(e=str(e)), username)
    except telethon_errors.RPCError as e:
        context.user_data['parsing_in_progress'] = False
        await message.reply_text(texts['rpc_error'].format(e=str(e)))
        await log_to_channel(context, texts['rpc_error'].format(e=str(e)), username)
    finally:
        if client_telethon.is_connected():
            await client_telethon.disconnect()
        context.user_data['parsing_in_progress'] = False

# Обработчик кнопок (продолжение)
    if query.data == 'subscribe':
        keyboard = [
            [InlineKeyboardButton(texts['subscription_1h'], callback_data='sub_1h')],
            [InlineKeyboardButton(texts['subscription_3d'], callback_data='sub_3d')],
            [InlineKeyboardButton(texts['subscription_7d'], callback_data='sub_7d')],
            [InlineKeyboardButton(texts['close'], callback_data='home')]
        ]
        await query.message.reply_text("Выберите тип подписки:", reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, "Пользователь открыл меню подписки", username)
        return

    if query.data.startswith('sub_'):
        sub_type = query.data.split('_')[1]
        amount = {'1h': 2, '3d': 5, '7d': 7}[sub_type]
        context.user_data['sub_type'] = sub_type
        keyboard = [
            [InlineKeyboardButton(texts['payment_paid'], callback_data='paid')],
            [InlineKeyboardButton(texts['payment_cancel'], callback_data='home')]
        ]
        await query.message.reply_text(
            texts['payment_wallet'].format(amount=amount, address=TON_WALLET_ADDRESS),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        await log_to_channel(context, f"Пользователь выбрал подписку: {sub_type}", username)
        return

    if query.data == 'paid':
        context.user_data['waiting_for_hash'] = True
        await query.message.reply_text(texts['payment_hash'])
        await log_to_channel(context, "Пользователь нажал 'Я оплатил'", username)
        return

    if query.data == 'update_menu':
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.message.edit_text(menu_text, reply_markup=menu_keyboard)
        await log_to_channel(context, "Пользователь обновил меню", username)
        return

    if query.data.startswith('reject_'):
        rejected_user_id = query.data.split('_')[1]
        rejected_username = load_users().get(str(rejected_user_id), {}).get('name', 'Неизвестно')
        rejected_lang = load_users().get(str(rejected_user_id), {}).get('language', 'Русский')
        await context.bot.send_message(
            chat_id=rejected_user_id,
            text=LANGUAGES[rejected_lang]['payment_error']
        )
        await query.message.edit_text(f"Транзакция пользователя {rejected_username} (ID: {rejected_user_id}) отклонена.")
        await log_to_channel(context, f"Администратор отклонил транзакцию пользователя {rejected_username} (ID: {rejected_user_id})", username)
        return

    if query.data == 'requisites':
        await query.message.reply_text(texts['requisites'].format(support=SUPPORT_USERNAME))
        await log_to_channel(context, "Пользователь запросил реквизиты", username)
        return

    if query.data == 'logs_channel':
        if str(user_id) in ADMIN_IDS:
            await query.message.reply_text(texts['logs_channel'])
            await log_to_channel(context, "Администратор запросил канал с логами", username)
        return

    if query.data.startswith('limit_'):
        limit = int(query.data.split('_')[1])
        max_limit = check_parse_limit(user_id, float('inf'))
        if limit > max_limit:
            await query.message.reply_text(texts['invalid_limit'].format(max_limit=max_limit))
            return
        context.user_data['limit'] = limit
        await ask_for_filters(query.message, context)
        await log_to_channel(context, f"Пользователь выбрал лимит: {limit}", username)
        return

    if query.data == 'skip_limit':
        context.user_data['limit'] = check_parse_limit(user_id, 150)
        await ask_for_filters(query.message, context)
        await log_to_channel(context, "Пользователь пропустил выбор лимита", username)
        return

    if query.data == 'filter_yes':
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        filters[context.user_data['current_filter']] = True
        context.user_data['filters'] = filters
        next_filter = {'only_with_username': 'exclude_bots', 'exclude_bots': 'only_active', 'only_active': None}
        if next_filter[context.user_data['current_filter']]:
            context.user_data['current_filter'] = next_filter[context.user_data['current_filter']]
            keyboard = [
                [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
                 InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
                [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
            ]
            if context.user_data['current_filter'] == 'exclude_bots':
                await query.message.edit_text(texts['filter_bots'], reply_markup=InlineKeyboardMarkup(keyboard))
            elif context.user_data['current_filter'] == 'only_active':
                await query.message.edit_text(texts['filter_active'], reply_markup=InlineKeyboardMarkup(keyboard))
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
            keyboard = [
                [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
                 InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
                [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
            ]
            if context.user_data['current_filter'] == 'exclude_bots':
                await query.message.edit_text(texts['filter_bots'], reply_markup=InlineKeyboardMarkup(keyboard))
            elif context.user_data['current_filter'] == 'only_active':
                await query.message.edit_text(texts['filter_active'], reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await process_parsing(query.message, context)
        return

    if query.data == 'skip_filters':
        context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False}
        await process_parsing(query.message, context)
        return

    if query.data == 'fix_link':
        await query.message.reply_text(texts['retry_link'])
        await log_to_channel(context, "Пользователь запросил исправление ссылки", username)
        return

    if query.data.startswith('info_'):
        info_type = query.data.split('_')[1]
        info_texts = {
            'identifiers': "Инструмент для получения ID пользователей, чатов или постов.",
            'parser': "Собирает данные из чатов и постов Telegram.",
            'subscribe': "Подписка увеличивает лимиты парсинга и запросов.",
            'requisites': "Информация о способах оплаты подписки.",
            'logs': "Канал с логами доступен только администраторам."
        }
        await query.message.reply_text(info_texts.get(info_type, "Информация недоступна"))
        await log_to_channel(context, f"Пользователь запросил информацию: {info_type}", username)
        return

# Запуск бота
def main():
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("language", language))
    application.add_handler(CommandHandler("info", info))
    application.add_handler(CommandHandler("home", home))
    application.add_handler(CommandHandler("set_plan", set_plan))
    application.add_handler(CommandHandler("remove_plan", remove_plan))
    application.add_handler(CommandHandler("note", note))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(button))

    application.run_polling()

if __name__ == '__main__':
    main()
