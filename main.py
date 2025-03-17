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
LOG_CHANNEL_ID = int(os.environ.get('LOG_CHANNEL_ID', -1002342891238))
SUBSCRIPTION_CHANNEL_ID = int(os.environ.get('SUBSCRIPTION_CHANNEL_ID', -1002342891238))
SUPPORT_USERNAME = os.environ.get('SUPPORT_USERNAME', '@alex_strayker')
TON_WALLET_ADDRESS = os.environ.get('TON_WALLET_ADDRESS', 'UQAP4wrP0Jviy03CTeniBjSnAL5UHvcMFtxyi1Ip1exl9pLu')
TON_API_KEY = os.environ.get('TON_API_KEY', 'YOUR_TON_API_KEY')
ADMIN_IDS = os.environ.get('ADMIN_IDS', '282198872').split(',')

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
    try:
        with open(DB_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {}

def save_users(users):
    try:
        with open(DB_FILE, 'w') as f:
            json.dump(users, f, indent=4)
    except IOError as e:
        print(f"Ошибка сохранения пользователей: {e}")

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
        'identifiers': 'Отправь мне @username, ссылку на публикацию или перешли сообщение, чтобы узнать ID.',
        'parser': 'Выбери, что хочешь спарсить:',
        'subscribe_button': 'Оформить подписку',
        'support': 'Поддержка: {support}',
        'requisites': 'Возможности оплаты:\n1. [Метод 1]\n2. [Метод 2]\nСвяжитесь с {support} для оплаты.',
        'logs_channel': 'Канал с логами: t.me/YourLogChannel',
        'link_group': 'Отправь мне ссылку на группу или канал, например: https://t.me/group_name, @group_name или group_name\nМожно указать несколько ссылок через Enter.',
        'link_post': 'Отправь мне ссылку на пост, например: https://t.me/channel_name/12345\nИли перешли пост. Можно указать несколько ссылок через Enter.',
        'limit': 'Сколько пользователей парсить? Выбери или укажи своё число (макс. 150 для бесплатной подписки, 15000 для платной).',
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
        'note_cmd': 'Заметка успешно сохранена (бот не будет реагировать).',
        'info_cmd': 'Информация о боте:\nВерсия: 1.0\nРазработчик: @alex_strayker\nФункции: Парсинг Telegram данных\nПоддержка: {support}',
        'home_cmd': 'Вернуться в главное меню',
        'parsing_checklist': 'Чек-лист парсинга:\n- Исключены удалённые аккаунты: ✅\n- Только с username: {username_filter}\n- Исключены боты: {bots_filter}\n- Только активные: {active_filter}',
        'rate_parsing': 'Оцените пожалуйста работу TGParser:\n(1)(2)(3)(4)(5)',
        'thanks': 'Спасибо за оценку!'
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
        'limit': 'Скільки користувачів парсити? Обери або вкажи своє число (макс. 150 для безкоштовної підписки, 15000 для платної).',
        'filter_username': 'Фільтрувати лише користувачів з username?',
        'filter_bots': 'Виключити ботів?',
        'filter_active': 'Тільки активних нещодавно (за 30 днів)?',
        'invalid_limit': 'Вкажи число від 1 до {max_limit}!',
        'invalid_number': 'Будь ласка, вкажи число!',
        'invalid_link': 'Будь ласка, надішли коректне посилання на пост/чат, наприклад: https://t.me/channel_name/12345, @channel_name або group_name\nАбо кілька посилань через Enter.',
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
        'note_cmd': 'Примітка успішно збережено (бот не реагуватиме).',
        'info_cmd': 'Інформація про бота:\nВерсія: 1.0\nРозробник: @alex_strayker\nФункції: Парсинг Telegram даних\nПідтримка: {support}',
        'home_cmd': 'Повернутися до головного меню',
        'parsing_checklist': 'Чек-лист парсингу:\n- Виключено видалені акаунти: ✅\n- Тільки з username: {username_filter}\n- Виключено ботів: {bots_filter}\n- Тільки активні: {active_filter}',
        'rate_parsing': 'Оцініть будь ласка роботу TGParser:\n(1)(2)(3)(4)(5)',
        'thanks': 'Дякую за оцінку!'
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
        'limit': 'How many users to parse? Choose or enter your number (max 150 for free subscription, 15000 for paid).',
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
        'note_cmd': 'Note successfully saved (bot will not respond).',
        'info_cmd': 'Bot information:\nVersion: 1.0\nDeveloper: @alex_strayker\nFeatures: Telegram data parsing\nSupport: {support}',
        'home_cmd': 'Return to main menu',
        'parsing_checklist': 'Parsing checklist:\n- Excluded deleted accounts: ✅\n- Only with username: {username_filter}\n- Excluded bots: {bots_filter}\n- Only active: {active_filter}',
        'rate_parsing': 'Please rate TGParser:\n(1)(2)(3)(4)(5)',
        'thanks': 'Thank you for your rating!'
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
        'limit': 'Wie viele Benutzer sollen geparst werden? Wähle oder gib eine Zahl ein (max. 150 für kostenloses Abonnement, 15000 für bezahltes).',
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
        'note_cmd': 'Notiz erfolgreich gespeichert (der Bot wird nicht reagieren).',
        'info_cmd': 'Bot-Informationen:\nVersion: 1.0\nEntwickler: @alex_strayker\nFunktionen: Telegram-Daten-Parsing\nSupport: {support}',
        'home_cmd': 'Zum Hauptmenü zurückkehren',
        'parsing_checklist': 'Parsing-Checkliste:\n- Gelöschte Konten ausgeschlossen: ✅\n- Nur mit Username: {username_filter}\n- Bots ausgeschlossen: {bots_filter}\n- Nur aktive: {active_filter}',
        'rate_parsing': 'Bitte bewerten Sie TGParser:\n(1)(2)(3)(4)(5)',
        'thanks': 'Vielen Dank für Ihre Bewertung!'
    }
}

# Логирование в канал
async def log_to_channel(context, message, username=None, file=None):
    try:
        user = context.user_data.get('user', {})
        name = user.get('name', username or 'Неизвестно')
        log_message = f"{name} (@{username or 'Без username'}): {message}" if username else f"{message}"
        if file:
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
    user['name'] = name if name else user.get('name', 'Неизвестно')
    if context:
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
    hours_left = 24 - (now - last_reset).seconds // 3600
    return daily_requests['count'] < max_requests, hours_left if hours_left > 0 else 24

# Проверка лимита парсинга
def check_parse_limit(user_id, limit, parse_type, context=None):
    users = load_users()
    user_id_str = str(user_id)
    user = users.get(user_id_str, {})
    subscription = user.get('subscription', {'type': 'Бесплатная', 'end': None})
    now = datetime.now()
    if subscription['type'].startswith('Платная') and subscription['end']:
        if datetime.fromisoformat(subscription['end']) < now:
            update_user_data(user_id, user.get('name', 'Неизвестно'), context, subscription={'type': 'Бесплатная', 'end': None})
            lang = user.get('language', 'Русский')
            texts = LANGUAGES[lang]
            if context:
                asyncio.create_task(
                    context.bot.send_message(
                        chat_id=user_id,
                        text="⚠️ Ваша платная подписка истекла. Теперь у вас бесплатная подписка с лимитом 150 пользователей на парсинг." if lang == 'Русский' else
                             "⚠️ Ваша платна підписка закінчилася. Тепер у вас безкоштовна підписка з лімітом 150 користувачів на парсинг." if lang == 'Украинский' else
                             "⚠️ Your paid subscription has expired. You now have a free subscription with a limit of 150 users for parsing." if lang == 'English' else
                             "⚠️ Dein bezahltes Abonnement ist abgelaufen. Du hast jetzt ein kostenloses Abonnement mit einem Limit von 150 Benutzern zum Parsen."
                    )
                )
            subscription = {'type': 'Бесплатная', 'end': None}
    
    max_limit = 150 if subscription['type'] == 'Бесплатная' else 15000
    return min(limit, max_limit)

# Создание файла Excel
async def create_excel_in_memory(data):
    try:
        df = pd.DataFrame(data, columns=['ID', 'Username', 'First Name', 'Last Name', 'Country', 'Age', 'Nickname'])
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
    except Exception as e:
        print(f"Ошибка создания Excel файла: {e}")
        return None

# Создание VCF файла для контактов
def create_vcf_file(data):
    try:
        vcf_content = io.BytesIO()
        for _, row in data.iterrows():
            if row['Phone'] and row['First Name']:
                vcard = vobject.vCard()
                vcard.add('fn').value = f"{row['First Name']} {row['Last Name'] or ''}".strip()
                vcard.add('tel').value = row['Phone']
                if row['Username']:
                    vcard.add('url').value = f"https://t.me/{row['Username']}"
                vcf_content.write(vcard.serialize().encode('utf-8'))
                vcf_content.write(b'\n')
        vcf_content.seek(0)
        return vcf_content
    except Exception as e:
        print(f"Ошибка создания VCF файла: {e}")
        return io.BytesIO()

# Фильтрация данных
def filter_data(data, filters):
    filtered_data = []
    for row in data:
        if not row or len(row) < 7:  # Проверка корректности данных
            continue
        user = row[6]  # Объект пользователя
        if hasattr(user, 'deleted') and user.deleted:  # Исключаем удалённые аккаунты
            continue
        if filters.get('only_with_username') and not row[1]:
            continue
        if filters.get('exclude_bots') and getattr(user, 'bot', False):
            continue
        if filters.get('only_active') and not is_active_recently(user):
            continue
        filtered_data.append(row)
    return filtered_data

def is_active_recently(user):
    if not user or not hasattr(user, 'status') or not user.status:
        return True
    if hasattr(user.status, 'was_online') and user.status.was_online:
        return (datetime.now() - user.status.was_online).days < 30
    return True

# Подсчёт статистики
def get_statistics(data):
    total = len(data)
    with_username = sum(1 for row in data if row[1])
    bots = sum(1 for row in data if getattr(row[6], 'bot', False))
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
        f"{max(0, (datetime.fromisoformat(sub_end) - datetime.now()).days * 24 + (datetime.fromisoformat(sub_end) - datetime.now()).seconds // 3600)} часов"
    )
    requests = user_data.get('requests', 0)
    name = user_data.get('name', 'Неизвестно')
    limit_left, hours_left = check_request_limit(user_id)
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
         InlineKeyboardButton("(!)", callback_data='info_requisites')],
        [InlineKeyboardButton(texts['home_cmd'], callback_data='home')]
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton("Канал с логами" if lang == 'Русский' else "Канал з логами" if lang == 'Украинский' else "Logs channel" if lang == 'English' else "Log-Kanal", callback_data='logs_channel'), 
                        InlineKeyboardButton("(!)", callback_data='info_logs')])
    
    return texts['start_menu'].format(
        name=name, user_id=user_id, lang=lang, sub_type=sub_type, sub_time=sub_time, requests=requests, limit=max(0, limit_display)
    ), InlineKeyboardMarkup(buttons)

# Обработчик команды /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
    name = update.effective_user.full_name or "Без имени"
    users = load_users()

    try:
        await client_telethon.connect()
        if not await client_telethon.is_user_authorized():
            await update.message.reply_text(LANGUAGES['Русский']['enter_phone'])
            context.user_data['waiting_for_phone'] = True
            await log_to_channel(context, f"Запрос номера телефона", username)
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
        await log_to_channel(context, f"Ошибка подключения/авторизации: {str(e)}", username)
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
    await log_to_channel(context, f"Команда /language вызвана", username)

# Обработчик команды /info
async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    await update.message.reply_text(texts['info_cmd'].format(support=SUPPORT_USERNAME))
    await log_to_channel(context, f"Команда /info вызвана", username)

# Обработчик команды /home
async def home(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
    menu_text, menu_keyboard = get_main_menu(user_id, context)
    await update.message.reply_text(menu_text, reply_markup=menu_keyboard)
    await log_to_channel(context, f"Команда /home вызвана", username)

# Обработчик команды /set_plan
async def set_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    username = update.effective_user.username or "Без username"
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет прав для этой команды.")
        await log_to_channel(context, f"Попытка вызова /set_plan без прав", username)
        return
    
    args = context.args
    if len(args) != 3:
        await update.message.reply_text("Использование: /set_plan <user_id> <type> <duration>")
        await log_to_channel(context, f"Неверное использование /set_plan", username)
        return
    
    try:
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
            await log_to_channel(context, f"Неверный тип подписки в /set_plan: {sub_type}", username)
            return
        
        subscription_type = f'Платная ({sub_type})' if sub_type in ['1h', '3d', '7d'] else 'Платная (бессрочная)'
        target_name = load_users().get(str(target_user_id), {}).get('name', 'Неизвестно')
        update_user_data(target_user_id, target_name, context, subscription={'type': subscription_type, 'end': end_time.isoformat() if end_time else None})
        
        lang = load_users().get(str(target_user_id), {}).get('language', 'Русский')
        texts = LANGUAGES[lang]
        
        notification = texts['payment_success'].format(end_time=end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'бессрочно')
        await context.bot.send_message(chat_id=target_user_id, text=f"🎉 {notification}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_update'], callback_data='update_menu')]]))
        
        await update.message.reply_text(f"Подписка для пользователя {target_user_id} ({target_name}) обновлена до {end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'бессрочно'}.")
        await log_to_channel(context, f"Администратор установил подписку для {target_user_id} ({target_name}): {sub_type}, до {end_time if end_time else 'бессрочно'}", username)
    except ValueError:
        await update.message.reply_text("Ошибка: <duration> должно быть числом.")
        await log_to_channel(context, f"Ошибка в /set_plan: duration не число", username)

# Обработчик команды /remove_plan
async def remove_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    username = update.effective_user.username or "Без username"
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет прав для этой команды.")
        await log_to_channel(context, f"Попытка вызова /remove_plan без прав", username)
        return
    
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Использование: /remove_plan <user_id>")
        await log_to_channel(context, f"Неверное использование /remove_plan", username)
        return
    
    target_user_id = args[0]
    users = load_users()
    if str(target_user_id) not in users:
        await update.message.reply_text("Пользователь не найден.")
        await log_to_channel(context, f"Пользователь {target_user_id} не найден в /remove_plan", username)
        return
    
    target_name = users.get(str(target_user_id), {}).get('name', 'Неизвестно')
    update_user_data(target_user_id, target_name, context, subscription={'type': 'Бесплатная', 'end': None})
    await update.message.reply_text(f"Платная подписка для пользователя {target_user_id} ({target_name}) удалена, установлен бесплатный план.")
    await log_to_channel(context, f"Администратор удалил платную подписку для {target_user_id} ({target_name})", username)

# Обработчик команды /note
async def note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Без username"
    if not context.args:
        await update.message.reply_text("Использование: /note <текст>")
        await log_to_channel(context, f"Неверное использование /note", username)
        return
    note_text = " ".join(context.args)
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    await log_to_channel(context, f"Заметка: {note_text}", username)
    await update.message.reply_text(LANGUAGES[lang]['note_cmd'])

# Обработчик текстовых сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data['user_id'] = user_id
    username = update.effective_user.username or "Без username"
    name = update.effective_user.full_name or "Без имени"
    users = load_users()
    text = update.message.text.strip() if update.message.text else ""
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]

    await log_to_channel(context, f"Сообщение: {text}", username)

    try:
        await client_telethon.connect()
    except telethon_errors.RPCError as e:
        await update.message.reply_text(texts['auth_error'].format(error=str(e)))
        await log_to_channel(context, f"Ошибка подключения: {str(e)}", username)
        return
    except Exception as e:
        print(f"Неизвестная ошибка подключения Telethon: {str(e)}\n{traceback.format_exc()}")
        await log_to_channel(context, f"Неизвестная ошибка подключения: {str(e)}", username)
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
            await update.message.reply_text(texts['enter_code'])
            context.user_data['waiting_for_code'] = True
            del context.user_data['waiting_for_phone']
            await log_to_channel(context, f"Телефон введён: {text}", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(texts['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода номера: {str(e)}", username)
        finally:
            await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_code'):
        try:
            await client_telethon.sign_in(context.user_data['phone'], text)
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
            await log_to_channel(context, f"Требуется пароль 2FA", username)
        except telethon_errors.RPCError as e:
            await update.message.reply_text(texts['auth_error'].format(error=str(e)))
            await log_to_channel(context, f"Ошибка ввода кода: {str(e)}", username)
        finally:
            await client_telethon.disconnect()
        return

    if context.user_data.get('waiting_for_password'):
        try:
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
            await client_telethon.disconnect()
        return

    if str(user_id) not in users or 'language' not in users[str(user_id)]:
        await client_telethon.disconnect()
        return
    
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
                    text=f"Пользователь {name} (@{username}) (ID: {user_id}) отправил хэш транзакции:\n{text}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("Подтвердить", callback_data=f'accept_{user_id}'),
                         InlineKeyboardButton("Отклонить", callback_data=f'reject_{user_id}')]
                    ])
                )
            except telegram_error.BadRequest as e:
                print(f"Ошибка отправки хэша администратору {admin_id}: {e}")
        await log_to_channel(context, f"Хэш транзакции: {text}", username)
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
                await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=[{"type": "emoji", "emoji": "🎉"}])
                await log_to_channel(context, f"ID получен: {entity.id}", username)
            except telethon_errors.RPCError as e:
                await update.message.reply_text(texts['entity_error'])
                await log_to_channel(context, f"Ошибка получения ID: {str(e)}", username)
        elif update.message.forward_origin and hasattr(update.message.forward_origin, 'chat'):
            chat_id = update.message.forward_origin.chat.id
            msg = await update.message.reply_text(texts['id_result'].format(id=chat_id), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
            ]))
            await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=[{"type": "emoji", "emoji": "🎉"}])
            await log_to_channel(context, f"Chat ID получен: {chat_id}", username)
        elif update.message.forward_origin and hasattr(update.message.forward_origin, 'sender_user'):
            user_id_forward = update.message.forward_origin.sender_user.id
            msg = await update.message.reply_text(texts['id_result'].format(id=user_id_forward), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(texts['close'], callback_data='close_id'), InlineKeyboardButton(texts['continue_id'], callback_data='continue_id')]
            ]))
            await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=[{"type": "emoji", "emoji": "🎉"}])
            await log_to_channel(context, f"User ID получен: {user_id_forward}", username)
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
                await context.bot.set_message_reaction(chat_id=update.message.chat_id, message_id=msg.message_id, reaction=[{"type": "emoji", "emoji": "🎉"}])
                await log_to_channel(context, f"ID через ссылку: {entity.id}", username)
            except telethon_errors.RPCError as e:
                await update.message.reply_text(texts['entity_error'])
                await log_to_channel(context, f"Ошибка получения ID по ссылке: {str(e)}", username)
        del context.user_data['waiting_for_id']
        await client_telethon.disconnect()
        return
    
    if 'waiting_for_limit' in context.user_data:
        try:
            limit = int(text)
            max_limit = 150 if users[str(user_id)]['subscription']['type'] == 'Бесплатная' else 15000
            if limit <= 0 or limit > max_limit:
                await update.message.reply_text(texts['invalid_limit'].format(max_limit=max_limit), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
                await client_telethon.disconnect()
                return
            context.user_data['limit'] = limit
            del context.user_data['waiting_for_limit']
            await ask_for_filters(update.message, context)
        except ValueError:
            await update.message.reply_text(texts['invalid_number'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
        finally:
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
                link = link.strip()
                if not link:
                    continue
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
                if not normalized_links:
                    await update.message.reply_text(texts['invalid_link'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['fix_link'], callback_data='fix_link')]]))
                    context.user_data['last_input'] = text
                    await client_telethon.disconnect()
                    return
                context.user_data['links'] = normalized_links
            await ask_for_limit(update.message, context)
        elif update.message.forward_origin and hasattr(update.message.forward_origin, 'chat') and context.user_data['parse_type'] == 'parse_post_commentators':
            if update.message.forward_origin.chat.username:
                context.user_data['links'] = [f"https://t.me/{update.message.forward_origin.chat.username}/{update.message.forward_origin.message_id}"]
            else:
                context.user_data['links'] = [f"https://t.me/c/{str(update.message.forward_origin.chat.id)[4:]}/{update.message.forward_origin.message_id}"]
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
        [InlineKeyboardButton("1000", callback_data='limit_1000'), InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]
    ]
    if is_paid:
        keyboard.insert(0, [InlineKeyboardButton("15000", callback_data='limit_15000')])
    keyboard.append([InlineKeyboardButton("Другое" if lang == 'Русский' else "Інше" if lang == 'Украинский' else "Other" if lang == 'English' else "Andere", callback_data='limit_custom')])
    if is_paid:
        keyboard.append([InlineKeyboardButton(texts['no_filter'], callback_data='no_filter')])
    await message.reply_text(texts['limit'], reply_markup=InlineKeyboardMarkup(keyboard))
    await log_to_channel(context, f"Запрос лимита парсинга", message.from_user.username or "Без username")

# Запрос фильтров
async def ask_for_filters(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    username = message.from_user.username or "Без username"
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
    await log_to_channel(context, f"Запрос фильтра username", username)

# Функции парсинга
async def parse_commentators(group_link, limit):
    try:
        entity = await client_telethon.get_entity(group_link)
        commentators = set()
        messages = await client_telethon.get_messages(entity, limit=min(limit, 1000))  # Ограничение Telegram API
        for message in messages:
            if hasattr(message, 'sender_id') and message.sender_id:
                commentators.add(message.sender_id)
        
        data = []
        for commentator_id in list(commentators)[:limit]:
            try:
                participant = await client_telethon.get_entity(commentator_id)
                if isinstance(participant, tl.types.User):
                    data.append([
                        participant.id,
                        participant.username or "",
                        participant.first_name or "",
                        participant.last_name or "",
                        "",  # Country
                        0,   # Age
                        participant
                    ])
            except (telethon_errors.RPCError, ValueError) as e:
                print(f"Ошибка получения сущности для ID {commentator_id}: {str(e)}")
        return data
    except Exception as e:
        print(f"Ошибка парсинга комментаторов: {e}")
        return []

async def parse_participants(group_link, limit):
    try:
        entity = await client_telethon.get_entity(group_link)
        participants = await client_telethon.get_participants(entity, limit=limit)
        data = []
        for participant in participants:
            if isinstance(participant, tl.types.User):
                data.append([
                    participant.id,
                    participant.username or "",
                    participant.first_name or "",
                    participant.last_name or "",
                    "",  # Country
                    0,   # Age
                    participant
                ])
        return data
    except Exception as e:
        print(f"Ошибка парсинга участников: {e}")
        return []

async def parse_post_commentators(link, limit):
    try:
        parts = link.split('/')
        chat_id = parts[-2] if parts[-2].startswith('+') else f'@{parts[-2]}'
        message_id = int(parts[-1])
        entity = await client_telethon.get_entity(chat_id)
        message = await client_telethon.get_messages(entity, ids=message_id)
        if not message or not message[0]:
            return []
        
        commentators = set()
        replies = await client_telethon.get_messages(entity, limit=limit, reply_to=message[0].id)
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
                        participant.username or "",
                        participant.first_name or "",
                        participant.last_name or "",
                        "",  # Country
                        0,   # Age
                        participant
                    ])
            except (telethon_errors.RPCError, ValueError) as e:
                print(f"Ошибка получения сущности для ID {commentator_id}: {str(e)}")
        return data
    except Exception as e:
        print(f"Ошибка парсинга комментаторов поста: {e}")
        return []

async def parse_phone_contacts(group_link, limit):
    try:
        entity = await client_telethon.get_entity(group_link)
        participants = await client_telethon.get_participants(entity, limit=limit)
        data = []
        for participant in participants:
            if isinstance(participant, tl.types.User) and participant.phone:
                data.append([
                    participant.id,
                    participant.username or "",
                    participant.first_name or "",
                    participant.last_name or "",
                    "",  # Country
                    0,   # Age
                    participant
                ])
        return data
    except Exception as e:
        print(f"Ошибка парсинга контактов: {e}")
        return []

async def parse_auth_access(link, context):
    user_id = context.user_data.get('user_id')
    username = context.user_data.get('username', 'Без username')
    name = load_users().get(str(user_id), {}).get('name', 'Неизвестно')
    lang = load_users().get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    
    try:
        parts = link.split('/')
        chat_id = parts[-1] if parts[-1].startswith('+') else f'@{parts[-1]}'
        entity = await client_telethon.get_entity(chat_id)
        if hasattr(entity, 'participants_count'):
            await context.bot.send_message(chat_id=user_id, text=texts['auth_success'])
            await log_to_channel(context, f"Доступ к закрытому чату {chat_id} предоставлен", username)
        else:
            await context.bot.send_message(chat_id=user_id, text=texts['auth_error'])
            await log_to_channel(context, f"Ошибка доступа к {chat_id}", username)
    except telethon_errors.RPCError as e:
        await context.bot.send_message(chat_id=user_id, text=texts['auth_error'])
        await log_to_channel(context, f"Ошибка авторизации доступа: {str(e)}", username)

# Обработка парсинга
async def process_parsing(message, context):
    user_id = context.user_data.get('user_id', message.from_user.id)
    username = message.from_user.username or "Без username"
    name = message.from_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    subscription = users.get(str(user_id), {}).get('subscription', {'type': 'Бесплатная', 'end': None})
    
    context.user_data['parsing_in_progress'] = True
    
    try:
        await client_telethon.connect()
        all_data = []
        for link in context.user_data.get('links', []):
            try:
                if link.startswith('@'):
                    normalized_link = f"https://t.me/{link[1:]}"
                elif not link.startswith('http'):
                    normalized_link = f"https://t.me/{link}"
                else:
                    normalized_link = link
                
                entity_key = normalized_link.split('/')[-2] if context.user_data['parse_type'] in ['parse_post_commentators', 'parse_auth_access'] else normalized_link
                await client_telethon.get_entity(entity_key)
            except telethon_errors.RPCError as e:
                await message.reply_text(texts['no_access'].format(link=link))
                await log_to_channel(context, f"Нет доступа к {link}: {str(e)}", username)
                context.user_data['parsing_in_progress'] = False
                await client_telethon.disconnect()
                return
            
            parse_type = context.user_data['parse_type']
            limit = check_parse_limit(user_id, context.user_data.get('limit', 150 if subscription['type'] == 'Бесплатная' else 15000), parse_type, context)
            
            if parse_type == 'parse_commentators':
                data = await parse_commentators(normalized_link, limit)
                caption = texts['caption_commentators']
            elif parse_type == 'parse_participants':
                data = await parse_participants(normalized_link, limit)
                caption = texts['caption_participants']
            elif parse_type == 'parse_post_commentators':
                data = await parse_post_commentators(normalized_link, limit)
                caption = texts['caption_post_commentators']
            elif parse_type == 'parse_phone_contacts':
                data = await parse_phone_contacts(normalized_link, limit)
                caption = texts['caption_phones']
            elif parse_type == 'parse_auth_access':
                await parse_auth_access(normalized_link, context)
                context.user_data['parsing_in_progress'] = False
                await client_telethon.disconnect()
                return
            else:
                await message.reply_text("Неизвестный тип парсинга.")
                await log_to_channel(context, f"Неизвестный тип парсинга: {parse_type}", username)
                context.user_data['parsing_in_progress'] = False
                await client_telethon.disconnect()
                return
            
            filtered_data = filter_data(data, context.user_data.get('filters', {}))
            all_data.extend(filtered_data)

        if not all_data:
            await message.reply_text("Не удалось собрать данные. Проверьте ссылку или доступ к чату.")
            await log_to_channel(context, f"Нет данных для парсинга", username)
            context.user_data['parsing_in_progress'] = False
            await client_telethon.disconnect()
            return
        
        context.user_data['parsing_done'] = True
        
        excel_file = await create_excel_in_memory(all_data)
        if not excel_file:
            await message.reply_text("Ошибка создания файла Excel.")
            await log_to_channel(context, f"Ошибка создания Excel файла", username)
            context.user_data['parsing_in_progress'] = False
            await client_telethon.disconnect()
            return
        
        update_user_data(user_id, name, context, requests=1)
        
        checklist = texts['parsing_checklist'].format(
            username_filter='✅' if context.user_data.get('filters', {}).get('only_with_username') else '❌',
            bots_filter='✅' if context.user_data.get('filters', {}).get('exclude_bots') else '❌',
            active_filter='✅' if context.user_data.get('filters', {}).get('only_active') else '❌'
        )
        stats = get_statistics(all_data)
        
        if parse_type == 'parse_phone_contacts':
            df = pd.DataFrame(all_data, columns=['ID', 'Username', 'First Name', 'Last Name', 'Country', 'Age', 'User'])
            df['Phone'] = [user.phone if hasattr(user, 'phone') else "" for user in df['User']]
            vcf_file = create_vcf_file(df)
            excel_filename = f"phones_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            vcf_filename = f"phones_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.vcf"
            
            await message.reply_document(document=excel_file, filename=excel_filename, caption=f"{caption}\n\n{checklist}\n\n{stats}")
            await message.reply_document(document=vcf_file, filename=vcf_filename, caption="VCF файл с контактами")
            await log_to_channel(context, f"Файлы отправлены: {excel_filename}, {vcf_filename}", username, file=excel_file)
        else:
            filename = f"{parse_type}_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            await message.reply_document(document=excel_file, filename=filename, caption=f"{caption}\n\n{checklist}\n\n{stats}")
            await log_to_channel(context, f"Файл отправлен: {filename}", username, file=excel_file)
        
        rating_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("1", callback_data='rate_1'), InlineKeyboardButton("2", callback_data='rate_2'), 
             InlineKeyboardButton("3", callback_data='rate_3'), InlineKeyboardButton("4", callback_data='rate_4'), 
             InlineKeyboardButton("5", callback_data='rate_5')]
        ])
        await message.reply_text(texts['rate_parsing'], reply_markup=rating_keyboard)
        
    except telethon_errors.FloodWaitError as e:
        await message.reply_text(texts['flood_error'].format(e=f"Слишком много запросов. Повторите через {e.seconds} секунд"))
        await log_to_channel(context, f"FloodWaitError: {str(e)}", username)
    except telethon_errors.RPCError as e:
        await message.reply_text(texts['rpc_error'].format(e=str(e)))
        await log_to_channel(context, f"RPCError: {str(e)}", username)
    except Exception as e:
        await message.reply_text(f"Произошла ошибка: {str(e)}")
        await log_to_channel(context, f"Неизвестная ошибка парсинга: {str(e)}\n{traceback.format_exc()}", username)
    finally:
        context.user_data['parsing_in_progress'] = False
        if 'parsing_done' not in context.user_data:
            context.user_data['parsing_done'] = True
        if client_telethon.is_connected():
            await client_telethon.disconnect()

# Обработчик callback-запросов
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    username = query.from_user.username or "Без username"
    name = query.from_user.full_name or "Без имени"
    users = load_users()
    lang = users.get(str(user_id), {}).get('language', 'Русский')
    texts = LANGUAGES[lang]
    data = query.data

    await query.answer()

    if data.startswith('lang_'):
        lang = data.split('_')[1]
        update_user_data(user_id, name, context, lang=lang)
        await query.edit_message_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data='subscribed')]]))
        await log_to_channel(context, f"Язык изменён на {lang}", username)
        return

    if data == 'subscribed':
        try:
            member = await context.bot.get_chat_member(SUBSCRIPTION_CHANNEL_ID, user_id)
            if member.status in ['member', 'administrator', 'creator']:
                menu_text, menu_keyboard = get_main_menu(user_id, context)
                await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
                await log_to_channel(context, f"Подписка подтверждена", username)
            else:
                await query.edit_message_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data='subscribed')]]))
                await log_to_channel(context, f"Пользователь не подписан на канал", username)
        except telegram_error.BadRequest:
            await query.edit_message_text(texts['subscribe'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['subscribed'], callback_data='subscribed')]]))
            await log_to_channel(context, f"Ошибка проверки подписки", username)
        return

    if data == 'identifiers':
        await query.edit_message_text(texts['identifiers'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='close_menu')]]))
        context.user_data['waiting_for_id'] = True
        await log_to_channel(context, f"Выбран пункт 'Идентификаторы'", username)
        return
    
    if data == 'close_id':
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
        if 'waiting_for_id' in context.user_data:
            del context.user_data['waiting_for_id']
        await log_to_channel(context, f"Закрытие поиска ID", username)
        return
    
    if data == 'continue_id':
        await query.edit_message_text(texts['identifiers'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='close_menu')]]))
        context.user_data['waiting_for_id'] = True
        await log_to_channel(context, f"Продолжение поиска ID", username)
        return

    if data == 'parser':
        keyboard = [
            [InlineKeyboardButton("Авторы сообщений" if lang == 'Русский' else "Автори повідомлень" if lang == 'Украинский' else "Message authors" if lang == 'English' else "Nachrichtenautoren", callback_data='parse_commentators')],
            [InlineKeyboardButton("Участники чата" if lang == 'Русский' else "Учасники чату" if lang == 'Украинский' else "Chat participants" if lang == 'English' else "Chat-Teilnehmer", callback_data='parse_participants')],
            [InlineKeyboardButton("Комментаторы поста" if lang == 'Русский' else "Коментатори поста" if lang == 'Украинский' else "Post commentators" if lang == 'English' else "Beitragskommentatoren", callback_data='parse_post_commentators')],
            [InlineKeyboardButton(texts['phone_contacts'], callback_data='parse_phone_contacts')],
            [InlineKeyboardButton(texts['auth_access'], callback_data='parse_auth_access')],
            [InlineKeyboardButton(texts['close'], callback_data='close_menu')]
        ]
        await query.edit_message_text(texts['parser'], reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Выбран пункт 'Парсер'", username)
        return

    if data.startswith('parse_'):
        parse_type = data
        context.user_data['parse_type'] = parse_type
        if parse_type == 'parse_commentators' or parse_type == 'parse_participants' or parse_type == 'parse_phone_contacts' or parse_type == 'parse_auth_access':
            await query.edit_message_text(texts['link_group'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='close_menu')]]))
        elif parse_type == 'parse_post_commentators':
            await query.edit_message_text(texts['link_post'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='close_menu')]]))
        await log_to_channel(context, f"Выбран тип парсинга: {parse_type}", username)
        return

    if data.startswith('limit_'):
        if data == 'limit_custom':
            await query.edit_message_text(texts['limit'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['skip'], callback_data='skip_limit')]]))
            context.user_data['waiting_for_limit'] = True
            await log_to_channel(context, f"Пользователь выбрал кастомный лимит", username)
        else:
            limit = int(data.split('_')[1])
            context.user_data['limit'] = limit
            await ask_for_filters(query.message, context)
            await log_to_channel(context, f"Лимит установлен: {limit}", username)
        return

    if data == 'skip_limit':
        subscription = users.get(str(user_id), {}).get('subscription', {'type': 'Бесплатная', 'end': None})
        context.user_data['limit'] = 150 if subscription['type'] == 'Бесплатная' else 15000
        await ask_for_filters(query.message, context)
        await log_to_channel(context, f"Лимит пропущен, установлен максимальный по подписке: {context.user_data['limit']}", username)
        return

    if data == 'filter_yes':
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        filters[context.user_data.get('current_filter', 'only_with_username')] = True
        context.user_data['filters'] = filters
        next_filter = {
            'only_with_username': 'exclude_bots',
            'exclude_bots': 'only_active',
            'only_active': None
        }
        current_filter = context.user_data.get('current_filter', 'only_with_username')
        if next_filter[current_filter]:
            context.user_data['current_filter'] = next_filter[current_filter]
            keyboard = [
                [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
                 InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
                [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
            ]
            if next_filter[current_filter] == 'exclude_bots':
                await query.edit_message_text(texts['filter_bots'], reply_markup=InlineKeyboardMarkup(keyboard))
            elif next_filter[current_filter] == 'only_active':
                await query.edit_message_text(texts['filter_active'], reply_markup=InlineKeyboardMarkup(keyboard))
            await log_to_channel(context, f"Фильтр {current_filter} установлен: Да", username)
        else:
            await process_parsing(query.message, context)
            await log_to_channel(context, f"Все фильтры установлены, начало парсинга", username)
        return

    if data == 'filter_no' or data == 'skip_filters':
        filters = context.user_data.get('filters', {'only_with_username': False, 'exclude_bots': False, 'only_active': False})
        current_filter = context.user_data.get('current_filter', 'only_with_username')
        next_filter = {
            'only_with_username': 'exclude_bots',
            'exclude_bots': 'only_active',
            'only_active': None
        }
        if next_filter[current_filter] and data != 'skip_filters':
            context.user_data['current_filter'] = next_filter[current_filter]
            keyboard = [
                [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data='filter_yes'),
                 InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='filter_no')],
                [InlineKeyboardButton(texts['skip'], callback_data='skip_filters')]
            ]
            if next_filter[current_filter] == 'exclude_bots':
                await query.edit_message_text(texts['filter_bots'], reply_markup=InlineKeyboardMarkup(keyboard))
            elif next_filter[current_filter] == 'only_active':
                await query.edit_message_text(texts['filter_active'], reply_markup=InlineKeyboardMarkup(keyboard))
            await log_to_channel(context, f"Фильтр {current_filter} установлен: Нет", username)
        else:
            await process_parsing(query.message, context)
            await log_to_channel(context, f"Пропуск фильтров или завершение их установки, начало парсинга", username)
        return

    if data == 'no_filter':
        context.user_data['filters'] = {'only_with_username': False, 'exclude_bots': False, 'only_active': False}
        await process_parsing(query.message, context)
        await log_to_channel(context, f"Фильтры отключены, начало парсинга", username)
        return

    if data == 'fix_link':
        last_input = context.user_data.get('last_input', "")
        if last_input:
            suggested_link = f"https://t.me/{last_input.strip('@')}" if not last_input.startswith('http') else last_input
            await query.edit_message_text(texts['suggest_link'].format(link=suggested_link), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Да" if lang == 'Русский' else "Так" if lang == 'Украинский' else "Yes" if lang == 'English' else "Ja", callback_data=f'use_link_{suggested_link}')],
                [InlineKeyboardButton("Нет" if lang == 'Русский' else "Ні" if lang == 'Украинский' else "No" if lang == 'English' else "Nein", callback_data='retry_link')]
            ]))
            await log_to_channel(context, f"Предложение исправления ссылки: {suggested_link}", username)
        else:
            await query.edit_message_text(texts['retry_link'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='close_menu')]]))
            await log_to_channel(context, f"Запрос повторной отправки ссылки", username)
        return

    if data.startswith('use_link_'):
        link = data.split('use_link_')[1]
        context.user_data['links'] = [link]
        await ask_for_limit(query.message, context)
        await log_to_channel(context, f"Использована исправленная ссылка: {link}", username)
        return

    if data == 'retry_link':
        if context.user_data['parse_type'] == 'parse_post_commentators':
            await query.edit_message_text(texts['link_post'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='close_menu')]]))
        else:
            await query.edit_message_text(texts['link_group'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='close_menu')]]))
        await log_to_channel(context, f"Повторный запрос ссылки", username)
        return

    if data == 'subscribe':
        keyboard = [
            [InlineKeyboardButton(texts['subscription_1h'], callback_data='sub_1h')],
            [InlineKeyboardButton(texts['subscription_3d'], callback_data='sub_3d')],
            [InlineKeyboardButton(texts['subscription_7d'], callback_data='sub_7d')],
            [InlineKeyboardButton(texts['close'], callback_data='close_menu')]
        ]
        await query.edit_message_text("Выберите тип подписки:", reply_markup=InlineKeyboardMarkup(keyboard))
        await log_to_channel(context, f"Выбран пункт 'Оформить подписку'", username)
        return

    if data.startswith('sub_'):
        sub_type = data.split('_')[1]
        amount = {'1h': 2, '3d': 5, '7d': 7}[sub_type]
        keyboard = [
            [InlineKeyboardButton(texts['payment_paid'], callback_data=f'paid_{sub_type}')],
            [InlineKeyboardButton(texts['payment_cancel'], callback_data='close_menu')]
        ]
        await query.edit_message_text(
            texts['payment_wallet'].format(amount=amount, address=TON_WALLET_ADDRESS),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        await log_to_channel(context, f"Выбрана подписка: {sub_type}, сумма: {amount} USDT", username)
        return

    if data.startswith('paid_'):
        sub_type = data.split('_')[1]
        context.user_data['waiting_for_hash'] = True
        context.user_data['sub_type'] = sub_type
        await query.edit_message_text(texts['payment_hash'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['payment_cancel'], callback_data='close_menu')]]))
        await log_to_channel(context, f"Ожидание хэша транзакции для подписки {sub_type}", username)
        return

    if data.startswith('accept_'):
        target_user_id = data.split('_')[1]
        target_name = users.get(str(target_user_id), {}).get('name', 'Неизвестно')
        sub_type = context.user_data.get('sub_type', '1h')
        now = datetime.now()
        end_time = now + timedelta(hours=1 if sub_type == '1h' else 72 if sub_type == '3d' else 168)
        subscription_type = f'Платная ({sub_type})'
        update_user_data(target_user_id, target_name, None, subscription={'type': subscription_type, 'end': end_time.isoformat()})
        
        target_lang = users.get(str(target_user_id), {}).get('language', 'Русский')
        target_texts = LANGUAGES[target_lang]
        await context.bot.send_message(
            chat_id=target_user_id,
            text=target_texts['payment_success'].format(end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(target_texts['payment_update'], callback_data='update_menu')]])
        )
        await query.edit_message_text(f"Подписка для {target_name} (ID: {target_user_id}) подтверждена до {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        await log_to_channel(context, f"Подписка подтверждена для {target_name} (ID: {target_user_id}): {sub_type}", username)
        return

    if data.startswith('reject_'):
        target_user_id = data.split('_')[1]
        target_name = users.get(str(target_user_id), {}).get('name', 'Неизвестно')
        target_lang = users.get(str(target_user_id), {}).get('language', 'Русский')
        target_texts = LANGUAGES[target_lang]
        await context.bot.send_message(chat_id=target_user_id, text=target_texts['payment_error'])
        await query.edit_message_text(f"Транзакция для {target_name} (ID: {target_user_id}) отклонена.")
        await log_to_channel(context, f"Транзакция отклонена для {target_name} (ID: {target_user_id})", username)
        return

    if data == 'update_menu':
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
        await log_to_channel(context, f"Обновление меню после оплаты", username)
        return

    if data == 'requisites':
        await query.edit_message_text(texts['requisites'].format(support=SUPPORT_USERNAME), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='close_menu')]]))
        await log_to_channel(context, f"Выбран пункт 'Реквизиты'", username)
        return

    if data == 'logs_channel':
        if str(user_id) in ADMIN_IDS:
            await query.edit_message_text(texts['logs_channel'], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='close_menu')]]))
            await log_to_channel(context, f"Администратор запросил канал с логами", username)
        else:
            menu_text, menu_keyboard = get_main_menu(user_id, context)
            await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
            await log_to_channel(context, f"Попытка доступа к логам без прав", username)
        return

    if data == 'close_menu' or data == 'home':
        menu_text, menu_keyboard = get_main_menu(user_id, context)
        await query.edit_message_text(menu_text, reply_markup=menu_keyboard)
        context.user_data.clear()
        await log_to_channel(context, f"Возврат в главное меню", username)
        return

    if data.startswith('rate_'):
        rating = data.split('_')[1]
        await query.edit_message_text(texts['thanks'])
        await log_to_channel(context, f"Оценка парсинга: {rating}", username)
        return

    if data.startswith('info_'):
        info_type = data.split('_')[1]
        info_texts = {
            'identifiers': "Функция для получения ID пользователей, чатов или постов.",
            'parser': "Инструмент для сбора данных из групп, каналов и комментариев.",
            'subscribe': "Оформление платной подписки для расширенных возможностей.",
            'requisites': "Информация о способах оплаты подписки.",
            'logs': "Канал с логами доступен только администраторам."
        }
        await query.edit_message_text(info_texts.get(info_type, "Информация недоступна"), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(texts['close'], callback_data='close_menu')]]))
        await log_to_channel(context, f"Запрос информации: {info_type}", username)

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

    print("Бот запущен...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
