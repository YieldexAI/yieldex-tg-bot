import asyncio
import json
import datetime
import time
import requests
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import aiohttp

import config
import database

# Кеш для хранения отформатированных сообщений
_formatted_data_cache = {}

# Кэш для хранения статуса администратора (ключ: user_id, значение: {is_admin: bool, timestamp: time})
_admin_status_cache = {}

# Create scheduler for daily notifications
scheduler = AsyncIOScheduler(timezone=pytz.UTC)

# Создаем сессию один раз при запуске бота
session = None

async def setup_aiohttp_session():
    global session
    session = aiohttp.ClientSession()

# Function to log bot actions
async def log_bot_action(action, user_id=None, username=None):
    """Log bot actions and save to database if user_id is provided"""
    timestamp = datetime.datetime.now().isoformat()
    user_info = f"[USER:{user_id}|{username}]" if user_id else ""
    print(f"[{timestamp}] {user_info} Bot Action: {action}")
    
    # Логируем действие в базу данных 
    # (будет пропущено для админов и игнорируемых действий - см. log_user_action)
    if user_id:
        await database.log_user_action(action, user_id, username)

# Асинхронная функция для отправки запросов к Telegram API
async def send_telegram_request_async(method, params=None):
    """Send request to Telegram API using async client"""
    global session
    url = f"{config.TELEGRAM_API_URL}/{method}"
    
    try:
        async with session.post(url, json=params if params else {}) as response:
            return await response.json()
    except Exception as e:
        print(f"Error sending Telegram request: {e}")
        # Пробуем восстановить сессию, если она закрылась
        if session.closed:
            session = aiohttp.ClientSession()
            async with session.post(url, json=params if params else {}) as response:
                return await response.json()
        raise

# Асинхронная обертка для синхронного кода
def send_telegram_request(method, params=None):
    """Create async task for telegram request and return placeholder"""
    asyncio.create_task(send_telegram_request_async(method, params))
    return {"ok": True, "result": []}  # Возвращаем заглушку вместо None

# Оптимизированное форматирование для частых случаев
def format_top_apy_data(data, position):
    """Format APY data for display with ranking"""
    if not data:
        return "No data available"
    
    # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: добавляем APY в ключ кэша, чтобы учитывать изменения в значениях
    cache_key = f"{data.get('pool_id', '')}_{data.get('apy')}_{position}"
    
    if cache_key in _formatted_data_cache:
        return _formatted_data_cache[cache_key]
    
    # Extract protocol name from pool_id
    pool_id = data.get('pool_id', '')
    protocol_parts = pool_id.split('_')
    
    # Проверяем, есть ли как минимум 3 части в pool_id
    if len(protocol_parts) >= 3:
        # Берем третий элемент и все последующие, объединяя их снова с '_'
        protocol = '_'.join(protocol_parts[2:])
    else:
        # Если частей меньше 3, используем первую часть или пустую строку
        protocol = protocol_parts[0] if protocol_parts else ''
    
    # Format TVL
    tvl = data.get('tvl', 0)
    if tvl >= 1000000:
        tvl_formatted = f"${tvl / 1000000:.1f}M"
    elif tvl >= 1000:
        tvl_formatted = f"${tvl / 1000:.1f}K"
    else:
        tvl_formatted = f"${tvl:.0f}"
    
    # Select emoji for position
    position_emoji = '🥇' if position == 1 else '🥈' if position == 2 else '🥉' if position == 3 else '🏅'
    
    # Безопасное форматирование APY данных с проверкой на None и нулевые значения
    apy = data.get('apy')
    apy_base = data.get('apy_base')
    apy_reward = data.get('apy_reward')
    apy_mean_30d = data.get('apy_mean_30d')
    
    # Проверка и форматирование для всех значений APY
    apy_total = f"{apy:.2f}%" if apy is not None else 'N/A'
    apy_base_fmt = f"{apy_base:.2f}%" if apy_base is not None else 'N/A'
    apy_reward_fmt = f"{apy_reward:.2f}%" if apy_reward is not None else 'N/A'
    apy_mean_30d_fmt = f"{apy_mean_30d:.2f}%" if apy_mean_30d is not None else 'N/A'
    
    # Экранируем спецсимволы в названии протокола
    protocol_safe = protocol.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]')
    
    # Add link to pool site, if available
    site_link = f"   ├ [Pool Site]({data.get('site_url')})\n" if data.get('site_url') else ''
    
    try:
        result = (
            f"{position_emoji} *{data.get('asset')}* on *{data.get('chain')}*\n"
            f"   ┌ Protocol: *{protocol_safe}*\n"
            f"{site_link}"
            f"   ├ APY Total: *{apy_total}*\n"
            f"   ├ APY Base: {apy_base_fmt}\n"
            f"   ├ APY Reward: {apy_reward_fmt}\n"
            f"   ├ Avg APY 30d: {apy_mean_30d_fmt}\n"
            f"   └ TVL: {tvl_formatted}"
        )
    except Exception as e:
        print(f"[FORMAT ERROR] Error formatting pool data: {e}")
        # Упрощённое форматирование в случае ошибки
        result = (
            f"{position_emoji} Asset: {data.get('asset')} on {data.get('chain')}\n"
            f"Protocol: {protocol}\n"
            f"APY: {apy_total}\n"
            f"TVL: {tvl_formatted}"
        )
    
    # Сохраняем и возвращаем результат
    _formatted_data_cache[cache_key] = result
    return result

# Function to create paginated assets keyboard
async def create_paginated_assets_keyboard(page=0, items_per_page=12):
    """Create a keyboard with pagination for assets"""
    assets = await database.get_all_assets()
    
    # Calculate assets for the current page
    start_index = page * items_per_page
    end_index = min(start_index + items_per_page, len(assets))
    page_assets = assets[start_index:end_index]
    
    # Format keyboard: 3 assets per row
    keyboard = []
    row = []
    
    for i, asset in enumerate(page_assets):
        row.append({"text": asset, "callback_data": f"asset_{asset}"})
        
        # Add 3 buttons per row
        if len(row) == 3 or i == len(page_assets) - 1:
            keyboard.append(row.copy())
            row = []
    
    # Add navigation buttons
    nav_row = []
    if page > 0:
        nav_row.append({"text": "⬅️ Previous", "callback_data": f"page_{page-1}"})
    
    # Изменяем кнопку Back - убираем иконку, если она есть
    nav_row.append({"text": "Back", "callback_data": "back_to_main"})
    
    if end_index < len(assets):
        nav_row.append({"text": "Next ➡️", "callback_data": f"page_{page+1}"})
    
    keyboard.append(nav_row)
    
    return {"inline_keyboard": keyboard}

# Function to create paginated chains keyboard
async def create_paginated_chains_keyboard(page=0, items_per_page=12):
    """Create a keyboard with pagination for chains"""
    chains = await database.get_all_chains()
    
    # Calculate chains for the current page
    start_index = page * items_per_page
    end_index = min(start_index + items_page, len(chains))
    page_chains = chains[start_index:end_index]
    
    # Format keyboard: 3 chains per row
    keyboard = []
    row = []
    
    for i, chain in enumerate(page_chains):
        row.append({"text": chain, "callback_data": f"chain_{chain}"})
        
        # Add 3 buttons per row
        if len(row) == 3 or i == len(page_chains) - 1:
            keyboard.append(row)
            row = []
    
    # Add pagination buttons if needed
    has_prev = page > 0
    has_next = end_index < len(chains)
    
    pagination_row = []
    if has_prev:
        pagination_row.append({"text": "◀️ Prev", "callback_data": f"chains_page_{page-1}"})
        
    pagination_row.append({"text": "Back", "callback_data": "back_to_main"})
    
    if has_next:
        pagination_row.append({"text": "Next ▶️", "callback_data": f"chains_page_{page+1}"})
    
    if pagination_row:
        keyboard.append(pagination_row)
    
    return {"inline_keyboard": keyboard}

# Function to create main menu
async def create_main_menu(user_id):
    """Create the main menu with admin options if applicable"""
    # Проверка прав администратора с использованием кэша
    is_admin = False
    
    # Проверяем кэш - если запись свежее 1 часа, используем кэшированный результат
    if str(user_id) in _admin_status_cache and (time.time() - _admin_status_cache[str(user_id)]["timestamp"]) < 3600:
        is_admin = _admin_status_cache[str(user_id)]["is_admin"]
    else:
        # Запрашиваем права из базы
        is_admin = await database.is_user_admin(user_id)
        # Кэшируем результат
        _admin_status_cache[str(user_id)] = {"is_admin": is_admin, "timestamp": time.time()}
    
    # Basic buttons for all users
    keyboard = {
        "inline_keyboard": [
            [{"text": "🥇 TOP-1", "callback_data": "show_top_1"}],
            [{"text": "🥉 TOP-3", "callback_data": "show_top_3"}],
            [{"text": "💲 Select Assets", "callback_data": "show_assets"}],
            [{"text": "🔗 Select Chains", "callback_data": "show_chains"}],
            [{"text": "💻 Request Feature", "callback_data": "feedback"}]
        ]
    }
    
    # Add analytics button only for admins
    if is_admin:
        keyboard["inline_keyboard"].append([{"text": "Analytics", "callback_data": "show_analytics"}])
    
    return keyboard

# Process incoming message
async def process_message(message):
    """Process incoming Telegram messages"""
    # Extract message data
    message_id = message.get('message_id')
    chat_id = message.get('chat', {}).get('id')
    text = message.get('text', '')
    user_id = message.get('from', {}).get('id')
    username = message.get('from', {}).get('username', f"user_{user_id}")
    
    # Process commands
    if text == '/start':
        await handle_start_command(chat_id, user_id, username)
    elif text == '/top':
        await handle_top_command(chat_id)
    elif text == '/assets':
        await handle_assets_command(chat_id)
    elif text == '/chains':
        # Команда доступна всем пользователям
        # Получаем информацию о задаче уведомления
        notification_job = scheduler.get_job('daily_notification')
        
        if notification_job:
            next_run = notification_job.next_run_time
            now = datetime.datetime.now(pytz.UTC)
            time_until_next = next_run - now
            
            # Отправляем информацию о следующем запуске
            await send_telegram_request_async("sendMessage", {
                "chat_id": chat_id,
                "text": f"📅 Next notification scheduled for:\n{next_run.strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n⏱️ Time remaining: {time_until_next}"
            })
        else:
            await send_telegram_request_async("sendMessage", {
                "chat_id": chat_id,
                "text": "❌ Notification task not found!"
            })
    elif text == '/refresh' and await database.is_user_admin(user_id):
        await send_telegram_request_async("sendMessage", {
            "chat_id": chat_id,
            "text": "🔄 Forcing refresh of all caches..."
        })
        
        success = await database.force_refresh_all_caches()
        
        if success:
            await send_telegram_request_async("sendMessage", {
                "chat_id": chat_id,
                "text": "✅ All caches successfully updated with current data"
            })
        else:
            await send_telegram_request_async("sendMessage", {
                "chat_id": chat_id,
                "text": "❌ Error updating caches"
            })

# Handle /start command
async def handle_start_command(chat_id, user_id, username):
    """Handle the /start command"""
    await log_bot_action("start command", user_id, username)
    
    # Register user
    await database.get_or_create_user(user_id, username)
    
    keyboard = await create_main_menu(user_id)
    
    await send_telegram_request_async("sendMessage", {
        "chat_id": chat_id,
        "text": (
            "Welcome to the Stablecoin Yield Bot by Yieldex!\n"
            "This bot tracks the most profitable stablecoin pools in the DeFi market sorted by total APY (native+reward) and shares updates daily.\n\n"
            "What would you like to do next?\n\n"
            "_(The data about the best pool is sent at 12:00 UTC daily)_"
        ),
        "reply_markup": keyboard,
        "parse_mode": "Markdown"
    })

# Handle /top command
async def handle_top_command(chat_id):
    """Handle the /top command"""
    top_apys = await database.get_top_three_apy()
    
    if top_apys:
        message = "✨ TOP STABLE OPPORTUNITIES ✨\n\n"
        
        for i, item in enumerate(top_apys):
            message += format_top_apy_data(item, i + 1)
            if i < len(top_apys) - 1:
                message += "\n\n"
        
        # Add back button
        back_keyboard = {
            "inline_keyboard": [
                [{"text": "Back to Menu", "callback_data": "back_to_main"}]
            ]
        }
        
        await send_telegram_request_async("sendMessage", {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "reply_markup": back_keyboard,
            "disable_web_page_preview": True
        })
    else:
        await send_telegram_request_async("sendMessage", {
            "chat_id": chat_id,
            "text": "Failed to retrieve data about the best APY.",
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                ]
            }
        })

# Handle /assets command
async def handle_assets_command(chat_id):
    """Handle the /assets command to show list of assets"""
    assets = await database.get_all_assets()
    
    if not assets:
        await send_telegram_request_async("sendMessage", {
            "chat_id": chat_id,
            "text": "Sorry, I couldn't retrieve the list of assets at the moment. Please try again later.",
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                ]
            }
        })
        return
    
    # Show assets with pagination
    keyboard = await create_paginated_assets_keyboard()
    
    await send_telegram_request_async("sendMessage", {
        "chat_id": chat_id,
        "text": "Select Asset:",
        "reply_markup": keyboard
    })

# Handle /chains command
async def handle_chains_command(chat_id):
    """Handle the /chains command to show list of chains"""
    chains = await database.get_all_chains()
    
    if not chains:
        await send_telegram_request_async("sendMessage", {
            "chat_id": chat_id,
            "text": "Sorry, I couldn't retrieve the list of chains at the moment. Please try again later.",
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                ]
            }
        })
        return
    
    # Show chains with pagination
    keyboard = await create_paginated_chains_keyboard()
    
    await send_telegram_request_async("sendMessage", {
        "chat_id": chat_id,
        "text": "Select Chain:",
        "reply_markup": keyboard
    })

# Handle callback queries
async def handle_callback_query(callback_query):
    """Handle callback queries from inline buttons"""
    # Объявление global в начале функции
    global _formatted_data_cache
    
    # Сначала отвечаем на callback, чтобы убрать индикатор загрузки сразу
    query_id = callback_query.get("id")
    await send_telegram_request_async("answerCallbackQuery", {
        "callback_query_id": query_id
    })
    
    # Затем извлекаем другие данные
    message_id = callback_query.get("message", {}).get("message_id")
    chat_id = callback_query.get("message", {}).get("chat", {}).get("id")
    data = callback_query.get("data")
    user_id = callback_query.get("from", {}).get("id")
    username = callback_query.get("from", {}).get("username", f"user_{user_id}")
    
    # Log action in background, не ждем результат
    asyncio.create_task(log_bot_action(data, user_id, username))
    
    # Обработка callback...
    
    # Handle 'show_top_3' button
    if data == "show_top_3":
        # Очистка кэша перед запросом свежих данных
        _formatted_data_cache.clear()
        
        top_apys = await database.get_top_three_apy()
        
        if top_apys:
            # Format date as DD/MM/YY
            today = datetime.datetime.now()
            formatted_date = today.strftime("%d/%m/%y")
            
            message = f"💰TOP STABLECOIN POOLS {formatted_date}\n\n"
            
            for i, item in enumerate(top_apys):
                message += format_top_apy_data(item, i + 1)
                if i < len(top_apys) - 1:
                    message += "\n\n"
            
            message += "\n\n_Only the pools with more than $1M TVL are shown_"
            
            # Add back button
            back_keyboard = {
                "inline_keyboard": [
                    [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                ]
            }
            
            await send_telegram_request_async("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": message,
                "reply_markup": back_keyboard,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            })
        else:
            await send_telegram_request_async("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": "Failed to retrieve data about the top APY opportunities.",
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                    ]
                }
            })
        
        await send_telegram_request_async("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'show_assets' button
    if data == "show_assets":
        keyboard = await create_paginated_assets_keyboard()
        
        await send_telegram_request_async("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": "Select Asset:",
            "reply_markup": keyboard
        })
        
        await send_telegram_request_async("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'show_top_1' button
    if data == "show_top_1":
        await handle_show_top_1(chat_id, message_id, query_id)
        return
    
    # Handle 'feedback' button
    if data == "feedback":
        await send_telegram_request_async("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": "To request a feature or leave feedback, feel free to send a DM to @konstantin_hardcore",
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                ]
            }
        })
        
        await send_telegram_request_async("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'back_to_main' button
    if data == "back_to_main":
        keyboard = await create_main_menu(user_id)
        
        # Проверяем, откуда пришел запрос (из текста сообщения)
        message_text = callback_query.get("message", {}).get("text", "")
        
        # Если запрос пришел из списка активов или цепей, редактируем сообщение
        if message_text in ["Select Asset:", "Select Chain:"]:
            # Редактируем текущее сообщение
            await send_telegram_request_async("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": (
                    "Welcome to the Stablecoin Yield Bot by Yieldex!\n"
                    "This bot tracks the most profitable stablecoin pools in the DeFi market sorted by total APY (native+reward) and shares updates daily.\n\n"
                    "What would you like to do next?\n\n"
                    "_(The data about the best pool is sent at 12:00 UTC daily)_"
                ),
                "reply_markup": keyboard,
                "parse_mode": "Markdown"
            })
        else:
            # Для остальных случаев создаем новое сообщение (как раньше)
            await send_telegram_request_async("sendMessage", {
                "chat_id": chat_id,
                "text": (
                    "Welcome to the Stablecoin Yield Bot by Yieldex!\n"
                    "This bot tracks the most profitable stablecoin pools in the DeFi market sorted by total APY (native+reward) and shares updates daily.\n\n"
                    "What would you like to do next?\n\n"
                    "_(The data about the best pool is sent at 12:00 UTC daily)_"
                ),
                "reply_markup": keyboard,
                "parse_mode": "Markdown"
            })
        
        await send_telegram_request_async("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle asset selection
    if data.startswith("asset_"):
        asset = data[6:]  # Extract asset name
        top_asset_apy = await database.get_top_apy_for_asset(asset)
        
        if top_asset_apy:
            message = f"*Top APY for {asset}*\n\n"
            
            for i, item in enumerate(top_asset_apy):
                message += format_top_apy_data(item, i + 1)
                if i < len(top_asset_apy) - 1:
                    message += "\n\n"
            
            # Add back buttons
            back_keyboard = {
                "inline_keyboard": [
                    [{"text": "Back to Assets", "callback_data": "show_assets"}],
                    [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                ]
            }
            
            await send_telegram_request_async("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": message,
                "reply_markup": back_keyboard,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            })
        else:
            await send_telegram_request_async("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": f"Failed to retrieve APY data for asset {asset}.",
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "Back to Assets", "callback_data": "show_assets"}],
                        [{"text": "Back to Main Menu", "callback_data": "back_to_main"}]
                    ]
                }
            })
        
        await send_telegram_request_async("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle pagination
    if data.startswith("page_"):
        page = int(data.split("_")[1])
        keyboard = await create_paginated_assets_keyboard(page)
        
        await send_telegram_request_async("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": "Select Asset:",
            "reply_markup": keyboard
        })
        
        await send_telegram_request_async("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'show_menu' button
    if data == "show_menu":
        keyboard = await create_main_menu(user_id)
        
        await send_telegram_request_async("sendMessage", {
            "chat_id": chat_id,
            "text": (
                "Welcome to the Stablecoin Yield Bot by Yieldex!\n"
                "This bot tracks the most profitable stablecoin pools in the DeFi market sorted by total APY (native+reward) and shares updates daily.\n\n"
                "What would you like to do next?\n\n"
                "_(The data about the best pool is sent at 12:00 UTC daily)_"
            ),
            "reply_markup": keyboard,
            "parse_mode": "Markdown"
        })
        
        await send_telegram_request_async("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'show_analytics' button
    if data == "show_analytics":
        is_admin = await database.is_user_admin(user_id)
        
        if not is_admin:
            await send_telegram_request_async("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": "Access denied. This feature is available only for administrators.",
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                    ]
                }
            })
            
            await send_telegram_request_async("answerCallbackQuery", {
                "callback_query_id": query_id
            })
            return
        
        analytics = await database.get_analytics()
        
        if not analytics:
            await send_telegram_request_async("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": "Failed to generate analytics report.",
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                    ]
                }
            })
            
            await send_telegram_request_async("answerCallbackQuery", {
                "callback_query_id": query_id
            })
            return
        
        # Function to safely escape markdown characters
        def escape_markdown(text):
            if not isinstance(text, str):
                text = str(text)
            return text.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)').replace('~', '\\~').replace('`', '\\`').replace('>', '\\>').replace('#', '\\#').replace('+', '\\+').replace('-', '\\-').replace('=', '\\=').replace('|', '\\|').replace('{', '\\{').replace('}', '\\}').replace('.', '\\.').replace('!', '\\!')
        
        # Format the report
        message = "📊 *Bot Analytics Report*\n\n"
        
        # User information
        message += "👥 *Users*\n"
        message += f"• Total: {analytics['new_users']['total']}\n"
        message += f"• New today: {analytics['new_users']['today']}\n"
        message += f"• New this week: {analytics['new_users']['week']}\n"
        message += f"• New this month: {analytics['new_users']['month']}\n\n"
        
        # All-time actions
        message += "🔄 *All Time Actions*\n"
        if analytics['actions']:
            for item in analytics['actions']:
                message += f"• {escape_markdown(item['action'])}: {item['count']}\n"
        else:
            message += "No actions recorded.\n"
        
        # Today's actions
        message += "\n📈 *Today's Actions*\n"
        if analytics['today_actions']:
            for item in analytics['today_actions']:
                message += f"• {escape_markdown(item['action'])}: {item['count']}\n"
        else:
            message += "No actions recorded today.\n"
        
        # Add back button
        back_keyboard = {
            "inline_keyboard": [
                [{"text": "Back to Menu", "callback_data": "back_to_main"}]
            ]
        }
        
        await send_telegram_request_async("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": message,
            "reply_markup": back_keyboard,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        })
        
        await send_telegram_request_async("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'show_chains' button
    if data == "show_chains":
        keyboard = await create_paginated_chains_keyboard()
        
        await send_telegram_request_async("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": "Select Chain:",
            "reply_markup": keyboard
        })
        
        await send_telegram_request_async("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle chain pagination
    if data.startswith("chains_page_"):
        page = int(data.split("_")[2])
        keyboard = await create_paginated_chains_keyboard(page)
        
        await send_telegram_request_async("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": "Select Chain:",
            "reply_markup": keyboard
        })
        
        await send_telegram_request_async("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle chain selection
    if data.startswith("chain_"):
        chain = data[6:]  # Extract chain name
        await log_bot_action(f"chain_{chain}", user_id, username)
        
        try:
            # Получаем данные для выбранной цепи
            top_apys = await database.get_top_apy_for_chain(chain)
            
            if top_apys:
                # Унифицируем обработку всех цепей
                message = f"✨ TOP OPPORTUNITIES ON {chain.upper()} ✨\n\n"
                
                for i, item in enumerate(top_apys):
                    try:
                        print(f"[DEBUG] Formatting pool {i+1} for {chain}: asset={item.get('asset')}, apy={item.get('apy')}")
                        # Используем единую функцию форматирования для всех пулов
                        pool_text = format_top_apy_data(item, i + 1)
                        message += pool_text
                        if i < len(top_apys) - 1:
                            message += "\n\n"
                    except Exception as e:
                        print(f"[DEBUG] Error formatting pool {i+1}: {e}")
                        # Если не получилось отформатировать, добавляем базовую информацию
                        message += f"Pool {i+1}: {item.get('asset')} with APY {item.get('apy', 'N/A')}%\n"
                
                # Add back button
                back_keyboard = {
                    "inline_keyboard": [
                        [{"text": "Back to Chains", "callback_data": "show_chains"}],
                        [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                    ]
                }
                
                try:
                    await send_telegram_request_async("editMessageText", {
                        "chat_id": chat_id,
                        "message_id": message_id,
                        "text": message,
                        "parse_mode": "Markdown",
                        "reply_markup": back_keyboard,
                        "disable_web_page_preview": True
                    })
                    print(f"[DEBUG] Successfully sent message for chain '{chain}'")
                except Exception as e:
                    print(f"[DEBUG] Error sending message: {e}")
                    # Пробуем отправить без Markdown
                    await send_telegram_request_async("editMessageText", {
                        "chat_id": chat_id,
                        "message_id": message_id,
                        "text": f"Error formatting message for {chain}. Technical details: {str(e)}",
                        "reply_markup": back_keyboard
                    })
            else:
                # Если нет данных для любой цепи
                await send_telegram_request_async("editMessageText", {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": f"No data available for chain '{chain}'. This chain may not have any pools or may be listed under a different name.",
                    "reply_markup": {
                        "inline_keyboard": [
                            [{"text": "Back to Chains", "callback_data": "show_chains"}],
                            [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                        ]
                    }
                })
        except Exception as e:
            print(f"[DEBUG] Unexpected error processing chain '{chain}': {e}")
            # Обработка ошибок...
        
        await send_telegram_request_async("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Answer callback query to remove loading state
    await send_telegram_request_async("answerCallbackQuery", {
        "callback_query_id": query_id
    })

# Обновленная функция отправки уведомлений
async def send_daily_notification():
    """Send daily notification to all subscribed users"""
    # Добавляем global в начало функции
    global _formatted_data_cache
    
    timestamp = datetime.datetime.now().isoformat()
    print(f"[{timestamp}] ⏰ Executing scheduled daily notification")
    
    try:
        print(f"[{timestamp}] 🔄 Forcing full cache refresh")
        success = await database.force_refresh_all_caches()
        
        if success:
            print(f"[{timestamp}] ✅ Cache forcefully refreshed with new data")
        else:
            print(f"[{timestamp}] ⚠️ Cache refresh failed, using existing data")
        
        # Гарантированно очищаем кэш отформатированных сообщений
        _formatted_data_cache.clear()
        
        # Получаем топовый пул с актуальными данными
        top_apy = await database.get_top_apy()
        
        if not top_apy:
            print(f"[{timestamp}] ❌ Failed to retrieve top APY data, aborting notification")
            return
        
        # Получаем всех подписчиков (без проверки TEST_NOTIFICATION)
        subscribers = await database.get_subscribed_users()
        print(f"[{timestamp}] 📋 Found {len(subscribers)} subscribers")
        
        # Клавиатура для уведомления
        menu_keyboard = {
            "inline_keyboard": [
                [{"text": "Open Main Menu", "callback_data": "show_menu"}]
            ]
        }
        
        # Форматируем дату
        today = datetime.datetime.now()
        formatted_date = today.strftime("%d/%m/%y")
        
        # Форматируем сообщение
        message = f"💰TOP STABLECOIN POOL {formatted_date}\n\n"
        message += format_top_apy_data(top_apy, 1)
        message += "\n\n_Only the pools with more than $1M TVL are shown_"
        
        # Счетчики для логирования
        sent_count = 0
        error_count = 0
        
        # Отправляем уведомления всем подписчикам
        for user in subscribers:
            user_id = user.get('telegram_id')
            print(f"[{timestamp}] 📤 Sending notification to user {user_id}")
            
            try:
                result = await send_telegram_request_async("sendMessage", {
                    "chat_id": user_id,
                    "text": message,
                    "parse_mode": "Markdown",
                    "reply_markup": menu_keyboard,
                    "disable_web_page_preview": True
                })
                
                if result and result.get("ok"):
                    print(f"[{timestamp}] ✅ Notification sent to user {user_id}")
                    sent_count += 1
                    await log_bot_action("notification_sent", user_id, user.get("username"))
                else:
                    print(f"[{timestamp}] ⚠️ Failed to send notification to user {user_id}: {result}")
                    
                    # Пробуем отправить с числовым ID
                    try:
                        numeric_id = int(user_id)
                        result = await send_telegram_request_async("sendMessage", {
                            "chat_id": numeric_id,
                            "text": message,
                            "parse_mode": "Markdown",
                            "reply_markup": menu_keyboard,
                            "disable_web_page_preview": True
                        })
                        
                        if result and result.get("ok"):
                            print(f"[{timestamp}] ✅ Notification sent to user {numeric_id} (numeric)")
                            sent_count += 1
                            await log_bot_action("notification_sent", user_id, user.get("username"))
                        else:
                            print(f"[{timestamp}] ❌ Failed with numeric ID for user {user_id}: {result}")
                            error_count += 1
                    except Exception as e:
                        print(f"[{timestamp}] ❌ Error sending with numeric ID to {user_id}: {e}")
                        error_count += 1
            except Exception as e:
                print(f"[{timestamp}] ❌ Error sending notification to {user_id}: {e}")
                error_count += 1
        
        print(f"[{timestamp}] 📊 Notification summary: {sent_count} sent, {error_count} errors")
    except Exception as e:
        print(f"[{timestamp}] 🚨 CRITICAL ERROR in send_daily_notification: {e}")

    # Очищаем форматированный кэш
    _formatted_data_cache = {}

# Setup polling
async def poll_updates():
    """Poll for updates from Telegram"""
    offset = None
    
    while True:
        try:
            params = {"timeout": 30}
            if offset:
                params["offset"] = offset
            
            # Используем асинхронную версию вместо синхронной
            response = await send_telegram_request_async("getUpdates", params)
            
            if response and response.get("ok") and response.get("result"):
                updates = response["result"]
                
                for update in updates:
                    offset = update["update_id"] + 1
                    
                    if "message" in update and "text" in update["message"]:
                        await process_message(update["message"])
                    
                    if "callback_query" in update:
                        await handle_callback_query(update["callback_query"])
            
            # If no updates, wait a bit to avoid hammering the API
            if not response or not response.get("result"):
                await asyncio.sleep(1)
                
        except Exception as e:
            print(f"Error in polling: {e}")
            await asyncio.sleep(5)  # Wait a bit longer if there's an error

# Установка задачи на обновление кэша каждые 15 минут
async def setup_cache_updater():
    """Настройка планировщика обновления кэша с обработкой ошибок"""
    print("[SETUP] Setting up cache updater job...")
    scheduler.add_job(
        database.update_all_caches,
        'interval',
        minutes=15,
        id='cache_updater',
        replace_existing=True
    )
    print("[SETUP] Cache updater job scheduled to run every 15 minutes")
    
    # Выполняем первоначальное обновление кэша с обработкой ошибок
    print("[SETUP] Starting initial cache update...")
    try:
        await database.update_all_caches()
        print("[SETUP] Initial cache update completed successfully")
    except Exception as e:
        print(f"[SETUP] Error during initial cache update: {e}")
        print("[SETUP] Bot will continue with empty cache and retry later")
    
    print("[SETUP] Cache setup completed")

async def preload_common_data():
    """Preload commonly accessed data in background"""
    try:
        # Запускаем параллельную загрузку всех часто запрашиваемых данных
        await asyncio.gather(
            database.get_top_apy(),
            database.get_top_three_apy(),
            database.get_all_assets(),
            database.get_all_chains()
        )
        print("[PRELOAD] Common data preloaded successfully")
    except Exception as e:
        print(f"[PRELOAD] Error preloading data: {e}")

# Добавьте эту функцию для проверки состояния уведомлений
async def check_notification_schedule():
    """Check and output info about scheduled notifications"""
    notification_job = scheduler.get_job('daily_notification')
    
    if notification_job:
        next_run = notification_job.next_run_time
        now = datetime.datetime.now(pytz.UTC)
        time_until_next = next_run - now
        
        print(f"🕒 Next notification scheduled for: {next_run}")
        print(f"⏱️ Time until next notification: {time_until_next}")
        print(f"📋 Job details: Trigger type: {type(notification_job.trigger).__name__}")
        
        # Проверка параметров триггера
        if isinstance(notification_job.trigger, CronTrigger):
            fields = notification_job.trigger.fields
            for field in fields:
                if field.name in ('hour', 'minute'):
                    print(f"  - {field.name}: {field}")
        
        return True
    else:
        print("❌ No notification job found!")
        return False

# Обработчик для кнопки "show_top_1"
async def handle_show_top_1(chat_id, message_id, query_id):
    """Handle 'show_top_1' callback - show top-1 APY"""
    # Объявление global в начале функции
    global _formatted_data_cache
    
    # Очистка кэша перед запросом свежих данных
    _formatted_data_cache.clear()
    
    # Используем log_bot_action вместо напрямую database.log_user_action
    await log_bot_action("show_top_1")
    
    data = await database.get_top_apy()
    
    if not data:
        await send_telegram_request_async("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": "No data available at the moment.",
            "reply_markup": {
                "inline_keyboard": [[{"text": "Back to Menu", "callback_data": "back_to_main"}]]
            }
        })
        await send_telegram_request_async("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Format date as DD/MM/YY
    today = datetime.datetime.now()
    formatted_date = today.strftime("%d/%m/%y")
    
    message = f"💰TOP STABLECOIN POOL {formatted_date}\n\n"
    message += format_top_apy_data(data, 1)
    message += "\n\n_Only the pools with more than $1M TVL are shown_"
    
    # Add back button
    back_keyboard = {
        "inline_keyboard": [
            [{"text": "Back to Menu", "callback_data": "back_to_main"}]
        ]
    }
    
    await send_telegram_request_async("editMessageText", {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": message,
        "reply_markup": back_keyboard,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    })
    
    await send_telegram_request_async("answerCallbackQuery", {
        "callback_query_id": query_id
    })

# Main function
async def main():
    """Main function to start the bot"""
    print("Starting bot...")
    
    # Инициализация HTTP-сессии для асинхронных запросов
    await setup_aiohttp_session()
    
    # Получаем и форматируем время для уведомлений
    notification_hour = config.NOTIFICATION_HOUR
    notification_minute = config.NOTIFICATION_MINUTE
    
    # Форматируем время с ведущими нулями
    formatted_time = f"{notification_hour:02d}:{notification_minute:02d}"
    print(f"Setting up daily notification for {formatted_time} UTC")
    
    # Устанавливаем задачу в планировщик с явным указанием часового пояса UTC
    job = scheduler.add_job(
        send_daily_notification,
        CronTrigger(hour=notification_hour, minute=notification_minute, timezone=pytz.UTC),
        id='daily_notification',
        replace_existing=True
    )
    
    # Установка обновления кэша и ожидание первичного обновления
    await setup_cache_updater()  # Ждем завершения
    
    # Запуск планировщика задач после инициализации кэша
    scheduler.start()
    
    # Проверяем, правильно ли настроено время следующего запуска
    next_run = job.next_run_time
    utc_now = datetime.datetime.now(pytz.UTC)
    
    print(f"Current UTC time: {utc_now}")
    print(f"Scheduler started - next notification at {next_run}")
    print(f"Time until next notification: {next_run - utc_now}")
    
    # Start polling
    try:
        await poll_updates()
    finally:
        if session:
            await session.close()

# Start the bot
if __name__ == "__main__":
    asyncio.run(main())

async def send_top_apy(chat_id, message_id=None):
    """Send top APY with debugging info"""
    try:
        # Получаем данные напрямую из кэша
        data = await database.get_top_apy()
        
        if not data:
            await send_telegram_request_async("sendMessage", {
                "chat_id": chat_id,
                "text": "No APY data available at the moment."
            })
            return
            
        # Форматирование и отправка данных
        message = "💰 TOP STABLECOIN POOL (LIVE DATA)\n\n"
        message += format_top_apy_data(data, 1)
        message += "\n\n_Only pools with more than $1M TVL are shown_"
        
        # Клавиатура для сообщения
        keyboard = {
            "inline_keyboard": [[{"text": "Back to Menu", "callback_data": "back_to_main"}]]
        }
        
        # Отправка или обновление сообщения
        if message_id:
            await send_telegram_request_async("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": message,
                "parse_mode": "Markdown",
                "reply_markup": keyboard,
                "disable_web_page_preview": True
            })
        else:
            await send_telegram_request_async("sendMessage", {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "reply_markup": keyboard,
                "disable_web_page_preview": True
            })
            
    except Exception as e:
        # Обработка ошибок
        error_message = f"Error fetching top APY data: {e}"
        print(f"[ERROR] {error_message}")
        
        await send_telegram_request_async("sendMessage", {
            "chat_id": chat_id,
            "text": "Sorry, couldn't fetch APY data at the moment. Please try again later."
        }) 