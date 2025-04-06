import asyncio
import json
import datetime
import time
import requests
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import config
from database import (
    get_top_apy, get_top_apy_for_asset, get_all_assets, get_or_create_user,
    get_subscribed_users, update_subscription_status, get_top_three_apy,
    get_top_ten_apy, get_user_subscription_status, get_latest_full_apy,
    log_user_action, is_user_admin, get_analytics, get_all_chains, get_top_apy_for_chain
)

# Create scheduler for daily notifications
scheduler = AsyncIOScheduler()

# Function to log bot actions
async def log_bot_action(action, user_id=None, username=None):
    """Log bot actions and save to database if user_id is provided"""
    timestamp = datetime.datetime.now().isoformat()
    user_info = f"[USER:{user_id}|{username}]" if user_id else ""
    print(f"[{timestamp}] {user_info} Bot Action: {action}")
    
    # Логируем действие в базу данных 
    # (будет пропущено для админов и игнорируемых действий - см. log_user_action)
    if user_id:
        await log_user_action(action, user_id, username)

# Function to send Telegram API requests
def send_telegram_request(method, params=None):
    """Send request to Telegram Bot API"""
    url = f"{config.TELEGRAM_API_URL}/{method}"
    response = requests.post(url, json=params if params else {})
    return response.json()

# Format APY data with detailed information
def format_top_apy_data(data, position):
    """Format APY data for display with ranking"""
    if not data:
        return "No data available"
    
    # Extract protocol name from pool_id - теперь берем третий элемент и дальше
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
    
    # Format APY data with checks for None values
    apy_total = f"{data.get('apy'):.2f}%" if data.get('apy') is not None else 'N/A'
    apy_base = f"{data.get('apy_base'):.2f}%" if data.get('apy_base') is not None else 'N/A'
    apy_reward = f"{data.get('apy_reward'):.2f}%" if data.get('apy_reward') is not None else 'N/A'
    apy_mean_30d = f"{data.get('apy_mean_30d'):.2f}%" if data.get('apy_mean_30d') is not None else 'N/A'
    
    # Add link to pool site, if available
    site_link = f"   ├ [Pool Site]({data.get('site_url')})\n" if data.get('site_url') else ''
    
    return (
        f"{position_emoji} *{data.get('asset')}* on *{data.get('chain')}*\n"
        f"   ┌ Protocol: *{protocol}*\n"
        f"{site_link}"
        f"   ├ APY Total: *{apy_total}*\n"
        f"   ├ APY Base: {apy_base}\n"
        f"   ├ APY Reward: {apy_reward}\n"
        f"   ├ Avg APY 30d: {apy_mean_30d}\n"
        f"   └ TVL: {tvl_formatted}"
    )

# Function to create paginated assets keyboard
async def create_paginated_assets_keyboard(page=0, items_per_page=12):
    """Create a keyboard with pagination for assets"""
    assets = await get_all_assets()
    
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
    chains = await get_all_chains()
    
    # Calculate chains for the current page
    start_index = page * items_per_page
    end_index = min(start_index + items_per_page, len(chains))
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
    # Check if the user is an admin
    is_admin = await is_user_admin(user_id)
    
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
        await log_bot_action("chains_command", user_id, username)
        await handle_chains_command(chat_id)

# Handle /start command
async def handle_start_command(chat_id, user_id, username):
    """Handle the /start command"""
    await log_bot_action("start command", user_id, username)
    
    # Register user
    await get_or_create_user(user_id, username)
    
    keyboard = await create_main_menu(user_id)
    
    send_telegram_request("sendMessage", {
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
    top_apys = await get_top_three_apy()
    
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
        
        send_telegram_request("sendMessage", {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "reply_markup": back_keyboard,
            "disable_web_page_preview": True
        })
    else:
        send_telegram_request("sendMessage", {
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
    assets = await get_all_assets()
    
    if not assets:
        send_telegram_request("sendMessage", {
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
    
    send_telegram_request("sendMessage", {
        "chat_id": chat_id,
        "text": "Select Asset:",
        "reply_markup": keyboard
    })

# Handle /chains command
async def handle_chains_command(chat_id):
    """Handle the /chains command to show list of chains"""
    chains = await get_all_chains()
    
    if not chains:
        send_telegram_request("sendMessage", {
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
    
    send_telegram_request("sendMessage", {
        "chat_id": chat_id,
        "text": "Select Chain:",
        "reply_markup": keyboard
    })

# Handle callback queries
async def handle_callback_query(callback_query):
    """Handle callback queries from inline buttons"""
    query_id = callback_query.get("id")
    message_id = callback_query.get("message", {}).get("message_id")
    chat_id = callback_query.get("message", {}).get("chat", {}).get("id")
    data = callback_query.get("data")
    user_id = callback_query.get("from", {}).get("id")
    username = callback_query.get("from", {}).get("username", f"user_{user_id}")
    
    await log_bot_action(data, user_id, username)
    
    # Handle 'show_assets' button
    if data == "show_assets":
        keyboard = await create_paginated_assets_keyboard()
        
        send_telegram_request("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": "Select Asset:",
            "reply_markup": keyboard
        })
        
        send_telegram_request("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'show_top_1' button
    if data == "show_top_1":
        top_apy = await get_top_apy()
        
        if top_apy:
            # Format date as DD/MM/YY
            today = datetime.datetime.now()
            formatted_date = today.strftime("%d/%m/%y")
            
            message = f"💰TOP STABLECOIN POOL {formatted_date}\n\n"
            message += format_top_apy_data(top_apy, 1)
            message += "\n\n_Only the pools with more than $1M TVL are shown_"
            
            # Add back button
            back_keyboard = {
                "inline_keyboard": [
                    [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                ]
            }
            
            send_telegram_request("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": message,
                "reply_markup": back_keyboard,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            })
        else:
            send_telegram_request("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": "Failed to retrieve data about the best APY.",
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                    ]
                }
            })
        
        send_telegram_request("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'show_top_3' button
    if data == "show_top_3":
        top_apys = await get_top_three_apy()
        
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
            
            send_telegram_request("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": message,
                "reply_markup": back_keyboard,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            })
        else:
            send_telegram_request("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": "Failed to retrieve data about the top APY opportunities.",
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                    ]
                }
            })
        
        send_telegram_request("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'feedback' button
    if data == "feedback":
        send_telegram_request("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": "To request a feature or leave feedback, feel free to send a DM to @konstantin_hardcore",
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                ]
            }
        })
        
        send_telegram_request("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'back_to_main' button
    if data == "back_to_main":
        # Получаем клавиатуру главного меню
        keyboard = await create_main_menu(user_id)
        
        # Создаем новое сообщение вместо редактирования существующего
        send_telegram_request("sendMessage", {
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
        
        # Скрываем индикатор загрузки кнопки
        send_telegram_request("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle asset selection
    if data.startswith("asset_"):
        asset_name = data.replace("asset_", "")
        top_asset_apy = await get_top_apy_for_asset(asset_name)
        
        if top_asset_apy:
            message = f"*Top APY for {asset_name}*\n\n"
            
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
            
            send_telegram_request("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": message,
                "reply_markup": back_keyboard,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            })
        else:
            send_telegram_request("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": f"Failed to retrieve APY data for asset {asset_name}.",
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "Back to Assets", "callback_data": "show_assets"}],
                        [{"text": "Back to Main Menu", "callback_data": "back_to_main"}]
                    ]
                }
            })
        
        send_telegram_request("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle pagination
    if data.startswith("page_"):
        page = int(data.split("_")[1])
        keyboard = await create_paginated_assets_keyboard(page)
        
        send_telegram_request("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": "Select Asset:",
            "reply_markup": keyboard
        })
        
        send_telegram_request("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'show_menu' button
    if data == "show_menu":
        keyboard = await create_main_menu(user_id)
        
        send_telegram_request("sendMessage", {
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
        
        send_telegram_request("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'show_analytics' button
    if data == "show_analytics":
        is_admin = await is_user_admin(user_id)
        
        if not is_admin:
            send_telegram_request("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": "Access denied. This feature is available only for administrators.",
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                    ]
                }
            })
            
            send_telegram_request("answerCallbackQuery", {
                "callback_query_id": query_id
            })
            return
        
        analytics = await get_analytics()
        
        if not analytics:
            send_telegram_request("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": "Failed to generate analytics report.",
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                    ]
                }
            })
            
            send_telegram_request("answerCallbackQuery", {
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
        
        send_telegram_request("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": message,
            "reply_markup": back_keyboard,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        })
        
        send_telegram_request("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle 'show_chains' button
    if data == "show_chains":
        keyboard = await create_paginated_chains_keyboard()
        
        send_telegram_request("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": "Select Chain:",
            "reply_markup": keyboard
        })
        
        send_telegram_request("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle chain pagination
    if data.startswith("chains_page_"):
        page = int(data.split("_")[2])
        keyboard = await create_paginated_chains_keyboard(page)
        
        send_telegram_request("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": "Select Chain:",
            "reply_markup": keyboard
        })
        
        send_telegram_request("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Handle chain selection
    if data.startswith("chain_"):
        chain = data[6:]  # Remove "chain_" prefix
        top_apys = await get_top_apy_for_chain(chain)
        
        if top_apys:
            message = f"✨ TOP OPPORTUNITIES ON {chain.upper()} ✨\n\n"
            
            for i, item in enumerate(top_apys):
                message += format_top_apy_data(item, i + 1)
                if i < len(top_apys) - 1:
                    message += "\n\n"
            
            # Add back button
            back_keyboard = {
                "inline_keyboard": [
                    [{"text": "Back to Chains", "callback_data": "show_chains"}],
                    [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                ]
            }
            
            send_telegram_request("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": message,
                "parse_mode": "Markdown",
                "reply_markup": back_keyboard,
                "disable_web_page_preview": True
            })
        else:
            send_telegram_request("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": f"No data available for chain {chain}.",
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "Back to Chains", "callback_data": "show_chains"}],
                        [{"text": "Back to Menu", "callback_data": "back_to_main"}]
                    ]
                }
            })
        
        send_telegram_request("answerCallbackQuery", {
            "callback_query_id": query_id
        })
        return
    
    # Answer callback query to remove loading state
    send_telegram_request("answerCallbackQuery", {
        "callback_query_id": query_id
    })

# Send daily notification to all subscribed users
async def send_daily_notification():
    """Send daily notification to all subscribed users"""
    timestamp = datetime.datetime.now().isoformat()
    print(f"[{timestamp}] Action: sending daily notifications")
    
    top_apy = await get_top_apy()
    
    if not top_apy:
        print("Failed to retrieve data for daily notification, skipping")
        return
    
    subscribers = await get_subscribed_users()
    
    # Add menu button to notification
    menu_keyboard = {
        "inline_keyboard": [
            [{"text": "Open Main Menu", "callback_data": "show_menu"}]
        ]
    }
    
    # Format date as DD/MM/YY
    today = datetime.datetime.now()
    formatted_date = today.strftime("%d/%m/%y")
    
    for user in subscribers:
        try:
            send_telegram_request("sendMessage", {
                "chat_id": user.get('telegram_id'),
                "text": f"💰TOP STABLECOIN POOL {formatted_date}\n\n{format_top_apy_data(top_apy, 1)}\n\n_Only the pools with more than $1M TVL are shown_",
                "parse_mode": "Markdown",
                "reply_markup": menu_keyboard,
                "disable_web_page_preview": True
            })
            await log_bot_action("notification_sent", user.get('telegram_id'), user.get('username'))
        except Exception as e:
            print(f"Error sending notification to user {user.get('telegram_id')}:", e)

# Setup polling
async def poll_updates():
    """Poll for updates from Telegram"""
    offset = None
    
    while True:
        try:
            params = {"timeout": 30}
            if offset:
                params["offset"] = offset
            
            response = send_telegram_request("getUpdates", params)
            
            if response.get("ok") and response.get("result"):
                updates = response["result"]
                
                for update in updates:
                    offset = update["update_id"] + 1
                    
                    if "message" in update and "text" in update["message"]:
                        await process_message(update["message"])
                    
                    if "callback_query" in update:
                        await handle_callback_query(update["callback_query"])
            
            # If no updates, wait a bit to avoid hammering the API
            if not response.get("result"):
                await asyncio.sleep(1)
                
        except Exception as e:
            print(f"Error in polling: {e}")
            await asyncio.sleep(5)  # Wait a bit longer if there's an error

# Main function
async def main():
    """Main function to start the bot"""
    print("Starting bot...")
    
    # Schedule daily notification
    scheduler.add_job(
        send_daily_notification,
        CronTrigger(hour=config.NOTIFICATION_HOUR, minute=config.NOTIFICATION_MINUTE),
        id='daily_notification',
        replace_existing=True
    )
    
    scheduler.start()
    print(f"Scheduled daily notification for {config.NOTIFICATION_HOUR}:{config.NOTIFICATION_MINUTE} UTC")
    
    # Start polling
    await poll_updates()

# Start the bot
if __name__ == "__main__":
    asyncio.run(main()) 