import datetime
import json
import time
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_KEY

# Инициализация Supabase клиента
try:
    # Пробуем создать клиента обычным способом
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except TypeError as e:
    if 'proxy' in str(e):
        # Используем альтернативный способ, если проблема с аргументом proxy
        from supabase._sync.client import SyncClient
        options = {}  # Базовые опции без proxy
        supabase = SyncClient(SUPABASE_URL, SUPABASE_KEY, options)
    else:
        raise e

# Кеширование для часто запрашиваемых данных
assets_cache = None
assets_cache_time = 0
top_apy_cache = None
top_apy_cache_time = 0
full_apy_cache = None  # Добавляем переменную для кеширования полных данных APY
full_apy_cache_time = 0  # Добавляем переменную для времени кеширования
CACHE_TTL = 5 * 60  # 5 минут в секундах
chains_cache = None
chains_cache_time = 0

def log_query(operation, user_id=None, username=None):
    """Log database queries"""
    timestamp = datetime.datetime.now().isoformat()
    user_info = f"[USER:{user_id}|{username}]" if user_id else ""
    print(f"[{timestamp}] {user_info} DB Action: {operation}")

# Проверка наличия ошибки в ответе Supabase
def has_error(response):
    """Check if Supabase response contains an error"""
    if hasattr(response, 'error'):
        return response.error
    # Для версии API, где нет свойства error
    if hasattr(response, 'data') and isinstance(response.data, dict) and 'error' in response.data:
        return response.data['error']
    return None

async def get_latest_full_apy():
    """Get the latest APY data using a more optimized approach"""
    log_query('get_latest_full_apy')
    
    # Используем кеширование для уменьшения нагрузки на БД
    global full_apy_cache, full_apy_cache_time
    now = time.time()
    
    if full_apy_cache and (now - full_apy_cache_time < CACHE_TTL):
        return full_apy_cache
    
    try:
        # Вызываем созданную RPC функцию с пустым словарем параметров
        response = supabase.rpc('get_latest_apy_data', {}).execute()
        
        if has_error(response):
            print(f"Error calling get_latest_apy_data: {response.error}")
            return []
            
        result = response.data if response.data else []
        
        # Постобработка: извлекаем project из pool_id
        for item in result:
            # Извлечение project из pool_id (первый элемент после разделения по подчеркиванию)
            if 'pool_id' in item and item['pool_id']:
                parts = item['pool_id'].split('_')
                item['project'] = parts[0] if parts else ''
        
        # Кешируем результат
        full_apy_cache = result
        full_apy_cache_time = now
        
        return result
    except Exception as e:
        print(f"Error in get_latest_full_apy: {e}")
        return []

async def get_top_apy():
    """Get top-1 APY from all assets (filtered by TVL >= 1,000,000)"""
    log_query('get_top_apy')
    
    global top_apy_cache, top_apy_cache_time
    now = time.time()
    
    if top_apy_cache and (now - top_apy_cache_time < CACHE_TTL):
        return top_apy_cache
    
    data = await get_latest_full_apy()
    
    if not data:
        return None  # Возвращаем None вместо мок-данных
    
    # Filter by TVL >= 1,000,000 and sort by APY
    filtered_data = sorted(
        [item for item in data if item.get('tvl', 0) >= 1000000],
        key=lambda x: x.get('apy', 0),
        reverse=True
    )
    
    result = filtered_data[0] if filtered_data else None
    
    # Save to cache
    top_apy_cache = result
    top_apy_cache_time = now
    
    return result

async def get_top_apy_for_asset(asset):
    """Get top-3 APY for specified asset (across different chains)"""
    log_query('get_top_apy_for_asset')
    
    data = await get_latest_full_apy()
    
    if not data:
        return []
    
    # Filter by the specified asset
    asset_data = [item for item in data if item.get('asset') == asset]
    
    # Group records by chains and take the highest APY for each chain
    chain_map = {}
    for item in asset_data:
        chain_key = item.get('chain')
        
        if chain_key not in chain_map or item.get('apy', 0) > chain_map[chain_key].get('apy', 0):
            chain_map[chain_key] = item
    
    # Convert map back to a list and sort by APY (highest to lowest)
    unique_data = sorted(
        list(chain_map.values()),
        key=lambda x: x.get('apy', 0),
        reverse=True
    )[:3]  # Limit to three results
    
    return unique_data

async def get_all_assets():
    """Get all unique asset values with caching"""
    log_query('get_all_assets')
    
    global assets_cache, assets_cache_time
    now = time.time()
    
    if assets_cache and (now - assets_cache_time < CACHE_TTL):
        return assets_cache
    
    try:
        data = await get_latest_full_apy()
        
        if not data:
            return []
        
        # Get unique assets
        all_assets = [item.get('asset') for item in data if item.get('asset')]
        unique_assets = sorted(list(set(all_assets)))
        
        # Save to cache
        assets_cache = unique_assets
        assets_cache_time = now
        
        return unique_assets
    except Exception as e:
        print('Error getting assets:', e)
        return []

async def get_or_create_user(user_id, username):
    """Get or create a user"""
    log_query('get_or_create_user', user_id, username)
    
    try:
        # Check if the user exists
        response = supabase.table('bot_users').select('*').eq('telegram_id', str(user_id)).execute()
        
        # Проверяем ошибки с использованием новой функции
        error = has_error(response)
        if error:
            print('Error checking user:', error)
            return None
        
        if response.data:
            return response.data[0]
        
        # User not found, create a new one
        response = supabase.table('bot_users').insert({
            'telegram_id': str(user_id),
            'username': username,
            'subscribed': True
        }).execute()
        
        error = has_error(response)
        if error:
            print('Error creating user:', error)
            return None
        
        return response.data[0] if response.data else None
    except Exception as e:
        print('Error in get_or_create_user:', e)
        return None

async def get_subscribed_users():
    """Get all subscribed users"""
    log_query('get_subscribed_users')
    
    try:
        response = supabase.table('bot_users').select('*').eq('subscribed', True).execute()
        
        error = has_error(response)
        if error:
            print('Error getting subscribed users:', error)
            return []
        
        return response.data
    except Exception as e:
        print('Error in get_subscribed_users:', e)
        return []

async def update_subscription_status(user_id, status):
    """Update user subscription status"""
    log_query('update_subscription_status', user_id)
    
    try:
        response = supabase.table('bot_users').update({
            'subscribed': status
        }).eq('telegram_id', str(user_id)).execute()
        
        error = has_error(response)
        if error:
            print('Error updating subscription status:', error)
            return False
        
        return True
    except Exception as e:
        print('Error in update_subscription_status:', e)
        return False

async def get_top_three_apy():
    """Get top-3 APY from all assets (filtered by TVL >= 1,000,000)"""
    log_query('get_top_three_apy')
    
    data = await get_latest_full_apy()
    
    if not data:
        return []
    
    # Filter by TVL >= 1,000,000 and sort by APY
    filtered_data = sorted(
        [item for item in data if item.get('tvl', 0) >= 1000000],
        key=lambda x: x.get('apy', 0),
        reverse=True
    )[:3]  # Limit to three results
    
    return filtered_data

async def get_top_ten_apy():
    """Get top-10 APY from all assets (filtered by TVL >= 1,000,000)"""
    log_query('get_top_ten_apy')
    
    data = await get_latest_full_apy()
    
    if not data:
        return []
    
    # Filter by TVL >= 1,000,000 and sort by APY
    filtered_data = sorted(
        [item for item in data if item.get('tvl', 0) >= 1000000],
        key=lambda x: x.get('apy', 0),
        reverse=True
    )[:10]  # Limit to ten results
    
    return filtered_data

async def get_user_subscription_status(user_id):
    """Get user subscription status"""
    log_query('get_user_subscription_status', user_id)
    
    try:
        response = supabase.table('bot_users').select('*').eq('telegram_id', str(user_id)).execute()
        
        error = has_error(response)
        if error:
            print('Error getting subscription status:', error)
            return None
        
        return response.data[0] if response.data else None
    except Exception as e:
        print('Error in get_user_subscription_status:', e)
        return None

async def log_user_action(action, user_id, username):
    """Log user actions to the database"""
    try:
        # Проверяем, является ли пользователь администратором
        is_admin = await is_user_admin(user_id)
        if is_admin:
            # Не логируем действия администраторов
            return
            
        # Список действий, которые не нужно логировать ни для кого
        ignored_actions = ["back_to_main", "show_analytics", "notification_sent"]
        if action in ignored_actions:
            # Не логируем игнорируемые действия
            return
            
        # Логируем действие для обычных пользователей
        response = supabase.table('user_actions').insert({
            'user_id': str(user_id),
            'username': username,
            'action': action
        }).execute()
        
        error = has_error(response)
        if error:
            print('Error logging action to database:', error)
    except Exception as e:
        print('Error in log_user_action:', e)

async def is_user_admin(user_id):
    """Check if the user is an administrator"""
    try:
        response = supabase.table('bot_users').select('is_admin').eq('telegram_id', str(user_id)).execute()
        
        error = has_error(response)
        if error:
            print('Error checking admin status:', error)
            return False
        
        # Проверяем, что данные не пусты и user помечен как admin
        return response.data and response.data[0].get('is_admin', False)
    except Exception as e:
        print('Error in is_user_admin:', e)
        return False

async def get_analytics():
    """Get analytics data for admin dashboard"""
    try:
        # Get time boundaries
        now = datetime.datetime.now()
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        one_week_ago = now - datetime.timedelta(days=7)
        one_month_ago = now - datetime.timedelta(days=30)
        
        # New users today
        today_users_response = supabase.table('bot_users').select('telegram_id').gte(
            'created_at', today.isoformat()
        ).execute()
        
        # New users this week
        week_users_response = supabase.table('bot_users').select('telegram_id').gte(
            'created_at', one_week_ago.isoformat()
        ).execute()
        
        # New users this month
        month_users_response = supabase.table('bot_users').select('telegram_id').gte(
            'created_at', one_month_ago.isoformat()
        ).execute()
        
        # Total users
        total_users_response = supabase.table('bot_users').select('telegram_id').execute()
        
        # Get admin user IDs to exclude from analytics
        admin_users_response = supabase.table('bot_users').select('telegram_id').eq('is_admin', True).execute()
        admin_user_ids = [admin.get('telegram_id') for admin in admin_users_response.data] if admin_users_response.data else []
        
        # Action statistics - using RPC
        actions_response = supabase.rpc('count_actions_by_type', {}).execute()
        
        # Today's action statistics - using RPC
        today_actions_response = supabase.rpc('count_todays_actions_by_type', {
            'today_date': today.isoformat()
        }).execute()
        
        # Filter out admin actions from statistics
        actions = actions_response.data if actions_response.data else []
        today_actions = today_actions_response.data if today_actions_response.data else []
        
        # Count users excluding any errors
        today_users_count = len(today_users_response.data) if not has_error(today_users_response) else 0
        week_users_count = len(week_users_response.data) if not has_error(week_users_response) else 0
        month_users_count = len(month_users_response.data) if not has_error(month_users_response) else 0
        total_users_count = len(total_users_response.data) if not has_error(total_users_response) else 0
        
        return {
            'new_users': {
                'today': today_users_count,
                'week': week_users_count,
                'month': month_users_count,
                'total': total_users_count
            },
            'actions': actions,
            'today_actions': today_actions
        }
    except Exception as e:
        print('Error generating analytics:', e)
        return None

async def get_all_chains():
    """Get all unique chain values with caching"""
    log_query('get_all_chains')
    
    # Можно добавить кеширование аналогично assets
    global chains_cache, chains_cache_time
    now = time.time()
    
    if chains_cache and (now - chains_cache_time < CACHE_TTL):
        return chains_cache
    
    try:
        data = await get_latest_full_apy()
        
        if not data:
            return []
        
        # Get unique chains
        all_chains = [item.get('chain') for item in data if item.get('chain')]
        unique_chains = sorted(list(set(all_chains)))
        
        # Save to cache
        chains_cache = unique_chains
        chains_cache_time = now
        
        return unique_chains
    except Exception as e:
        print('Error getting chains:', e)
        return []

async def get_top_apy_for_chain(chain):
    """Get top-3 APY for specified chain (across different assets)"""
    log_query('get_top_apy_for_chain')
    
    data = await get_latest_full_apy()
    
    if not data:
        return []
    
    # Filter by the specified chain
    chain_data = [item for item in data if item.get('chain') == chain]
    
    # Group records by assets and take the highest APY for each asset
    asset_map = {}
    for item in chain_data:
        asset_key = item.get('asset')
        
        if asset_key not in asset_map or item.get('apy', 0) > asset_map[asset_key].get('apy', 0):
            asset_map[asset_key] = item
    
    # Convert map back to a list and sort by APY (highest to lowest)
    unique_data = sorted(
        list(asset_map.values()),
        key=lambda x: x.get('apy', 0),
        reverse=True
    )[:3]  # Limit to three results
    
    return unique_data 