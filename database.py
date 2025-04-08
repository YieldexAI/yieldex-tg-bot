import datetime
import json
import time
import asyncio
import threading
import random  # Добавьте импорт в начало файла
from typing import Dict, Any, List, Optional, Tuple
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_KEY
import sys

# В начале database.py добавим отладочный код для отслеживания экземпляров
print(f"Loading database.py with id: {id(sys.modules['__main__'] if '__main__' in sys.modules else 0)}")

# Глобальные кэши
_formatted_data_cache = {}  # Кэш для хранения форматированных сообщений

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

# Глобальная переменная для блокировки обновления кэша
_cache_update_lock = threading.Lock()
_cache_updating = False

# Глобальная переменная для отслеживания последнего обновления
_last_cache_update_time = 0

# Блокировки для кэша
_format_cache_lock = asyncio.Lock()

# Структура для кэша - использует двойную буферизацию для избежания блокировок
class CacheData:
    def __init__(self, name="Unnamed"):
        self.name = name  # Имя кэша для логирования
        self.data = None  # Актуальные данные для использования
        self.timestamp = 0  # Время последнего обновления
        self._new_data = None  # Новые данные во время обновления
        self._update_lock = asyncio.Lock()  # Блокировка для обновления
    
    async def update(self, data_fetcher):
        """Обновляет кэш, используя переданную функцию для получения данных"""
        if self._update_lock.locked():
            print(f"[CACHE] {self.name} cache update already in progress, skipping")
            return False  # Уже идет обновление
        
        async with self._update_lock:
            try:
                start_time = time.time()
                print(f"[CACHE] Starting update for {self.name} cache")
                
                # Получаем новые данные
                self._new_data = await data_fetcher()
                
                # Если данные получены успешно, обновляем основной кэш
                if self._new_data is not None:
                    self.data = self._new_data
                    self.timestamp = time.time()
                    data_size = len(self._new_data) if hasattr(self._new_data, '__len__') else "N/A"
                    self._new_data = None
                    
                    elapsed = time.time() - start_time
                    print(f"[CACHE] {self.name} cache updated successfully in {elapsed:.2f}s - size: {data_size}")
                    return True
                    
                print(f"[CACHE] {self.name} cache update failed - no data received")
                return False
            except Exception as e:
                print(f"[CACHE] Error updating {self.name} cache: {e}")
                return False
    
    def get(self):
        """Получает данные из кэша"""
        return self.data
    
    def is_valid(self, ttl=900):  # увеличим время жизни кэша до 15 минут
        """Проверяет, актуален ли кэш"""
        return self.data is not None and (time.time() - self.timestamp) < ttl

# Инициализация кэшей для различных типов данных
full_apy_cache = CacheData("Full APY")   # Кэш для всех APY данных
top_apy_cache = CacheData("Top APY")    # Кэш для top-1 APY
top_three_cache = CacheData("Top Three APY")  # Кэш для top-3 APY
top_ten_cache = CacheData("Top Ten APY")    # Кэш для top-10 APY
assets_cache = CacheData("Assets")     # Кэш для списка активов
chains_cache = CacheData("Chains")     # Кэш для списка блокчейнов

def log_query(operation, user_id=None, username=None, verbose=False):
    """Log database queries with configurable verbosity"""
    if not verbose:
        return  # Выходим без логирования для ускорения работы
    
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

async def _fetch_latest_full_apy():
    """Внутренняя функция для получения полных данных APY из базы данных"""
    try:
        # Вызываем созданную RPC функцию с пустым словарем параметров
        response = supabase.rpc('get_latest_apy_data', {}).execute()
        
        if has_error(response):
            print(f"Error calling get_latest_apy_data: {response.error}")
            return None
        
        result = response.data if response.data else []
        
        # Постобработка: извлекаем project из pool_id
        for item in result:
            if 'pool_id' in item and item['pool_id']:
                parts = item['pool_id'].split('_')
                if len(parts) >= 3:
                    item['project'] = '_'.join(parts[2:])
                else:
                    item['project'] = parts[0] if parts else ''
        
        return result
    except Exception as e:
        print(f"Error in _fetch_latest_full_apy: {e}")
        # В случае ошибки возвращаем предыдущий кэш, если он есть
        if full_apy_cache.get() is not None:
            print(f"Using existing cache ({len(full_apy_cache.get())} records) due to error")
            return full_apy_cache.get()
        return None

async def get_latest_full_apy(skip_cache=False):
    """Get latest APY data from database or cache"""
    log_query('get_latest_full_apy')
    
    # Если кэш действителен и не запрошен пропуск кэша, используем кэш
    if full_apy_cache.is_valid() and not skip_cache:
        age = time.time() - full_apy_cache.timestamp
        cached_data = full_apy_cache.get()
        print(f"[CACHE HIT] get_latest_full_apy - using cached data from {age:.2f} seconds ago")
        return cached_data
    
    try:
        print("[CACHE MISS] get_latest_full_apy - fetching from database")
        
        # Используем правильную RPC-функцию вместо прямого обращения к таблице
        data = await _fetch_latest_full_apy()
        
        # Обновляем кэш, если не запрошен пропуск кэша
        if data is not None and not skip_cache:
            full_apy_cache.data = data
            full_apy_cache.timestamp = time.time()
            print(f"[CACHE UPDATE] get_latest_full_apy - cached {len(data)} records")
        
        return data
    except Exception as e:
        print(f"Error getting APY data: {e}")
        # Если произошла ошибка, возвращаем кэш даже если он устарел
        if full_apy_cache.get() is not None:
            print("Returning outdated cache data after database error")
            return full_apy_cache.get()
        return None

async def _fetch_top_apy():
    """Внутренняя функция для получения top-1 APY"""
    data = await get_latest_full_apy()
    
    if not data:
        return None
    
    # Filter by TVL >= 1,000,000 and sort by APY
    filtered_data = sorted(
        [item for item in data if item.get('tvl', 0) >= 1000000],
        key=lambda x: x.get('apy', 0),
        reverse=True
    )
    
    return filtered_data[0] if filtered_data else None

async def get_top_apy():
    """Get top-1 APY from cached data"""
    # Для отладки: печатаем ID экземпляра модуля
    print(f"[DEBUG] get_top_apy called from module instance {id(sys.modules[__name__])}")
    
    # Используем только кэш для максимальной скорости
    cached_data = top_apy_cache.get()
    if cached_data is not None:
        # Для диагностики
        cache_age = time.time() - top_apy_cache.timestamp
        print(f"[CACHE] Using cached top APY ({cache_age:.2f}s old): {cached_data.get('asset')} with APY={cached_data.get('apy')}")
        return cached_data
    
    # Если в кэше нет данных, вычисляем из основного кэша
    data = full_apy_cache.get()
    if data is not None:
        filtered_data = sorted(
            [item for item in data if item.get('tvl', 0) >= 1000000],
            key=lambda x: x.get('apy', 0),
            reverse=True
        )
        
        result = filtered_data[0] if filtered_data else None
        
        # Сохраняем в кэш для последующих запросов
        top_apy_cache.data = result
        top_apy_cache.timestamp = time.time()
        
        print(f"[CACHE] Calculated top APY from full data: {result.get('asset')} with APY={result.get('apy')}")
        return result
    
    # Если нет никаких данных в кэше, делаем запрос к БД
    print("[CACHE MISS] No cached data available, fetching from database")
    result = await _fetch_top_apy()
    return result

async def _fetch_top_three_apy():
    """Внутренняя функция для получения top-3 APY"""
    data = await get_latest_full_apy()
    
    if not data:
        return []
    
    # Filter by TVL >= 1,000,000 and sort by APY
    filtered_data = sorted(
        [item for item in data if item.get('tvl', 0) >= 1000000],
        key=lambda x: x.get('apy', 0),
        reverse=True
    )[:3]  # Limit to three results
    
    # Сохраняем в кэш
    top_three_cache.data = filtered_data
    top_three_cache.timestamp = time.time()
    print(f"[CACHE UPDATE] get_top_three_apy - cached {len(filtered_data)} results")
    
    return filtered_data

async def get_top_three_apy():
    """Get top-3 APY from cached data"""
    log_query('get_top_three_apy')
    
    # Возвращаем данные из кэша, если они актуальны
    if top_three_cache.get() is not None:
        print(f"[CACHE HIT] get_top_three_apy - using cached data from {time.time() - top_three_cache.timestamp:.2f} seconds ago")
        return top_three_cache.get()
    
    print("[CACHE MISS] get_top_three_apy - calculating from full data")
    
    # Если у нас есть кэшированные full_apy данные, используем их для расчета
    if full_apy_cache.get() is not None:
        data = full_apy_cache.get()
        print("[CACHE REUSE] Using cached full_apy_cache for calculation")
    else:
        # Если нет, получаем данные
        data = await _fetch_latest_full_apy()
    
    if not data:
        return []
        
    # Filter by TVL >= 1,000,000 and sort by APY
    filtered_data = sorted(
        [item for item in data if item.get('tvl', 0) >= 1000000],
        key=lambda x: x.get('apy', 0),
        reverse=True
    )[:3]  # Limit to three results
    
    # Сохраняем в кэш
    top_three_cache.data = filtered_data
    top_three_cache.timestamp = time.time()
    print(f"[CACHE UPDATE] get_top_three_apy - cached {len(filtered_data)} results")
    
    return filtered_data

async def _fetch_top_ten_apy():
    """Внутренняя функция для получения top-10 APY"""
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

async def get_top_ten_apy():
    """Get top-10 APY from cached data"""
    log_query('get_top_ten_apy')
    
    # Возвращаем данные из кэша, если они актуальны
    if top_ten_cache.get() is not None:
        return top_ten_cache.get()
    
    # Если кэш пустой, получаем данные и обновляем кэш
    data = await _fetch_top_ten_apy()
    if data is not None:
        # Устанавливаем данные напрямую в первый раз
        top_ten_cache.data = data
        top_ten_cache.timestamp = time.time()
    
    return data

async def _fetch_all_assets():
    """Внутренняя функция для получения списка всех активов"""
    data = await get_latest_full_apy()
    
    if not data:
        return []
    
    # Get unique assets
    all_assets = [item.get('asset') for item in data if item.get('asset')]
    unique_assets = sorted(list(set(all_assets)))
    
    return unique_assets

async def get_all_assets():
    """Get all unique asset values with caching"""
    log_query('get_all_assets')
    
    # Возвращаем данные из кэша, если они актуальны
    if assets_cache.get() is not None:
        return assets_cache.get()
    
    # Если кэш пустой, получаем данные и обновляем кэш
    data = await _fetch_all_assets()
    if data is not None:
        # Устанавливаем данные напрямую в первый раз
        assets_cache.data = data
        assets_cache.timestamp = time.time()
    
    return data

async def _fetch_all_chains():
    """Внутренняя функция для получения списка всех блокчейнов"""
    data = await get_latest_full_apy()
    
    if not data:
        return []
    
    # Get unique chains
    all_chains = [item.get('chain') for item in data if item.get('chain')]
    unique_chains = sorted(list(set(all_chains)))
    
    return unique_chains

async def get_all_chains():
    """Get all unique chain values with caching"""
    log_query('get_all_chains')
    
    # Возвращаем данные из кэша, если они актуальны
    if chains_cache.get() is not None:
        return chains_cache.get()
    
    # Если кэш пустой, получаем данные и обновляем кэш
    data = await _fetch_all_chains()
    if data is not None:
        # Устанавливаем данные напрямую в первый раз
        chains_cache.data = data
        chains_cache.timestamp = time.time()
    
    return data

async def update_all_caches():
    """Update all caches with proper cache invalidation"""
    timestamp = datetime.datetime.now().isoformat()
    print(f"[{timestamp}] [CACHE] Starting update")
    
    global _cache_updating, _formatted_data_cache
    with _cache_update_lock:
        if _cache_updating:
            print(f"[{timestamp}] [CACHE] Update already in progress, skipping")
            return
        _cache_updating = True
    
    try:
        start_time = time.time()
        
        # Получаем свежие данные из базы
        print(f"[{timestamp}] [CACHE] Fetching fresh data directly from database")
        fresh_data = await _fetch_latest_full_apy()
        
        if not fresh_data:
            print("[CACHE] Failed to get fresh data from database!")
            return
        
        # Перед обновлением кэшей сохраняем ссылку на старые данные для сравнения
        old_data = full_apy_cache.get()
        
        # ВАЖНО: Полностью сбрасываем все кэши прежде чем устанавливать новые данные
        top_apy_cache.data = None
        top_three_cache.data = None
        top_ten_cache.data = None
        assets_cache.data = None
        chains_cache.data = None
        
        # Очищаем кэш отформатированных сообщений для обновления представления
        _formatted_data_cache.clear()
        print("[CACHE] Formatted message cache cleared")
        
        # Обновляем основной кэш
        full_apy_cache.data = fresh_data
        full_apy_cache.timestamp = time.time()
        print(f"[CACHE] Full APY cache updated with new data - size: {len(fresh_data)}")
        
        # Выполняем сравнение, если есть старые данные
        if old_data and len(old_data) > 0 and len(fresh_data) > 0:
            old_top = sorted([i for i in old_data if i.get('tvl', 0) >= 1000000], 
                           key=lambda x: x.get('apy', 0), reverse=True)
            new_top = sorted([i for i in fresh_data if i.get('tvl', 0) >= 1000000], 
                           key=lambda x: x.get('apy', 0), reverse=True)
            
            if old_top and new_top:
                print(f"[CACHE] Previous top APY: {old_top[0].get('asset')} on {old_top[0].get('chain')} with APY={old_top[0].get('apy')}")
                print(f"[CACHE] New top APY: {new_top[0].get('asset')} on {new_top[0].get('chain')} with APY={new_top[0].get('apy')}")
        
        # ВАЖНО: Принудительно вычисляем все производные кэши сразу
        await pre_calculate_all_caches()
        
        # Проверяем, действительно ли обновился производный кэш
        if top_apy_cache.get():
            print(f"[CACHE VERIFY] Top APY after update: {top_apy_cache.get().get('asset')} with APY={top_apy_cache.get().get('apy')}")
        else:
            print("[CACHE ERROR] Top APY cache not calculated properly!")
        
        elapsed = time.time() - start_time
        print(f"[{timestamp}] [CACHE] Update completed in {elapsed:.2f} seconds")
    finally:
        with _cache_update_lock:
            _cache_updating = False

# Дополнительная функция для безопасного пересчета кэшей
async def pre_calculate_all_caches_safe():
    """Safe version of pre_calculate_all_caches with error handling"""
    try:
        await pre_calculate_all_caches()
    except Exception as e:
        print(f"[CACHE PRECALC] Error during precalculation: {e}")
        # При ошибке гарантируем, что кэш не будет заблокирован

# Вспомогательная функция для проверки изменений в данных
def check_data_changes(old_data, new_data):
    """Check if there are significant changes between old and new data"""
    if not old_data or not new_data:
        return True
    
    if len(old_data) != len(new_data):
        return True
    
    # Проверяем sample записей на изменения
    sample_indices = [0, min(10, len(new_data)-1), min(50, len(new_data)-1)]
    for idx in sample_indices:
        if idx < len(old_data) and idx < len(new_data):
            if (old_data[idx].get('apy') != new_data[idx].get('apy') or 
                old_data[idx].get('tvl') != new_data[idx].get('tvl')):
                return True
    
    return False

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

async def get_top_apy_for_asset(asset_name):
    """Get top APY for a specific asset across all chains"""
    log_query(f'get_top_apy_for_asset: {asset_name}')
    
    data = await get_latest_full_apy()
    
    if not data:
        return []
    
    # Фильтруем только по имени актива и TVL >= 100,000
    filtered_data = [item for item in data if item.get('asset') == asset_name and item.get('tvl', 0) >= 100000]
    
    # Просто сортируем все пулы по APY, без группировки по цепям
    sorted_data = sorted(filtered_data, key=lambda x: x.get('apy', 0), reverse=True)
    
    # Возвращаем только 3 лучших пула вместо 10
    return sorted_data[:3]

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

async def get_top_apy_for_chain(chain_name):
    """Get top APY for a specific chain across all assets"""
    log_query(f'get_top_apy_for_chain: {chain_name}')
    
    data = await get_latest_full_apy()
    
    if not data:
        return []
    
    # Отладочное логирование
    all_chains = set(item.get('chain') for item in data if item.get('chain'))
    print(f"[CHAIN] Looking for chain '{chain_name}' among available chains: {all_chains}")
    
    # Сначала пробуем точное регистро-независимое совпадение БЕЗ фильтра по TVL
    filtered_data = [item for item in data if item.get('chain') and 
                    item.get('chain').lower() == chain_name.lower()]
    
    # Если точных совпадений нет, пробуем частичное совпадение
    if not filtered_data:
        print(f"[CHAIN] No exact match for chain '{chain_name}', trying partial match")
        
        # Пробуем найти цепь как подстроку (с удалением подчеркиваний для унификации)
        filtered_data = [item for item in data if item.get('chain') and 
                     (chain_name.lower().replace('_', '') in item.get('chain').lower().replace('_', '') or
                      item.get('chain').lower().replace('_', '') in chain_name.lower().replace('_', ''))]
    
    # Просто сортируем все пулы по APY, без группировки по активам и без фильтра по TVL
    sorted_data = sorted(filtered_data, key=lambda x: x.get('apy', 0), reverse=True)
    
    # Возвращаем только 3 лучших пула
    if sorted_data:
        print(f"[CHAIN] Found {len(sorted_data)} pools for chain '{chain_name}'")
    else:
        print(f"[CHAIN] No pools found for chain '{chain_name}' after matching")
    
    return sorted_data[:3]

async def pre_calculate_all_caches():
    """Предварительно рассчитывает все производные кэши на основе full_apy_cache"""
    if full_apy_cache.get() is None:
        print("[CACHE PRECALC] No full APY data available, skipping precalculation")
        return False
        
    print("[CACHE PRECALC] Starting precalculation of all caches")
    start_time = time.time()
    
    # Получаем данные из основного кэша один раз
    data = full_apy_cache.get()
    current_time = time.time()
    
    # Расчет для top APY (с фильтрацией по TVL)
    filtered_data = sorted(
        [item for item in data if item.get('tvl', 0) >= 1000000],
        key=lambda x: x.get('apy', 0),
        reverse=True
    )
    
    print(f"[CACHE PRECALC] Found {len(filtered_data)} pools with TVL >= 1M")
    
    # Обновляем top_apy_cache
    top_apy_cache.data = filtered_data[0] if filtered_data else None
    top_apy_cache.timestamp = current_time
    
    if filtered_data:
        print(f"[CACHE PRECALC] Top APY: {filtered_data[0].get('asset')} on {filtered_data[0].get('chain')} with APY={filtered_data[0].get('apy')}")
    
    # Обновляем top_three_cache
    top_three_cache.data = filtered_data[:3]
    top_three_cache.timestamp = current_time
    
    # Обновляем top_ten_cache
    top_ten_cache.data = filtered_data[:10]
    top_ten_cache.timestamp = current_time
    
    # Получаем уникальные активы и цепочки
    all_assets = sorted(list(set(item.get('asset') for item in data if item.get('asset'))))
    assets_cache.data = all_assets
    assets_cache.timestamp = current_time
    
    all_chains = sorted(list(set(item.get('chain') for item in data if item.get('chain'))))
    chains_cache.data = all_chains
    chains_cache.timestamp = current_time
    
    end_time = time.time()
    print(f"[CACHE PRECALC] All caches precalculated in {end_time - start_time:.2f} seconds")
    return True

# Добавьте эту функцию для полного сброса всех кэшей
async def force_refresh_all_caches():
    """Force refresh all caches by directly fetching from database"""
    print("[FORCE REFRESH] Starting forced refresh of all caches")
    
    try:
        # Устанавливаем признак блокировки
        global _cache_updating, _formatted_data_cache
        with _cache_update_lock:
            if _cache_updating:
                print("[FORCE REFRESH] Update already in progress, skipping")
                return False
            _cache_updating = True
        
        # Получаем свежие данные напрямую из БД
        print("[FORCE REFRESH] Calling database for fresh data")
        fresh_data = await _fetch_latest_full_apy()
        
        if not fresh_data:
            print("[FORCE REFRESH] Failed to get data from database")
            return False
        
        # Полностью сбрасываем все кэши
        print("[FORCE REFRESH] Clearing all caches")
        full_apy_cache.data = None
        top_apy_cache.data = None
        top_three_cache.data = None
        top_ten_cache.data = None
        assets_cache.data = None
        chains_cache.data = None
        _formatted_data_cache.clear()
        
        # Обновляем основной кэш с новыми данными
        full_apy_cache.data = fresh_data
        full_apy_cache.timestamp = time.time()
        print(f"[FORCE REFRESH] Primary cache updated with {len(fresh_data)} records")
        
        # Используем новую функцию для принудительного расчета всех производных кэшей
        await _force_calculate_all_caches()
        
        print("[FORCE REFRESH] All caches completely refreshed with new data")
        
        # Проверка, что топ APY действительно обновился
        if top_apy_cache.get():
            print(f"[FORCE REFRESH] Verified top APY: {top_apy_cache.get().get('asset')} with {top_apy_cache.get().get('apy')}")
        else:
            print("[FORCE REFRESH] Warning: top_apy_cache is still empty after refresh!")
        
        return True
    except Exception as e:
        print(f"[FORCE REFRESH] Error during force refresh: {e}")
        return False
    finally:
        with _cache_update_lock:
            _cache_updating = False

# Отдельная функция для принудительного расчета производных кэшей
async def _force_calculate_all_caches():
    """Принудительный расчет всех производных кэшей с подробным логированием"""
    print("[FORCE CALC] Starting forced calculation of all derivative caches")
    start_time = time.time()
    
    # Принудительно получаем данные из основного кэша
    data = full_apy_cache.get()
    if not data:
        print("[FORCE CALC] Error: No data in full_apy_cache!")
        return False
    
    # Текущее время для всех кэшей (для синхронизации)
    current_time = time.time()
    
    # Расчет для top APY
    filtered_data = sorted(
        [item for item in data if item.get('tvl', 0) >= 1000000],
        key=lambda x: x.get('apy', 0),
        reverse=True
    )
    
    print(f"[FORCE CALC] Found {len(filtered_data)} pools with TVL >= 1M")
    
    # Обновляем top_apy_cache (первое место)
    if filtered_data:
        top_pool = filtered_data[0]
        print(f"[FORCE CALC] Setting top pool: {top_pool.get('asset')} on {top_pool.get('chain')} with APY={top_pool.get('apy')}")
        
        # Обновляем локальный кэш
        top_apy_cache.data = top_pool
        top_apy_cache.timestamp = current_time
    else:
        top_apy_cache.data = None
        print("[FORCE CALC] Warning: No pools with sufficient TVL")
    
    # Обновляем top_three_cache
    top_three = filtered_data[:3]
    top_three_cache.data = top_three
    top_three_cache.timestamp = current_time
    
    # Обновляем top_ten_cache
    top_ten = filtered_data[:10]
    top_ten_cache.data = top_ten
    top_ten_cache.timestamp = current_time
    
    # Обновляем остальные кэши
    unique_assets = sorted(list(set(item.get('asset') for item in data if item.get('asset'))))
    assets_cache.data = unique_assets
    assets_cache.timestamp = current_time
    
    unique_chains = sorted(list(set(item.get('chain') for item in data if item.get('chain'))))
    chains_cache.data = unique_chains
    chains_cache.timestamp = current_time
    
    # Финальная проверка
    print(f"[FORCE CALC] Verification - Top APY: {top_pool.get('asset') if 'top_pool' in locals() else 'None'} with APY={top_pool.get('apy') if 'top_pool' in locals() else 'N/A'}")
    
    end_time = time.time()
    print(f"[FORCE CALC] All caches forcefully calculated in {end_time - start_time:.2f} seconds")
    return True

# Завершаем оптимизированную функцию format_top_apy_data
def format_top_apy_data(data, position):
    """Format APY data for display with ranking (optimized)"""
    if not data:
        return "No data available"
    
    # Используем пул-специфичный ключ кэша, независимый от позиции
    cache_key = f"{data.get('pool_id', '')}"
    if cache_key in _formatted_data_cache:
        formatted_text = _formatted_data_cache[cache_key]
        # Заменяем только emoji в зависимости от позиции
        position_emoji = '🥇' if position == 1 else '🥈' if position == 2 else '🥉' if position == 3 else '🏅'
        return formatted_text.replace("POSITION_EMOJI", position_emoji)
    
    # Extract protocol name from pool_id
    pool_id = data.get('pool_id', '')
    protocol_parts = pool_id.split('_')
    
    # Проверяем, есть ли как минимум 3 части в pool_id
    if len(protocol_parts) >= 3:
        protocol = '_'.join(protocol_parts[2:])
    else:
        protocol = protocol_parts[0] if protocol_parts else ''
    
    # Format TVL
    tvl = data.get('tvl', 0)
    if tvl >= 1000000:
        tvl_formatted = f"${tvl / 1000000:.1f}M"
    elif tvl >= 1000:
        tvl_formatted = f"${tvl / 1000:.1f}K"
    else:
        tvl_formatted = f"${tvl:.0f}"
    
    # Безопасное форматирование APY данных с проверкой на None
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
    
    # Используем шаблон с заменой позиции позже
    try:
        result = (
            f"POSITION_EMOJI *{data.get('asset')}* on *{data.get('chain')}*\n"
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
            f"POSITION_EMOJI Asset: {data.get('asset')} on {data.get('chain')}\n"
            f"Protocol: {protocol}\n"
            f"APY: {apy_total}\n"
            f"TVL: {tvl_formatted}"
        )
    
    # Кэшируем результат
    _formatted_data_cache[cache_key] = result
    
    # Заменяем эмодзи позиции перед возвратом
    position_emoji = '🥇' if position == 1 else '🥈' if position == 2 else '🥉' if position == 3 else '🏅'
    return result.replace("POSITION_EMOJI", position_emoji)

async def update_format_cache(key, value):
    """Safely update format cache with lock"""
    async with _format_cache_lock:
        _formatted_data_cache[key] = value 

def get_latest_top_apy_value():
    """Get the current value of the top APY from cache"""
    data = top_apy_cache.get()
    if data:
        return data.get('apy')
    return None 