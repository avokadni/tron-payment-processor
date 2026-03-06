import os
import uuid
import threading
import time
import random
import secrets
import logging
import hashlib
import re
import functools
import math
import collections
from concurrent.futures import ThreadPoolExecutor, as_completed
import ipaddress
from datetime import datetime, timedelta
from typing import Dict, Optional, Callable, List, Any
from dotenv import load_dotenv

from database import DatabaseManager
from tronscan_api import TronScanAPI

load_dotenv()

BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
BASE58_MAP = {char: index for index, char in enumerate(BASE58_ALPHABET)}

def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0, 
                    exceptions: tuple = (Exception,)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        break
                    
                    if args and hasattr(args[0], 'logger'):
                        args[0].logger.warning(
                            f"Попытка {attempt + 1}/{max_retries + 1} не удалась для {func.__name__}: {e}. "
                            f"Повтор через {current_delay:.1f} секунд"
                        )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
                    
            raise last_exception
        return wrapper
    return decorator

class PaymentProcessor:
    OFFICIAL_USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    
    def __init__(self, log_level: str = None):
        self.logger = logging.getLogger(__name__)
        if log_level is None:
            log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        try:
            self._validate_env_vars()
            api_rate_limit = self._get_api_rate_limit()
            
            self.db = DatabaseManager(os.getenv('DATABASE_PATH', 'transaction.db'))
            self.tronscan = TronScanAPI(
                os.getenv('TRONSCAN_API_URL', 'https://apilist.tronscanapi.com/api'),
                requests_per_minute=api_rate_limit
            )
            self.wallet_address = os.getenv('WALLET_ADDRESS')
            
            if not self.wallet_address:
                raise ValueError("WALLET_ADDRESS не указан в .env файле")
            
            self.monitoring = False
            self.monitor_thread = None
            self.payment_callbacks = {}
            self._form_creation_lock = threading.Lock()
            self._last_form_creation_time = 0
            self._processed_transactions = set()
            self._max_processed_transactions = 10000
            self._processing_transactions = set()
            self._transaction_cache_lock = threading.Lock()
            self._last_block_timestamp = 0
            self._form_cache = {}
            self._form_cache_lock = threading.Lock()
            self._cache_expiry = int(os.getenv('CACHE_EXPIRY_SECONDS', 300))
            self._api_cache = {}
            self._api_cache_lock = threading.Lock()
            self._api_cache_ttl = int(os.getenv('API_CACHE_TTL_SECONDS', 30))
            
            self._payment_processing_lock = threading.RLock()
            self._transaction_processing_lock = threading.RLock()
            self._form_status_lock = threading.RLock()

            self._user_form_counts = {}
            self._user_form_lock = threading.Lock()
            self._user_last_form_time = {}
            self._user_form_timestamps = collections.defaultdict(collections.deque)
            self._user_rate_limit_lock = threading.Lock()
            self._max_user_counters = int(os.getenv('MAX_USER_COUNTERS', 10000))
            
            self.logger.info(f"PaymentProcessor инициализирован для кошелька: {self._mask_wallet_address(self.wallet_address)}")
            self.logger.info(f"Уровень логирования: {log_level}")
            
        except Exception as e:
            self.logger.error(f"Ошибка инициализации PaymentProcessor: {e}")
            raise

    def set_log_level(self, level: str):
        self.logger.setLevel(getattr(logging, level.upper()))
        self.logger.info(f"Уровень логирования изменен на: {level.upper()}")
    
    def _mask_wallet_address(self, address: str) -> str:
        if not address or len(address) < 8:
            return "****"
        return f"{address[:4]}...{address[-4:]}"
    
    def _mask_amount(self, amount: float) -> str:
        return "***.**"
    
    def _validate_env_vars(self):
        wallet = os.getenv('WALLET_ADDRESS')
        if not wallet or not self._validate_tron_address(wallet):
            raise ValueError("Некорректный WALLET_ADDRESS в переменных окружения")
        
        api_url = os.getenv('TRONSCAN_API_URL', 'https://apilist.tronscanapi.com/api')
        if not api_url.startswith(('http://', 'https://')):
            raise ValueError("Некорректный TRONSCAN_API_URL в переменных окружения")
        
        try:
            rate_limit = self._get_api_rate_limit()
            if rate_limit < 1 or rate_limit > 1000:
                raise ValueError("API_REQUESTS_PER_MINUTE/API_RATE_LIMIT должен быть от 1 до 1000")
        except ValueError:
            raise ValueError("Некорректный API_REQUESTS_PER_MINUTE/API_RATE_LIMIT в переменных окружения")

    def _get_api_rate_limit(self) -> int:
        raw_rate_limit = os.getenv('API_REQUESTS_PER_MINUTE')
        if raw_rate_limit is None:
            raw_rate_limit = os.getenv('API_RATE_LIMIT', '20')
        return int(raw_rate_limit)
    
    def _validate_description(self, description: str) -> bool:
        if not isinstance(description, str):
            return False
            
        max_description_length = int(os.getenv('MAX_DESCRIPTION_LENGTH', 500))
        if len(description) > max_description_length:
            return False
        
        if len(description.strip()) == 0:
            return True
        
        dangerous_chars = ['<', '>', '"', "'", '&', '\n', '\r', '\t', '\0', '\x1a', '\x00']
        if any(char in description for char in dangerous_chars):
            return False
        
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 
                       'ALTER', 'EXEC', 'UNION', 'SCRIPT', 'JAVASCRIPT', 'EXECUTE',
                       'TRUNCATE', 'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK']
        description_upper = description.upper()
        if any(keyword in description_upper for keyword in sql_keywords):
            return False
        
        if any(ord(char) < 32 and char not in [' ', '\t'] for char in description):
            return False
        
        dangerous_patterns = [
            r'javascript:',
            r'data:text/html',
            r'vbscript:',
            r'<script[^>]*>',
            r'</script>',
            r'onload\s*=',
            r'onerror\s*=',
            r'onclick\s*=',
            r'onmouseover\s*='
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, description, re.IGNORECASE):
                return False
            
        return True
    
    def _validate_tron_address(self, address: str) -> bool:
        if not address or not isinstance(address, str):
            return False
        
        if len(address) != 34:
            return False
        
        if not re.match(r'^T[A-Za-z0-9]{33}$', address):
            return False
        
        if address == 'T0000000000000000000000000000000000':
            return False
        
        decoded = self._decode_tron_base58(address)
        if not decoded or len(decoded) != 25:
            return False
        
        payload, checksum = decoded[:-4], decoded[-4:]
        if not payload or payload[0] != 0x41:
            return False
        
        expected_checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
        if checksum != expected_checksum:
            return False
        
        return True
    
    def _decode_tron_base58(self, value: str) -> Optional[bytes]:
        try:
            num = 0
            for char in value:
                if char not in BASE58_MAP:
                    return None
                num = num * 58 + BASE58_MAP[char]
            
            decoded = num.to_bytes((num.bit_length() + 7) // 8, byteorder='big') if num > 0 else b''
            leading_zeros = len(value) - len(value.lstrip('1'))
            return b'\x00' * leading_zeros + decoded
        except Exception:
            return None
    
    def _validate_amount(self, amount: float, currency: str) -> bool:
        if not isinstance(amount, (int, float)):
            return False
            
        if not isinstance(currency, str) or not currency.strip():
            return False
        
        try:
            if math.isnan(amount):
                return False
        except (TypeError, ValueError):
            return False
            
        if amount == float('inf') or amount == float('-inf'):
            return False
        
        if amount <= 0:
            return False
        
        max_amount_limit = float(os.getenv('MAX_AMOUNT_LIMIT', 1e15))
        if amount > max_amount_limit:
            return False
        
        if amount != round(amount, 4):
            self.logger.warning(f"Сумма имеет слишком высокую точность: {amount}")
            return False
        
        min_limits = {
            'USDT': float(os.getenv('MIN_USDT_AMOUNT', 0.1)),
            'TRX': float(os.getenv('MIN_TRX_AMOUNT', 1.0))
        }
        
        max_limits = {
            'USDT': float(os.getenv('MAX_USDT_AMOUNT', 10000.0)),
            'TRX': float(os.getenv('MAX_TRX_AMOUNT', 100000.0))
        }
        
        if currency in min_limits and amount < min_limits[currency]:
            self.logger.warning(f"Сумма ниже минимального лимита для {currency}: {amount}")
            return False
        
        if currency in max_limits and amount > max_limits[currency]:
            self.logger.warning(f"Превышен лимит для {currency}: {amount}")
            return False
        
        return True
    
    def _validate_sender_address(self, from_address: str) -> bool:
        if not self._validate_tron_address(from_address):
            return False
        
        blacklisted_addresses = os.getenv('BLACKLISTED_ADDRESSES', '').split(',')
        if from_address.lower() in [addr.lower().strip() for addr in blacklisted_addresses if addr.strip()]:
            self.logger.warning(f"Платеж от заблокированного адреса: {self._mask_wallet_address(from_address)}")
            return False
        
        if from_address.lower() == self.wallet_address.lower():
            self.logger.warning("Попытка самоперевода")
            return False
        
        return True
    
    def _validate_transaction_timestamp(self, transaction: Dict) -> bool:
        tx_timestamp = transaction.get('timestamp', 0)
        current_time = int(datetime.now().timestamp() * 1000)
        
        max_age_hours = int(os.getenv('MONITOR_TRANSACTION_HOURS', 2))
        max_age = max_age_hours * 60 * 60 * 1000
        
        if current_time - tx_timestamp > max_age:
            age_minutes = (current_time - tx_timestamp) / 1000 / 60
            self.logger.warning(f"Транзакция слишком старая: {age_minutes:.1f} минут")
            return False
        
        future_tolerance_minutes = int(os.getenv('FUTURE_TOLERANCE_MINUTES', 5))
        future_tolerance = future_tolerance_minutes * 60 * 1000
        
        if tx_timestamp > current_time + future_tolerance:
            self.logger.warning("Транзакция из будущего")
            return False
        
        return True
    
    def _validate_transaction_confirmations(self, transaction: Dict) -> bool:
        min_confirmations = {
            'USDT': int(os.getenv('MIN_CONFIRMATIONS_USDT', 19)),
            'TRX': int(os.getenv('MIN_CONFIRMATIONS_TRX', 19))
        }
        
        currency = transaction.get('currency', '')
        default_confirmations = int(os.getenv('DEFAULT_MIN_CONFIRMATIONS', 19))
        required_confirmations = min_confirmations.get(currency, default_confirmations)
        
        try:
            tx_confirmations = transaction.get('confirmations')
            if isinstance(tx_confirmations, int) and tx_confirmations >= required_confirmations:
                return True
            
            if not transaction.get('confirmed', False):
                self.logger.info(f"Транзакция не подтверждена: {transaction.get('transaction_id', 'unknown')}")
                return False
            
            tx_details = self.tronscan.get_transaction_details(transaction['transaction_id'])
            if not tx_details:
                self.logger.warning(f"Не удалось получить детали транзакции {transaction['transaction_id']}")
                return False
            
            confirmations = tx_details.get('confirmations')
            if confirmations is None:
                self.logger.warning(f"В деталях транзакции {transaction['transaction_id']} отсутствует поле confirmations")
                return transaction.get('confirmed', False)
            
            confirmations = int(confirmations)
            if confirmations < required_confirmations:
                self.logger.info(f"Недостаточно подтверждений: {confirmations}/{required_confirmations}")
                return False
            
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при проверке подтверждений: {e}")
            return False
    
    def _validate_usdt_contract(self, transaction: Dict) -> bool:
        if transaction.get('currency') != 'USDT':
            return True
        
        try:
            if 'trc20_transfer' in transaction:
                trc20_data = transaction['trc20_transfer']
                token_info = trc20_data.get('tokenInfo', {})
                contract_address = (
                    trc20_data.get('contract_address')
                    or token_info.get('tokenId')
                    or token_info.get('token_id')
                    or ''
                )
                
                if contract_address == self.OFFICIAL_USDT_CONTRACT:
                    return True
                elif contract_address:
                    self.logger.warning(f"❌ Поддельный USDT контракт: {contract_address}")
                    return False
                else:
                    self.logger.warning("❌ Невозможно определить контракт USDT транзакции")
                    return False
            
            if 'trc20TransferInfo' in transaction:
                transfers = transaction['trc20TransferInfo']
                found_contract = False
                for transfer in transfers:
                    token_info = transfer.get('tokenInfo', {})
                    contract_address = token_info.get('tokenId', '')
                    
                    if contract_address:
                        found_contract = True
                    if contract_address and contract_address != self.OFFICIAL_USDT_CONTRACT:
                        self.logger.warning(f"❌ Поддельный USDT контракт: {contract_address}")
                        return False
                if found_contract:
                    return True
                self.logger.warning("❌ Невозможно определить контракт USDT транзакции")
                return False
            
            self.logger.warning("❌ Отсутствуют данные контракта для USDT транзакции")
            return False
        except Exception as e:
            self.logger.error(f"Ошибка при проверке USDT контракта: {e}")
            return False
    
    def _generate_payment_hash(self, form_id: str, amount: float, currency: str) -> str:
        data = f"{form_id}:{amount}:{currency}:{datetime.now().isoformat()}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]
    
    def _check_form_creation_limits(self, client_ip: str = None, user_id: str = None):
        try:
            current_time = time.time()
            
            active_forms_count = len(self.db.get_active_payment_forms(current_time))
            max_total_forms = int(os.getenv('MAX_TOTAL_FORMS', 1000))
            
            if active_forms_count >= max_total_forms:
                raise Exception(f"Превышен общий лимит активных форм: {active_forms_count}/{max_total_forms}")
            
            with self._form_creation_lock:
                time_since_last = current_time - self._last_form_creation_time
                min_interval_seconds = float(os.getenv('MIN_FORM_CREATION_INTERVAL_SECONDS', 0.5))
                if time_since_last < min_interval_seconds:
                    raise Exception(f"Слишком частое создание форм. Подождите {min_interval_seconds - time_since_last:.1f} секунд")
                self._last_form_creation_time = current_time
            
            if user_id:
                if not self._validate_telegram_user_id(str(user_id)):
                    raise ValueError("Некорректный user_id")
                
                with self._user_rate_limit_lock:
                    user_key = str(user_id)
                    rate_window_seconds = int(os.getenv('USER_FORM_RATE_WINDOW_SECONDS', 3600))
                    
                    if user_key in self._user_last_form_time:
                        time_since_user_last = current_time - self._user_last_form_time[user_key]
                        min_user_interval_seconds = float(os.getenv('MIN_USER_FORM_INTERVAL_SECONDS', 2.0))
                        if time_since_user_last < min_user_interval_seconds:
                            raise Exception(f"Слишком частое создание форм. Подождите {min_user_interval_seconds - time_since_user_last:.1f} секунд")
                    
                    user_timestamps = self._user_form_timestamps[user_key]
                    while user_timestamps and current_time - user_timestamps[0] >= rate_window_seconds:
                        user_timestamps.popleft()
                    
                    max_user_forms_per_hour = int(os.getenv('MAX_USER_FORMS_PER_HOUR', 20))
                    if len(user_timestamps) >= max_user_forms_per_hour:
                        raise Exception(f"Превышен лимит форм для пользователя: {len(user_timestamps)}/{max_user_forms_per_hour} в час")
                    
                    self._user_last_form_time[user_key] = current_time
                    user_timestamps.append(current_time)
                    self._user_form_counts[user_key] = len(user_timestamps)
                    
                    self._cleanup_user_counters(current_time)
            
            if client_ip:
                self.logger.info(f"Создание формы с IP: {client_ip}")
            
            if user_id:
                self.logger.info(f"Создание формы для пользователя: {user_id}")
            
        except Exception as e:
            self.logger.error(f"Превышен лимит создания форм: {e}")
            raise
    
    def _cleanup_user_counters(self, current_time: float):
        try:
            max_users = int(os.getenv('MAX_USER_COUNTERS', 10000))
            cleanup_hours = int(os.getenv('USER_COUNTERS_CLEANUP_HOURS', 1))
            cleanup_threshold = current_time - (cleanup_hours * 3600)
            rate_window_seconds = int(os.getenv('USER_FORM_RATE_WINDOW_SECONDS', 3600))
            
            for user, timestamps in list(self._user_form_timestamps.items()):
                while timestamps and current_time - timestamps[0] >= rate_window_seconds:
                    timestamps.popleft()
                if timestamps:
                    self._user_form_counts[user] = len(timestamps)
                else:
                    self._user_form_timestamps.pop(user, None)
                    self._user_form_counts.pop(user, None)
                    last_time = self._user_last_form_time.get(user)
                    if last_time is None or last_time < cleanup_threshold:
                        self._user_last_form_time.pop(user, None)
            
            if len(self._user_last_form_time) > max_users:
                oldest_users = sorted(
                    self._user_last_form_time.items(),
                    key=lambda x: x[1]
                )[:len(self._user_last_form_time) - max_users + 1000]
                for user, _ in oldest_users:
                    self._user_last_form_time.pop(user, None)
                    self._user_form_counts.pop(user, None)
                    self._user_form_timestamps.pop(user, None)
                self.logger.warning(f"Принудительная очистка счетчиков: удалено {len(oldest_users)} пользователей")
                
        except Exception as e:
            self.logger.error(f"Ошибка при очистке счетчиков пользователей: {e}")
    
    def _get_recent_transaction_amounts(self, currency: str) -> List[float]:
        try:
            current_time = datetime.now().timestamp()
            active_forms = self.db.get_active_payment_forms(current_time)
            recent_txs = self.db.get_pending_transactions()
            
            amounts = []
            
            for form in active_forms:
                if form['currency'] == currency:
                    amounts.append(form['amount'])
            
            for tx in recent_txs:
                if tx['currency'] == currency:
                    amounts.append(tx['amount'])
                    if len(amounts) >= 20:
                        break
            
            return amounts
        except Exception as e:
            self.logger.error(f"Ошибка при получении сумм активных форм и транзакций: {e}")
            return []
    
    def _get_blockchain_transaction_amounts(self, currency: str, hours_back: int = 1) -> List[float]:
        cache_key = f"blockchain_amounts_{currency}_{hours_back}"
        current_time = time.time()
        
        with self._api_cache_lock:
            if cache_key in self._api_cache:
                cached_data, cache_time = self._api_cache[cache_key]
                if current_time - cache_time < self._api_cache_ttl:
                    return cached_data
                else:
                    del self._api_cache[cache_key]
        
        try:
            since_timestamp = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
            
            blockchain_txs = self.tronscan.get_account_transactions(
                self.wallet_address, 
                limit=20,
                start=0
            )
            
            amounts = []
            for tx in blockchain_txs:
                tx_timestamp = tx.get('timestamp', 0)
                if tx_timestamp < since_timestamp:
                    break
                    
                parsed_tx = self.tronscan.parse_transaction(tx)
                if parsed_tx and parsed_tx['currency'] == currency:
                    if parsed_tx['to_address'].lower() == self.wallet_address.lower():
                        amounts.append(parsed_tx['amount'])
                        if len(amounts) >= 20:
                            break
            
            with self._api_cache_lock:
                self._api_cache[cache_key] = (amounts, current_time)
                
                max_api_cache_size = int(os.getenv('MAX_API_CACHE_SIZE', 100))
                if len(self._api_cache) > max_api_cache_size:
                    sorted_items = sorted(
                        self._api_cache.items(),
                        key=lambda x: x[1][1]
                    )
                    items_to_remove = len(self._api_cache) - max_api_cache_size + 20
                    for key, _ in sorted_items[:items_to_remove]:
                        del self._api_cache[key]
            
            self.logger.debug(f"Получено {len(amounts)} сумм из блокчейна за {hours_back} часов")
            return amounts
            
        except Exception as e:
            self.logger.error(f"Ошибка при получении сумм из блокчейна: {e}")
            return []
    
    def _generate_unique_amount(self, base_amount: float, currency: str, max_attempts: int = 100, 
                              max_total_amount: float = None) -> float:
        recent_amounts = self._get_recent_transaction_amounts(currency)
        hours_back = int(os.getenv('UNIQUE_AMOUNT_CHECK_HOURS', 2))
        blockchain_amounts = self._get_blockchain_transaction_amounts(currency, hours_back=hours_back)
        
        all_amounts = recent_amounts + blockchain_amounts
        recent_amounts_sorted = sorted(set(all_amounts))
        
        for attempt in range(max_attempts):
            random_addition = secrets.randbelow(9999) / 10000.0
            if random_addition < 0.0001:
                random_addition = 0.0001
            
            final_amount = round(base_amount + random_addition, 4)
            if max_total_amount is not None and final_amount > max_total_amount:
                continue
            
            is_unique = True
            for recent_amount in recent_amounts_sorted:
                if abs(final_amount - recent_amount) < 0.0001:
                    is_unique = False
                    break
                if recent_amount > final_amount + 0.0001:
                    break
            
            if is_unique:
                self.logger.debug(f"Сгенерирована уникальная сумма: {final_amount} (попытка {attempt + 1})")
                return final_amount
        
        random_suffix = random.uniform(0.0001, 0.9999)
        final_amount = round(base_amount + random_suffix, 4)
        if max_total_amount is not None:
            final_amount = min(final_amount, round(max_total_amount, 4))
        self.logger.warning(f"Использован случайный суффикс для генерации суммы: {final_amount}")
        return final_amount
    
    def _check_recent_transactions(self, amount: float, currency: str) -> bool:
        try:
            local_amounts = self._get_recent_transaction_amounts(currency)
            
            for recent_amount in local_amounts:
                if abs(amount - recent_amount) < 0.01:
                    self.logger.warning(f"Сумма {amount} слишком похожа на локальную транзакцию: {recent_amount}")
                    return False
            
            blockchain_amounts = self._get_blockchain_transaction_amounts(currency)
            
            for blockchain_amount in blockchain_amounts:
                if abs(amount - blockchain_amount) < 0.01:
                    self.logger.warning(f"Сумма {amount} слишком похожа на блокчейн транзакцию: {blockchain_amount}")
                    return False
            
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при проверке последних транзакций: {e}")
            return True
    
    def create_payment_form(self, amount: float, currency: str = "TRX", 
                          description: str = "", expires_hours: int = None, 
                          client_ip: str = None, user_id: str = None) -> Dict:
        
        if not isinstance(amount, (int, float)):
            raise ValueError(f"amount должен быть числом, получен {type(amount)}")
            
        if not isinstance(currency, str):
            raise ValueError(f"currency должен быть строкой, получен {type(currency)}")
            
        if not isinstance(description, str):
            raise ValueError(f"description должен быть строкой, получен {type(description)}")
        
        if expires_hours is None:
            expires_hours = int(os.getenv('MAX_FORM_LIFETIME', 24))
            
        if not isinstance(expires_hours, int):
            raise ValueError(f"expires_hours должен быть целым числом, получен {type(expires_hours)}")
            
        if client_ip is not None and not isinstance(client_ip, str):
            raise ValueError(f"client_ip должен быть строкой или None, получен {type(client_ip)}")
            
        if user_id is not None and not isinstance(user_id, str):
            raise ValueError(f"user_id должен быть строкой или None, получен {type(user_id)}")

        if not self._validate_amount(amount, currency):
            raise ValueError(f"Некорректная сумма: {amount} {currency}")
        
        if not self._validate_description(description):
            raise ValueError("Некорректное описание платежа")
        
        if not self._validate_tron_address(self.wallet_address):
            raise ValueError(f"Некорректный адрес кошелька: {self.wallet_address}")
        
        if currency not in ['TRX', 'USDT']:
            raise ValueError(f"Неподдерживаемая валюта: {currency}")
        
        if expires_hours < 1 or expires_hours > 168:
            raise ValueError(f"Некорректное время истечения: {expires_hours} часов")
        
        self._check_form_creation_limits(client_ip, user_id)
        
        if not self._check_recent_transactions(amount, currency):
            raise Exception("Сумма слишком похожа на недавние транзакции. Попробуйте другую сумму.")
        
        currency_max_limits = {
            'USDT': float(os.getenv('MAX_USDT_AMOUNT', 10000.0)),
            'TRX': float(os.getenv('MAX_TRX_AMOUNT', 100000.0))
        }
        max_total_amount = currency_max_limits.get(currency, float(os.getenv('MAX_AMOUNT_LIMIT', 1e15)))
        
        final_amount = self._generate_unique_amount(amount, currency, max_total_amount=max_total_amount)
        
        if not self._validate_amount(final_amount, currency):
            raise ValueError("Не удалось сгенерировать итоговую сумму в допустимых лимитах")
        
        form_id = str(uuid.uuid4())
        
        success = self.db.create_payment_form(
            form_id=form_id,
            amount=final_amount,
            currency=currency,
            description=description,
            wallet_address=self.wallet_address,
            expires_hours=expires_hours
        )
        
        if success:
            self.logger.info(f"Создана платежная форма {form_id}: {self._mask_amount(final_amount)} {currency}")
            return {
                'form_id': form_id,
                'amount': final_amount,
                'original_amount': amount,
                'currency': currency,
                'description': description,
                'wallet_address': self.wallet_address,
                'expires_at': datetime.now() + timedelta(hours=expires_hours),
                'status': 'pending'
            }
        else:
            raise Exception("Не удалось создать платежную форму")
    
    def get_payment_form(self, form_id: str) -> Optional[Dict]:
        if not self._validate_form_id(form_id):
            return None
            
        with self._form_cache_lock:
            cache_key = f"form_{form_id}"
            current_time = time.time()
            
            if cache_key in self._form_cache:
                cached_data, cache_time = self._form_cache[cache_key]
                if current_time - cache_time < self._cache_expiry:
                    return cached_data
                else:
                    del self._form_cache[cache_key]
            
            form_data = self.db.get_payment_form(form_id)
            if form_data:
                self._form_cache[cache_key] = (form_data, current_time)
            
            return form_data
    
    def generate_payment_url(self, form_id: str) -> str:
        form_data = self.get_payment_form(form_id)
        if not form_data:
            raise ValueError("Платежная форма не найдена")
        
        amount = form_data['amount']
        currency = form_data['currency']
        
        if currency == "TRX":
            return f"tronlink://send?address={self.wallet_address}&amount={amount}"
        elif currency == "USDT":
            return f"tronlink://send?address={self.wallet_address}&amount={amount}&token={self.OFFICIAL_USDT_CONTRACT}"
        else:
            return f"tronlink://send?address={self.wallet_address}&amount={amount}&token={currency}"
    
    def generate_payment_qr_data(self, form_id: str) -> str:
        form_data = self.get_payment_form(form_id)
        if not form_data:
            raise ValueError("Платежная форма не найдена")
        
        amount = form_data['amount']
        currency = form_data['currency']
        
        if currency == "TRX":
            return f"tron:{self.wallet_address}?amount={amount}"
        elif currency == "USDT":
            return f"tron:{self.wallet_address}?amount={amount}&token={self.OFFICIAL_USDT_CONTRACT}"
        else:
            return f"tron:{self.wallet_address}?amount={amount}&token={currency}"
    
    def check_payment_status(self, form_id: str) -> Dict:
        form_data = self.get_payment_form(form_id)
        if not form_data:
            return {'status': 'not_found'}
        
        if datetime.now().timestamp() > float(form_data['expires_at']):
            return {'status': 'expired'}
        
        transactions = self.db.get_transactions_by_form(form_id)
        
        if transactions:
            latest_tx = transactions[0]
            if latest_tx['status'] == 'confirmed':
                return {
                    'status': 'paid',
                    'transaction_id': latest_tx['transaction_id'],
                    'amount': latest_tx['amount'],
                    'currency': latest_tx['currency']
                }
            elif latest_tx['status'] == 'pending':
                return {
                    'status': 'pending',
                    'transaction_id': latest_tx['transaction_id']
                }
        
        return {'status': 'waiting'}
    
    def start_monitoring(self, check_interval: int = 3):
        if self.monitoring:
            return
        
        self.monitoring = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_payments,
            args=(check_interval,),
            daemon=True
        )
        self.monitor_thread.start()
        self.logger.info("Мониторинг платежей запущен")
    
    def stop_monitoring(self):
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        self.logger.info("Мониторинг платежей остановлен")
    
    def _monitor_payments(self, check_interval: int):
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while self.monitoring:
            try:
                pending_forms = self._get_active_payment_forms()
                
                if not pending_forms:
                    self.logger.debug("💤 Нет активных платежных форм для мониторинга")
                    time.sleep(check_interval)
                    continue
                
                self.logger.info(f"🔄 Начат цикл мониторинга: проверяем {len(pending_forms)} активных форм")
                
                monitor_hours = int(os.getenv('MONITOR_TRANSACTION_HOURS', 2))
                since_timestamp = max(self._last_block_timestamp, 
                                    int((datetime.now() - timedelta(hours=monitor_hours)).timestamp() * 1000))
                
                try:
                    recent_txs = self.tronscan.check_recent_transactions(
                        self.wallet_address, 
                        since_timestamp=since_timestamp
                    )
                    
                    new_transactions = self._filter_new_transactions(recent_txs)
                    
                    self.logger.info(f"📥 Получено {len(new_transactions)} новых транзакций из {len(recent_txs)} за {monitor_hours} часов для проверки {len(pending_forms)} форм")
                    
                    if new_transactions:
                        self._update_last_block_timestamp(new_transactions)
                        
                        with ThreadPoolExecutor(max_workers=min(10, len(pending_forms))) as executor:
                            future_to_form = {}
                            
                            for form_data in pending_forms:
                                future = executor.submit(self._check_form_against_transactions_optimized, 
                                                       form_data, new_transactions)
                                future_to_form[future] = form_data
                            
                            for future in as_completed(future_to_form, timeout=30):
                                if not self.monitoring:
                                    break
                                    
                                try:
                                    result = future.result(timeout=5)
                                except Exception as e:
                                    form_data = future_to_form[future]
                                    form_id = form_data['form_id']
                                    self.logger.error(f"Ошибка при проверке формы {form_id}: {e}")
                    
                    self._cleanup_cache()
                                    
                except Exception as e:
                    self.logger.error(f"Ошибка при получении транзакций: {e}")
                    consecutive_errors += 1
                    error_sleep = min(check_interval * consecutive_errors, 300)
                    
                    if consecutive_errors >= max_consecutive_errors:
                        self.logger.critical(
                            f"Превышено максимальное количество последовательных ошибок ({max_consecutive_errors}). Остановка мониторинга."
                        )
                        self.monitoring = False
                        break
                    
                    self.logger.info(f"Пауза {error_sleep} секунд перед повтором мониторинга")
                    time.sleep(error_sleep)
                    continue
                
                consecutive_errors = 0
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                self.logger.info("Мониторинг остановлен пользователем")
                break
            except Exception as e:
                consecutive_errors += 1
                error_sleep = min(check_interval * consecutive_errors, 300)
                
                self.logger.error(f"Критическая ошибка при мониторинге (#{consecutive_errors}): {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    self.logger.critical(f"Превышено максимальное количество последовательных ошибок ({max_consecutive_errors}). Остановка мониторинга.")
                    self.monitoring = False
                    break
                
                self.logger.info(f"Пауза {error_sleep} секунд перед повтором мониторинга")
                time.sleep(error_sleep)
    
    def _filter_new_transactions(self, transactions: List[Dict]) -> List[Dict]:
        with self._transaction_cache_lock:
            new_transactions = []
            seen_in_batch = set()
            for tx in transactions:
                tx_hash = tx.get('hash', '')
                if not tx_hash:
                    continue
                
                if tx_hash in seen_in_batch:
                    continue
                seen_in_batch.add(tx_hash)
                
                if tx_hash in self._processed_transactions:
                    self.logger.debug(f"⏭️ Транзакция уже обработана: {tx_hash[:16]}")
                    continue
                
                new_transactions.append(tx)
                self.logger.debug(f"🆕 Новая транзакция: {tx_hash[:16]}")
            
            if len(self._processed_transactions) > self._max_processed_transactions:
                oldest_txs = list(self._processed_transactions)[:5000]
                for tx_hash in oldest_txs:
                    self._processed_transactions.discard(tx_hash)
                self.logger.info(f"🧹 Очищен кэш: удалено {len(oldest_txs)} старых транзакций")
                        
            return new_transactions
    
    def _cleanup_cache(self):
        with self._form_cache_lock:
            current_time = time.time()
            
            keys_to_remove = [
                key for key, (_, cache_time) in self._form_cache.items()
                if current_time - cache_time > self._cache_expiry
            ]
            
            for key in keys_to_remove:
                del self._form_cache[key]
            
            max_cache_size = int(os.getenv('MAX_FORM_CACHE_SIZE', 1000))
            if len(self._form_cache) > max_cache_size:
                sorted_items = sorted(
                    self._form_cache.items(),
                    key=lambda x: x[1][1]
                )
                items_to_remove = len(self._form_cache) - max_cache_size + 100
                for key, _ in sorted_items[:items_to_remove]:
                    del self._form_cache[key]
    
    def _update_last_block_timestamp(self, transactions: List[Dict]):
        if transactions:
            max_timestamp = max(
                self._normalize_tx_timestamp(tx.get('timestamp', 0))
                for tx in transactions
            )
            if max_timestamp > self._last_block_timestamp:
                self._last_block_timestamp = max_timestamp
    
    def _normalize_tx_timestamp(self, timestamp: Any) -> int:
        try:
            ts = int(timestamp)
            if ts < 1000000000000:
                ts *= 1000
            return ts
        except (TypeError, ValueError):
            return 0
    
    def _get_active_payment_forms(self) -> list:
        try:
            with self._form_cache_lock:
                cache_key = "active_forms"
                current_time = time.time()
                
                if cache_key in self._form_cache:
                    cached_data, cache_time = self._form_cache[cache_key]
                    if current_time - cache_time < 10:
                        return cached_data
                    else:
                        del self._form_cache[cache_key]
                
                current_timestamp = datetime.now().timestamp()
                expired_count = self.db.expire_old_forms(current_timestamp)
                if expired_count > 0:
                    self.logger.info(f"Истекло {expired_count} платежных форм")
                
                active_forms = self.db.get_active_payment_forms(current_timestamp)
                self._form_cache[cache_key] = (active_forms, current_time)
                
                return active_forms
        except Exception as e:
            self.logger.error(f"Ошибка при получении активных форм: {e}")
            return []
    
    def _check_form_against_transactions_optimized(self, form_data: Dict, transactions: List[Dict]) -> bool:
        form_id = form_data['form_id']
        form_amount = form_data['amount']
        form_currency = form_data['currency']
        wallet_address_lower = self.wallet_address.lower()
        
        self.logger.debug(f"🔍 Проверка формы {form_id} (сумма: {form_amount} {form_currency}) против {len(transactions)} транзакций")
        
        for tx in transactions:
            if not self.monitoring:
                return False
                
            try:
                tx_hash = tx.get('hash', '')
                
                with self._transaction_cache_lock:
                    if tx_hash in self._processing_transactions:
                        continue
                    
                    if self.db.get_transaction_by_id(tx_hash):
                        self._processed_transactions.add(tx_hash)
                        continue
                
                parsed_tx = self._parse_transaction_fast(tx)
                if not parsed_tx:
                    self.logger.debug(f"❌ Не удалось распарсить транзакцию {tx_hash[:16]}")
                    continue
                
                amount_diff = abs(parsed_tx['amount'] - form_amount)
                currency_match = parsed_tx['currency'] == form_currency
                address_match = parsed_tx['to_address'].lower() == wallet_address_lower
                confirmed = parsed_tx.get('confirmed', False)
                
                self.logger.debug(f"🔍 Проверка транзакции {tx_hash[:16]} для формы {form_id}:")
                self.logger.debug(f"  Сумма: {parsed_tx['amount']} vs {form_amount} (разница: {amount_diff})")
                self.logger.debug(f"  Валюта: {parsed_tx['currency']} vs {form_currency} (совпадает: {currency_match})")
                self.logger.debug(f"  Адрес: {parsed_tx['to_address'][:10]}... vs {wallet_address_lower[:10]}... (совпадает: {address_match})")
                self.logger.debug(f"  Подтверждена: {confirmed}")
                
                if (amount_diff < 0.0001 and currency_match and address_match and confirmed):
                    if self._validate_transaction_fast(parsed_tx):
                        self.logger.info(f"✅ Найден подходящий платеж для формы {form_id}!")
                        self._process_payment(parsed_tx, form_id)
                        return True
                    else:
                        self.logger.warning(f"❌ Транзакция {tx_hash[:16]} не прошла валидацию для формы {form_id}")
                else:
                    self.logger.debug(f"❌ Транзакция {tx_hash[:16]} не подходит для формы {form_id}")
                    if not confirmed:
                        self.logger.debug(f"   Причина: транзакция не подтверждена")
                    if amount_diff >= 0.0001:
                        self.logger.debug(f"   Причина: разница в суммах {amount_diff}")
                    if not currency_match:
                        self.logger.debug(f"   Причина: не совпадает валюта")
                    if not address_match:
                        self.logger.debug(f"   Причина: не совпадает адрес")
                    
            except Exception as e:
                tx_hash = tx.get('hash', 'unknown')
                self.logger.error(f"Ошибка при обработке транзакции {tx_hash[:16]} для формы {form_id}: {e}")
                continue
                
        return False
    
    def _parse_transaction_fast(self, tx_data: Dict) -> Optional[Dict]:
        try:
            tx_id = tx_data.get('hash', '')
            timestamp = tx_data.get('timestamp', 0)
            
            if 'trc20_transfer' in tx_data:
                transfer = tx_data['trc20_transfer']
                amount_str = transfer.get('quant', '0')
                from_addr = transfer.get('from_address', '')
                to_addr = transfer.get('to_address', '')
                
                token_info = transfer.get('tokenInfo', {})
                symbol = token_info.get('tokenAbbr', 'UNKNOWN')
                raw_decimals = token_info.get('tokenDecimal', 6)
                
                try:
                    decimals = int(raw_decimals)
                    if decimals < 0 or decimals > 30:
                        return None
                    amount = float(amount_str)
                    if decimals > 0:
                        amount = amount / (10 ** decimals)
                except (ValueError, TypeError, OverflowError):
                    return None
                
                contract_address = (
                    transfer.get('contract_address')
                    or token_info.get('tokenId')
                    or token_info.get('token_id')
                    or ''
                )
                
                return {
                    'transaction_id': tx_id,
                    'from_address': from_addr,
                    'to_address': to_addr,
                    'amount': amount,
                    'currency': symbol,
                    'timestamp': timestamp * 1000 if timestamp < 1000000000000 else timestamp,
                    'confirmed': tx_data.get('confirmed', True),
                    'trc20_transfer': {
                        'contract_address': contract_address,
                        'tokenInfo': token_info
                    }
                }
            
            return None
        except Exception:
            return None
    
    def _validate_transaction_fast(self, transaction: Dict) -> bool:
        from_address = transaction.get('from_address', '')
        if not self._validate_sender_address(from_address):
            return False
        
        if not self._validate_transaction_timestamp(transaction):
            return False
        
        if not self._validate_transaction_confirmations(transaction):
            return False
            
        if transaction.get('currency') == 'USDT':
            return self._validate_usdt_contract(transaction)
        
        return True


    def _is_payment_for_form(self, transaction: Dict, form_data: Dict) -> bool:
        tx_id = transaction['transaction_id']
        form_id = form_data['form_id']
        
        self.logger.debug(f"🔍 Проверка соответствия транзакции {tx_id[:16]} форме {form_id[:8]}")
        
        existing_tx = self.db.get_transaction_by_id(transaction['transaction_id'])
        if existing_tx:
            self.logger.debug(f"🔄 Транзакция {tx_id[:16]} уже обработана ранее")
            return False
        
        from_address = transaction.get('from_address', '')
        if not self._validate_sender_address(from_address):
            self.logger.debug(f"🚫 Невалидный отправитель: {self._mask_wallet_address(from_address)}")
            return False
        
        if not self._validate_transaction_timestamp(transaction):
            self.logger.debug(f"⏰ Транзакция {tx_id[:16]} не прошла проверку времени")
            return False
        
        if not self._validate_transaction_confirmations(transaction):
            self.logger.debug(f"📋 Транзакция {tx_id[:16]} недостаточно подтверждений")
            return False
        
        if not self._validate_usdt_contract(transaction):
            self.logger.debug(f"📄 Транзакция {tx_id[:16]} неверный USDT контракт")
            return False
        
        tx_amount = transaction['amount']
        form_amount = form_data['amount'] 
        amount_match = abs(tx_amount - form_amount) < 0.0001
        
        tx_currency = transaction['currency']
        form_currency = form_data['currency']
        currency_match = tx_currency == form_currency
        
        tx_to_address = transaction.get('to_address', '').lower()
        wallet_address = self.wallet_address.lower()
        address_match = tx_to_address == wallet_address
        
        is_confirmed = transaction.get('confirmed', False)
        
        self.logger.debug(f"📊 Проверка соответствия для {tx_id[:16]}:")
        self.logger.debug(f"   💰 Сумма: {tx_amount} vs {form_amount} = {'✅' if amount_match else '❌'}")
        self.logger.debug(f"   💱 Валюта: {tx_currency} vs {form_currency} = {'✅' if currency_match else '❌'}")
        self.logger.debug(f"   📍 Адрес: {self._mask_wallet_address(tx_to_address)} vs {self._mask_wallet_address(wallet_address)} = {'✅' if address_match else '❌'}")
        self.logger.debug(f"   ✔️  Подтверждена: {'✅' if is_confirmed else '❌'}")
        
        if amount_match and currency_match and address_match and is_confirmed:
            self.logger.info(f"🎉 НАЙДЕН ПОДХОДЯЩИЙ ПЛАТЕЖ! Транзакция {tx_id[:16]} → Форма {form_id[:8]}")
            return True
        
        reasons = []
        if not amount_match:
            reasons.append(f"сумма ({tx_amount} ≠ {form_amount})")
        if not currency_match:
            reasons.append(f"валюта ({tx_currency} ≠ {form_currency})")
        if not address_match:
            reasons.append(f"адрес ({self._mask_wallet_address(tx_to_address)} ≠ {self._mask_wallet_address(wallet_address)})")
        if not is_confirmed:
            reasons.append("не подтверждена")
        
        self.logger.debug(f"❌ Транзакция {tx_id[:16]} отклонена: {', '.join(reasons)}")
        
        return False
    
    def _process_payment(self, transaction: Dict, form_id: str):
        tx_id = transaction['transaction_id']
        
        with self._transaction_processing_lock:
            if tx_id in self._processing_transactions:
                self.logger.debug(f"Транзакция {tx_id[:16]} уже обрабатывается")
                return
            
            self._processing_transactions.add(tx_id)
            
            existing_tx = self.db.get_transaction_by_id(tx_id)
            if existing_tx:
                self.logger.debug(f"Транзакция {tx_id[:16]} уже существует в БД")
                self._processing_transactions.discard(tx_id)
                return
        
        try:
            result = self.db.process_payment_atomic(
                transaction_id=tx_id,
                from_address=transaction['from_address'],
                to_address=transaction['to_address'],
                amount=transaction['amount'],
                currency=transaction['currency'],
                form_id=form_id
            )
            
            if result['status'] == 'success':
                self.logger.info(f"Успешно обработан платеж для формы {form_id}: {self._mask_amount(transaction['amount'])} {transaction['currency']}")
                
                if form_id in self.payment_callbacks:
                    try:
                        self.payment_callbacks[form_id](transaction, form_id)
                    except Exception as e:
                        self.logger.error(f"Ошибка в callback для формы {form_id}: {e}")
            else:
                self.logger.warning(f"Не удалось обработать платеж для формы {form_id}: {result['message']}")
                
        except Exception as e:
            self.logger.error(f"Критическая ошибка при обработке платежа {tx_id[:16]}: {e}")
        finally:
            with self._transaction_processing_lock:
                self._processing_transactions.discard(tx_id)
    
    def register_payment_callback(self, form_id: str, callback: Callable):
        self.payment_callbacks[form_id] = callback
    
    def unregister_payment_callback(self, form_id: str):
        if form_id in self.payment_callbacks:
            del self.payment_callbacks[form_id]
    
    def get_transaction_history(self, form_id: str = None) -> list:
        if form_id:
            return self.db.get_transactions_by_form(form_id)
        else:
            return self.db.get_pending_transactions()
    
    def _validate_ip_address(self, ip: str) -> bool:
        try:
            ipaddress.ip_address(ip)
            return True
        except ValueError:
            return False
    
    def _validate_telegram_user_id(self, user_id: str) -> bool:
        if not user_id or not isinstance(user_id, str):
            return False
        
        if not user_id.isdigit():
            return False
        
        try:
            user_id_int = int(user_id)
            if user_id_int <= 0 or user_id_int > 2**63 - 1:
                return False
        except ValueError:
            return False
        
        return True
    
    def _validate_form_id(self, form_id: str) -> bool:
        if not form_id or not isinstance(form_id, str):
            return False
        
        if len(form_id) != 36:
            return False
        
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        if not re.match(uuid_pattern, form_id, re.IGNORECASE):
            return False
        
        return True
