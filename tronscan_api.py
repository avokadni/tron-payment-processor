import requests
import time
import logging
from urllib.parse import urlparse
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from threading import Lock
import urllib3
import os
from decimal import Decimal, InvalidOperation

class TronScanAPI:
    MAX_TOKEN_DECIMALS = 30
    TRX_DECIMALS = 6

    def __init__(self, api_url: str = "https://apilist.tronscanapi.com/api",
                 requests_per_minute: Optional[int] = None):
        
        self.logger = logging.getLogger(__name__)
        
        self._validate_api_url(api_url)
        self.api_url = api_url
        
        self.session = requests.Session()
        self.verify_ssl = os.getenv('TRONSCAN_VERIFY_SSL', 'true').strip().lower() not in {'0', 'false', 'no'}
        
        self.session.verify = self.verify_ssl
        if not self.verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.logger.warning("SSL проверка отключена через TRONSCAN_VERIFY_SSL")
        
        adapter = requests.adapters.HTTPAdapter()
        self.session.mount('https://', adapter)
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Connection': 'keep-alive'
        })
        
        self.requests_per_minute = requests_per_minute or int(os.getenv('API_REQUESTS_PER_MINUTE', 20))
        self.request_times: List[float] = []
        self.rate_limit_lock = Lock()
        self.min_request_interval = 60.0 / self.requests_per_minute
        self.last_429_time: float = 0.0
        self.backoff_multiplier = 1
        self._response_cache: Dict[str, tuple[List[Dict[str, Any]], float]] = {}
        self._cache_lock = Lock()
        self._cache_ttl = int(os.getenv('API_CACHE_TTL_SECONDS', 30))

    def _to_decimal(self, value: Any) -> Optional[Decimal]:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, bool):
            return None
        try:
            amount = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None
        if not amount.is_finite():
            return None
        return amount

    def _parse_token_decimals(self, value: Any, default: int = 6) -> Optional[int]:
        try:
            decimals = int(value)
        except (ValueError, TypeError):
            decimals = default
        if decimals < 0 or decimals > self.MAX_TOKEN_DECIMALS:
            return None
        return decimals
    
    def _validate_api_url(self, url: str) -> bool:
        allowed_domains = [
            'apilist.tronscanapi.com',
            'api.trongrid.io',
            'api.tronscan.org',
            'nile.trongrid.io'
        ]
        
        try:
            parsed = urlparse(url)
            
            if parsed.scheme != 'https':
                raise ValueError(f"API URL должен использовать HTTPS, получен: {parsed.scheme}")
            
            if parsed.hostname not in allowed_domains:
                raise ValueError(f"Недопустимый API домен: {parsed.hostname}. Разрешенные: {allowed_domains}")
            
            if parsed.query:
                self.logger.warning(f"API URL содержит параметры запроса: {parsed.query}")
            
            if parsed.port and parsed.port not in [443]:
                raise ValueError(f"Подозрительный порт в API URL: {parsed.port}")
            
            self.logger.info(f"API URL прошел валидацию: {parsed.hostname}")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка валидации API URL: {e}")
            raise ValueError(f"Некорректный API URL: {e}")
    
    def _wait_for_rate_limit(self):
        with self.rate_limit_lock:
            current_time = time.time()
            
            if self.last_429_time > 0:
                time_since_429 = current_time - self.last_429_time
                backoff_delay = self.backoff_multiplier * 30
                
                if time_since_429 < backoff_delay:
                    sleep_time = backoff_delay - time_since_429
                    self.logger.warning(f"Exponential backoff: ожидание {sleep_time:.1f} секунд после 429 ошибки")
                    time.sleep(sleep_time)
                    current_time = time.time()
            
            self.request_times = [t for t in self.request_times if current_time - t < 60]
            
            if len(self.request_times) >= self.requests_per_minute:
                sleep_time = 60 - (current_time - self.request_times[0])
                if sleep_time > 0:
                    self.logger.info(f"Rate limit: ожидание {sleep_time:.1f} секунд")
                    time.sleep(sleep_time)
                    current_time = time.time()
                    self.request_times = [t for t in self.request_times if current_time - t < 60]
            
            if self.request_times:
                time_since_last = current_time - self.request_times[-1]
                min_interval = max(3.0, self.min_request_interval)
                
                if time_since_last < min_interval:
                    sleep_time = min_interval - time_since_last
                    time.sleep(sleep_time)
                    current_time = time.time()
            
            self.request_times.append(current_time)
    
    def _validate_ssl_certificate(self, hostname: str) -> bool:
        return True
    
    def _make_request(self, url: str, params: Optional[Dict[str, Any]] = None,
                      timeout: int = 5, max_retries: int = 3) -> requests.Response:
        if not url.startswith(self.api_url):
            raise ValueError(f"Подозрительный URL запроса: {url}")
        
        last_exception: Optional[requests.exceptions.RequestException] = None
        
        for attempt in range(max_retries):
            try:
                self._wait_for_rate_limit()
                
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=timeout,
                    verify=self.verify_ssl,
                    allow_redirects=False
                )
                
                if response.status_code == 429:
                    self.last_429_time = time.time()
                    self.backoff_multiplier = min(self.backoff_multiplier * 2, 8)
                    
                    if attempt < max_retries - 1:
                        retry_after = int(response.headers.get('Retry-After', 60))
                        self.logger.warning(f"Получена 429 ошибка, повтор через {retry_after} секунд (попытка {attempt + 1}/{max_retries})")
                        time.sleep(retry_after)
                        continue
                    else:
                        raise requests.exceptions.RequestException(f"Rate limit exceeded after {max_retries} attempts")
                else:
                    if response.status_code == 200:
                        self.backoff_multiplier = 1
                        self.last_429_time = 0
                    elif response.status_code >= 500:
                        if attempt < max_retries - 1:
                            self.logger.warning(f"Server error {response.status_code}, повтор через 10 секунд (попытка {attempt + 1}/{max_retries})")
                            time.sleep(10)
                            continue
                        else:
                            raise requests.exceptions.RequestException(f"Server error {response.status_code}")
                    elif response.status_code >= 400:
                        raise requests.exceptions.RequestException(f"Client error {response.status_code}")
                
                return response
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                if attempt < max_retries - 1:
                    self.logger.warning(f"Timeout при запросе к API, повтор через 5 секунд (попытка {attempt + 1}/{max_retries})")
                    time.sleep(5)
                    continue
            except requests.exceptions.SSLError as e:
                last_exception = e
                self.logger.error(f"SSL ошибка при запросе к API: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                if attempt < max_retries - 1:
                    self.logger.warning(f"Connection error, повтор через 10 секунд (попытка {attempt + 1}/{max_retries})")
                    time.sleep(10)
                    continue
            except requests.exceptions.RequestException as e:
                last_exception = e
                if attempt < max_retries - 1:
                    self.logger.warning(f"Ошибка запроса к API: {e}, повтор через 10 секунд (попытка {attempt + 1}/{max_retries})")
                    time.sleep(10)
                    continue
        
        if last_exception:
            raise last_exception
        else:
            raise requests.exceptions.RequestException(f"Не удалось выполнить запрос после {max_retries} попыток")
    
    def _validate_api_response(self, response_data: Dict[str, Any], expected_fields: Optional[List[str]] = None) -> bool:
        if not isinstance(response_data, dict):
            self.logger.error("API ответ не является словарем")
            return False
        
        if expected_fields:
            for field in expected_fields:
                if field not in response_data:
                    self.logger.warning(f"Отсутствует ожидаемое поле в ответе API: {field}")
        
        suspicious_fields = ['__proto__', 'constructor', 'prototype', 'eval', 'function']
        for field in suspicious_fields:
            if field in response_data:
                self.logger.error(f"Обнаружено подозрительное поле в ответе API: {field}")
                return False
        
        return True
    
    def _validate_transaction_data(self, tx_data: dict) -> bool:
        required_fields = ['hash', 'timestamp']
        
        for field in required_fields:
            if field not in tx_data:
                self.logger.error(f"Отсутствует обязательное поле транзакции: {field}")
                return False
        
        tx_hash = tx_data.get('hash', '')
        if not isinstance(tx_hash, str) or len(tx_hash) != 64:
            self.logger.error(f"Некорректный формат hash транзакции: {tx_hash}")
            return False
        
        try:
            int(tx_hash, 16)
        except ValueError:
            self.logger.error(f"Hash транзакции содержит недопустимые символы: {tx_hash}")
            return False
        
        timestamp = tx_data.get('timestamp', 0)
        current_time = int(datetime.now().timestamp() * 1000)
        
        max_age_hours = int(os.getenv('MONITOR_TRANSACTION_HOURS', 2))
        max_age = max_age_hours * 60 * 60 * 1000
        
        if timestamp < current_time - max_age:
            self.logger.error(f"Транзакция слишком старая: {timestamp}")
            return False
        
        future_tolerance_hours = int(os.getenv('FUTURE_TOLERANCE_HOURS', 2))
        future_tolerance = future_tolerance_hours * 60 * 60 * 1000
        
        if timestamp > current_time + future_tolerance:
            self.logger.error(f"Транзакция из будущего: {timestamp}")
            return False
        
        return True
    
    def get_account_transactions(self, address: str, limit: int = 20, start: int = 0) -> List[Dict]:
        cache_key = f"tx_{address}_{limit}_{start}"
        
        with self._cache_lock:
            if cache_key in self._response_cache:
                cached_data, cache_time = self._response_cache[cache_key]
                if time.time() - cache_time < self._cache_ttl:
                    return cached_data
                else:
                    del self._response_cache[cache_key]
        
        try:
            url = f"{self.api_url}/transaction"
            params = {
                'address': address,
                'limit': min(limit, 50),
                'start': max(start, 0),
                'sort': '-timestamp'
            }
            
            response = self._make_request(url, params=params, timeout=5)
            
            try:
                data = response.json()
            except ValueError as e:
                self.logger.error(f"Некорректный JSON в ответе API: {e}")
                return []
            
            if not self._validate_api_response(data, ['data']):
                return []
            
            transactions = data.get('data', [])
            
            validated_transactions = []
            for tx in transactions:
                if self._validate_transaction_data(tx):
                    validated_transactions.append(tx)
                else:
                    self.logger.warning(f"Отклонена невалидная транзакция: {tx.get('hash', 'unknown')}")
            
            with self._cache_lock:
                self._response_cache[cache_key] = (validated_transactions, time.time())
                
                if len(self._response_cache) > 100:
                    sorted_items = sorted(self._response_cache.items(), 
                                        key=lambda x: x[1][1])
                    items_to_remove = len(self._response_cache) - 50
                    for key, _ in sorted_items[:items_to_remove]:
                        del self._response_cache[key]
            
            self.logger.info(f"Получено {len(validated_transactions)} валидных транзакций из {len(transactions)}")
            return validated_transactions
            
        except requests.RequestException as e:
            self.logger.error(f"Ошибка при получении транзакций: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при получении транзакций: {e}")
            return []
    
    def get_trc20_transfers(self, address: str, limit: int = 20, start: int = 0) -> List[Dict]:
        cache_key = f"trc20_{address}_{limit}_{start}"
        
        with self._cache_lock:
            if cache_key in self._response_cache:
                cached_data, cache_time = self._response_cache[cache_key]
                if time.time() - cache_time < self._cache_ttl:
                    return cached_data
                else:
                    del self._response_cache[cache_key]
        
        try:
            url = f"{self.api_url}/token_trc20/transfers"
            params = {
                'relatedAddress': address,
                'limit': min(limit, 50),
                'start': max(start, 0),
                'sort': '-timestamp'
            }
            
            response = self._make_request(url, params=params, timeout=5)
            
            try:
                data = response.json()
            except ValueError as e:
                self.logger.error(f"Некорректный JSON в ответе TRC20 API: {e}")
                return []
            
            if 'token_transfers' in data:
                transfers = data.get('token_transfers', [])
            elif 'data' in data:
                transfers = data.get('data', [])
            else:
                transfers = data if isinstance(data, list) else []
            
            trc20_transactions = []
            for transfer in transfers:
                try:
                    tx = {
                        'hash': transfer.get('transaction_id', ''),
                        'timestamp': transfer.get('block_ts', 0),
                        'confirmed': True,
                        'contractType': 31,
                        'trc20_transfer': transfer
                    }
                    trc20_transactions.append(tx)
                except Exception as e:
                    self.logger.warning(f"Ошибка при обработке TRC20 перевода: {e}")
                    continue
            
            with self._cache_lock:
                self._response_cache[cache_key] = (trc20_transactions, time.time())
            
            self.logger.info(f"Получено {len(trc20_transactions)} TRC20 переводов")
            return trc20_transactions
            
        except requests.RequestException as e:
            self.logger.error(f"Ошибка при получении TRC20 переводов: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при получении TRC20 переводов: {e}")
            return []
    
    def get_transaction_details(self, transaction_id: str) -> Optional[Dict]:
        try:
            url = f"{self.api_url}/transaction-info"
            params = {'hash': transaction_id}
            
            response = self._make_request(url, params=params, timeout=5)
            
            try:
                data = response.json()
                return data
            except ValueError as e:
                self.logger.error(f"Некорректный JSON в ответе API для транзакции {transaction_id}: {e}")
                return None
        except requests.RequestException as e:
            self.logger.error(f"Ошибка при получении деталей транзакции {transaction_id}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при получении деталей транзакции {transaction_id}: {e}")
            return None
    
    def get_account_info(self, address: str) -> Optional[Dict]:
        try:
            url = f"{self.api_url}/account"
            params = {'address': address}
            
            response = self._make_request(url, params=params, timeout=5)
            
            try:
                data = response.json()
                return data
            except ValueError as e:
                self.logger.error(f"Некорректный JSON в ответе API для аккаунта {address}: {e}")
                return None
        except requests.RequestException as e:
            self.logger.error(f"Ошибка при получении информации об аккаунте {address}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при получении информации об аккаунте {address}: {e}")
            return None
    
    def check_recent_transactions(self, wallet_address: str, since_timestamp: Optional[int] = None) -> List[Dict]:
        if since_timestamp is None:
            since_timestamp = int((datetime.now() - timedelta(hours=2)).timestamp() * 1000)
        
        try:
            transactions = self.get_account_transactions(wallet_address, limit=50)
            trc20_transfers = self.get_trc20_transfers(wallet_address, limit=50)
            all_transactions = transactions + trc20_transfers
            
            recent_transactions = []
            for tx in all_transactions:
                tx_timestamp = tx.get('timestamp', 0)
                if tx_timestamp < 1000000000000:
                    tx_timestamp = tx_timestamp * 1000
                    
                if tx_timestamp >= since_timestamp:
                    tx['timestamp'] = tx_timestamp
                    recent_transactions.append(tx)
            recent_transactions.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
            
            self.logger.info(f"Найдено {len(recent_transactions)} транзакций за 2 часа (TRX: {len(transactions)}, TRC20: {len(trc20_transfers)})")
            return recent_transactions
        except Exception as e:
            self.logger.error(f"Ошибка при проверке последних транзакций: {e}")
            return []
    
    def is_transaction_confirmed(self, transaction_id: str) -> bool:
        try:
            tx_details = self.get_transaction_details(transaction_id)
            if tx_details:
                return tx_details.get('confirmed', False)
            return False
        except Exception as e:
            self.logger.error(f"Ошибка при проверке подтверждения транзакции {transaction_id}: {e}")
            return False
    
    def parse_transaction(self, tx_data: Dict) -> Optional[Dict]:
        try:
            tx_id = tx_data.get('hash', '')
            timestamp = tx_data.get('timestamp', 0)
            
            self.logger.debug(f"🔄 Парсинг транзакции {tx_id[:16]}...")
            
            if 'trc20_transfer' in tx_data:
                transfer = tx_data['trc20_transfer']
                amount_str = transfer.get('quant', '0')
                from_addr = transfer.get('from_address', '')
                to_addr = transfer.get('to_address', '')
                
                token_info = transfer.get('tokenInfo', {})
                symbol = token_info.get('tokenAbbr', 'UNKNOWN')
                decimals = self._parse_token_decimals(token_info.get('tokenDecimal', 6), default=6)
                raw_amount = self._to_decimal(amount_str)
                if decimals is None or raw_amount is None:
                    return None
                amount = raw_amount / (Decimal(10) ** decimals) if decimals > 0 else raw_amount
                
                self.logger.debug(f"🪙 TRC20 перевод: {amount} {symbol} от {from_addr[:8]}...{from_addr[-4:]} к {to_addr[:8]}...{to_addr[-4:]}")
                
                return {
                    'transaction_id': tx_id,
                    'from_address': from_addr,
                    'to_address': to_addr,
                    'amount': amount,
                    'currency': symbol,
                    'timestamp': timestamp * 1000 if timestamp < 1000000000000 else timestamp,
                    'confirmed': tx_data.get('confirmed', True)
                }
            
            tx_details = self.get_transaction_details(tx_id)
            if not tx_details:
                self.logger.debug(f"❌ Не удалось получить детали транзакции {tx_id[:16]}")
                return None
            
            transfers = tx_details.get('trc20TransferInfo', [])
            confirmed = tx_details.get('confirmed', False)
            
            self.logger.debug(f"📋 Транзакция {tx_id[:16]}: подтверждена={confirmed}, TRC20 переводов={len(transfers)}")
            
            if not transfers:
                contract_data = tx_details.get('contractData', {})
                if contract_data:
                    raw_amount = self._to_decimal(contract_data.get('amount', 0))
                    if raw_amount is None:
                        return None
                    amount = raw_amount / (Decimal(10) ** self.TRX_DECIMALS)
                    from_addr = contract_data.get('owner_address', '')
                    to_addr = contract_data.get('to_address', '')
                    
                    self.logger.debug(f"💎 TRX перевод: {amount} TRX от {from_addr[:8]}...{from_addr[-4:]} к {to_addr[:8]}...{to_addr[-4:]}")
                    
                    return {
                        'transaction_id': tx_id,
                        'from_address': from_addr,
                        'to_address': to_addr,
                        'amount': amount,
                        'currency': 'TRX',
                        'timestamp': timestamp,
                        'confirmed': confirmed
                    }
            else:
                for i, transfer in enumerate(transfers):
                    raw_amount = self._to_decimal(transfer.get('amount_str', 0))
                    from_addr = transfer.get('from_address', '')
                    to_addr = transfer.get('to_address', '')
                    token_info = transfer.get('tokenInfo', {})
                    symbol = token_info.get('symbol', 'UNKNOWN')
                    decimals = self._parse_token_decimals(token_info.get('decimals', 6), default=6)
                    
                    if raw_amount is None or decimals is None:
                        continue
                    amount = raw_amount / (Decimal(10) ** decimals) if decimals > 0 else raw_amount
                    
                    self.logger.debug(f"🪙 TRC20 перевод #{i+1}: {amount} {symbol} от {from_addr[:8]}...{from_addr[-4:]} к {to_addr[:8]}...{to_addr[-4:]}")
                    
                    return {
                        'transaction_id': tx_id,
                        'from_address': from_addr,
                        'to_address': to_addr,
                        'amount': amount,
                        'currency': symbol,
                        'timestamp': timestamp,
                        'confirmed': confirmed
                    }
            
            self.logger.debug(f"⚠️  Транзакция {tx_id[:16]} не содержит переводов")
            return None
        except Exception as e:
            self.logger.error(f"💥 Ошибка при парсинге транзакции {tx_id[:16] if 'tx_id' in locals() else 'unknown'}: {e}")
            return None
    
    def monitor_payments(self, wallet_address: str, callback_func, check_interval: int = 30):
        last_check_time = int(datetime.now().timestamp() * 1000)
        
        while True:
            try:
                recent_txs = self.check_recent_transactions(wallet_address, last_check_time)
                
                for tx in recent_txs:
                    parsed_tx = self.parse_transaction(tx)
                    if parsed_tx and parsed_tx['to_address'].lower() == wallet_address.lower():
                        callback_func(parsed_tx)
                
                last_check_time = int(datetime.now().timestamp() * 1000)
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                self.logger.info("Мониторинг остановлен пользователем")
                break
            except Exception as e:
                self.logger.error(f"Ошибка при мониторинге: {e}")
                time.sleep(check_interval)
