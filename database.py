import sqlite3
import os
import threading
import logging
import queue
import contextlib
import time
from datetime import datetime
from typing import List, Dict, Optional

class DatabaseManager:
    def __init__(self, db_path: str = "transaction.db", pool_size: int = None):
        self.db_path = db_path
        self._lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        
        self.pool_size = pool_size or int(os.getenv('DB_POOL_SIZE', 5))
        self.connection_pool = queue.Queue(maxsize=self.pool_size)
        self.pool_lock = threading.Lock()
        
        self.init_database()
        self._init_connection_pool()
    
    def _init_connection_pool(self):
        for _ in range(self.pool_size):
            conn = self._create_connection()
            self.connection_pool.put(conn)
    
    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=float(os.getenv('DB_CONNECTION_TIMEOUT', 30.0)),
            isolation_level=None,
            check_same_thread=False
        )
        
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute(f'PRAGMA cache_size={os.getenv("DB_CACHE_SIZE", 10000)}')
        conn.execute('PRAGMA temp_store=MEMORY')
        conn.execute(f'PRAGMA mmap_size={os.getenv("DB_MMAP_SIZE", 268435456)}')
        
        return conn
    
    @contextlib.contextmanager
    def get_connection(self):
        conn = None
        from_pool = False
        try:
            conn = self.connection_pool.get(timeout=float(os.getenv('DB_POOL_TIMEOUT', 10.0)))
            from_pool = True
            
            try:
                conn.execute('SELECT 1')
            except sqlite3.Error:
                conn.close()
                conn = self._create_connection()
                from_pool = True
            
            yield conn
            
        except queue.Empty:
            self.logger.warning("Connection pool exhausted, creating temporary connection")
            conn = self._create_connection()
            from_pool = False
            yield conn
            
        finally:
            if conn:
                if from_pool:
                    try:
                        self.connection_pool.put_nowait(conn)
                    except queue.Full:
                        conn.close()
                else:
                    conn.close()
    
    def init_database(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id TEXT UNIQUE NOT NULL,
                    from_address TEXT NOT NULL,
                    to_address TEXT NOT NULL,
                    amount REAL NOT NULL,
                    currency TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    payment_form_id TEXT,
                    description TEXT
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transaction_id ON transactions(transaction_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_payment_form_id ON transactions(payment_form_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON transactions(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_status_created ON transactions(status, created_at)')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS payment_forms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    form_id TEXT UNIQUE NOT NULL,
                    amount REAL NOT NULL,
                    currency TEXT NOT NULL,
                    description TEXT,
                    status TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    wallet_address TEXT NOT NULL
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_form_id ON payment_forms(form_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_form_status ON payment_forms(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_form_expires ON payment_forms(expires_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_form_status_expires ON payment_forms(status, expires_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_form_created_at ON payment_forms(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_form_status_created ON payment_forms(status, created_at)')
            
            cursor.execute("PRAGMA user_version")
            version_result = cursor.fetchone()
            current_version = version_result[0] if version_result else 0
            
            if current_version < 1:
                try:
                    cursor.execute('ALTER TABLE payment_forms ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
                    cursor.execute("PRAGMA user_version = 1")
                    self.logger.info("Добавлена колонка updated_at в таблицу payment_forms")
                except sqlite3.OperationalError:
                    pass
            
            conn.commit()
    
    def create_payment_form(self, form_id: str, amount: float, currency: str, 
                          description: str, wallet_address: str, expires_hours: int = None) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                default_expires_hours = int(os.getenv('DEFAULT_FORM_EXPIRES_HOURS', 24))
                expires_hours = expires_hours or default_expires_hours
                expires_at = datetime.now().timestamp() + (expires_hours * 3600)
                
                cursor.execute('''
                    INSERT INTO payment_forms 
                    (form_id, amount, currency, description, status, expires_at, wallet_address)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (form_id, amount, currency, description, 'pending', expires_at, wallet_address))
                
                conn.commit()
                return True
        except sqlite3.IntegrityError:
            return False
        except Exception as e:
            self.logger.error(f"Ошибка при создании платежной формы: {e}")
            return False
    
    def get_payment_form(self, form_id: str) -> Optional[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM payment_forms WHERE form_id = ?
            ''', (form_id,))
            
            row = cursor.fetchone()
            
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            
            return None
    
    def process_payment_atomic(self, transaction_id: str, from_address: str, to_address: str,
                              amount: float, currency: str, form_id: str) -> Dict[str, str]:
        max_retries = 3
        for attempt in range(max_retries):
            with self._lock:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    try:
                        cursor.execute('BEGIN IMMEDIATE')
                        
                        cursor.execute('SELECT id FROM transactions WHERE transaction_id = ?', (transaction_id,))
                        if cursor.fetchone():
                            conn.rollback()
                            return {'status': 'error', 'message': 'Transaction already processed'}
                        
                        cursor.execute('''
                            SELECT status, expires_at, amount, currency 
                            FROM payment_forms 
                            WHERE form_id = ? AND status = 'pending'
                        ''', (form_id,))
                        
                        form_row = cursor.fetchone()
                        if not form_row:
                            conn.rollback()
                            return {'status': 'error', 'message': 'Payment form not found or not pending'}
                        
                        form_status, expires_at, expected_amount, expected_currency = form_row
                        
                        if datetime.now().timestamp() > expires_at:
                            conn.rollback()
                            return {'status': 'error', 'message': 'Payment form expired'}
                        
                        if abs(amount - expected_amount) > 0.0001 or currency != expected_currency:
                            conn.rollback()
                            return {'status': 'error', 'message': 'Amount or currency mismatch'}
                        
                        cursor.execute('''
                            INSERT INTO transactions 
                            (transaction_id, from_address, to_address, amount, currency, status, payment_form_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (transaction_id, from_address, to_address, amount, currency, 'confirmed', form_id))
                        
                        cursor.execute('''
                            UPDATE payment_forms 
                            SET status = 'paid', updated_at = CURRENT_TIMESTAMP 
                            WHERE form_id = ? AND status = 'pending'
                        ''', (form_id,))
                        
                        if cursor.rowcount == 0:
                            conn.rollback()
                            return {'status': 'error', 'message': 'Payment form was already processed'}
                        
                        conn.commit()
                        return {'status': 'success', 'message': 'Payment processed successfully'}
                        
                    except sqlite3.OperationalError as e:
                        conn.rollback()
                        if 'database is locked' in str(e).lower() and attempt < max_retries - 1:
                            time.sleep(0.1 * (2 ** attempt))
                            continue
                        return {'status': 'error', 'message': f'Database error: {e}'}
                    except sqlite3.Error as e:
                        conn.rollback()
                        return {'status': 'error', 'message': f'Database error: {e}'}
        
        return {'status': 'error', 'message': 'Max retries exceeded'}
    
    def add_transaction(self, transaction_id: str, from_address: str, to_address: str,
                       amount: float, currency: str, status: str, 
                       payment_form_id: str = None, description: str = None) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO transactions 
                    (transaction_id, from_address, to_address, amount, currency, status, payment_form_id, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (transaction_id, from_address, to_address, amount, currency, status, payment_form_id, description))
                
                conn.commit()
                return True
        except sqlite3.IntegrityError:
            return False
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении транзакции: {e}")
            return False
    
    def get_transactions_by_form(self, form_id: str) -> List[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM transactions WHERE payment_form_id = ? ORDER BY created_at DESC
            ''', (form_id,))
            
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            
            return [dict(zip(columns, row)) for row in rows]
    
    def get_transaction_by_id(self, transaction_id: str) -> Optional[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, transaction_id, status FROM transactions WHERE transaction_id = ? LIMIT 1
            ''', (transaction_id,))
            
            row = cursor.fetchone()
            
            if row:
                return {'id': row[0], 'transaction_id': row[1], 'status': row[2]}
            
            return None
    
    def update_transaction_status(self, transaction_id: str, status: str) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    UPDATE transactions 
                    SET status = ?, updated_at = CURRENT_TIMESTAMP 
                    WHERE transaction_id = ?
                ''', (status, transaction_id))
                
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            self.logger.error(f"Ошибка при обновлении статуса транзакции: {e}")
            return False
    
    def get_pending_transactions(self) -> List[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM transactions WHERE status = 'pending' ORDER BY created_at DESC
            ''')
            
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            
            return [dict(zip(columns, row)) for row in rows]
    
    def get_active_payment_forms(self, current_time: float) -> List[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM payment_forms 
                WHERE status = 'pending' AND expires_at > ?
                ORDER BY created_at DESC
            ''', (current_time,))
            
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            
            return [dict(zip(columns, row)) for row in rows]
    
    def expire_old_forms(self, current_time: float) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE payment_forms 
                SET status = 'expired', updated_at = CURRENT_TIMESTAMP 
                WHERE status = 'pending' AND expires_at <= ?
            ''', (current_time,))
            
            expired_count = cursor.rowcount
            conn.commit()
            return expired_count
    
    def get_all_payment_forms(self) -> List[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT form_id, amount, currency, status, created_at, expires_at, description 
                FROM payment_forms 
                ORDER BY created_at DESC
            ''')
            
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            
            return [dict(zip(columns, row)) for row in rows]
    
    def close_pool(self):
        while not self.connection_pool.empty():
            try:
                conn = self.connection_pool.get_nowait()
                conn.close()
            except queue.Empty:
                break
        self.logger.info("Connection pool closed")
