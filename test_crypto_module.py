import pytest
import os
import tempfile
import time
import uuid
from collections import deque
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from database import DatabaseManager
from payment_processor import PaymentProcessor, retry_on_failure
from tronscan_api import TronScanAPI
from qr_generator import QRCodeGenerator


class TestDatabaseManager:
    
    def setup_method(self):
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db = DatabaseManager(self.temp_db.name, pool_size=2)
    
    def teardown_method(self):
        self.db.close_pool()
        os.unlink(self.temp_db.name)
    
    def test_database_initialization(self):
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='transactions'")
            assert cursor.fetchone() is not None
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='payment_forms'")
            assert cursor.fetchone() is not None
    
    def test_connection_pool(self):
        connections = []
        
        for _ in range(2):  # pool_size = 2
            conn_manager = self.db.get_connection()
            conn = conn_manager.__enter__()
            connections.append((conn_manager, conn))
        
        for _, conn in connections:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1
        
        for conn_manager, _ in connections:
            conn_manager.__exit__(None, None, None)
    
    def test_create_payment_form(self):
        form_id = str(uuid.uuid4())
        result = self.db.create_payment_form(
            form_id=form_id,
            amount=100.0,
            currency="USDT",
            description="Test payment",
            wallet_address="TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH"
        )
        
        assert result is True
        form = self.db.get_payment_form(form_id)
        assert form is not None
        assert form['amount'] == 100.0
        assert form['currency'] == "USDT"
        assert form['status'] == "pending"
    
    def test_duplicate_payment_form(self):
        form_id = str(uuid.uuid4())
        
        result1 = self.db.create_payment_form(
            form_id=form_id,
            amount=100.0,
            currency="USDT",
            description="Test payment",
            wallet_address="TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH"
        )
        assert result1 is True
        
        result2 = self.db.create_payment_form(
            form_id=form_id,
            amount=200.0,
            currency="TRX",
            description="Duplicate payment",
            wallet_address="TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH"
        )
        assert result2 is False
    
    def test_process_payment_atomic(self):
        form_id = str(uuid.uuid4())
        self.db.create_payment_form(
            form_id=form_id,
            amount=100.0,
            currency="USDT",
            description="Test payment",
            wallet_address="TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH"
        )
        
        tx_id = "test_transaction_" + str(uuid.uuid4())
        result = self.db.process_payment_atomic(
            transaction_id=tx_id,
            from_address="TFrom123456789012345678901234567890",
            to_address="TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH",
            amount=100.0,
            currency="USDT",
            form_id=form_id
        )
        
        assert result['status'] == 'success'
        
        form = self.db.get_payment_form(form_id)
        assert form['status'] == 'paid'
        
        transaction = self.db.get_transaction_by_id(tx_id)
        assert transaction is not None
        assert transaction['status'] == 'confirmed'
    
    def test_process_payment_already_processed(self):
        form_id = str(uuid.uuid4())
        self.db.create_payment_form(
            form_id=form_id,
            amount=100.0,
            currency="USDT",
            description="Test payment",
            wallet_address="TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH"
        )
        
        tx_id = "test_transaction_" + str(uuid.uuid4())
        
        result1 = self.db.process_payment_atomic(
            transaction_id=tx_id,
            from_address="TFrom123456789012345678901234567890",
            to_address="TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH",
            amount=100.0,
            currency="USDT",
            form_id=form_id
        )
        assert result1['status'] == 'success'
        
        result2 = self.db.process_payment_atomic(
            transaction_id=tx_id,
            from_address="TFrom123456789012345678901234567890",
            to_address="TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH",
            amount=100.0,
            currency="USDT",
            form_id=form_id
        )
        assert result2['status'] == 'error'
        assert 'already processed' in result2['message']


class TestTronScanAPI:
    
    def setup_method(self):
        self.api = TronScanAPI("https://apilist.tronscanapi.com/api", requests_per_minute=60)
    
    def test_api_url_validation_valid(self):
        api = TronScanAPI("https://apilist.tronscanapi.com/api")
        assert api.api_url == "https://apilist.tronscanapi.com/api"
    
    def test_api_url_validation_invalid_scheme(self):
        with pytest.raises(ValueError, match="должен использовать HTTPS"):
            TronScanAPI("http://apilist.tronscanapi.com/api")
    
    def test_api_url_validation_invalid_domain(self):
        with pytest.raises(ValueError, match="Недопустимый API домен"):
            TronScanAPI("https://malicious-site.com/api")
    
    def test_transaction_data_validation_valid(self):
        tx_data = {
            'hash': '1234567890abcdef' * 4,
            'timestamp': int(datetime.now().timestamp() * 1000)
        }
        
        result = self.api._validate_transaction_data(tx_data)
        assert result is True
    
    def test_transaction_data_validation_invalid_hash(self):
        tx_data = {
            'hash': 'invalid_hash',
            'timestamp': int(datetime.now().timestamp() * 1000)
        }
        
        result = self.api._validate_transaction_data(tx_data)
        assert result is False
    
    def test_transaction_data_validation_old_timestamp(self):
        old_timestamp = int((datetime.now() - timedelta(days=400)).timestamp() * 1000)
        tx_data = {
            'hash': '1234567890abcdef' * 4,
            'timestamp': old_timestamp
        }
        
        result = self.api._validate_transaction_data(tx_data)
        assert result is False
    
    @patch('requests.Session.get')
    def test_ssl_certificate_validation(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_get.return_value = mock_response
        
        with patch.object(self.api, '_validate_ssl_certificate', return_value=True):
            result = self.api.get_account_transactions("TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH")
            assert isinstance(result, list)
    
    def test_ssl_verification_enabled_by_default(self):
        assert self.api.verify_ssl is True
        assert self.api.session.verify is True
    
    def test_make_request_uses_ssl_verification(self):
        mock_response = Mock()
        mock_response.status_code = 200
        
        with patch.object(self.api, '_wait_for_rate_limit'):
            with patch.object(self.api.session, 'get', return_value=mock_response) as mock_get:
                self.api._make_request(
                    "https://apilist.tronscanapi.com/api/transaction",
                    params={'address': 'TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH'},
                    timeout=1,
                    max_retries=1
                )
                
                assert mock_get.call_args.kwargs['verify'] is True
    
    def test_rate_limiting(self):
        original_requests_per_minute = self.api.requests_per_minute
        self.api.requests_per_minute = 1
        
        start_time = time.time()
        self.api._wait_for_rate_limit()
        first_call_time = time.time()
        
        self.api._wait_for_rate_limit()
        second_call_time = time.time()
        
        self.api.requests_per_minute = original_requests_per_minute
        
        assert first_call_time - start_time < 1.0
        assert second_call_time - first_call_time >= 3.0


class TestPaymentProcessor:
    
    def setup_method(self):
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        
        os.environ['DATABASE_PATH'] = self.temp_db.name
        os.environ['WALLET_ADDRESS'] = 'TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH'
        os.environ['TRONSCAN_API_URL'] = 'https://apilist.tronscanapi.com/api'
        os.environ['API_RATE_LIMIT'] = '60'
        os.environ['LOG_LEVEL'] = 'ERROR'
        
        with patch('payment_processor.TronScanAPI'):
            self.processor = PaymentProcessor()
    
    def teardown_method(self):
        self.processor.db.close_pool()
        os.unlink(self.temp_db.name)
        
        for key in [
            'DATABASE_PATH',
            'WALLET_ADDRESS',
            'TRONSCAN_API_URL',
            'API_RATE_LIMIT',
            'API_REQUESTS_PER_MINUTE',
            'LOG_LEVEL',
            'MIN_FORM_CREATION_INTERVAL_SECONDS',
            'MIN_USER_FORM_INTERVAL_SECONDS',
            'MAX_USER_FORMS_PER_HOUR',
            'USER_FORM_RATE_WINDOW_SECONDS',
            'MAX_USDT_AMOUNT'
        ]:
            if key in os.environ:
                del os.environ[key]
    
    def test_tron_address_validation_valid(self):
        valid_address = "TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH"
        result = self.processor._validate_tron_address(valid_address)
        assert result is True
    
    def test_tron_address_validation_invalid(self):
        invalid_addresses = [
            "",
            "invalid",
            "BTC1234567890123456789012345678901234",
            "T123",
            "TTest123456789012345678901234567890123"
        ]
        
        for address in invalid_addresses:
            result = self.processor._validate_tron_address(address)
            assert result is False, f"Address {address} should be invalid"
    
    def test_amount_validation_valid(self):
        valid_amounts = [
            (1.0, "USDT"),
            (100.5, "USDT"),
            (10.0, "TRX"),
            (1000.0, "TRX")
        ]
        
        for amount, currency in valid_amounts:
            result = self.processor._validate_amount(amount, currency)
            assert result is True, f"Amount {amount} {currency} should be valid"
    
    def test_amount_validation_invalid(self):
        invalid_amounts = [
            (0.0, "USDT"),
            (-10.0, "USDT"),
            (0.05, "USDT"),
            (15000.0, "USDT"),
            (float('inf'), "USDT"),
            (float('nan'), "USDT")
        ]
        
        for amount, currency in invalid_amounts:
            result = self.processor._validate_amount(amount, currency)
            assert result is False, f"Amount {amount} {currency} should be invalid"
    
    def test_description_validation_valid(self):
        valid_descriptions = [
            "Простое описание",
            "Payment for order #123",
            "Тест с русскими символами",
            "",
            "A" * 500
        ]
        
        for desc in valid_descriptions:
            result = self.processor._validate_description(desc)
            assert result is True, f"Description '{desc}' should be valid"
    
    def test_description_validation_invalid(self):
        invalid_descriptions = [
            "A" * 501,
            "Description with <script>",
            "SELECT * FROM users",
            "Description with\nnewline",
            "Description with\ttab"
        ]
        
        for desc in invalid_descriptions:
            result = self.processor._validate_description(desc)
            assert result is False, f"Description '{desc}' should be invalid"
    
    def test_create_payment_form_valid(self):
        form = self.processor.create_payment_form(
            amount=100.0,
            currency="USDT",
            description="Test payment",
            expires_hours=24
        )
        
        assert 'form_id' in form
        assert form['amount'] > 100.0
        assert form['original_amount'] == 100.0
        assert form['currency'] == "USDT"
        assert form['status'] == "pending"
    
    def test_create_payment_form_invalid_input_types(self):
        with pytest.raises(ValueError, match="amount должен быть числом"):
            self.processor.create_payment_form(
                amount="invalid",
                currency="USDT"
            )
        
        with pytest.raises(ValueError, match="currency должен быть строкой"):
            self.processor.create_payment_form(
                amount=100.0,
                currency=123
            )
    
    def test_unique_amount_generation(self):
        base_amount = 100.0
        currency = "USDT"
        
        amounts = set()
        for _ in range(10):
            unique_amount = self.processor._generate_unique_amount(base_amount, currency)
            assert unique_amount >= base_amount
            assert unique_amount < base_amount + 1.0
            amounts.add(unique_amount)
        
        assert len(amounts) == 10
    
    def test_new_transactions_are_checked_after_filtering(self):
        os.environ['MIN_FORM_CREATION_INTERVAL_SECONDS'] = '0'
        form = self.processor.create_payment_form(
            amount=100.0,
            currency="USDT",
            description="Monitor test"
        )
        
        tx_hash = "a" * 64
        tx = {
            'hash': tx_hash,
            'timestamp': int(time.time() * 1000),
            'confirmed': True,
            'trc20_transfer': {
                'quant': str(int(round(form['amount'] * 1_000_000))),
                'from_address': 'TQ5i4rntM6rA6NwczwQ6VykBLL8GMDJ9Zh',
                'to_address': self.processor.wallet_address,
                'contract_address': self.processor.OFFICIAL_USDT_CONTRACT,
                'tokenInfo': {
                    'tokenAbbr': 'USDT',
                    'tokenDecimal': 6,
                    'tokenId': self.processor.OFFICIAL_USDT_CONTRACT
                }
            }
        }
        self.processor.tronscan.get_transaction_details.return_value = {
            'confirmations': 25,
            'confirmed': True
        }
        
        new_transactions = self.processor._filter_new_transactions([tx])
        assert len(new_transactions) == 1
        
        self.processor.monitoring = True
        try:
            matched = self.processor._check_form_against_transactions_optimized(form, new_transactions)
        finally:
            self.processor.monitoring = False
        
        assert matched is True
        assert self.processor.db.get_transaction_by_id(tx_hash) is not None
    
    def test_validate_transaction_fast_requires_usdt_contract_data(self):
        tx = {
            'transaction_id': 'b' * 64,
            'from_address': 'TQ5i4rntM6rA6NwczwQ6VykBLL8GMDJ9Zh',
            'to_address': self.processor.wallet_address,
            'amount': 1.0,
            'currency': 'USDT',
            'timestamp': int(time.time() * 1000),
            'confirmed': True
        }
        
        assert self.processor._validate_transaction_fast(tx) is False
    
    def test_user_hourly_limit_uses_time_window(self):
        os.environ['MIN_FORM_CREATION_INTERVAL_SECONDS'] = '0'
        os.environ['MIN_USER_FORM_INTERVAL_SECONDS'] = '0'
        os.environ['MAX_USER_FORMS_PER_HOUR'] = '2'
        os.environ['USER_FORM_RATE_WINDOW_SECONDS'] = '3600'
        
        self.processor.create_payment_form(10.0, "USDT", user_id='123')
        self.processor.create_payment_form(10.0, "USDT", user_id='123')
        
        self.processor._user_form_timestamps['123'] = deque([
            time.time() - 7200,
            time.time() - 7100
        ])
        self.processor._user_last_form_time['123'] = time.time() - 7200
        
        form = self.processor.create_payment_form(10.0, "USDT", user_id='123')
        assert 'form_id' in form
    
    def test_final_amount_respects_currency_max_limit(self):
        os.environ['MIN_FORM_CREATION_INTERVAL_SECONDS'] = '0'
        os.environ['MAX_USDT_AMOUNT'] = '1.0'
        
        with patch('payment_processor.secrets.randbelow', return_value=9998):
            form = self.processor.create_payment_form(amount=1.0, currency="USDT")
        
        assert form['amount'] <= 1.0
    
    def test_api_rate_limit_prefers_api_requests_per_minute(self):
        os.environ['API_RATE_LIMIT'] = '15'
        os.environ['API_REQUESTS_PER_MINUTE'] = '42'
        assert self.processor._get_api_rate_limit() == 42
    
    def test_ip_address_validation(self):
        valid_ips = [
            "192.168.1.1",
            "10.0.0.1",
            "127.0.0.1",
            "2001:db8::1",
            "::1"
        ]
        
        invalid_ips = [
            "invalid",
            "256.256.256.256",
            "192.168.1",
            "192.168.1.1.1"
        ]
        
        for ip in valid_ips:
            result = self.processor._validate_ip_address(ip)
            assert result is True, f"IP {ip} should be valid"
        
        for ip in invalid_ips:
            result = self.processor._validate_ip_address(ip)
            assert result is False, f"IP {ip} should be invalid"


class TestQRCodeGenerator:
    
    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.qr_gen = QRCodeGenerator(self.temp_dir)
    
    def teardown_method(self):
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_generate_qr_code_bytes(self):
        test_data = "tron:TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH?amount=100.0"
        qr_bytes = self.qr_gen.generate_qr_code(test_data)
        
        assert qr_bytes is not None
        assert isinstance(qr_bytes, bytes)
        assert len(qr_bytes) > 0
    
    def test_generate_qr_code_file(self):
        test_data = "tron:TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH?amount=100.0"
        filename = "test_qr.png"
        
        result = self.qr_gen.generate_qr_code_file(test_data, filename)
        assert result is True
        
        filepath = os.path.join(self.temp_dir, filename)
        assert os.path.exists(filepath)
        assert os.path.getsize(filepath) > 0
    
    def test_generate_qr_code_in_folder(self):
        test_data = "tron:TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH?amount=100.0"
        
        filepath = self.qr_gen.generate_qr_code_in_folder(test_data)
        assert filepath is not None
        assert os.path.exists(filepath)
        assert filepath.startswith(self.temp_dir)
        assert filepath.endswith('.png')


class TestRetryDecorator:
    
    def test_retry_success_first_attempt(self):
        call_count = 0
        
        @retry_on_failure(max_retries=3, delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = test_function()
        assert result == "success"
        assert call_count == 1
    
    def test_retry_success_after_failures(self):
        call_count = 0
        
        @retry_on_failure(max_retries=3, delay=0.01)
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Temporary error")
            return "success"
        
        result = test_function()
        assert result == "success"
        assert call_count == 3
    
    def test_retry_max_attempts_exceeded(self):
        call_count = 0
        
        @retry_on_failure(max_retries=2, delay=0.01)
        def test_function():
            nonlocal call_count
            call_count += 1
            raise Exception("Persistent error")
        
        with pytest.raises(Exception, match="Persistent error"):
            test_function()
        
        assert call_count == 3


class TestIntegration:
    
    def setup_method(self):
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.temp_qr_dir = tempfile.mkdtemp()
        
        os.environ['DATABASE_PATH'] = self.temp_db.name
        os.environ['WALLET_ADDRESS'] = 'TLyqzVGLV1srkB7dToTAEqgDSfPtXRJZYH'
        os.environ['TRONSCAN_API_URL'] = 'https://apilist.tronscanapi.com/api'
        os.environ['API_RATE_LIMIT'] = '60'
        os.environ['LOG_LEVEL'] = 'ERROR'
        
        with patch('payment_processor.TronScanAPI'):
            self.processor = PaymentProcessor()
        self.qr_gen = QRCodeGenerator(self.temp_qr_dir)
    
    def teardown_method(self):
        self.processor.db.close_pool()
        os.unlink(self.temp_db.name)
        
        import shutil
        shutil.rmtree(self.temp_qr_dir)
        
        for key in [
            'DATABASE_PATH',
            'WALLET_ADDRESS',
            'TRONSCAN_API_URL',
            'API_RATE_LIMIT',
            'API_REQUESTS_PER_MINUTE',
            'LOG_LEVEL',
            'MIN_FORM_CREATION_INTERVAL_SECONDS',
            'MIN_USER_FORM_INTERVAL_SECONDS',
            'MAX_USER_FORMS_PER_HOUR',
            'USER_FORM_RATE_WINDOW_SECONDS',
            'MAX_USDT_AMOUNT'
        ]:
            if key in os.environ:
                del os.environ[key]
    
    def test_full_payment_workflow(self):
        form = self.processor.create_payment_form(
            amount=100.0,
            currency="USDT",
            description="Integration test payment"
        )
        
        assert 'form_id' in form
        form_id = form['form_id']
        
        qr_data = self.processor.generate_payment_qr_data(form_id)
        assert qr_data.startswith("tron:")
        
        qr_bytes = self.qr_gen.generate_qr_code(qr_data)
        assert qr_bytes is not None
        
        status = self.processor.check_payment_status(form_id)
        assert status['status'] == 'waiting'
        
        tx_id = "integration_test_tx_" + str(uuid.uuid4())
        result = self.processor.db.process_payment_atomic(
            transaction_id=tx_id,
            from_address="TFrom123456789012345678901234567890",
            to_address=self.processor.wallet_address,
            amount=form['amount'],
            currency="USDT",
            form_id=form_id
        )
        
        assert result['status'] == 'success'
        
        final_status = self.processor.check_payment_status(form_id)
        assert final_status['status'] == 'paid'
        assert final_status['transaction_id'] == tx_id


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
