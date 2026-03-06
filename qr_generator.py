import qrcode
import io
import os
import time
import logging
import re
from PIL import Image
from typing import Optional

class QRCodeGenerator:
    def __init__(self, qr_codes_dir: str = None):
        self.qr_codes_dir = qr_codes_dir or os.getenv('QR_CODES_DIR', 'qr_codes')
        os.makedirs(self.qr_codes_dir, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        
        self.qr = qrcode.QRCode(
            version=int(os.getenv('QR_VERSION', 1)),
            error_correction=getattr(qrcode.constants, f"ERROR_CORRECT_{os.getenv('QR_ERROR_CORRECTION', 'L')}"),
            box_size=int(os.getenv('QR_BOX_SIZE', 10)),
            border=int(os.getenv('QR_BORDER', 4)),
        )
    
    def generate_qr_code(self, data: str, size: tuple = None) -> Optional[bytes]:
        try:
            if size is None:
                default_size = int(os.getenv('QR_DEFAULT_SIZE', 300))
                size = (default_size, default_size)
                
            self.qr.clear()
            self.qr.add_data(data)
            self.qr.make(fit=True)
            
            fill_color = os.getenv('QR_FILL_COLOR', 'black')
            back_color = os.getenv('QR_BACK_COLOR', 'white')
            img = self.qr.make_image(fill_color=fill_color, back_color=back_color)
            
            img = img.resize(size, Image.Resampling.LANCZOS)
            
            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format='PNG')
            img_byte_arr = img_byte_arr.getvalue()
            
            return img_byte_arr
        except Exception as e:
            self.logger.error(f"Ошибка при генерации QR-кода: {e}")
            return None
    
    def generate_qr_code_file(self, data: str, filename: str, size: tuple = None) -> bool:
        try:
            if not self._validate_filename(filename):
                self.logger.error(f"Недопустимое имя файла: {filename}")
                return False
            
            if not self._validate_qr_data(data):
                self.logger.error("Недопустимые данные для QR-кода")
                return False
            
            if size is None:
                default_size = int(os.getenv('QR_DEFAULT_SIZE', 300))
                size = (default_size, default_size)
            
            self.qr.clear()
            self.qr.add_data(data)
            self.qr.make(fit=True)
            
            fill_color = os.getenv('QR_FILL_COLOR', 'black')
            back_color = os.getenv('QR_BACK_COLOR', 'white')
            img = self.qr.make_image(fill_color=fill_color, back_color=back_color)
            
            img = img.resize(size, Image.Resampling.LANCZOS)
            
            filepath = os.path.join(self.qr_codes_dir, filename)
            
            if not self._validate_filepath(filepath):
                self.logger.error(f"Недопустимый путь к файлу: {filepath}")
                return False
            
            img.save(filepath, 'PNG')
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении QR-кода: {e}")
            return False
    
    def generate_qr_code_in_folder(self, data: str, filename: str = None, size: tuple = None) -> Optional[str]:
        try:
            if filename is None:
                filename = f"qr_{int(time.time())}.png"
            
            success = self.generate_qr_code_file(data, filename, size)
            if success:
                return os.path.join(self.qr_codes_dir, filename)
            return None
        except Exception as e:
            self.logger.error(f"Ошибка при генерации QR-кода в папку: {e}")
            return None
    
    def _validate_filename(self, filename: str) -> bool:
        if not filename or not isinstance(filename, str):
            return False
        
        max_filename_length = int(os.getenv('MAX_FILENAME_LENGTH', 255))
        if len(filename) > max_filename_length:
            return False
        
        if '..' in filename or '/' in filename or '\\' in filename:
            return False
        
        if filename.startswith('.') or filename.startswith('-'):
            return False
        
        if not re.match(r'^[a-zA-Z0-9._-]+$', filename):
            return False
        
        valid_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.bmp']
        if not any(filename.lower().endswith(ext) for ext in valid_extensions):
            return False
        
        return True
    
    def _validate_qr_data(self, data: str) -> bool:
        if not data or not isinstance(data, str):
            return False
        
        max_qr_data_length = int(os.getenv('MAX_QR_DATA_LENGTH', 2000))
        if len(data) > max_qr_data_length:
            return False
        
        dangerous_patterns = [
            r'<script[^>]*>.*?</script>',
            r'javascript:',
            r'data:text/html',
            r'vbscript:',
            r'onload\s*=',
            r'onerror\s*='
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, data, re.IGNORECASE):
                return False
        
        return True
    
    def _validate_filepath(self, filepath: str) -> bool:
        try:
            abs_path = os.path.abspath(filepath)
            abs_dir = os.path.abspath(self.qr_codes_dir)
            
            if not abs_path.startswith(abs_dir):
                return False
            
            return True
        except Exception:
            return False
