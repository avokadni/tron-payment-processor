import os
import time
from dotenv import load_dotenv
from payment_processor import PaymentProcessor
from qr_generator import QRCodeGenerator

load_dotenv()

def main():
    print("🚀 Инициализация модуля обработки платежей...")
    
    try:
        payment_processor = PaymentProcessor()
        qr_generator = QRCodeGenerator()
        
        print("✅ Модуль успешно инициализирован")
        print(f"🏦 Адрес кошелька: {payment_processor._mask_wallet_address(payment_processor.wallet_address)}")
        
        print("\n💳 Создание тестовой платежной формы...")
        payment_form = payment_processor.create_payment_form(
            amount=1.0,
            currency="USDT",
            description="Тестовый платеж"
        )
        
        print(f"✅ Платежная форма создана:")
        print(f"   ID: {payment_form['form_id']}")
        print(f"   Запрошенная сумма: {payment_form.get('original_amount', 1.0)} {payment_form['currency']}")
        print(f"   К доплате: {payment_form['amount']} {payment_form['currency']}")
        print(f"   Описание: {payment_form['description']}")
        print(f"   Действительна до: {payment_form['expires_at']}")
        
        print("\n📱 Генерация QR-кода...")
        qr_data = payment_processor.generate_payment_qr_data(payment_form['form_id'])
        qr_bytes = qr_generator.generate_qr_code(qr_data)
        qr_filepath = None
        
        if qr_bytes:
            qr_filename = f"payment_{payment_form['form_id'][:8]}.png"
            qr_filepath = qr_generator.generate_qr_code_in_folder(qr_data, qr_filename)
            if qr_filepath:
                print(f"✅ QR-код сохранен в файл: {qr_filepath}")
            else:
                print("❌ Ошибка при сохранении QR-кода")
        else:
            print("❌ Ошибка при генерации QR-кода")
        
        def on_payment_received(transaction, form_id):
            print(f"\n🎉 ПЛАТЕЖ ПОЛУЧЕН!")
            print(f"   Сумма: {transaction['amount']} {transaction['currency']}")
            print(f"   От: {transaction['from_address']}")
            print(f"   ID транзакции: {transaction['transaction_id']}")
        
        payment_processor.register_payment_callback(
            payment_form['form_id'], 
            on_payment_received
        )
        
        print("\n👀 Запуск мониторинга платежей...")
        payment_processor.start_monitoring(check_interval=10)
        
        print("\n📋 Информация для оплаты:")
        print(f"   Адрес кошелька: {payment_processor._mask_wallet_address(payment_processor.wallet_address)}")
        print(f"   К доплате: {payment_form['amount']} {payment_form['currency']}")
        print(f"   QR-код: {qr_filepath if qr_filepath else 'Не создан'}")
        print(f"   QR данные: {qr_data}")
        
        print("\n⏳ Ожидание платежа... (Ctrl+C для выхода)")
        
        try:
            while True:
                status = payment_processor.check_payment_status(payment_form['form_id'])
                print(f"   Статус: {status['status']}")
                
                if status['status'] == 'paid':
                    print("✅ Платеж подтвержден!")
                    break
                elif status['status'] == 'expired':
                    print("❌ Платежная форма истекла")
                    break
                
                time.sleep(30)
                
        except KeyboardInterrupt:
            print("\n👋 Мониторинг остановлен пользователем")
        
        payment_processor.stop_monitoring()
        print("✅ Модуль завершил работу")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
