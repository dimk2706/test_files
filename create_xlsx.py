import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import json
import random
import os

def generate_random_data(num_rows=10):
    """Генерирует случайные данные в указанном формате"""
    
    # Списки для случайного выбора
    symbols = ['CNY/RUB', 'USD/RUB', 'EUR/RUB', 'GBP/RUB', 'JPY/RUB']
    states = [0, 1]
    tenors = ['TOM', 'SPOT', 'TOD', 'ON']
    tiers = ['TRADER1', 'TRADER2', 'TRADER3', 'TRADER4', 'TRADER5']
    
    data = []
    
    for _ in range(num_rows):
        # Базовые данные
        time = datetime.now() - timedelta(days=random.randint(0, 30), 
                                         hours=random.randint(0, 23),
                                         minutes=random.randint(0, 59))
        ulid = str(uuid.uuid4())[:24].upper()
        symbol = random.choice(symbols)
        state = random.choice(states)
        tenor = random.choice(tenors)
        
        # Value date (ближайшая дата)
        value_date_near = time + timedelta(days=random.randint(1, 5))
        
        # Флаги
        global_tradable = random.choice([0, 1])
        global_indicative = 1 - global_tradable  # обычно взаимоисключающие
        
        # Rate ID (случайный большой номер)
        rate_id = random.randint(10000000000, 99999999999)
        
        # Tier
        tier = random.choice(tiers)
        
        # Price levels (генерируем случайные цены и размеры)
        bid_price = random.randint(8000000, 12000000)
        ask_price = bid_price + random.randint(100000, 500000)
        size = random.randint(100000, 5000000)
        
        price_levels = {
            'bid': {
                'price': str(bid_price),
                'size': str(size)
            },
            'ask': {
                'price': str(ask_price),
                'size': str(size)
            }
        }
        
        data.append({
            'time': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'ulid': ulid,
            'symbol': symbol,
            'state': state,
            'tenor': tenor,
            'valueDateNear': value_date_near.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'globalTradable': global_tradable,
            'globalIndicative': global_indicative,
            'rateId': rate_id,
            'tier': tier,
            'priceLevels': json.dumps(price_levels)
        })
    
    return data

def get_next_file_number(base_name="", extension="xlsx"):
    """Определяет следующий номер файла для текущей даты"""
    today = datetime.now().strftime("%Y-%m-%d")
    pattern = f"{today}_"
    
    # Ищем существующие файлы с этой датой
    existing_files = []
    for file in os.listdir('.'):
        if file.startswith(pattern) and file.endswith(f".{extension}"):
            existing_files.append(file)
    
    if not existing_files:
        return 1
    
    # Извлекаем номера из имен файлов
    numbers = []
    for file in existing_files:
        try:
            # Формат: Книга1_2024-01-15_3.xlsx
            number_part = file.replace(f"{pattern}", "").replace(f".{extension}", "")
            number = int(number_part)
            numbers.append(number)
        except ValueError:
            continue
    
    return max(numbers) + 1 if numbers else 1

def create_excel_file_with_date(num_rows=10):
    """Создает Excel файл с текущей датой и номером в названии"""
    
    # Получаем следующй номер файла
    file_number = get_next_file_number()
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"{today}_{file_number}.xlsx"
    
    # Генерируем данные
    data = generate_random_data(num_rows)
    
    # Создаем DataFrame
    df = pd.DataFrame(data)
    
    # Переименовываем колонки для соответствия исходному формату
    df.columns = ['time', 'ulid', 'symbol', 'state', 'tenor', 'valueDateNear', 
                  'globalTradable', 'globalIndicative', 'rateId', 'tier', 'priceLevels']
    
    # Сохраняем в Excel
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Лист1', index=False)
        
        # Получаем workbook и worksheet для настройки
        workbook = writer.book
        worksheet = writer.sheets['Лист1']
        
        # Настраиваем ширину колонок для лучшего отображения
        column_widths = {
            'A': 20,  # time
            'B': 30,  # ulid
            'C': 12,  # symbol
            'D': 8,   # state
            'E': 8,   # tenor
            'F': 20,  # valueDateNear
            'G': 15,  # globalTradable
            'H': 18,  # globalIndicative
            'I': 15,  # rateId
            'J': 10,  # tier
            'K': 50   # priceLevels
        }
        
        for col, width in column_widths.items():
            worksheet.column_dimensions[col].width = width
    
    return filename

def create_multiple_files(num_files=3, rows_per_file=10):
    """Создает несколько файлов с последовательной нумерацией"""
    created_files = []
    
    for i in range(num_files):
        print(f"\nСоздание файла {i+1} из {num_files}...")
        filename = create_excel_file_with_date(rows_per_file)
        created_files.append(filename)
    
    return created_files

if __name__ == "__main__":
    # Создаем один файл с автоматическим номером
    filename = create_excel_file_with_date(100)