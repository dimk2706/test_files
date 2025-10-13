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

def get_next_file_number(base_name="Книга1", extension="xlsx"):
    """Определяет следующий номер файла для текущей даты"""
    today = datetime.now().strftime("%Y-%m-%d")
    pattern = f"{base_name}_{today}_"
    
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

def create_data_files(num_rows=10):
    """Создает Excel файл и Parquet базу данных с текущей датой и номером"""
    
    # Получаем следующий номер файла
    file_number = get_next_file_number()
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Генерируем данные
    data = generate_random_data(num_rows)
    
    # Создаем DataFrame
    df = pd.DataFrame(data)
    
    # Переименовываем колонки для соответствия исходному формату
    df.columns = ['time', 'ulid', 'symbol', 'state', 'tenor', 'valueDateNear', 
                  'globalTradable', 'globalIndicative', 'rateId', 'tier', 'priceLevels']
    
    # Имена файлов
    excel_filename = f"Книга1_{today}_{file_number}.xlsx"
    parquet_filename = f"database_{today}_{file_number}.parquet"
    
    # Сохраняем в Excel
    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
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
    
    # Сохраняем в Parquet
    df.to_parquet(parquet_filename, index=False, engine='pyarrow')
    
    print(f"✅ Excel файл '{excel_filename}' успешно создан с {num_rows} строками данных")
    print(f"✅ Parquet база данных '{parquet_filename}' успешно создана")
    
    return excel_filename, parquet_filename, df

def create_consolidated_database():
    """Создает консолидированную базу данных из всех Parquet файлов"""
    parquet_files = [f for f in os.listdir('.') if f.startswith('database_') and f.endswith('.parquet')]
    
    if not parquet_files:
        print("❌ Parquet файлы не найдены для консолидации")
        return None
    
    all_data = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            df['source_file'] = file  # Добавляем информацию о источнике
            all_data.append(df)
        except Exception as e:
            print(f"❌ Ошибка при чтении файла {file}: {e}")
    
    if all_data:
        consolidated_df = pd.concat(all_data, ignore_index=True)
        consolidated_filename = f"consolidated_database_{datetime.now().strftime('%Y-%m-%d')}.parquet"
        consolidated_df.to_parquet(consolidated_filename, index=False, engine='pyarrow')
        print(f"✅ Консолидированная база данных '{consolidated_filename}' создана")
        print(f"   Объединено {len(parquet_files)} файлов, всего {len(consolidated_df)} записей")
        return consolidated_filename
    else:
        print("❌ Не удалось создать консолидированную базу данных")
        return None

def read_and_display_parquet(filename):
    """Читает и отображает данные из Parquet файла"""
    try:
        df = pd.read_parquet(filename)
        print(f"\n📊 Данные из {filename}:")
        print(f"   Количество записей: {len(df)}")
        print(f"   Колонки: {list(df.columns)}")
        print("\nПервые 3 строки:")
        print(df.head(3))
        return df
    except Exception as e:
        print(f"❌ Ошибка при чтении Parquet файла: {e}")
        return None

if __name__ == "__main__":
    # Создаем файлы
    excel_file, parquet_file, df = create_data_files(150)
    
    # Показываем данные из Parquet
    #read_and_display_parquet(parquet_file)
    
    # Создаем консолидированную базу (опционально)
    print("\n" + "="*50)
    consolidated_file = create_consolidated_database()
    if consolidated_file:
        read_and_display_parquet(consolidated_file)
    