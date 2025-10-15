import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import json
import random
import os
import asyncio
from dotenv import load_dotenv

# Асинхронный S3-клиент
import boto3
from botocore.config import Config
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# Загружаем .env
load_dotenv()

# Параметры из окружения
OBS_ACCESS_KEY = os.getenv("OBS_ACCESS_KEY")
OBS_SECRET_KEY = os.getenv("OBS_SECRET_KEY")
OBS_REGION = os.getenv("OBS_REGION")
OBS_ENDPOINT = os.getenv("OBS_ENDPOINT")
OBS_BUCKET = os.getenv("OBS_BUCKET")

required_vars = {
    "OBS_ACCESS_KEY": OBS_ACCESS_KEY,
    "OBS_SECRET_KEY": OBS_SECRET_KEY,
    "OBS_REGION": OBS_REGION,
    "OBS_ENDPOINT": OBS_ENDPOINT,
    "OBS_BUCKET": OBS_BUCKET,
}

for name, value in required_vars.items():
    if not value:
        raise EnvironmentError(f"Переменная окружения {name} не задана")


def create_excel_alternative(df, filename):
    """Создает Excel файл напрямую через openpyxl"""
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    
    # Записываем заголовки
    headers = list(df.columns)
    ws.append(headers)
    
    # Записываем данные
    for _, row in df.iterrows():
        ws.append(row.tolist())
    
    # Настраиваем ширину колонок
    widths = {
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
    
    for col, width in widths.items():
        ws.column_dimensions[col].width = width
    
    # Сохраняем файл
    wb.save(filename)
    print(f"✅ Excel создан через openpyxl: {filename}")


def upload_to_cloud_sync(filepath: str):
    """Синхронная загрузка файла в S3-совместимое облако"""
    object_name = os.path.basename(filepath)

    # Определяем Content-Type
    if filepath.lower().endswith('.xlsx'):
        content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif filepath.lower().endswith('.parquet'):
        content_type = 'application/octet-stream'
    else:
        content_type = 'application/octet-stream'

    # Создаем клиент
    session = boto3.session.Session()
    config = Config(s3={'addressing_style': 'virtual'})

    client = session.client(
        's3',
        region_name=OBS_REGION,
        endpoint_url=OBS_ENDPOINT,
        aws_access_key_id=OBS_ACCESS_KEY,
        aws_secret_access_key=OBS_SECRET_KEY,
        config=config
    )

    try:
        # Загружаем файл
        with open(filepath, 'rb') as file_obj:
            client.put_object(
                Bucket=OBS_BUCKET,
                Key=object_name,
                Body=file_obj,
                ContentType=content_type
            )
        print(f"✅ Загружено: {object_name}")
        return True
    except Exception as e:
        print(f"❌ Ошибка загрузки {filepath}: {e}")
        return False


def generate_random_data(num_rows=10):
    symbols = ['CNY/RUB', 'USD/RUB', 'EUR/RUB', 'GBP/RUB', 'JPY/RUB']
    states = [0, 1]
    tenors = ['TOM', 'SPOT', 'TOD', 'ON']
    tiers = ['TRADER1', 'TRADER2', 'TRADER3', 'TRADER4', 'TRADER5']
    
    data = []
    for _ in range(num_rows):
        time = datetime.now() - timedelta(days=random.randint(0, 30),
                                         hours=random.randint(0, 23),
                                         minutes=random.randint(0, 59))
        ulid = str(uuid.uuid4())[:24].upper()
        symbol = random.choice(symbols)
        state = random.choice(states)
        tenor = random.choice(tenors)
        value_date_near = time + timedelta(days=random.randint(1, 5))
        global_tradable = random.choice([0, 1])
        global_indicative = 1 - global_tradable
        rate_id = random.randint(10000000000, 99999999999)
        tier = random.choice(tiers)
        bid_price = random.randint(8000000, 12000000)
        ask_price = bid_price + random.randint(100000, 500000)
        size = random.randint(100000, 5000000)
        price_levels = {
            'bid': {'price': str(bid_price), 'size': str(size)},
            'ask': {'price': str(ask_price), 'size': str(size)}
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


def get_next_file_number(base_name="Book1", extension="xlsx"):
    today = datetime.now().strftime("%Y-%m-%d")
    pattern = f"{base_name}_{today}_"
    existing_files = [f for f in os.listdir('.') if f.startswith(pattern) and f.endswith(f".{extension}")]
    if not existing_files:
        return 1
    numbers = []
    for file in existing_files:
        try:
            num_part = file.replace(f"{pattern}", "").replace(f".{extension}", "")
            numbers.append(int(num_part))
        except ValueError:
            continue
    return max(numbers) + 1 if numbers else 1


def create_data_files_sync(num_rows=10):
    """Создаёт файлы синхронно (Excel/Parquet), возвращает имена"""
    file_number = get_next_file_number()
    today = datetime.now().strftime("%Y-%m-%d")
    
    data = generate_random_data(num_rows)
    df = pd.DataFrame(data)
    df.columns = ['time', 'ulid', 'symbol', 'state', 'tenor', 'valueDateNear',
                  'globalTradable', 'globalIndicative', 'rateId', 'tier', 'priceLevels']
    
    excel_filename = f"Book1_{today}_{file_number}.xlsx"
    parquet_filename = f"database_{today}_{file_number}.parquet"
    
    # Создаем Excel альтернативным способом
    try:
        create_excel_alternative(df, excel_filename)
        
        # Проверяем что файл создался и открывается
        test_df = pd.read_excel(excel_filename, engine='openpyxl')
        print(f"✅ Excel файл проверен: {len(test_df)} строк")
        
    except Exception as e:
        print(f"❌ Ошибка создания Excel: {e}")
        return None, None, None
    
    # Parquet
    try:
        df.to_parquet(parquet_filename, index=False, engine='pyarrow')
        print(f"✅ Parquet файл создан: {parquet_filename}")
    except Exception as e:
        print(f"❌ Ошибка создания Parquet: {e}")
        return None, None, None
    
    # Проверяем размеры файлов
    excel_size = os.path.getsize(excel_filename)
    parquet_size = os.path.getsize(parquet_filename)
    print(f"📊 Размер Excel: {excel_size} байт")
    print(f"📊 Размер Parquet: {parquet_size} байт")
    
    return excel_filename, parquet_filename, df


def create_consolidated_database_sync():
    parquet_files = [f for f in os.listdir('.') if f.startswith('database_') and f.endswith('.parquet')]
    if not parquet_files:
        print("❌ Нет Parquet-файлов для консолидации")
        return None
    all_dfs = []
    for f in parquet_files:
        try:
            df = pd.read_parquet(f)
            df['source_file'] = f
            all_dfs.append(df)
        except Exception as e:
            print(f"❌ Ошибка чтения {f}: {e}")
    if not all_dfs:
        return None
    consolidated = pd.concat(all_dfs, ignore_index=True)
    cons_filename = f"consolidated_database_{datetime.now().strftime('%Y-%m-%d')}.parquet"
    consolidated.to_parquet(cons_filename, index=False)
    print(f"✅ Консолидированная БД: {cons_filename}")
    return cons_filename


def verify_local_excel(filepath: str) -> bool:
    """Проверяет локальный Excel файл"""
    try:
        df = pd.read_excel(filepath, engine='openpyxl')
        print(f"✅ Локальный файл {filepath} открывается: {len(df)} строк")
        return True
    except Exception as e:
        print(f"❌ Локальный файл {filepath} не открывается: {e}")
        return False


async def main():
    print("🚀 Начало процесса генерации и загрузки данных...")
    
    # 1. Генерация данных
    print("📊 Генерация данных...")
    excel_file, parquet_file, df = create_data_files_sync(num_rows=150)
    
    if not excel_file or not parquet_file:
        print("❌ Ошибка создания файлов")
        return
    
    # 2. Проверяем локальные файлы
    print("🔍 Проверка локальных файлов...")
    if not verify_local_excel(excel_file):
        print("❌ Локальный Excel файл поврежден, пропускаем загрузку")
        return
    
    # 3. Загрузка в облако
    print("☁️ Загрузка файлов в облако...")
    upload_to_cloud_sync(excel_file)
    upload_to_cloud_sync(parquet_file)
    
    print("\n" + "="*60)
    
    # 4. Консолидация и загрузка
    print("🔄 Консолидация данных...")
    consolidated_file = create_consolidated_database_sync()
    if consolidated_file:
        print("☁️ Загрузка консолидированной БД...")
        upload_to_cloud_sync(consolidated_file)
    
    print("\n✅ Процесс завершен!")


# --- Запуск ---
if __name__ == "__main__":
    # Для асинхронного запуска
    asyncio.run(main())
    
    # Или для синхронного запуска (раскомментируйте если нужно):
    # main()