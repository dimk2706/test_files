import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import json
import random
import os
import boto3
from botocore.config import Config
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Получаем параметры из .env
OBS_ACCESS_KEY = os.getenv("OBS_ACCESS_KEY")
OBS_SECRET_KEY = os.getenv("OBS_SECRET_KEY")
OBS_REGION = os.getenv("OBS_REGION")
OBS_ENDPOINT = os.getenv("OBS_ENDPOINT")
OBS_BUCKET = os.getenv("OBS_BUCKET")

# Проверка обязательных переменных
required_vars = {
    "OBS_ACCESS_KEY": OBS_ACCESS_KEY,
    "OBS_SECRET_KEY": OBS_SECRET_KEY,
    "OBS_REGION": OBS_REGION,
    "OBS_ENDPOINT": OBS_ENDPOINT,
    "OBS_BUCKET": OBS_BUCKET,
}

for name, value in required_vars.items():
    if not value:
        raise EnvironmentError(f"Переменная окружения {name} не задана в .env файле")

def upload_to_cloud(filepath):
    object_name = os.path.basename(filepath)
    
    # Определяем Content-Type
    if filepath.lower().endswith('.xlsx'):
        content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif filepath.lower().endswith('.parquet'):
        content_type = 'application/octet-stream'
    else:
        content_type = 'application/octet-stream'
    
    s3_params = {
        "service_name": "s3",
        "aws_access_key_id": OBS_ACCESS_KEY,
        "aws_secret_access_key": OBS_SECRET_KEY,
        "region_name": OBS_REGION,
        "endpoint_url": OBS_ENDPOINT,
        "config": Config(s3={"addressing_style": "virtual"})
    }

    try:
        s3_client = boto3.client(**s3_params)
        s3_client.upload_file(
            Filename=filepath,
            Bucket=OBS_BUCKET,
            Key=object_name,
            ExtraArgs={'ContentType': content_type}
        )
        print(f"✅ Загружено: {object_name} (Content-Type: {content_type})")
    except Exception as e:
        print(f"❌ Ошибка загрузки {filepath}: {e}")


def generate_random_data(num_rows=10, symbol='CNY/RUB'):
    """Генерирует случайные данные в указанном формате"""
    
    #symbols = ['CNY/RUB', 'USD/RUB', 'EUR/RUB', 'GBP/RUB', 'JPY/RUB']
    states = [0, 1]
    tenors = ['TOM', 'TOD',]
    tiers = ['TRADER1', 'TRADER2', 'TRADER3', 'TRADER4', 'TRADER5']
    
    data = []

    for _ in range(num_rows):
        time = datetime.now() - timedelta(days=random.randint(0, 30), 
                                         hours=random.randint(0, 23),
                                         minutes=random.randint(0, 59))
        ulid = str(uuid.uuid4())[:24].upper()
        #symbol = random.choice(symbols)
        state = random.choice(states)
        tenor = random.choice(tenors)
        value_date_near = time + timedelta(days=random.randint(1, 5))

        global_tradable = random.choice([0, 1])
        global_indicative = 1 - global_tradable

        rate_id = random.randint(10000000000, 99999999999)
        tier = random.choice(tiers)

        if symbol == 'CNY/RUB':
            bid_price = random.randint(10000000, 14000000)
            ask_price = bid_price + random.randint(100000, 500000)
        elif symbol == 'USD/RUB':
            bid_price = random.randint(78000000, 110000000)
            ask_price = bid_price + random.randint(100000, 500000)
        elif symbol == 'EUR/RUB':
            bid_price = random.randint(88000000, 120000000)
            ask_price = bid_price + random.randint(100000, 500000)
        elif symbol == 'INR/RUB':
            bid_price = random.randint(800000, 1200000)
            ask_price = bid_price + random.randint(100000, 500000)
        else:
            bid_price = random.randint(800000, 120000000)
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


def get_next_file_number(base_name="test1", extension="xlsx"):
    """Определяет следующий номер файла для текущей даты"""
    today = datetime.now().strftime("%Y-%m-%d")
    pattern = f"{base_name}_{today}_"
    
    existing_files = []
    for file in os.listdir('.'):
        if file.startswith(pattern) and file.endswith(f".{extension}"):
            existing_files.append(file)
    
    if not existing_files:
        return 1
    
    numbers = []
    for file in existing_files:
        try:
            number_part = file.replace(f"{pattern}", "").replace(f".{extension}", "")
            number = int(number_part)
            numbers.append(number)
        except ValueError:
            continue
    
    return max(numbers) + 1 if numbers else 1


def create_data_files(num_rows=10, upload_enabled=True):
    """Создаёт Excel и Parquet файлы и (опционально) загружает их в облако"""
    
    file_number = get_next_file_number()
    today = datetime.now().strftime("%Y-%m-%d")
    symbols = ['CNY/RUB', 'USD/RUB', 'EUR/RUB', 'GBP/RUB', 'JPY/RUB']

    for symbol in symbols:
        data = generate_random_data(num_rows, symbol)
        df = pd.DataFrame(data)
        df.columns = ['time', 'ulid', 'symbol', 'state', 'tenor', 'valueDateNear', 
                      'globalTradable', 'globalIndicative', 'rateId', 'tier', 'priceLevels']

        excel_filename = f"test1_{today}_{file_number}.xlsx"
        parquet_filename = f"database_{today}_{file_number}.parquet"

        # Сохраняем Excel
        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='sheet1', index=False)
            workbook = writer.book
            worksheet = writer.sheets['sheet1']
            column_widths = {
                'A': 20, 'B': 30, 'C': 12, 'D': 8, 'E': 8, 'F': 20,
                'G': 15, 'H': 18, 'I': 15, 'J': 10, 'K': 50
            }
            for col, width in column_widths.items():
                worksheet.column_dimensions[col].width = width

        # Сохраняем Parquet
        df.to_parquet(parquet_filename, index=False, engine='pyarrow')

        print(f"✅ Excel файл '{excel_filename}' создан с {num_rows} строками")
        print(f"✅ Parquet файл '{parquet_filename}' создан")

        # Загрузка в облако
        if upload_enabled:
            upload_to_cloud(excel_filename)
            upload_to_cloud(parquet_filename)

        return excel_filename, parquet_filename, df


def create_consolidated_database(upload_enabled=True):
    """Создаёт консолидированную Parquet-базу из всех database_*.parquet файлов"""
    parquet_files = [f for f in os.listdir('.') if f.startswith('database_') and f.endswith('.parquet')]
    
    if not parquet_files:
        print("❌ Parquet файлы не найдены для консолидации")
        return None
    
    all_data = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            df['source_file'] = file
            all_data.append(df)
        except Exception as e:
            print(f"❌ Ошибка при чтении файла {file}: {e}")
    
    if all_data:
        consolidated_df = pd.concat(all_data, ignore_index=True)
        consolidated_filename = f"consolidated_database_{datetime.now().strftime('%Y-%m-%d')}.parquet"
        consolidated_df.to_parquet(consolidated_filename, index=False, engine='pyarrow')
        print(f"✅ Консолидированная база данных '{consolidated_filename}' создана")
        print(f"   Объединено {len(parquet_files)} файлов, всего {len(consolidated_df)} записей")
        
        if upload_enabled:
            upload_to_cloud(consolidated_filename)
            
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
    # Создаём новые файлы и загружаем в облако
    excel_file, parquet_file, df = create_data_files(num_rows=150, upload_enabled=True)
    
    print("\n" + "="*60)
    
    # Создаём и загружаем консолидированную базу
    consolidated_file = create_consolidated_database(upload_enabled=True)
    
    if consolidated_file:
        read_and_display_parquet(consolidated_file)