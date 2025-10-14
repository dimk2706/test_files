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
import os
from aiobotocore.session import AioSession
from aiobotocore.config import AioConfig

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


async def upload_to_cloud_async(filepath: str):
    """Асинхронная загрузка файла в S3-совместимое облако с aiobotocore (v2+)"""
    object_name = os.path.basename(filepath)

    # Определяем Content-Type
    if filepath.lower().endswith('.xlsx'):
        content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif filepath.lower().endswith('.parquet'):
        content_type = 'application/octet-stream'
    else:
        content_type = 'application/octet-stream'

    # Создаём асинхронную сессию
    session = AioSession()
    config = AioConfig(s3={'addressing_style': 'virtual'})

    async with session.create_client(
        's3',
        region_name=OBS_REGION,
        endpoint_url=OBS_ENDPOINT,
        aws_access_key_id=OBS_ACCESS_KEY,
        aws_secret_access_key=OBS_SECRET_KEY,
        config=config
    ) as client:
        try:
            with open(filepath, 'rb') as f:
                await client.put_object(
                    Bucket=OBS_BUCKET,
                    Key=object_name,
                    Body=f.read(),  # Читаем всё содержимое
                    ContentType=content_type
                )
            print(f"✅ Загружено: {object_name}")
        except Exception as e:
            print(f"❌ Ошибка загрузки {filepath}: {e}")


# --- Остальной код (без изменений, кроме вызова асинхронной загрузки) ---

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
    
    # Excel
    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        ws = writer.sheets['Sheet1']
        widths = {'A':20,'B':30,'C':12,'D':8,'E':8,'F':20,'G':15,'H':18,'I':15,'J':10,'K':50}
        for col, w in widths.items():
            ws.column_dimensions[col].width = w

    # Parquet
    df.to_parquet(parquet_filename, index=False, engine='pyarrow')
    
    print(f"✅ Созданы файлы: {excel_filename}, {parquet_filename}")
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


# --- Асинхронная основная функция ---

async def main():
    # 1. Генерация данных
    excel_file, parquet_file, _ = create_data_files_sync(num_rows=150)
    
    # 2. Асинхронная загрузка
    await asyncio.gather(
        upload_to_cloud_async(excel_file),
        upload_to_cloud_async(parquet_file)
    )
    
    print("\n" + "="*60)
    
    # 3. Консолидация и загрузка
    consolidated_file = create_consolidated_database_sync()
    if consolidated_file:
        await upload_to_cloud_async(consolidated_file)


# --- Запуск ---
if __name__ == "__main__":
    asyncio.run(main())