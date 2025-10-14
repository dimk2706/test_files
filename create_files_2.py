import io
from abc import ABC, abstractmethod
from functools import cached_property
from uuid import uuid4
from datetime import datetime
import json

import logfire
import pandas as pd
from aiobotocore.client import AioBaseClient
from botocore.client import Config
from environs import env
from sqlalchemy.engine import ScalarResult

env = Env()
env.read_env() 


params = {
    "service_name": "s3",
    "aws_access_key_id": env("OBS_ACCESS_KEY"),
    "aws_secret_access_key": env("OBS_SECRET_KEY"),
    "region_name": env("OBS_REGION"),
    "endpoint_url": env("OBS_ENDPOINT"),
    "config": Config(s3={"addressing_style": "virtual"})
}


class DataFilesHandler:
    """
    Handler for generating Excel and Parquet files and uploading them to S3
    """

    def __init__(
        self,
        botoclient: AioBaseClient,
        num_rows: int = 10,
        is_backup: bool = False
    ) -> None:

        with logfire.span("Generate random data"):
            self.data = self.generate_random_data(num_rows)
            self.df = pd.DataFrame(self.data)
            
        with logfire.span("Set the rest of the attributes"):
            self.client = botoclient
            self._is_backup = is_backup
            self._excel_body = io.BytesIO()
            self._parquet_body = io.BytesIO()

    def generate_random_data(self, num_rows=10):
        """Генерирует случайные данные в указанном формате"""
        
        symbols = ['CNY/RUB', 'USD/RUB', 'EUR/RUB', 'GBP/RUB', 'JPY/RUB']
        states = [0, 1]
        tenors = ['TOM', 'SPOT', 'TOD', 'ON']
        tiers = ['TRADER1', 'TRADER2', 'TRADER3', 'TRADER4', 'TRADER5']
        
        data = []
        
        for _ in range(num_rows):
            time = datetime.now() - timedelta(days=random.randint(0, 30), 
                                            hours=random.randint(0, 23),
                                            minutes=random.randint(0, 59))
            ulid = str(uuid4())[:24].upper()
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

    @cached_property
    def excel_body(self) -> io.BytesIO:
        """Create Excel file in memory"""
        with logfire.span("Create Excel file in memory"):
            with pd.ExcelWriter(self._excel_body, engine='openpyxl') as writer:
                self.df.to_excel(writer, sheet_name='Лист1', index=False)
            
            self._excel_body.seek(0)
            return self._excel_body

    @cached_property
    def parquet_body(self) -> io.BytesIO:
        """Create Parquet file in memory"""
        with logfire.span("Create Parquet file in memory"):
            self.df.to_parquet(self._parquet_body, index=False, engine='pyarrow')
            self._parquet_body.seek(0)
            return self._parquet_body

    @cached_property
    def excel_key(self) -> str:
        """Generate key for Excel file"""
        timestamp = datetime.now().strftime("%Y-%m-%d")
        if self._is_backup:
            return f"backups/data_excel_{timestamp}.xlsx"
        else:
            return f"data/excel/data_{timestamp}_{uuid4().hex[:8]}.xlsx"

    @cached_property
    def parquet_key(self) -> str:
        """Generate key for Parquet file"""
        timestamp = datetime.now().strftime("%Y-%m-%d")
        if self._is_backup:
            return f"backups/data_parquet_{timestamp}.parquet"
        else:
            return f"data/parquet/data_{timestamp}_{uuid4().hex[:8]}.parquet"

    async def upload_excel(self) -> str:
        """Upload Excel file to storage"""
        with logfire.span("Upload Excel to storage"):
            await self.client.put_object(
                Bucket=env("OBS_BUCKET"),
                Key=self.excel_key,
                Body=self.excel_body,
                ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            return self.excel_key

    async def upload_parquet(self) -> str:
        """Upload Parquet file to storage"""
        with logfire.span("Upload Parquet to storage"):
            await self.client.put_object(
                Bucket=env("OBS_BUCKET"),
                Key=self.parquet_key,
                Body=self.parquet_body,
                ContentType="application/octet-stream"
            )
            return self.parquet_key

    async def upload_all(self) -> dict:
        """Upload both Excel and Parquet files"""
        with logfire.span("Upload all files to storage"):
            excel_key = await self.upload_excel()
            parquet_key = await self.upload_parquet()
            
            return {
                "excel_file": excel_key,
                "parquet_file": parquet_key,
                "records_count": len(self.df)
            }


# Пример использования в вашем основном коде
async def main():
    """Пример использования обработчика"""
    # Создаем клиент (предполагается, что он уже настроен)
    # client = get_boto_client() 
    
    # Создаем обработчик
    handler = DataFilesHandler(
        botoclient=client,
        num_rows=100,  # количество строк данных
        is_backup=False
    )
    
    # Загружаем файлы в хранилище
    result = await handler.upload_all()
    
    print(f"Файлы успешно загружены:")
    print(f"Excel: {result['excel_file']}")
    print(f"Parquet: {result['parquet_file']}")
    print(f"Записей: {result['records_count']}")


# Альтернативный вариант - расширение существующего класса
class ExcelHandler(ScalarsHandler):
    """Существующий класс для работы с Excel"""
    
    @property
    def extension(self) -> str:
        return "xlsx"

    @property
    def content_type(self) -> str:
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    @cached_property
    def body(self) -> io.BytesIO:
        with logfire.span("Create Excel file from dataframe"):
            with pd.ExcelWriter(self._body, engine='openpyxl') as writer:
                self.df.to_excel(writer, sheet_name='Лист1', index=False)
            
            self._body.seek(0)
            return self._body


class ParquetHandler(ScalarsHandler):
    """Новый класс для работы с Parquet"""
    
    @property
    def extension(self) -> str:
        return "parquet"

    @property
    def content_type(self) -> str:
        return "application/octet-stream"

    @cached_property
    def body(self) -> io.BytesIO:
        with logfire.span("Create Parquet file from dataframe"):
            self.df.to_parquet(self._body, index=False, engine='pyarrow')
            self._body.seek(0)
            return self._body

    @cached_property
    def key(self) -> str:
        name = "bankiru_reviews_db_backup" if self._is_backup else uuid4()
        return f"{name}.{self.__class__.extension}"


# Фабрика для создания обработчиков
class DataHandlerFactory:
    @staticmethod
    def create_handler(handler_type: str, scalars: list[ScalarResult], botoclient: AioBaseClient, is_backup: bool = False):
        if handler_type == "excel":
            return ExcelHandler(scalars, botoclient, is_backup)
        elif handler_type == "parquet":
            return ParquetHandler(scalars, botoclient, is_backup)
        elif handler_type == "data_generator":
            return DataFilesHandler(botoclient, num_rows=100, is_backup=is_backup)
        else:
            raise ValueError(f"Unknown handler type: {handler_type}")


# Пример использования фабрики
async def process_data():
    """Пример обработки данных с загрузкой в хранилище"""
    # client = get_boto_client()
    
    # Для существующих данных из БД
    # excel_handler = DataHandlerFactory.create_handler("excel", scalars, client)
    # await excel_handler.upload_contents()
    
    # Для сгенерированных данных
    data_handler = DataHandlerFactory.create_handler("data_generator", None, client)
    result = await data_handler.upload_all()
    
    return result


if __name__ == "__main__":
    handler = DataFilesHandler(botoclient, num_rows=50, is_backup=False)
    result = handler.upload_all()