import pandas as pd



# Создаем и сохраняем DataFrame в одну строку
(pd.DataFrame({
    'Продукт': ['Ноутбук', 'Мышь', 'Клавиатура', 'Монитор'],
    'Цена': [50000, 1500, 3000, 25000],
    'Количество': [10, 50, 30, 15]
})
 .to_excel('kda_test_file.xlsx', index=False)
)