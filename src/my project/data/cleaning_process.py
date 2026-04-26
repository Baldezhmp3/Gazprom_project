import pandas as pd
import pandera.pandas as pa
from schema import apartment_schema # Импорт схемы из твоего файла
import ast


def process_and_validate():
    # 1. Загрузка данных
    df = pd.read_csv("C:/Users/0/PycharmProjects/Gazprom/DATA/apartments.csv")

    # 2. Очистка (CSV превращает списки в строки "[...]", возвращаем их обратно)
    df['images'] = df['images'].apply(lambda x: x.split(';') if isinstance(x, str) else [])

    # 3. Валидация
    try:
        # Проверяем и сразу удаляем строки, которые не прошли фильтр
        clean_df = apartment_schema.validate(df, lazy=True)
        print("✅ Данные успешно прошли валидацию!")
    except pa.errors.SchemaErrors as err:
        print("⚠️ Обнаружены аномалии, фильтруем данные...")
        # Оставляем только те строки, к которым нет претензий
        clean_df = err.data[~err.data.index.isin(err.failure_cases["index"])]

    # 4. Сохранение в Parquet (как просит задание)
    clean_df.to_parquet("C:/Users/0/PycharmProjects/Gazprom/DATA/apartments_clean.parquet", index=False)
    print("🚀 Файл DATA/apartments_clean.parquet готов!")

if __name__ == "__main__":
    process_and_validate()