import pandas as pd
from pathlib import Path


def inspect_features():
    # Определяем путь к файлу
    project_root = Path(__file__).resolve().parents[3]
    file_path = project_root / "DATA" / "apartments_features.parquet"

    if not file_path.exists():
        print(f"❌ Файл не найден по пути: {file_path}")
        print("Сначала запусти build_features.py")
        return

    # Загружаем данные
    df = pd.read_parquet(file_path)

    # 1. Основная информация о структуре
    print("\n" + "=" * 50)
    print("📊 ОБЩАЯ ИНФОРМАЦИЯ О ДАТАСЕТЕ")
    print("=" * 50)
    print(f"Всего записей: {df.shape[0]}")
    print(f"Всего признаков: {df.shape[1]}")
    print("\nСписок колонок:")
    print(df.columns.tolist())

    # 2. Просмотр первых строк (только важные фичи для компактности)
    print("\n" + "=" * 50)
    print("👀 ПЕРВЫЕ 10 СТРОК (Ключевые признаки)")
    print("=" * 50)
    display_cols = [
        'price_rub', 'area_m2', 'dist_to_center',
        'dist_to_metro', 'floor_ratio', 'latitude', 'longitude'
    ]
    # Выводим только те колонки, которые есть в наличии
    existing_cols = [c for c in display_cols if c in df.columns]
    print(df[existing_cols].head(10).to_string())

    # 3. Статистический анализ
    print("\n" + "=" * 50)
    print("📈 СТАТИСТИКА ПО РАССТОЯНИЯМ И ЦЕНАМ")
    print("=" * 50)
    stats_cols = ['price_rub', 'dist_to_center', 'dist_to_metro', 'floor_ratio']
    existing_stats = [c for c in stats_cols if c in df.columns]
    print(df[existing_stats].describe().round(2))

    # 4. Проверка на пустые значения
    print("\n" + "=" * 50)
    print("❓ ПРОВЕРКА НА ПРОПУСКИ (NaN)")
    print("=" * 50)
    null_counts = df[existing_stats].isnull().sum()
    print(null_counts[null_counts > 0] if null_counts.any() else "Пропусков нет, данные чистые!")


if __name__ == "__main__":
    inspect_features()