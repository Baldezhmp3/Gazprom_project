import pandas as pd
import numpy as np
from scipy.spatial import KDTree
from geopy.distance import geodesic
from pathlib import Path

from constants import KREMLIN, METRO_STATIONS


def add_geo_features(df):
    metro_coords = list(METRO_STATIONS.values())
    metro_tree = KDTree(metro_coords)

    def process_row(row):
        point = (row['latitude'], row['longitude'])

        # 1. Расстояние до центра
        dist_center = geodesic(point, KREMLIN).km

        # 2. Расстояние до ближайшего метро
        _, idx = metro_tree.query([row['latitude'], row['longitude']])
        dist_metro = geodesic(point, metro_coords[idx]).km

        return pd.Series([dist_center, dist_metro])

    print("Рассчитываем гео-признаки...")
    df[['dist_to_center', 'dist_to_metro']] = df.apply(process_row, axis=1)

    # Оставляем только относительный этаж
    # Он заменяет собой и номер этажа, и флаги первого/последнего
    df['floor_ratio'] = np.where(
        df['floor_total'] > 0,
        df['floor_current'] / df['floor_total'],
        0.5
    )

    return df


def main():
    project_root = Path(__file__).resolve().parents[3]
    input_path = project_root / "DATA" / "apartments_clean.parquet"
    output_path = project_root / "DATA" / "apartments_features.parquet"

    df = pd.read_parquet(input_path)
    df = add_geo_features(df)

    # Сохраняем только нужные колонки для обучения
    # Координаты оставляем для самообучения модели
    df.to_parquet(output_path, index=False)
    print(f"Очищенные фичи сохранены в {output_path}")


if __name__ == "__main__":
    main()