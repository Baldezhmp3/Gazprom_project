import pandas as pd
import numpy as np
from catboost import CatBoostRegressor, Pool, cv
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_percentage_error, mean_absolute_error
from pathlib import Path


def train_optimized_model():
    project_root = Path(__file__).resolve().parents[3]
    df = pd.read_parquet(project_root / "DATA" / "apartments_features.parquet")

    # Подготовка X и y
    X = df.drop(columns=['price_rub', 'id', 'url', 'title', 'description_text'], errors='ignore')
    y = df['price_rub']

    # Разделяем на Train (для поиска параметров) и Holdout (финальный тест)
    X_train, X_holdout, y_train, y_holdout = train_test_split(X, y, test_size=0.2, random_state=42)

    # 1. Инициализируем модель для поиска параметров
    model = CatBoostRegressor(
        loss_function='MAE',
        eval_metric='MAPE',
        random_seed=42,
        verbose=False
    )

    # 2. Сетка параметров для поиска
    # Мы ищем оптимальную глубину, силу регуляризации (L2) и скорость обучения
    grid = {
        'learning_rate': [0.01, 0.03, 0.05, 0.1],
        'depth': [4, 6, 8],
        'l2_leaf_reg': [1, 3, 7, 15],
        'iterations': [1500]
    }

    print("🔎 Начинаем подбор гиперпараметров (Grid Search)...")
    grid_search_result = model.grid_search(grid, X=X_train, y=y_train, cv=3, plot=False)

    print("\n✅ Лучшие параметры найдены:")
    print(grid_search_result['params'])

    # 3. Кросс-валидация на лучших параметрах для проверки стабильности
    print("\n🔄 Запускаем кросс-валидацию на всей выборке...")
    cv_params = model.get_params()
    cv_data = Pool(data=X, label=y)

    scores = cv(
        pool=cv_data,
        params=cv_params,
        fold_count=5,
        type='Requirement',  # 'Requirement' для стабильности
        partition_random_seed=42,
        verbose=False
    )

    # 4. Финальное обучение на "золотых" параметрах
    model.fit(X_train, y_train, eval_set=(X_holdout, y_holdout), verbose=200, use_best_model=True)

    # Результаты на отложенной выборке
    preds = model.predict(X_holdout)
    print("\n" + "=" * 30)
    print(f"ФИНАЛЬНЫЙ R2: {r2_score(y_holdout, preds):.4f}")
    print(f"ФИНАЛЬНЫЙ MAPE: {mean_absolute_percentage_error(y_holdout, preds) * 100:.2f}%")
    print(f"ФИНАЛЬНЫЙ MAE: {mean_absolute_error(y_holdout, preds):.2f}")
    print("=" * 30)

    # Сохраняем лучшую версию
    model.save_model(str(project_root / "models" / "catboost_optimized.cbm"))


if __name__ == "__main__":
    train_optimized_model()