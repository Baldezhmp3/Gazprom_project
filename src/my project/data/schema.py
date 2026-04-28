import pandera as pa
from pandera import Column, Check, DataFrameSchema
from pathlib import Path

import pandas as pd

apartment_schema = DataFrameSchema({
    "price_rub": Column(int, Check.in_range(1_000_000, 500_000_000)), # Адекватная цена
    "area_m2": Column(float, Check.in_range(10, 500)),              # Адекватная площадь
    "floor_current": Column(int, Check.in_range(1, 100), nullable=True),
    "latitude": Column(float, Check.in_range(40, 85), nullable=True),
    "longitude": Column(float, Check.in_range(20, 185), nullable=True)
})


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CSV = PROJECT_ROOT / "DATA" / "apartments.csv"
DEFAULT_PARQUET = PROJECT_ROOT / "DATA" / "apartments.parquet"


def main() -> None:
    df = pd.read_csv(DEFAULT_CSV, encoding="utf-8-sig")
    validated = apartment_schema.validate(df)
    DEFAULT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    validated.to_parquet(DEFAULT_PARQUET, index=False)
    print(f"Validated {len(validated)} rows from {DEFAULT_CSV}")
    print(f"Saved parquet to {DEFAULT_PARQUET}")


if __name__ == "__main__":
    main()