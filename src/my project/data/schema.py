import pandera as pa
from pandera import Column, Check, DataFrameSchema

apartment_schema = DataFrameSchema({
    "price_rub": Column(int, Check.in_range(1_000_000, 500_000_000)), # Адекватная цена
    "area_m2": Column(float, Check.in_range(10, 500)),              # Адекватная площадь
    "floor_current": Column(int, Check.in_range(1, 100), nullable=True),
    "latitude": Column(float, Check.in_range(40, 85), nullable=True),
    "longitude": Column(float, Check.in_range(20, 185), nullable=True)
})