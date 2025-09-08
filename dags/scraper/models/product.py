from typing import List, Optional

from pydantic import BaseModel


class Price(BaseModel):
    price_type: str
    amount: str
    condition: Optional[str] = None


class Discount(BaseModel):
    discount_type: str
    amount: str
    condition: Optional[str] = None


class Product(BaseModel):
    id: str
    name: str
    brand: Optional[str] = None
    unit: str
    prices: List[Price] = []
    discount_percents: List[Discount] = []
    base_price_value: Optional[str] = None
    base_price_unit: Optional[str] = None
    market: Optional[str] = None
    valid_from: Optional[str] = None
    valid_to: Optional[str] = None
    info: Optional[str] = None
