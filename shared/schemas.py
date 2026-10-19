from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field

from shared.models import OrderStatus


class OrderItemIn(BaseModel):
    sku: str = Field(min_length=1, max_length=64)
    quantity: int = Field(gt=0)


class CreateOrderRequest(BaseModel):
    customer_email: EmailStr
    items: list[OrderItemIn] = Field(min_length=1)
    is_vip: bool = False
    region: str = Field(default="us", min_length=2, max_length=8)


class OrderItemOut(BaseModel):
    sku: str
    quantity: int
    unit_price_cents: int

    model_config = {"from_attributes": True}


class OrderOut(BaseModel):
    id: UUID
    customer_email: str
    status: OrderStatus
    total_cents: int
    correlation_id: str
    is_vip: bool
    region: str
    created_at: datetime
    items: list[OrderItemOut]

    model_config = {"from_attributes": True}


class CreateOrderResponse(BaseModel):
    order: OrderOut
    event_id: str
    message: str
