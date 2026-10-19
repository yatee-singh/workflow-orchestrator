from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Integer, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from shared.db import Base


class OrderStatus(str, enum.Enum):
    pending = "pending"
    reserved = "reserved"
    reservation_failed = "reservation_failed"
    paid = "paid"
    payment_failed = "payment_failed"
    shipped = "shipped"


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_email: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[OrderStatus] = mapped_column(
        Enum(OrderStatus, name="order_status", native_enum=True),
        default=OrderStatus.pending,
        nullable=False,
    )
    total_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    correlation_id: Mapped[str] = mapped_column(String(64), nullable=False)
    is_vip: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    region: Mapped[str] = mapped_column(String(8), nullable=False, default="us")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    items: Mapped[list[OrderItem]] = relationship(back_populates="order", cascade="all, delete-orphan")


class OrderItem(Base):
    __tablename__ = "order_items"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"), nullable=False)
    sku: Mapped[str] = mapped_column(String(64), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price_cents: Mapped[int] = mapped_column(Integer, nullable=False)

    order: Mapped[Order] = relationship(back_populates="items")


class InventoryItem(Base):
    __tablename__ = "inventory"

    sku: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    quantity_available: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unit_price_cents: Mapped[int] = mapped_column(Integer, nullable=False)
