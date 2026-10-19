"""Order API — publishes to fanout (+ optional headers VIP alert)."""

from __future__ import annotations

import logging
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from api.deps import get_db
from shared.messaging.publisher import publish_event
from shared.messaging.topology import EXCHANGE_FANOUT, EXCHANGE_HEADERS
from shared.models import InventoryItem, Order, OrderItem, OrderStatus
from shared.schemas import CreateOrderRequest, CreateOrderResponse, OrderOut

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("", response_model=CreateOrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    body: CreateOrderRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> CreateOrderResponse:
    """Persist order, fanout `order.placed`, and optionally headers-route a VIP alert."""
    skus = [item.sku for item in body.items]
    result = await db.execute(select(InventoryItem).where(InventoryItem.sku.in_(skus)))
    catalog = {row.sku: row for row in result.scalars().all()}
    missing = [sku for sku in skus if sku not in catalog]
    if missing:
        raise HTTPException(status_code=400, detail=f"Unknown SKUs: {missing}")

    correlation_id = str(uuid4())
    order = Order(
        customer_email=str(body.customer_email),
        status=OrderStatus.pending,
        correlation_id=correlation_id,
        total_cents=0,
        is_vip=body.is_vip,
        region=body.region.lower(),
    )
    for line in body.items:
        inv = catalog[line.sku]
        order.items.append(
            OrderItem(
                sku=line.sku,
                quantity=line.quantity,
                unit_price_cents=inv.unit_price_cents,
            )
        )
        order.total_cents += inv.unit_price_cents * line.quantity

    db.add(order)
    await db.commit()
    await db.refresh(order, attribute_names=["items"])

    channel = request.app.state.rmq_channel
    payload = {
        "order_id": str(order.id),
        "customer_email": order.customer_email,
        "is_vip": order.is_vip,
        "region": order.region,
        "items": [
            {"sku": i.sku, "quantity": i.quantity, "unit_price_cents": i.unit_price_cents}
            for i in order.items
        ],
        "total_cents": order.total_cents,
    }

    # FANOUT: every bound queue (inventory, analytics, audit) gets a copy
    envelope = await publish_event(
        channel,
        exchange_name=EXCHANGE_FANOUT,
        routing_key="",  # ignored by fanout
        event_type="order.placed",
        order_id=str(order.id),
        correlation_id=correlation_id,
        payload=payload,
    )

    # HEADERS demo: VIP+EU orders also alert notify via header match (not routing key)
    if order.is_vip and order.region == "eu":
        await publish_event(
            channel,
            exchange_name=EXCHANGE_HEADERS,
            routing_key="",
            event_type="order.vip_alert",
            order_id=str(order.id),
            correlation_id=correlation_id,
            payload=payload,
            headers={"priority": "high", "region": "eu"},
        )

    return CreateOrderResponse(
        order=OrderOut.model_validate(order),
        event_id=envelope["event_id"],
        message="Order published via fanout"
        + (" + VIP headers alert" if order.is_vip and order.region == "eu" else ""),
    )


@router.get("/{order_id}", response_model=OrderOut)
async def get_order(order_id: UUID, db: AsyncSession = Depends(get_db)) -> OrderOut:
    result = await db.execute(
        select(Order).where(Order.id == order_id).options(selectinload(Order.items))
    )
    order = result.scalar_one_or_none()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return OrderOut.model_validate(order)
