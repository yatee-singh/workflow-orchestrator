"""Order API — Phase 4 additions: RPC availability, priority, delayed reminders, saga."""

from __future__ import annotations

import json
import logging
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from api.deps import get_db
from shared.messaging.publisher import publish_event
from shared.messaging.rpc import rpc_call
from shared.messaging.topology import (
    EXCHANGE_FANOUT,
    EXCHANGE_HEADERS,
    EXCHANGE_RPC,
    EXCHANGE_DELAY,
    QUEUE_REMINDER,
    RK_INVENTORY_AVAILABILITY,
    RK_NOTIFY_REMINDER,
)
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
    """Persist order, fanout `order.placed`, optionally VIP headers + priority."""
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

    # Priority header for q.payment (x-max-priority=10, higher = earlier)
    priority_hdrs = None
    if body.priority > 0:
        priority_hdrs = {"priority": body.priority}

    # FANOUT
    envelope = await publish_event(
        channel,
        exchange_name=EXCHANGE_FANOUT,
        routing_key="",
        event_type="order.placed",
        order_id=str(order.id),
        correlation_id=correlation_id,
        payload=payload,
        headers=priority_hdrs,
    )

    # HEADERS VIP alert
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
        + (" + VIP headers alert" if order.is_vip and order.region == "eu" else "")
        + (f" + priority={body.priority}" if body.priority > 0 else ""),
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


@router.get("/inventory/{sku}/availability")
async def check_availability(sku: str, request: Request) -> dict:
    """RPC call to inventory worker: ask if sku is available, get response."""
    channel = request.app.state.rmq_channel
    result = await rpc_call(
        channel,
        body={"action": "check_availability", "sku": sku},
    )
    if result is None:
        raise HTTPException(status_code=503, detail="Inventory check timed out")
    return result


@router.post("/{order_id}/remind", status_code=status.HTTP_202_ACCEPTED)
async def schedule_reminder(order_id: UUID, request: Request, db: AsyncSession = Depends(get_db)) -> dict:
    """Schedule a delayed reminder email for a pending order.

    Message sits in q.reminder for 60s, then DLX → notify.reminder → notify worker.
    """
    result = await db.execute(
        select(Order).where(Order.id == order_id).options(selectinload(Order.items))
    )
    order = result.scalar_one_or_none()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    if order.status == OrderStatus.shipped:
        raise HTTPException(status_code=400, detail="Order already shipped")

    channel = request.app.state.rmq_channel
    payload = {
        "order_id": str(order.id),
        "customer_email": order.customer_email,
        "status": order.status.value,
    }
    envelope = {
        "event_id": str(uuid4()),
        "correlation_id": order.correlation_id,
        "order_id": str(order.id),
        "type": "notify.reminder",
        "occurred_at": "scheduled",
        "payload": payload,
    }
    exchange = await channel.get_exchange(EXCHANGE_DELAY)
    exchange.publish(
        __import__("aio_pika").Message(
            body=json.dumps(envelope).encode(),
            delivery_mode=__import__("aio_pika").DeliveryMode.PERSISTENT,
            content_type="application/json",
            headers={"x-dead-letter-routing-key": RK_NOTIFY_REMINDER},
        ),
        routing_key=QUEUE_REMINDER,
    )
    logger.info("Scheduled reminder for order=%s corr=%s", order_id, order.correlation_id)
    return {
        "order_id": str(order.id),
        "message": "Reminder scheduled (delivered after ~60s TTL)",
    }