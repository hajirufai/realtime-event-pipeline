"""Configurable synthetic event producer for the real-time pipeline."""
import asyncio
import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import AsyncIterator

from .schemas import Event, EventType
from .config import ProducerConfig


# Realistic product catalog
PRODUCTS = [
    {"id": "P001", "name": "Wireless Headphones", "price": 79.99, "category": "electronics"},
    {"id": "P002", "name": "Running Shoes", "price": 129.99, "category": "sports"},
    {"id": "P003", "name": "Python Cookbook", "price": 49.99, "category": "books"},
    {"id": "P004", "name": "Mechanical Keyboard", "price": 149.99, "category": "electronics"},
    {"id": "P005", "name": "Yoga Mat", "price": 29.99, "category": "sports"},
    {"id": "P006", "name": "Coffee Maker", "price": 89.99, "category": "home"},
    {"id": "P007", "name": "Data Engineering Book", "price": 59.99, "category": "books"},
    {"id": "P008", "name": "Standing Desk", "price": 399.99, "category": "office"},
]

SEARCH_TERMS = [
    "wireless headphones", "best running shoes", "python books",
    "mechanical keyboard", "yoga mat", "coffee maker", "standing desk",
    "data engineering", "gift ideas", "sale",
]

PAGES = ["/", "/products", "/cart", "/checkout", "/search", "/account", "/deals"]


class EventGenerator:
    """Generates realistic e-commerce events with session-aware behavior."""

    def __init__(self, config: ProducerConfig | None = None):
        self.config = config or ProducerConfig()
        self._active_sessions: dict[str, dict] = {}

    def _get_or_create_session(self) -> tuple[str, str]:
        """Get an existing session or create a new one."""
        if self._active_sessions and random.random() < 0.7:
            session_id = random.choice(list(self._active_sessions.keys()))
            return session_id, self._active_sessions[session_id]["user_id"]

        user_id = f"user_{random.randint(1000, 9999)}"
        session_id = str(uuid.uuid4())[:8]
        self._active_sessions[session_id] = {
            "user_id": user_id,
            "events": 0,
            "cart": [],
        }

        # Expire old sessions
        if len(self._active_sessions) > 100:
            oldest = list(self._active_sessions.keys())[:20]
            for k in oldest:
                del self._active_sessions[k]

        return session_id, user_id

    def _generate_event(self) -> Event:
        """Generate a single realistic event."""
        session_id, user_id = self._get_or_create_session()
        session = self._active_sessions[session_id]

        # Weight event types based on funnel
        weights = [0.5, 0.2, 0.05, 0.15, 0.1]  # page_view heavy
        event_type = random.choices(list(EventType), weights=weights, k=1)[0]

        properties: dict = {}
        metadata = {
            "source": random.choice(["web", "mobile_app", "tablet"]),
            "browser": random.choice(["chrome", "firefox", "safari", "edge"]),
            "os": random.choice(["windows", "macos", "linux", "ios", "android"]),
        }

        if event_type == EventType.PAGE_VIEW:
            properties["page"] = random.choice(PAGES)
            properties["referrer"] = random.choice(["google", "direct", "social", "email", ""])

        elif event_type == EventType.SEARCH:
            properties["query"] = random.choice(SEARCH_TERMS)
            properties["results_count"] = random.randint(0, 50)

        elif event_type == EventType.ADD_TO_CART:
            product = random.choice(PRODUCTS)
            properties["product_id"] = product["id"]
            properties["product_name"] = product["name"]
            properties["price"] = product["price"]
            properties["quantity"] = random.randint(1, 3)
            session["cart"].append(product)

        elif event_type == EventType.PURCHASE:
            if session["cart"]:
                properties["items"] = [p["id"] for p in session["cart"]]
                properties["total"] = sum(p["price"] for p in session["cart"])
                properties["payment_method"] = random.choice(["card", "paypal", "apple_pay"])
                session["cart"] = []
            else:
                # No cart -> make it a page view instead
                event_type = EventType.PAGE_VIEW
                properties["page"] = "/checkout"

        elif event_type == EventType.REMOVE_FROM_CART:
            if session["cart"]:
                removed = session["cart"].pop()
                properties["product_id"] = removed["id"]
            else:
                event_type = EventType.PAGE_VIEW
                properties["page"] = "/cart"

        session["events"] += 1

        return Event(
            event_type=event_type,
            user_id=user_id,
            session_id=session_id,
            timestamp=datetime.now(timezone.utc),
            properties=properties,
            metadata=metadata,
        )

    async def stream(self, events_per_second: int | None = None) -> AsyncIterator[Event]:
        """Continuously generate events at the specified rate."""
        eps = events_per_second or self.config.events_per_second
        interval = 1.0 / eps

        while True:
            yield self._generate_event()
            await asyncio.sleep(interval)

    def generate_batch(self, count: int) -> list[Event]:
        """Generate a batch of events."""
        return [self._generate_event() for _ in range(count)]


async def main():
    """CLI entry point for event generation."""
    import argparse

    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce events")
    parser.add_argument("--events-per-second", type=int, default=10)
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--output", choices=["stdout", "kafka"], default="stdout")
    args = parser.parse_args()

    generator = EventGenerator()
    count = 0
    start = time.time()

    async for event in generator.stream(args.events_per_second):
        if time.time() - start > args.duration:
            break
        if args.output == "stdout":
            print(event.model_dump_json())
        count += 1

    print(f"\nGenerated {count} events in {args.duration}s", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
