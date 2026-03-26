"""
event_schema.py
E-commerce clickstream event definitions and Faker-based data generation.

Event types follow a realistic e-commerce funnel:
    page_view → product_view → add_to_cart → checkout_start → purchase
    + search, login, logout, remove_from_cart, wishlist_add
"""

from __future__ import annotations

import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Optional

# ─── Static reference data ────────────────────────────────────────────────────

CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home & Kitchen",
    "Books",
    "Sports & Outdoors",
    "Beauty",
    "Toys",
    "Automotive",
    "Health",
    "Garden",
]

PRODUCTS: dict[str, dict] = {
    f"PROD-{i:05d}": {
        "name": f"Product {i}",
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(4.99, 999.99), 2),
    }
    for i in range(1, 501)
}

DEVICE_TYPES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge", "Samsung Internet"]
REFERRERS = [
    "google.com",
    "facebook.com",
    "instagram.com",
    "email_campaign",
    "direct",
    "twitter.com",
    "affiliate_network",
    "",
]
PAYMENT_METHODS = ["credit_card", "paypal", "apple_pay", "google_pay", "debit_card"]

EVENT_TYPES = [
    "page_view",
    "product_view",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "wishlist_add",
    "checkout_start",
    "checkout_complete",
    "user_login",
    "user_logout",
]

# Weighted distribution simulating real funnel drop-off
EVENT_WEIGHTS = [30, 25, 15, 12, 3, 4, 5, 3, 2, 1]

PAGE_URLS = [
    "/",
    "/products",
    "/category/{cat}",
    "/product/{pid}",
    "/cart",
    "/checkout",
    "/account",
    "/search?q={q}",
    "/deals",
    "/new-arrivals",
    "/wishlist",
]

SEARCH_QUERIES = [
    "laptop",
    "running shoes",
    "coffee maker",
    "wireless headphones",
    "yoga mat",
    "kindle",
    "air fryer",
    "gaming chair",
    "standing desk",
    "protein powder",
    "bluetooth speaker",
]


# ─── Event dataclass ─────────────────────────────────────────────────────────


@dataclass
class ClickstreamEvent:
    event_id: str
    event_type: str
    event_timestamp: str  # ISO-8601 UTC
    user_id: str
    session_id: str
    device_type: str
    browser: str
    ip_address: str
    referrer: str
    page_url: str
    # Optional funnel fields
    product_id: Optional[str] = None
    product_name: Optional[str] = None
    category: Optional[str] = None
    price: Optional[float] = None
    quantity: Optional[int] = None
    search_query: Optional[str] = None
    cart_value: Optional[float] = None
    order_id: Optional[str] = None
    payment_method: Optional[str] = None
    # Computed
    is_authenticated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


# ─── Generator ───────────────────────────────────────────────────────────────


class ClickstreamEventGenerator:
    """
    Stateful generator that maintains per-user sessions and cart state
    to produce realistic, coherent clickstream sequences.
    """

    def __init__(self, num_users: int = 500, num_sessions: int = 200):
        self._num_users = num_users
        self._user_ids = [f"U-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_users)]
        self._sessions: dict[str, str] = {}  # user_id → session_id
        self._carts: dict[str, float] = {}  # session_id → running cart value
        self._auth: set[str] = set()  # authenticated user_ids

    # ── Public ───────────────────────────────────────────────────────────────

    def generate(self) -> ClickstreamEvent:
        """Generate one realistic clickstream event."""
        user_id = random.choice(self._user_ids)
        session_id = self._get_or_create_session(user_id)
        event_type = self._pick_event_type()
        return self._build_event(user_id, session_id, event_type)

    def generate_batch(self, n: int) -> list[ClickstreamEvent]:
        return [self.generate() for _ in range(n)]

    # ── Private ──────────────────────────────────────────────────────────────

    def _get_or_create_session(self, user_id: str) -> str:
        if user_id not in self._sessions or random.random() < 0.05:
            self._sessions[user_id] = f"S-{uuid.uuid4().hex[:12].upper()}"
        return self._sessions[user_id]

    def _pick_event_type(self) -> str:
        return random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    def _build_event(
        self, user_id: str, session_id: str, event_type: str
    ) -> ClickstreamEvent:
        now = datetime.now(timezone.utc).isoformat()

        base = dict(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            event_timestamp=now,
            user_id=user_id,
            session_id=session_id,
            device_type=random.choice(DEVICE_TYPES),
            browser=random.choice(BROWSERS),
            ip_address=self._random_ip(),
            referrer=random.choice(REFERRERS),
            page_url="/",
            is_authenticated=user_id in self._auth,
        )

        # Event-specific enrichment
        if event_type == "page_view":
            base["page_url"] = (
                random.choice(PAGE_URLS)
                .replace("{cat}", random.choice(CATEGORIES).lower().replace(" ", "-"))
                .replace("{pid}", random.choice(list(PRODUCTS.keys())))
            )

        elif event_type == "product_view":
            pid = random.choice(list(PRODUCTS.keys()))
            prod = PRODUCTS[pid]
            base.update(
                page_url=f"/product/{pid}",
                product_id=pid,
                product_name=prod["name"],
                category=prod["category"],
                price=prod["price"],
            )

        elif event_type == "search":
            q = random.choice(SEARCH_QUERIES)
            base.update(
                page_url=f"/search?q={q.replace(' ', '+')}",
                search_query=q,
            )

        elif event_type == "add_to_cart":
            pid = random.choice(list(PRODUCTS.keys()))
            prod = PRODUCTS[pid]
            qty = random.randint(1, 5)
            cart_val = round(prod["price"] * qty, 2)
            self._carts[session_id] = self._carts.get(session_id, 0) + cart_val
            base.update(
                page_url=f"/product/{pid}",
                product_id=pid,
                product_name=prod["name"],
                category=prod["category"],
                price=prod["price"],
                quantity=qty,
                cart_value=round(self._carts[session_id], 2),
            )

        elif event_type == "remove_from_cart":
            pid = random.choice(list(PRODUCTS.keys()))
            prod = PRODUCTS[pid]
            base.update(
                page_url="/cart",
                product_id=pid,
                price=prod["price"],
                cart_value=round(
                    max(0, self._carts.get(session_id, 0) - prod["price"]), 2
                ),
            )

        elif event_type == "wishlist_add":
            pid = random.choice(list(PRODUCTS.keys()))
            prod = PRODUCTS[pid]
            base.update(
                product_id=pid,
                product_name=prod["name"],
                category=prod["category"],
                price=prod["price"],
            )

        elif event_type == "checkout_start":
            base.update(
                page_url="/checkout",
                cart_value=round(
                    self._carts.get(session_id, random.uniform(20, 500)), 2
                ),
            )

        elif event_type == "checkout_complete":
            order_value = round(self._carts.pop(session_id, random.uniform(20, 500)), 2)
            base.update(
                page_url="/order-confirmation",
                order_id=f"ORD-{uuid.uuid4().hex[:10].upper()}",
                cart_value=order_value,
                payment_method=random.choice(PAYMENT_METHODS),
            )
            # New session after purchase
            self._sessions.pop(user_id, None)

        elif event_type == "user_login":
            self._auth.add(user_id)
            base.update(page_url="/account", is_authenticated=True)

        elif event_type == "user_logout":
            self._auth.discard(user_id)
            self._sessions.pop(user_id, None)
            base.update(page_url="/account", is_authenticated=False)

        return ClickstreamEvent(**base)

    @staticmethod
    def _random_ip() -> str:
        return ".".join(str(random.randint(1, 254)) for _ in range(4))
