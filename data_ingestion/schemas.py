"""
Dinh nghia schema cho cac event TMDT.
Moi event type tuong ung voi mot loai hanh vi cua nguoi dung tren he thong.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import ClassVar

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ============================================================
# Python dataclasses  (dung cho Kafka producer)
# ============================================================

@dataclass
class PageViewEvent:
    """Nguoi dung xem mot trang san pham."""
    event_type: ClassVar[str] = "page_view"
    user_id: str = ""
    product_id: str = ""
    product_name: str = ""
    category: str = ""
    timestamp: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["event_type"] = self.event_type
        return d


@dataclass
class AddToCartEvent:
    """Nguoi dung them san pham vao gio hang."""
    event_type: ClassVar[str] = "add_to_cart"
    user_id: str = ""
    product_id: str = ""
    product_name: str = ""
    category: str = ""
    quantity: int = 0
    price: float = 0.0
    timestamp: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["event_type"] = self.event_type
        return d


@dataclass
class PurchaseEvent:
    """Nguoi dung hoan tat mua hang."""
    event_type: ClassVar[str] = "purchase"
    user_id: str = ""
    order_id: str = ""
    product_ids: list[str] = field(default_factory=list)
    product_names: list[str] = field(default_factory=list)
    quantities: list[int] = field(default_factory=list)
    total_amount: float = 0.0
    payment_method: str = ""
    timestamp: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["event_type"] = self.event_type
        return d


@dataclass
class ReviewEvent:
    """Nguoi dung danh gia san pham."""
    event_type: ClassVar[str] = "review"
    user_id: str = ""
    product_id: str = ""
    product_name: str = ""
    rating: int = 0
    comment: str = ""
    timestamp: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["event_type"] = self.event_type
        return d


@dataclass
class PriceChangeEvent:
    """He thong thay doi gia san pham."""
    event_type: ClassVar[str] = "price_change"
    product_id: str = ""
    product_name: str = ""
    category: str = ""
    old_price: float = 0.0
    new_price: float = 0.0
    timestamp: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["event_type"] = self.event_type
        return d


@dataclass
class SearchQueryEvent:
    """Nguoi dung tim kiem san pham."""
    event_type: ClassVar[str] = "search_query"
    user_id: str = ""
    query_text: str = ""
    results_count: int = 0
    timestamp: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["event_type"] = self.event_type
        return d


# ============================================================
# PySpark StructType  (dung cho Spark Streaming / Batch)
# ============================================================

# Schema chung (union) cho tat ca event types
UNIFIED_EVENT_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_ids", ArrayType(StringType()), True),
        StructField("product_names", ArrayType(StringType()), True),
        StructField("quantities", ArrayType(IntegerType()), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("rating", IntegerType(), True),
        StructField("comment", StringType(), True),
        StructField("old_price", DoubleType(), True),
        StructField("new_price", DoubleType(), True),
        StructField("query_text", StringType(), True),
        StructField("results_count", IntegerType(), True),
        StructField("timestamp", LongType(), False),
    ]
)

# Schema rieng cho tung loai event (dung khi doc tu topic rieng)
PAGE_VIEW_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("timestamp", LongType(), False),
    ]
)

PURCHASE_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_ids", ArrayType(StringType()), True),
        StructField("product_names", ArrayType(StringType()), True),
        StructField("quantities", ArrayType(IntegerType()), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("timestamp", LongType(), False),
    ]
)

PRICE_CHANGE_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("old_price", DoubleType(), True),
        StructField("new_price", DoubleType(), True),
        StructField("timestamp", LongType(), False),
    ]
)
