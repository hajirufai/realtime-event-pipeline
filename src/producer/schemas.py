"""Event schemas for the real-time pipeline."""
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum
from typing import Any
import uuid


class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"
    SEARCH = "search"
    REMOVE_FROM_CART = "remove_from_cart"


class Event(BaseModel):
    """Core event model for the pipeline."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    user_id: str
    session_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    properties: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_kafka_message(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_message(cls, data: bytes) -> "Event":
        return cls.model_validate_json(data)


class EnrichedEvent(Event):
    """Event with enrichment data attached."""
    user_segment: str | None = None
    geo_region: str | None = None
    device_type: str | None = None
    processing_timestamp: datetime = Field(default_factory=datetime.utcnow)
    enrichment_version: str = "1.0"
