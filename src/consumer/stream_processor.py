"""Kafka stream processor with transformation and enrichment."""
import json
import logging
from datetime import datetime, timezone
from typing import Callable

from ..producer.schemas import Event, EnrichedEvent, EventType
from .enrichment import enrich_event

logger = logging.getLogger(__name__)


class StreamProcessor:
    """Processes events from Kafka with transformation and enrichment."""

    def __init__(
        self,
        kafka_config: dict | None = None,
        enrichment_fn: Callable | None = None,
        dlq_enabled: bool = True,
    ):
        self.kafka_config = kafka_config or {}
        self.enrichment_fn = enrichment_fn or enrich_event
        self.dlq_enabled = dlq_enabled
        self._processed_count = 0
        self._error_count = 0

    def process_message(self, raw_message: bytes) -> EnrichedEvent | None:
        """Process a single Kafka message."""
        try:
            event = Event.from_kafka_message(raw_message)
            enriched = self.enrichment_fn(event)
            self._processed_count += 1
            return enriched
        except Exception as e:
            self._error_count += 1
            logger.error(f"Failed to process message: {e}")
            if self.dlq_enabled:
                self._send_to_dlq(raw_message, str(e))
            return None

    def process_batch(self, messages: list[bytes]) -> list[EnrichedEvent]:
        """Process a batch of messages, filtering out failures."""
        results = []
        for msg in messages:
            enriched = self.process_message(msg)
            if enriched:
                results.append(enriched)
        return results

    def _send_to_dlq(self, raw_message: bytes, error: str):
        """Route failed messages to dead letter queue."""
        dlq_message = {
            "original_message": raw_message.decode("utf-8", errors="replace"),
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "processor_version": "1.0",
        }
        logger.info(f"Sent to DLQ: {error}")
        # In production: produce to Kafka DLQ topic
        return dlq_message

    @property
    def stats(self) -> dict:
        return {
            "processed": self._processed_count,
            "errors": self._error_count,
            "error_rate": (
                self._error_count / max(self._processed_count + self._error_count, 1)
            ),
        }
