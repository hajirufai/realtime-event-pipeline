"""Tests for the stream processor."""
import pytest
import json
from src.consumer import StreamProcessor
from src.producer import EventGenerator, Event


class TestStreamProcessor:
    def test_process_valid_message(self):
        processor = StreamProcessor()
        gen = EventGenerator()
        event = gen.generate_batch(1)[0]
        raw = event.to_kafka_message()

        result = processor.process_message(raw)
        assert result is not None
        assert result.event_id == event.event_id
        assert result.user_segment is not None
        assert result.geo_region is not None

    def test_process_invalid_message(self):
        processor = StreamProcessor()
        result = processor.process_message(b"invalid json")
        assert result is None
        assert processor.stats["errors"] == 1

    def test_process_batch(self):
        processor = StreamProcessor()
        gen = EventGenerator()
        events = gen.generate_batch(20)
        raw_messages = [e.to_kafka_message() for e in events]

        results = processor.process_batch(raw_messages)
        assert len(results) == 20
        assert processor.stats["processed"] == 20
        assert processor.stats["errors"] == 0

    def test_dlq_routing(self):
        processor = StreamProcessor(dlq_enabled=True)
        processor.process_message(b"bad data")
        assert processor.stats["errors"] == 1
