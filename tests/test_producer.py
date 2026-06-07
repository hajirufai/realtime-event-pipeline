"""Tests for the event producer."""
import pytest
from src.producer import EventGenerator, Event, EventType


class TestEventGenerator:
    def test_generate_single_event(self):
        gen = EventGenerator()
        events = gen.generate_batch(1)
        assert len(events) == 1
        assert isinstance(events[0], Event)
        assert events[0].event_type in EventType

    def test_generate_batch(self):
        gen = EventGenerator()
        events = gen.generate_batch(100)
        assert len(events) == 100
        # Should have multiple event types
        types = {e.event_type for e in events}
        assert len(types) >= 2

    def test_event_serialization(self):
        gen = EventGenerator()
        event = gen.generate_batch(1)[0]
        raw = event.to_kafka_message()
        restored = Event.from_kafka_message(raw)
        assert restored.event_id == event.event_id
        assert restored.event_type == event.event_type

    def test_session_reuse(self):
        gen = EventGenerator()
        events = gen.generate_batch(50)
        sessions = [e.session_id for e in events]
        # Some sessions should be reused
        assert len(set(sessions)) < len(sessions)
