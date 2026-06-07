"""Event enrichment logic."""
import hashlib
from ..producer.schemas import Event, EnrichedEvent


# Simple user segmentation based on user_id hash
SEGMENTS = ["new_visitor", "returning", "power_user", "at_risk"]
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-southeast"]


def enrich_event(event: Event) -> EnrichedEvent:
    """Enrich an event with derived fields."""
    user_hash = int(hashlib.md5(event.user_id.encode()).hexdigest()[:8], 16)

    device_type = "desktop"
    source = event.metadata.get("source", "web")
    if source == "mobile_app":
        device_type = "mobile"
    elif source == "tablet":
        device_type = "tablet"

    return EnrichedEvent(
        **event.model_dump(),
        user_segment=SEGMENTS[user_hash % len(SEGMENTS)],
        geo_region=REGIONS[user_hash % len(REGIONS)],
        device_type=device_type,
    )
