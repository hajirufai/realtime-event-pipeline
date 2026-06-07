"""Producer configuration."""
from pydantic_settings import BaseSettings


class ProducerConfig(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "events"
    events_per_second: int = 100
    batch_size: int = 50

    class Config:
        env_prefix = ""
