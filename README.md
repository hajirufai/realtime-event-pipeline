# 🚀 Real-Time Event Pipeline

A production-grade real-time event processing pipeline built with Python, Apache Kafka, and DuckDB. Demonstrates modern data engineering patterns: streaming ingestion, transformation, and analytics.

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)
![Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-red?logo=apachekafka)
![DuckDB](https://img.shields.io/badge/DuckDB-Analytics-yellow?logo=duckdb)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)
![License](https://img.shields.io/badge/License-MIT-green)

## Architecture

```
┌─────────────┐    ┌───────────┐    ┌──────────────┐    ┌──────────┐
│  Event       │───▶│  Kafka    │───▶│  Stream      │───▶│ DuckDB   │
│  Generator   │    │  Broker   │    │  Processor   │    │ Analytics│
└─────────────┘    └───────────┘    └──────────────┘    └──────────┘
                                           │
                                    ┌──────┴──────┐
                                    │  Dead Letter │
                                    │  Queue (DLQ) │
                                    └─────────────┘
```

## Features

- **Event Generation**: Configurable synthetic event producer (e-commerce events: page views, add-to-cart, purchases)
- **Kafka Streaming**: Partitioned topics with consumer groups for parallel processing
- **Stream Processing**: Real-time transformations, enrichment, and windowed aggregations
- **DuckDB Analytics**: Analytical queries on processed data with materialized views
- **Dead Letter Queue**: Malformed events are routed to DLQ for debugging
- **Monitoring**: Prometheus metrics + Grafana dashboards
- **Docker Compose**: One-command local deployment

## Quick Start

```bash
# Clone and start
git clone https://github.com/HajiMohamedRufai/realtime-event-pipeline.git
cd realtime-event-pipeline
docker-compose up -d

# Generate sample events
python src/producer/event_generator.py --events-per-second 100

# Run analytics
python src/analytics/dashboard.py
```

## Project Structure

```
├── src/
│   ├── producer/
│   │   ├── event_generator.py      # Configurable event producer
│   │   ├── schemas.py              # Pydantic event schemas
│   │   └── config.py               # Producer configuration
│   ├── consumer/
│   │   ├── stream_processor.py     # Kafka consumer + transformations
│   │   ├── enrichment.py           # Event enrichment logic
│   │   └── dlq_handler.py          # Dead letter queue handler
│   ├── analytics/
│   │   ├── queries.py              # DuckDB analytical queries
│   │   ├── materialized_views.sql  # Pre-computed aggregations
│   │   └── dashboard.py            # Simple analytics dashboard
│   └── common/
│       ├── kafka_client.py         # Kafka connection wrapper
│       ├── duckdb_client.py        # DuckDB connection wrapper
│       └── monitoring.py           # Prometheus metrics
├── tests/
│   ├── test_producer.py
│   ├── test_consumer.py
│   └── test_analytics.py
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── Makefile
└── README.md
```

## Event Schema

```python
from pydantic import BaseModel
from datetime import datetime
from enum import Enum

class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"
    SEARCH = "search"

class Event(BaseModel):
    event_id: str
    event_type: EventType
    user_id: str
    session_id: str
    timestamp: datetime
    properties: dict
    metadata: dict
```

## Analytics Queries

```sql
-- Real-time conversion funnel (last 1 hour)
SELECT
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    LAG(COUNT(DISTINCT user_id)) OVER (ORDER BY 
        CASE event_type 
            WHEN 'page_view' THEN 1 
            WHEN 'search' THEN 2
            WHEN 'add_to_cart' THEN 3 
            WHEN 'purchase' THEN 4 
        END
    ) as prev_step_users
FROM events
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY event_type
ORDER BY event_count DESC;
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPIC` | `events` | Main event topic |
| `KAFKA_DLQ_TOPIC` | `events-dlq` | Dead letter queue topic |
| `DUCKDB_PATH` | `./data/analytics.duckdb` | DuckDB database path |
| `EVENTS_PER_SECOND` | `100` | Event generation rate |
| `CONSUMER_GROUP` | `event-processor` | Kafka consumer group |

## Tech Stack

- **Python 3.11+** — Core language
- **Apache Kafka** — Event streaming
- **DuckDB** — OLAP analytics engine
- **Pydantic** — Data validation
- **Docker Compose** — Local orchestration
- **Prometheus + Grafana** — Monitoring
- **pytest** — Testing

## License

MIT License — See [LICENSE](LICENSE) for details.

---

*Built by [Haji Mohamed Rufai](https://linkedin.com/in/hajirufai) — Data Engineer*
