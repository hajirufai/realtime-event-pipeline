"""DuckDB analytical queries for the event pipeline."""
import duckdb
from pathlib import Path

MATERIALIZED_VIEWS_SQL = Path(__file__).parent / "materialized_views.sql"


class EventAnalytics:
    """Analytics engine using DuckDB for OLAP queries."""

    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id VARCHAR PRIMARY KEY,
                event_type VARCHAR NOT NULL,
                user_id VARCHAR NOT NULL,
                session_id VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                properties JSON,
                metadata JSON,
                user_segment VARCHAR,
                geo_region VARCHAR,
                device_type VARCHAR,
                processing_timestamp TIMESTAMP
            )
        """)

    def ingest_events(self, events: list[dict]):
        """Bulk insert events into DuckDB."""
        if not events:
            return
        self.conn.executemany(
            """INSERT OR REPLACE INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    e["event_id"], e["event_type"], e["user_id"],
                    e["session_id"], e["timestamp"],
                    str(e.get("properties", {})), str(e.get("metadata", {})),
                    e.get("user_segment"), e.get("geo_region"),
                    e.get("device_type"), e.get("processing_timestamp"),
                )
                for e in events
            ],
        )

    def conversion_funnel(self, hours: int = 1) -> list[dict]:
        """Real-time conversion funnel analysis."""
        result = self.conn.execute(f"""
            SELECT
                event_type,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users
            FROM events
            WHERE timestamp > NOW() - INTERVAL '{hours} hours'
            GROUP BY event_type
            ORDER BY event_count DESC
        """).fetchall()
        return [
            {"event_type": r[0], "event_count": r[1], "unique_users": r[2]}
            for r in result
        ]

    def revenue_by_segment(self) -> list[dict]:
        """Revenue breakdown by user segment."""
        result = self.conn.execute("""
            SELECT
                user_segment,
                COUNT(*) as purchase_count,
                SUM(CAST(json_extract(properties, '$.total') AS DOUBLE)) as total_revenue,
                AVG(CAST(json_extract(properties, '$.total') AS DOUBLE)) as avg_order_value
            FROM events
            WHERE event_type = 'purchase'
            GROUP BY user_segment
            ORDER BY total_revenue DESC
        """).fetchall()
        return [
            {
                "segment": r[0], "purchases": r[1],
                "revenue": r[2], "avg_order": r[3],
            }
            for r in result
        ]

    def top_products(self, limit: int = 10) -> list[dict]:
        """Most added-to-cart products."""
        result = self.conn.execute(f"""
            SELECT
                json_extract_string(properties, '$.product_name') as product,
                COUNT(*) as add_count,
                COUNT(DISTINCT user_id) as unique_users
            FROM events
            WHERE event_type = 'add_to_cart'
            GROUP BY product
            ORDER BY add_count DESC
            LIMIT {limit}
        """).fetchall()
        return [
            {"product": r[0], "add_count": r[1], "unique_users": r[2]}
            for r in result
        ]

    def hourly_traffic(self) -> list[dict]:
        """Hourly event volume."""
        result = self.conn.execute("""
            SELECT
                DATE_TRUNC('hour', timestamp) as hour,
                event_type,
                COUNT(*) as count
            FROM events
            GROUP BY hour, event_type
            ORDER BY hour DESC
            LIMIT 100
        """).fetchall()
        return [{"hour": str(r[0]), "event_type": r[1], "count": r[2]} for r in result]
