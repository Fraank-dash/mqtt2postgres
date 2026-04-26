from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from paho.mqtt import client as mqtt_client
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine

DEFAULT_DB_HOST = "127.0.0.1"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "mqtt"
DEFAULT_MQTT_HOST = "mqtt-broker"
DEFAULT_MQTT_PORT = 1883
DEFAULT_PAYLOAD_FORMAT = "json"
DEFAULT_QOS = 0
DEFAULT_FREQUENCY_SECONDS = 1.0
DEFAULT_MIN_QUALITY_SCORE = 5.0
DEFAULT_AGGREGATE_TABLE = "mqtt_ingest.message_24h_aggregates"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class TwinConfigError(ValueError):
    """Raised when aggregate-driven publisher config generation fails."""


@dataclass(frozen=True)
class AggregateTopicRow:
    topic: str
    device_id: str | None
    metric_name: str | None
    numeric_count: int
    numeric_avg: float | None
    numeric_min: float | None
    numeric_max: float | None
    numeric_p25: float | None
    numeric_p75: float | None
    numeric_variance_samp: float | None
    numeric_stddev_samp: float | None
    first_received_at: str | None
    last_received_at: str | None
    interval_gap_count: int | None
    interval_gap_avg_seconds: float | None
    quality_score: float | None
    quality_status: str | None


@dataclass(frozen=True)
class LearnedTopicProfile:
    topic: str
    device_id: str | None
    metric_name: str | None
    frequency_seconds: float
    generator: dict[str, Any]


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mqtt2postgres-twin-config",
        description="Generate publisher-config.json style digital-twin publishers from aggregate tables.",
    )
    parser.add_argument("--db-host", default=None, help="Postgres-compatible database host.")
    parser.add_argument("--db-port", type=int, default=None, help="Postgres-compatible database port.")
    parser.add_argument("--db-name", default=None, help="Database name.")
    parser.add_argument("--db-user", dest="db_user", default=None, help="Database username.")
    parser.add_argument("--db-password", dest="db_password", default=None, help="Database password.")
    parser.add_argument(
        "--aggregate-table",
        default=DEFAULT_AGGREGATE_TABLE,
        help=f"Qualified aggregate table to learn from. Defaults to {DEFAULT_AGGREGATE_TABLE}.",
    )
    parser.add_argument(
        "--topic-filter",
        action="append",
        default=None,
        help="MQTT topic filter to synthesize. Repeat for multiple filters.",
    )
    parser.add_argument(
        "--minimum-quality-score",
        type=float,
        default=DEFAULT_MIN_QUALITY_SCORE,
        help="Minimum aggregate quality_score required by default.",
    )
    parser.add_argument(
        "--include-low-quality",
        action="store_true",
        help="Include topics even when no aggregate rows meet the default quality filters.",
    )
    parser.add_argument("--mqtt-host", default=DEFAULT_MQTT_HOST, help="MQTT host to emit into generated config.")
    parser.add_argument("--mqtt-port", type=int, default=DEFAULT_MQTT_PORT, help="MQTT port to emit into generated config.")
    parser.add_argument(
        "--payload-format",
        choices=("json", "plain"),
        default=DEFAULT_PAYLOAD_FORMAT,
        help="Payload format to emit into generated config.",
    )
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=DEFAULT_QOS, help="MQTT QoS to emit into generated config.")
    parser.add_argument("--output", default=None, help="Optional path to write the generated publisher config JSON.")
    return parser


def load_rows_for_filters(
    engine: Engine,
    *,
    aggregate_table: str,
    topic_filters: Sequence[str],
) -> list[AggregateTopicRow]:
    qualified_table = quote_qualified_table_name(aggregate_table)
    statement = text(
        f"""
        SELECT
            topic,
            device_id,
            metric_name,
            numeric_count,
            numeric_avg,
            numeric_min,
            numeric_max,
            numeric_p25,
            numeric_p75,
            numeric_variance_samp,
            numeric_stddev_samp,
            first_received_at::text AS first_received_at,
            last_received_at::text AS last_received_at,
            interval_gap_count,
            interval_gap_avg_seconds,
            quality_score,
            quality_status
        FROM {qualified_table}
        WHERE numeric_count > 0
        ORDER BY topic, bucket_start
        """
    )
    with engine.connect() as connection:
        results = connection.execute(statement)
        rows = [AggregateTopicRow(**dict(row._mapping)) for row in results]
    return [row for row in rows if any(mqtt_client.topic_matches_sub(topic_filter, row.topic) for topic_filter in topic_filters)]


def learn_topic_profiles(
    rows: Sequence[AggregateTopicRow],
    *,
    minimum_quality_score: float,
    include_low_quality: bool,
) -> list[LearnedTopicProfile]:
    rows_by_topic: dict[str, list[AggregateTopicRow]] = defaultdict(list)
    for row in rows:
        rows_by_topic[row.topic].append(row)

    profiles: list[LearnedTopicProfile] = []
    for topic, topic_rows in sorted(rows_by_topic.items()):
        profile = learn_topic_profile(
            topic_rows,
            minimum_quality_score=minimum_quality_score,
            include_low_quality=include_low_quality,
        )
        if profile is not None:
            profiles.append(profile)
    return profiles


def learn_topic_profile(
    rows: Sequence[AggregateTopicRow],
    *,
    minimum_quality_score: float,
    include_low_quality: bool,
) -> LearnedTopicProfile | None:
    if not rows:
        return None

    preferred_rows = [
        row
        for row in rows
        if row.quality_status == "rated"
        and row.quality_score is not None
        and row.quality_score >= minimum_quality_score
        and row.numeric_count > 0
    ]
    usable_rows = preferred_rows or (list(rows) if include_low_quality else [])
    if not usable_rows:
        return None

    numeric_rows = [row for row in usable_rows if row.numeric_count > 0 and row.numeric_min is not None and row.numeric_max is not None]
    if not numeric_rows:
        return None

    representative = numeric_rows[0]
    total_numeric_count = sum(max(row.numeric_count, 0) for row in numeric_rows)
    weighted_mean = _weighted_average(
        ((row.numeric_avg, row.numeric_count) for row in numeric_rows if row.numeric_avg is not None),
    )
    pooled_stddev = pooled_sample_stddev(numeric_rows)
    if pooled_stddev is None or pooled_stddev <= 0:
        pooled_stddev = fallback_stddev_from_quantiles(numeric_rows)

    min_value = min(row.numeric_min for row in numeric_rows if row.numeric_min is not None)
    max_value = max(row.numeric_max for row in numeric_rows if row.numeric_max is not None)
    frequency_seconds = derive_frequency_seconds(numeric_rows)
    seed = deterministic_seed_for_topic(representative.topic)

    if weighted_mean is not None and pooled_stddev is not None and pooled_stddev > 0:
        generator = {
            "kind": "clipped_normal",
            "mean": round(weighted_mean, 6),
            "stddev": round(pooled_stddev, 6),
            "min_value": round(min_value, 6),
            "max_value": round(max_value, 6),
            "seed": seed,
        }
    else:
        generator = {
            "kind": "uniform",
            "min_value": round(min_value, 6),
            "max_value": round(max_value, 6),
            "seed": seed,
        }

    return LearnedTopicProfile(
        topic=representative.topic,
        device_id=representative.device_id,
        metric_name=representative.metric_name,
        frequency_seconds=frequency_seconds,
        generator=generator,
    )


def build_publisher_config_document(
    profiles: Sequence[LearnedTopicProfile],
    *,
    mqtt_host: str,
    mqtt_port: int,
    payload_format: str,
    qos: int,
) -> dict[str, Any]:
    groups: dict[str, list[LearnedTopicProfile]] = defaultdict(list)
    for profile in profiles:
        groups[group_key_for_profile(profile)].append(profile)

    publishers: list[dict[str, Any]] = []
    for group_key, group_profiles in sorted(groups.items()):
        frequency_seconds = _weighted_average(
            ((profile.frequency_seconds, 1.0) for profile in group_profiles if profile.frequency_seconds > 0),
        ) or DEFAULT_FREQUENCY_SECONDS
        publishers.append(
            {
                "host": mqtt_host,
                "port": mqtt_port,
                "client_id": f"publisher-{group_key}",
                "publisher_id": f"{group_key}-sim",
                "qos": qos,
                "payload_format": payload_format,
                "frequency_seconds": round(frequency_seconds, 6),
                "topics": [
                    {
                        "topic": profile.topic,
                        "generator": profile.generator,
                    }
                    for profile in sorted(group_profiles, key=lambda item: item.topic)
                ],
            }
        )
    return {"publishers": publishers}


def group_key_for_profile(profile: LearnedTopicProfile) -> str:
    if profile.device_id:
        return slugify(profile.device_id)
    first_segment = profile.topic.split("/", 1)[0].strip()
    if first_segment:
        return slugify(first_segment)
    return "simulated-topics"


def derive_frequency_seconds(rows: Sequence[AggregateTopicRow]) -> float:
    interval_average = _weighted_average(
        (
            (row.interval_gap_avg_seconds, float(row.interval_gap_count or row.numeric_count))
            for row in rows
            if row.interval_gap_avg_seconds is not None
            and (row.interval_gap_count or row.numeric_count) > 0
        ),
    )
    if interval_average is not None and interval_average > 0:
        return round(interval_average, 6)

    observed_average = _weighted_average(
        (
            (_observed_gap_seconds(row), float(max(row.numeric_count - 1, 1)))
            for row in rows
            if _observed_gap_seconds(row) is not None
        ),
    )
    if observed_average is not None and observed_average > 0:
        return round(observed_average, 6)
    return DEFAULT_FREQUENCY_SECONDS


def pooled_sample_stddev(rows: Sequence[AggregateTopicRow]) -> float | None:
    total_count = sum(row.numeric_count for row in rows if row.numeric_count > 0)
    if total_count < 2:
        return None
    mean = _weighted_average(
        ((row.numeric_avg, row.numeric_count) for row in rows if row.numeric_avg is not None and row.numeric_count > 0),
    )
    if mean is None:
        return None

    sum_of_squares = 0.0
    usable = False
    for row in rows:
        if row.numeric_count <= 0 or row.numeric_avg is None:
            continue
        usable = True
        if row.numeric_variance_samp is not None and row.numeric_count > 1:
            sum_of_squares += (row.numeric_count - 1) * row.numeric_variance_samp
        sum_of_squares += row.numeric_count * (row.numeric_avg - mean) ** 2
    if not usable:
        return None
    variance = sum_of_squares / (total_count - 1)
    if variance <= 0:
        return None
    return variance ** 0.5


def fallback_stddev_from_quantiles(rows: Sequence[AggregateTopicRow]) -> float | None:
    weighted_iqr = _weighted_average(
        (
            (row.numeric_p75 - row.numeric_p25, row.numeric_count)
            for row in rows
            if row.numeric_p25 is not None and row.numeric_p75 is not None and row.numeric_p75 >= row.numeric_p25
        ),
    )
    if weighted_iqr is None or weighted_iqr <= 0:
        return None
    return weighted_iqr / 1.349


def deterministic_seed_for_topic(topic: str) -> int:
    digest = hashlib.sha256(topic.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def quote_qualified_table_name(table_name: str) -> str:
    parts = [part.strip() for part in table_name.split(".")]
    if not parts or any(not part for part in parts):
        raise TwinConfigError("Aggregate table name must not be empty.")
    if len(parts) > 2:
        raise TwinConfigError("Aggregate table name must be either NAME or SCHEMA.NAME.")
    for part in parts:
        if not IDENTIFIER_PATTERN.fullmatch(part):
            raise TwinConfigError("Aggregate table name must contain only unquoted SQL identifiers.")
    return ".".join(f'"{part}"' for part in parts)


def slugify(value: str) -> str:
    lowered = value.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")
    return slug or "simulated-topics"


def create_db_engine(
    *,
    db_host: str,
    db_port: int,
    db_name: str,
    db_user: str,
    db_password: str,
) -> Engine:
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db_name,
    )
    return create_engine(url, future=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    env = os.environ

    db_user = args.db_user or env.get("POSTGRES_USERNAME")
    if not db_user:
        parser.error("A database username is required. Pass --db-user or set POSTGRES_USERNAME.")
    db_password = args.db_password or env.get("POSTGRES_PASSWORD")
    if not db_password:
        parser.error("A database password is required. Pass --db-password or set POSTGRES_PASSWORD.")
    topic_filters = tuple(args.topic_filter or [])
    if not topic_filters:
        parser.error("At least one --topic-filter is required.")

    engine = create_db_engine(
        db_host=args.db_host or env.get("POSTGRES_HOST") or DEFAULT_DB_HOST,
        db_port=args.db_port or int(env.get("POSTGRES_PORT", str(DEFAULT_DB_PORT))),
        db_name=args.db_name or env.get("POSTGRES_DB") or DEFAULT_DB_NAME,
        db_user=db_user,
        db_password=db_password,
    )
    try:
        rows = load_rows_for_filters(
            engine,
            aggregate_table=args.aggregate_table,
            topic_filters=topic_filters,
        )
        profiles = learn_topic_profiles(
            rows,
            minimum_quality_score=args.minimum_quality_score,
            include_low_quality=args.include_low_quality,
        )
        document = build_publisher_config_document(
            profiles,
            mqtt_host=args.mqtt_host,
            mqtt_port=args.mqtt_port,
            payload_format=args.payload_format,
            qos=args.qos,
        )
    finally:
        engine.dispose()

    rendered = json.dumps(document, indent=2, sort_keys=False) + "\n"
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0


def _observed_gap_seconds(row: AggregateTopicRow) -> float | None:
    if row.first_received_at is None or row.last_received_at is None or row.numeric_count < 2:
        return None
    try:
        first = _parse_db_timestamp(row.first_received_at)
        last = _parse_db_timestamp(row.last_received_at)
    except ValueError:
        return None
    total_seconds = (last - first).total_seconds()
    if total_seconds <= 0:
        return None
    return total_seconds / max(row.numeric_count - 1, 1)


def _parse_db_timestamp(raw_value: str):
    normalized = raw_value.replace(" ", "T")
    if normalized.endswith("+00"):
        normalized += ":00"
    return datetime.fromisoformat(normalized)


def _weighted_average(pairs: Iterable[tuple[float | None, float]]) -> float | None:
    total_weight = 0.0
    weighted_sum = 0.0
    for value, weight in pairs:
        if value is None or weight <= 0:
            continue
        weighted_sum += value * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return weighted_sum / total_weight
