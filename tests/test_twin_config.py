from __future__ import annotations

from mqtt2postgres.twin_config import (
    AggregateTopicRow,
    build_publisher_config_document,
    derive_frequency_seconds,
    deterministic_seed_for_topic,
    fallback_stddev_from_quantiles,
    learn_topic_profile,
    learn_topic_profiles,
    pooled_sample_stddev,
    quote_qualified_table_name,
)


def build_row(**overrides) -> AggregateTopicRow:
    base = AggregateTopicRow(
        topic="sensors/node-1/temp",
        device_id="node-1",
        metric_name="temp",
        numeric_count=24,
        numeric_avg=12.0,
        numeric_min=8.0,
        numeric_max=16.0,
        numeric_p25=10.0,
        numeric_p75=14.0,
        numeric_variance_samp=4.0,
        numeric_stddev_samp=2.0,
        first_received_at="2026-01-01 00:00:00+00",
        last_received_at="2026-01-01 23:00:00+00",
        interval_gap_count=23,
        interval_gap_avg_seconds=3600.0,
        quality_score=8.0,
        quality_status="rated",
    )
    values = base.__dict__ | overrides
    return AggregateTopicRow(**values)


def test_pooled_sample_stddev_uses_variance_and_means() -> None:
    value = pooled_sample_stddev(
        (
            build_row(numeric_avg=10.0, numeric_count=10, numeric_variance_samp=4.0),
            build_row(topic="sensors/node-1/humidity", numeric_avg=14.0, numeric_count=10, numeric_variance_samp=4.0),
        )
    )

    assert value is not None
    assert value > 2.0


def test_fallback_stddev_from_quantiles_uses_iqr() -> None:
    value = fallback_stddev_from_quantiles((build_row(numeric_variance_samp=None, numeric_stddev_samp=None),))

    assert value is not None
    assert round(value, 6) == round((14.0 - 10.0) / 1.349, 6)


def test_derive_frequency_seconds_prefers_interval_metrics() -> None:
    value = derive_frequency_seconds((build_row(interval_gap_avg_seconds=120.0, interval_gap_count=12),))

    assert value == 120.0


def test_learn_topic_profile_builds_clipped_normal_for_usable_rows() -> None:
    profile = learn_topic_profile(
        (build_row(),),
        minimum_quality_score=5.0,
        include_low_quality=False,
    )

    assert profile is not None
    assert profile.generator["kind"] == "clipped_normal"
    assert profile.frequency_seconds == 3600.0


def test_learn_topic_profile_skips_low_quality_by_default() -> None:
    profile = learn_topic_profile(
        (build_row(quality_score=3.0),),
        minimum_quality_score=5.0,
        include_low_quality=False,
    )

    assert profile is None


def test_learn_topic_profile_can_include_low_quality_rows_on_override() -> None:
    profile = learn_topic_profile(
        (build_row(quality_score=3.0),),
        minimum_quality_score=5.0,
        include_low_quality=True,
    )

    assert profile is not None


def test_learn_topic_profile_falls_back_to_uniform_when_spread_is_missing() -> None:
    profile = learn_topic_profile(
        (
            build_row(
                numeric_variance_samp=None,
                numeric_stddev_samp=None,
                numeric_p25=None,
                numeric_p75=None,
            ),
        ),
        minimum_quality_score=5.0,
        include_low_quality=False,
    )

    assert profile is not None
    assert profile.generator["kind"] == "uniform"


def test_learn_topic_profiles_groups_multiple_topics() -> None:
    profiles = learn_topic_profiles(
        (
            build_row(topic="sensors/node-1/temp", metric_name="temp"),
            build_row(topic="sensors/node-1/humidity", metric_name="humidity"),
            build_row(topic="sensors/node-2/temp", device_id="node-2", metric_name="temp"),
        ),
        minimum_quality_score=5.0,
        include_low_quality=False,
    )

    assert [profile.topic for profile in profiles] == [
        "sensors/node-1/humidity",
        "sensors/node-1/temp",
        "sensors/node-2/temp",
    ]


def test_build_publisher_config_document_groups_profiles_by_device() -> None:
    profiles = [
        learn_topic_profile((build_row(topic="sensors/node-1/temp", metric_name="temp"),), minimum_quality_score=5.0, include_low_quality=False),
        learn_topic_profile((build_row(topic="sensors/node-1/humidity", metric_name="humidity"),), minimum_quality_score=5.0, include_low_quality=False),
        learn_topic_profile((build_row(topic="sensors/node-2/temp", device_id="node-2", metric_name="temp"),), minimum_quality_score=5.0, include_low_quality=False),
    ]
    document = build_publisher_config_document(
        [profile for profile in profiles if profile is not None],
        mqtt_host="mqtt-broker",
        mqtt_port=1883,
        payload_format="json",
        qos=0,
    )

    assert len(document["publishers"]) == 2
    assert document["publishers"][0]["topics"][0]["topic"] == "sensors/node-1/humidity"
    assert document["publishers"][0]["topics"][1]["topic"] == "sensors/node-1/temp"
    assert document["publishers"][1]["topics"][0]["topic"] == "sensors/node-2/temp"


def test_quote_qualified_table_name_rejects_invalid_sql_identifier() -> None:
    try:
        quote_qualified_table_name('mqtt_ingest."message_24h_aggregates"')
    except ValueError as exc:
        assert "unquoted SQL identifiers" in str(exc)
    else:
        raise AssertionError("Expected invalid quoted identifier to fail.")


def test_deterministic_seed_for_topic_is_stable() -> None:
    assert deterministic_seed_for_topic("sensors/node-1/temp") == deterministic_seed_for_topic("sensors/node-1/temp")
