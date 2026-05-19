from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable

from confluent_kafka import Producer

from openf1_client import fetch_openf1_range, iso_utc, normalize_to_utc


TOPIC_BY_ENDPOINT = {
    "car_data": "openf1.car_data",
    "position": "openf1.position",
    "location": "openf1.location",
}


def _to_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    return int(value)


def _normalize_car_data(row: dict[str, Any], replay_id: str) -> dict[str, Any]:
    return {
        "replay_id": replay_id,
        "event_time": row["date"],
        "emitted_at": iso_utc(datetime.now(timezone.utc)),
        "meeting_key": _to_int(row.get("meeting_key")),
        "session_key": _to_int(row.get("session_key")),
        "driver_number": _to_int(row.get("driver_number")),
        "brake": _to_int(row.get("brake")),
        "drs": _to_int(row.get("drs")),
        "n_gear": _to_int(row.get("n_gear")),
        "rpm": _to_int(row.get("rpm")),
        "speed": _to_int(row.get("speed")),
        "throttle": _to_int(row.get("throttle")),
    }


def _normalize_position(row: dict[str, Any], replay_id: str) -> dict[str, Any]:
    return {
        "replay_id": replay_id,
        "event_time": row["date"],
        "emitted_at": iso_utc(datetime.now(timezone.utc)),
        "meeting_key": _to_int(row.get("meeting_key")),
        "session_key": _to_int(row.get("session_key")),
        "driver_number": _to_int(row.get("driver_number")),
        "position": _to_int(row.get("position")),
    }


def _normalize_location(row: dict[str, Any], replay_id: str) -> dict[str, Any]:
    return {
        "replay_id": replay_id,
        "event_time": row["date"],
        "emitted_at": iso_utc(datetime.now(timezone.utc)),
        "meeting_key": _to_int(row.get("meeting_key")),
        "session_key": _to_int(row.get("session_key")),
        "driver_number": _to_int(row.get("driver_number")),
        "x": _to_int(row.get("x")),
        "y": _to_int(row.get("y")),
        "z": _to_int(row.get("z")),
    }


NORMALIZER_BY_ENDPOINT = {
    "car_data": _normalize_car_data,
    "position": _normalize_position,
    "location": _normalize_location,
}


def _kafka_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "client.id": "openf1-replay-producer",
            "acks": "all",
            "linger.ms": 0,
            "enable.idempotence": True,
        }
    )


def _produce_with_backpressure(
    producer: Producer,
    topic: str,
    payload: dict[str, Any],
    key: str,
) -> None:
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    while True:
        try:
            producer.produce(topic=topic, value=encoded, key=key.encode("utf-8"))
            producer.poll(0)
            return
        except BufferError:
            producer.poll(0.05)


async def run_replay_job(
    *,
    replay_id: str,
    session_key: int,
    driver_number: int,
    replay_from: str,
    replay_to: str,
    speed_factor: float,
    update_job: Callable[[str, dict[str, Any]], None],
) -> None:
    base_url = os.getenv("OPENF1_API_BASE_URL", "https://api.openf1.org/v1")
    timeout_seconds = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "120"))
    flush_interval_seconds = float(os.getenv("REPLAY_FLUSH_INTERVAL_MS", "100")) / 1000.0

    start_dt = normalize_to_utc(replay_from)
    end_dt = normalize_to_utc(replay_to)

    try:
        update_job(
            replay_id,
            {
                "status": "fetching",
                "fetched": {"car_data": 0, "position": 0, "location": 0},
                "emitted": 0,
            },
        )

        car_rows, position_rows, location_rows = await asyncio.gather(
            fetch_openf1_range(
                base_url=base_url,
                endpoint="car_data",
                session_key=session_key,
                driver_number=driver_number,
                start_time=start_dt,
                end_time=end_dt,
                timeout_seconds=timeout_seconds,
            ),
            fetch_openf1_range(
                base_url=base_url,
                endpoint="position",
                session_key=session_key,
                driver_number=driver_number,
                start_time=start_dt,
                end_time=end_dt,
                timeout_seconds=timeout_seconds,
            ),
            fetch_openf1_range(
                base_url=base_url,
                endpoint="location",
                session_key=session_key,
                driver_number=driver_number,
                start_time=start_dt,
                end_time=end_dt,
                timeout_seconds=timeout_seconds,
            ),
        )

        rows_by_endpoint = {
            "car_data": car_rows,
            "position": position_rows,
            "location": location_rows,
        }

        events: list[tuple[datetime, str, dict[str, Any]]] = []
        for endpoint, rows in rows_by_endpoint.items():
            normalizer = NORMALIZER_BY_ENDPOINT[endpoint]
            for row in rows:
                if "date" not in row:
                    continue
                event_dt = normalize_to_utc(str(row["date"]))
                events.append((event_dt, endpoint, normalizer(row, replay_id)))

        events.sort(key=lambda item: item[0])

        update_job(
            replay_id,
            {
                "status": "replaying",
                "fetched": {name: len(rows) for name, rows in rows_by_endpoint.items()},
                "total_events": len(events),
                "emitted": 0,
            },
        )

        producer = _kafka_producer()
        last_flush_monotonic = time.monotonic()

        if not events:
            producer.flush(5)
            update_job(
                replay_id,
                {
                    "status": "completed",
                    "fetched": {name: len(rows) for name, rows in rows_by_endpoint.items()},
                    "total_events": 0,
                    "emitted": 0,
                    "message": "No events returned by OpenF1 for the selected replay range.",
                },
            )
            return

        first_event_time = events[0][0]
        replay_wall_start = time.monotonic()
        emitted_count = 0

        for event_dt, endpoint, payload in events:
            source_offset_seconds = max((event_dt - first_event_time).total_seconds(), 0.0)
            target_offset_seconds = source_offset_seconds / max(speed_factor, 0.001)

            while True:
                elapsed = time.monotonic() - replay_wall_start
                wait_seconds = target_offset_seconds - elapsed
                if wait_seconds <= 0:
                    break
                await asyncio.sleep(min(wait_seconds, 0.05))

            topic = TOPIC_BY_ENDPOINT[endpoint]
            kafka_key = f"{replay_id}:{payload.get('driver_number', 0)}"
            _produce_with_backpressure(producer, topic, payload, kafka_key)
            emitted_count += 1

            now = time.monotonic()
            if now - last_flush_monotonic >= flush_interval_seconds:
                producer.flush(0.2)
                last_flush_monotonic = now

            if emitted_count % 100 == 0 or emitted_count == len(events):
                update_job(
                    replay_id,
                    {
                        "status": "replaying",
                        "fetched": {name: len(rows) for name, rows in rows_by_endpoint.items()},
                        "total_events": len(events),
                        "emitted": emitted_count,
                    },
                )

        producer.flush(10)
        update_job(
            replay_id,
            {
                "status": "completed",
                "fetched": {name: len(rows) for name, rows in rows_by_endpoint.items()},
                "total_events": len(events),
                "emitted": emitted_count,
            },
        )

    except Exception as exc:
        update_job(
            replay_id,
            {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
