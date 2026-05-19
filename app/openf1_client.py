from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx


def normalize_to_utc(value: str) -> datetime:
    cleaned = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds")


async def fetch_openf1_range(
    *,
    base_url: str,
    endpoint: str,
    session_key: int,
    driver_number: int,
    start_time: datetime,
    end_time: datetime,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    """
    Fetch one OpenF1 endpoint for a bounded replay window.

    OpenF1 time filtering uses query keys like:
      date>2023-09-16T13:03:35.200
      date<2023-09-16T13:33:35.200
    """
    params = [
        ("session_key", str(session_key)),
        ("driver_number", str(driver_number)),
        ("date>", iso_utc(start_time)),
        ("date<", iso_utc(end_time)),
    ]

    url = f"{base_url.rstrip('/')}/{endpoint}"
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        payload = response.json()

    if not isinstance(payload, list):
        raise ValueError(f"Unexpected OpenF1 response for {endpoint}: expected list")
    return payload
