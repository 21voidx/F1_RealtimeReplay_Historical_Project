from __future__ import annotations

import asyncio
import os
import uuid
from datetime import timedelta, timezone
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse

from openf1_client import iso_utc, normalize_to_utc
from replay_worker import run_replay_job

app = FastAPI(
    title="OpenF1 Replay API",
    description="Run 30-minute OpenF1 historical replays and redirect back to Grafana.",
    version="1.0.0",
)

JOBS: dict[str, dict] = {}


def update_job(replay_id: str, patch: dict) -> None:
    current = JOBS.setdefault(replay_id, {})
    current.update(patch)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/replay/status/{replay_id}")
async def replay_status(replay_id: str) -> dict:
    job = JOBS.get(replay_id)
    if not job:
        raise HTTPException(status_code=404, detail="Replay job not found")
    return {"replay_id": replay_id, **job}


@app.get("/replay/run")
async def run_replay(
    session_key: int = Query(..., ge=1),
    driver_number: int = Query(..., ge=1),
    replay_from: str = Query(..., description="UTC timestamp, e.g. 2023-09-16T13:03:35"),
    speed_factor: float = Query(1.0, gt=0, le=100),
):
    replay_id = uuid.uuid4().hex
    replay_window_minutes = int(os.getenv("REPLAY_WINDOW_MINUTES", "30"))

    try:
        replay_from_dt = normalize_to_utc(replay_from)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid replay_from: {exc}") from exc

    replay_to_dt = replay_from_dt + timedelta(minutes=replay_window_minutes)

    JOBS[replay_id] = {
        "status": "queued",
        "session_key": session_key,
        "driver_number": driver_number,
        "replay_from": iso_utc(replay_from_dt),
        "replay_to": iso_utc(replay_to_dt),
        "speed_factor": speed_factor,
        "fetched": {"car_data": 0, "position": 0, "location": 0},
        "emitted": 0,
    }

    asyncio.create_task(
        run_replay_job(
            replay_id=replay_id,
            session_key=session_key,
            driver_number=driver_number,
            replay_from=iso_utc(replay_from_dt),
            replay_to=iso_utc(replay_to_dt),
            speed_factor=speed_factor,
            update_job=update_job,
        )
    )

    grafana_public_url = os.getenv("GRAFANA_PUBLIC_URL", "http://localhost:3000").rstrip("/")
    dashboard_uid = os.getenv("GRAFANA_DASHBOARD_UID", "openf1-replay")

    from_ms = int(replay_from_dt.astimezone(timezone.utc).timestamp() * 1000)
    to_ms = int(replay_to_dt.astimezone(timezone.utc).timestamp() * 1000)

    query = urlencode(
        {
            "orgId": 1,
            "from": from_ms,
            "to": to_ms,
            "var-session_key": session_key,
            "var-driver_number": driver_number,
            "var-replay_from": replay_from_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "var-speed_factor": speed_factor,
            "var-replay_id": replay_id,
        }
    )

    redirect_url = f"{grafana_public_url}/d/{dashboard_uid}/openf1-real-time-replay?{query}"
    return RedirectResponse(url=redirect_url, status_code=302)
