from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Iterable, List, Dict

sys.path.insert(0, "/app")

from include.eczachly.snowflake_queries import get_snowpark_session
from include.hitesh.scripts.f1_snowflake_etl_2 import F1DataIngestion

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

TABLE_BY_ENDPOINT = {
    "drivers": "F1_DRIVERS",
    "laps": "F1_LAPS",
    "pit": "F1_PIT",
    "position": "F1_POSITION",
    "intervals": "F1_INTERVALS",
    "stints": "F1_STINTS",
    "weather": "F1_WEATHER",
    "race_control": "F1_RACE_CONTROL",
}

REQUIRED_SNOWFLAKE_ENV = [
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_SCHEMA",
]


def ensure_env() -> None:
    missing = [name for name in REQUIRED_SNOWFLAKE_ENV if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Snowflake belum dikonfigurasi. Isi variabel berikut di file .env: " + ", ".join(missing)
        )


def first_n(items: Iterable[Dict], limit: int) -> List[Dict]:
    data = list(items or [])
    return data if limit <= 0 else data[:limit]


def create_and_load(ingestion: F1DataIngestion, table_name: str, data: List[Dict]) -> int:
    if not data:
        logging.info("No data for %s", table_name)
        return 0
    if not ingestion.table_exists(table_name):
        ingestion.create_table(table_name, data[0])
    ingestion.load_data(table_name, data)
    return len(data)


def main() -> None:
    parser = argparse.ArgumentParser(description="OpenF1 historical extractor/loader for Snowflake")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--meeting-key", default="", help="Optional meeting_key. Use latest or numeric key.")
    parser.add_argument("--session-key", default="", help="Optional specific session_key.")
    parser.add_argument("--limit-meetings", type=int, default=1)
    parser.add_argument("--limit-sessions", type=int, default=1)
    parser.add_argument(
        "--endpoints",
        default="drivers,laps,pit,position,intervals,stints,weather",
        help="Comma-separated OpenF1 endpoints. Use include-car-data separately for high frequency telemetry.",
    )
    parser.add_argument("--include-car-data", action="store_true", help="Also load car_data in adaptive windows. Heavy.")
    args = parser.parse_args()

    ensure_env()
    session = get_snowpark_session()
    ingestion = F1DataIngestion(session)
    ingestion.schema = os.getenv("SNOWFLAKE_SCHEMA", "HITESH")
    ingestion.execution_date = datetime.utcnow().date()

    if args.meeting_key:
        meeting_param = int(args.meeting_key) if str(args.meeting_key).isdigit() else args.meeting_key
        meetings = ingestion._make_request("meetings", {"meeting_key": meeting_param})
    else:
        meetings = ingestion._make_request("meetings", {"year": args.year})
        meetings = sorted(meetings, key=lambda row: row.get("date_start", ""))

    meetings = first_n(meetings, args.limit_meetings)
    logging.info("Meetings selected: %s", [m.get("meeting_key") for m in meetings])

    endpoint_names = [e.strip() for e in args.endpoints.split(",") if e.strip()]
    total = {}

    for meeting in meetings:
        create_and_load(ingestion, "F1_MEETINGS", [meeting])
        sessions = ingestion.get_session_data(meeting["meeting_key"])
        if args.session_key:
            sessions = [s for s in sessions if str(s.get("session_key")) == str(args.session_key)]
        sessions = first_n(sorted(sessions, key=lambda row: row.get("date_start", "")), args.limit_sessions)
        logging.info("Sessions selected for meeting %s: %s", meeting.get("meeting_key"), [s.get("session_key") for s in sessions])

        if sessions:
            create_and_load(ingestion, "F1_SESSIONS", sessions)

        for f1_session in sessions:
            session_key = f1_session["session_key"]
            for endpoint in endpoint_names:
                table = TABLE_BY_ENDPOINT.get(endpoint)
                if not table:
                    logging.warning("Endpoint %s is not mapped. Skipped.", endpoint)
                    continue
                try:
                    data = ingestion._make_request(endpoint, {"session_key": session_key})
                    count = create_and_load(ingestion, table, data)
                    total[table] = total.get(table, 0) + count
                except Exception as exc:
                    logging.exception("Failed loading endpoint=%s session_key=%s: %s", endpoint, session_key, exc)

            if args.include_car_data:
                result = ingestion.get_session_endpoint_data(f1_session, "car_data")
                total["F1_CAR_DATA"] = total.get("F1_CAR_DATA", 0) + int(result.get("records", 0) if isinstance(result, dict) else 0)

    logging.info("Historical load summary: %s", total)
    session.close()


if __name__ == "__main__":
    main()
