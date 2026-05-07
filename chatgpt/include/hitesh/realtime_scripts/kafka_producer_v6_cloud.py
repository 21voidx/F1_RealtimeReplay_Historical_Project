import argparse
import json
import logging
import os
import socket
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional

import requests
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class F1SingleStoreProducer:
    def __init__(self, session_key: str, start_time: str, end_time: str, driver_number: str):
        self.producer: Optional[Producer] = None
        self.session_key = session_key
        self.start_time = start_time
        self.end_time = end_time
        self.driver_number = driver_number
        self.base_url = os.getenv("OPENF1_BASE_URL", "https://api.openf1.org/v1")

        self.kafka_config = {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            "client.id": os.getenv("KAFKA_CLIENT_ID", f"f1-producer-{socket.gethostname()}"),
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 1000,
            "message.timeout.ms": 30000,
        }
        security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        if security_protocol and security_protocol != "PLAINTEXT":
            self.kafka_config.update({
                "security.protocol": security_protocol,
                "sasl.mechanisms": os.getenv("KAFKA_SASL_MECHANISMS", "PLAIN"),
                "sasl.username": os.getenv("KAFKA_SASL_USERNAME", ""),
                "sasl.password": os.getenv("KAFKA_SASL_PASSWORD", ""),
            })

        self.data_types = {
            "location": {"endpoint": "location", "topic": "topic_0"},
            "car": {"endpoint": "car_data", "topic": "topic_1"},
            "intervals": {"endpoint": "intervals", "topic": "topic_2"},
            "position": {"endpoint": "position", "topic": "topic_3"},
        }

    def delivery_callback(self, err, msg):
        if err is not None:
            logger.error("Message delivery failed: %s", err)
        else:
            logger.info("Delivered to %s [%s] @ %s", msg.topic(), msg.partition(), msg.offset())

    def connect_kafka(self) -> bool:
        if self.producer is None:
            logger.info("Connecting to Kafka at %s", self.kafka_config["bootstrap.servers"])
            self.producer = Producer(self.kafka_config)
        return True

    @staticmethod
    def _safe_float(value):
        if value in (None, "", "+1 LAP"):
            return None
        try:
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _safe_int(value, default=0):
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _format_date(value: str) -> Optional[str]:
        if not value:
            return None
        cleaned = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(cleaned).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return value.replace("T", " ").split("+")[0]

    def get_f1_data(self, data_type: str) -> List[Dict]:
        config = self.data_types[data_type]
        url = f"{self.base_url}/{config['endpoint']}"
        params = {
            "session_key": self.session_key,
            "driver_number": self.driver_number,
            "date>": self.start_time,
            "date<": self.end_time,
        }
        logger.info("Fetching %s from %s with %s", data_type, url, params)
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            return [data]
        return data or []

    def format_record(self, record: Dict, data_type: str) -> Dict:
        formatted = {"id": str(uuid.uuid4())}
        if data_type == "location":
            formatted.update({
                "x": self._safe_float(record.get("x")),
                "y": self._safe_float(record.get("y")),
                "z": self._safe_float(record.get("z")),
            })
        elif data_type == "position":
            formatted.update({"position": self._safe_int(record.get("position"))})
        elif data_type == "intervals":
            formatted.update({
                "gap_to_leader": self._safe_float(record.get("gap_to_leader")),
                "time_interval": self._safe_float(record.get("interval")),
            })
        elif data_type == "car":
            formatted.update({
                "brake": self._safe_int(record.get("brake")),
                "drs": self._safe_int(record.get("drs")),
                "n_gear": self._safe_int(record.get("n_gear")),
                "rpm": self._safe_int(record.get("rpm")),
                "speed": self._safe_int(record.get("speed")),
                "throttle": self._safe_int(record.get("throttle")),
            })

        formatted.update({
            "driver_number": self._safe_int(record.get("driver_number"), self._safe_int(self.driver_number)),
            "date": self._format_date(record.get("date", "")),
            "session_key": self._safe_int(record.get("session_key"), self._safe_int(self.session_key)),
            "meeting_key": self._safe_int(record.get("meeting_key")),
        })
        return formatted

    def prepare_record(self, record: Dict, data_type: str) -> Dict:
        return {"payload": self.format_record(record, data_type)}

    def send_record(self, data_type: str, record: Dict, record_num: int, total: int) -> bool:
        self.connect_kafka()
        assert self.producer is not None
        topic = self.data_types[data_type]["topic"]
        value = json.dumps(self.prepare_record(record, data_type), default=str).encode("utf-8")
        key = json.dumps({"driver_number": self._safe_int(record.get("driver_number"), self._safe_int(self.driver_number))}).encode("utf-8")
        logger.info("Sending %s record %s/%s to %s", data_type, record_num, total, topic)
        self.producer.produce(topic=topic, key=key, value=value, callback=self.delivery_callback)
        self.producer.poll(0)
        return True

    def process_data(self, data_type: str, data: List[Dict]) -> None:
        if not data:
            logger.warning("No %s data returned by OpenF1", data_type)
            return
        for i, record in enumerate(data, start=1):
            for attempt in range(1, 4):
                try:
                    self.send_record(data_type, record, i, len(data))
                    break
                except Exception as exc:
                    logger.warning("Attempt %s failed for %s record: %s", attempt, data_type, exc)
                    time.sleep(1)
        if self.producer:
            self.producer.flush(timeout=30)

    def run(self, data_types: Optional[List[str]] = None) -> None:
        selected = data_types or list(self.data_types.keys())
        for data_type in selected:
            if data_type not in self.data_types:
                logger.warning("Unknown data type %s. Skipped.", data_type)
                continue
            data = self.get_f1_data(data_type)
            self.process_data(data_type, data)
        if self.producer:
            self.producer.flush(timeout=30)


def main():
    parser = argparse.ArgumentParser(description="F1 OpenF1 -> Kafka producer")
    parser.add_argument("--param1", required=True, help="Start time, e.g. 2024-03-02T15:00:00.200")
    parser.add_argument("--param2", required=True, help="End time")
    parser.add_argument("--session", required=True, help="OpenF1 session_key")
    parser.add_argument("--driver", required=True, help="Driver number")
    parser.add_argument("--data-types", nargs="+", choices=["location", "position", "car", "intervals"], default=["car", "location"])
    args = parser.parse_args()

    producer = F1SingleStoreProducer(
        session_key=args.session,
        start_time=args.param1,
        end_time=args.param2,
        driver_number=args.driver,
    )
    producer.run(args.data_types)


if __name__ == "__main__":
    main()
