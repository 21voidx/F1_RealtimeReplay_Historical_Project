import json
import time
from urllib.request import urlopen
from confluent_kafka import Producer
import logging
from datetime import datetime
import uuid
from typing import Dict, List, Optional, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class F1SingleStoreProducer:
    def __init__(self):
        def __init__(self):
            self.producer = None
            self.session_key = args.session
            self.start_time = args.param1
            self.driver_number = args.driver
            self.end_time = args.param2

        # Updated Kafka configuration for Confluent Cloud masked for security reasons
        self.kafka_config = {
            'bootstrap.servers': '',
            'security.protocol': '',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': '',
            'sasl.password': '',
            'client.id': '',
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 1000,
            'session.timeout.ms': 45000,
            'linger.ms': 0,  # No batching delay
            'batch.size': 100,  # Extremely small batches
            'compression.type': 'none',
            'max.in.flight.requests.per.connection': 1000,  # More parallel requests
            'queue.buffering.max.messages': 100,
            'queue.buffering.max.ms': 5

        }

        # Data type configurations - updated topic name
        # Data type configurations
        self.data_types = {
            'location': {
                'endpoint': 'location',
                'topic': 'topic_0',
                'schema': {
                    "type": "struct",
                    "fields": [
                        {"type": "string", "optional": False, "field": "id"},
                        {"type": "float", "optional": True, "field": "x"},
                        {"type": "float", "optional": True, "field": "y"},
                        {"type": "float", "optional": True, "field": "z"},
                        {"type": "int32", "optional": False, "field": "driver_number"},
                        {"type": "string", "optional": True, "field": "date"},
                        {"type": "int32", "optional": False, "field": "session_key"},
                        {"type": "int32", "optional": False, "field": "meeting_key"}
                    ],
                    "optional": False,
                    "name": "f1_location_data"
                }
            },
            'position': {
                'endpoint': 'position',
                'topic': 'topic_3',  # Changed to topic_3
                'schema': {
                    "type": "struct",
                    "fields": [
                        {"type": "string", "optional": False, "field": "id"},
                        {"type": "int32", "optional": True, "field": "position"},
                        {"type": "int32", "optional": False, "field": "driver_number"},
                        {"type": "string", "optional": True, "field": "date"},
                        {"type": "int32", "optional": False, "field": "session_key"},
                        {"type": "int32", "optional": False, "field": "meeting_key"}
                    ],
                    "optional": False,
                    "name": "f1_position_data"
                }
            },
            'car': {
                'endpoint': 'car_data',
                'topic': 'topic_1',  # Changed to topic_1
                'schema': {
                    "type": "struct",
                    "fields": [
                        {"type": "string", "optional": False, "field": "id"},
                        {"type": "int32", "optional": True, "field": "brake"},
                        {"type": "int32", "optional": True, "field": "drs"},
                        {"type": "int32", "optional": True, "field": "n_gear"},
                        {"type": "int32", "optional": True, "field": "rpm"},
                        {"type": "int32", "optional": True, "field": "speed"},
                        {"type": "int32", "optional": True, "field": "throttle"},
                        {"type": "int32", "optional": False, "field": "driver_number"},
                        {"type": "string", "optional": True, "field": "date"},
                        {"type": "int32", "optional": False, "field": "session_key"},
                        {"type": "int32", "optional": False, "field": "meeting_key"}
                    ],
                    "optional": False,
                    "name": "f1_car_data"
                }
            },
            'intervals': {
                'endpoint': 'intervals',
                'topic': 'topic_2',  # Changed to topic_2
                'schema': {
                    "type": "struct",
                    "fields": [
                        {"type": "string", "optional": False, "field": "id"},
                        {"type": "int32", "optional": False, "field": "driver_number"},
                        {"type": "float", "optional": True, "field": "gap_to_leader"},
                        {"type": "float", "optional": True, "field": "time_interval"},
                        {"type": "string", "optional": True, "field": "date"},
                        {"type": "int32", "optional": False, "field": "session_key"},
                        {"type": "int32", "optional": False, "field": "meeting_key"}
                    ],
                    "optional": False,
                    "name": "f1_intervals_data"
                }
            }
        }

    def delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')

    def connect_kafka(self) -> bool:
        """Connect to Kafka"""
        try:
            if self.producer is not None:
                self.producer.close()
                self.producer = None

            logger.info("Connecting to Confluent Cloud Kafka...")
            self.producer = Producer(self.kafka_config)
            logger.info("Successfully connected to Confluent Cloud")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False

    def send_record(self, data_type: str, record: Dict, record_num: int, total: int) -> bool:
        """Send a record to Kafka"""
        try:
            if self.producer is None and not self.connect_kafka():
                return False

            prepared_record = self.prepare_record(record, data_type)
            driver_number = record.get('driver_number', 0)
            prepared_key = self.prepare_key(driver_number)
            topic = self.data_types[data_type]['topic']

            logger.info(f"Sending {data_type} record {record_num}/{total} for driver {driver_number}")

            # Convert to JSON string and encode
            key_bytes = json.dumps(prepared_key).encode('utf-8')
            value_bytes = json.dumps(prepared_record).encode('utf-8')

            self.producer.produce(
                topic=topic,
                key=key_bytes,
                value=value_bytes,
                callback=self.delivery_callback
            )

            # Poll to handle delivery reports
            self.producer.poll(0)

            return True

        except Exception as e:
            logger.error(f"Error sending record: {e}")
            if self.producer is not None:
                self.producer.close()
                self.producer = None
            return False
    def get_f1_data(self, data_type: str) -> List[Dict]:
        """Fetch data from OpenF1 API"""
        endpoint = self.data_types[data_type]['endpoint']
        url = (f"{self.base_url}/{endpoint}"
               f"?session_key={self.session_key}"
               f"&driver_number={self.driver_number}"
               f"&date>{self.start_time}"
               f"&date<{self.end_time}")

        try:
            logger.info(f"Fetching {data_type} data from: {url}")
            response = urlopen(url)
            data = json.loads(response.read().decode('utf-8'))
            logger.info(f"Retrieved {len(data)} records")
            return data
        except Exception as e:
            logger.error(f"Error fetching F1 data: {e}")
            return []
    def prepare_record(self, record: Dict, data_type: str) -> Dict:
        """Prepare record with schema"""
        try:
            formatted_record = self.format_record(record, data_type)
            return {
                "schema": self.data_types[data_type]['schema'],
                "payload": formatted_record
            }
        except Exception as e:
            logger.error(f"Error preparing record: {e}")
            raise
    def format_record(self, record: Dict, data_type: str) -> Dict:
        """Format the record based on data type"""
        formatted_record = {"id": str(uuid.uuid4())}

        if data_type == 'location':
            formatted_record.update({
                "x": float(record.get('x', 0.0)),
                "y": float(record.get('y', 0.0)),
                "z": float(record.get('z', 0.0))
            })
        elif data_type == 'position':
            formatted_record.update({
                "position": int(record.get('position', 0))
            })
        elif data_type == 'intervals':
            formatted_record.update({
                "gap_to_leader": float(record.get('gap_to_leader', 0.0)) if record.get('gap_to_leader') not in [None,
                                                                                                                '+1 LAP'] else None,
                "time_interval": float(record.get('interval', 0.0)) if record.get('interval') not in [None,
                                                                                                      '+1 LAP'] else None
            # Updated field name
        })
        elif data_type == 'car':  # car data
            formatted_record.update({
                "brake": int(record.get('brake', 0)),
                "drs": int(record.get('drs', 0)),
                "n_gear": int(record.get('n_gear', 0)),
                "rpm": int(record.get('rpm', 0)),
                "speed": int(record.get('speed', 0)),
                "throttle": int(record.get('throttle', 0))
            })

        # Common fields
        date_str = record.get('date', '')
        formatted_record.update({
            "driver_number": int(record.get('driver_number', 0)),
            "date": datetime.fromisoformat(date_str.replace('+00:00', '')).strftime('%Y-%m-%d %H:%M:%S') if date_str else None,
            "session_key": int(record.get('session_key', 0)),
            "meeting_key": int(record.get('meeting_key', 0))
        })

        return formatted_record
    def prepare_key(self, driver_number: int) -> Dict:
        """Prepare key with schema format"""
        return {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int32", "optional": False, "field": "driver_number"}
                ],
                "optional": False,
                "name": "f1_driver_key"
            },
            "payload": {
                "driver_number": int(driver_number)
            }
        }

    def process_data(self, data_type: str, data: List[Dict]) -> None:
        """Process data for a specific type"""
        if not data:
            logger.warning(f"No {data_type} data to process")
            return

        for i, record in enumerate(data, 1):
            success = False
            retries = 0
            while not success and retries < 3:
                success = self.send_record(data_type, record, i, len(data))
                if not success:
                    retries += 1
                    time.sleep(1)
            if not success:
                logger.error(f"Failed to send {data_type} record after {retries} attempts")

            # Flush after each record to ensure delivery
            self.producer.flush(timeout=10)
            time.sleep(0.1)

    # Rest of the methods remain the same...

    def run(self, data_types: Optional[List[str]] = None) -> None:
        """Main processing loop"""
        if data_types is None:
            data_types = list(self.data_types.keys())
        elif isinstance(data_types, str):
            data_types = [data_types]

        logger.info(f"Starting F1 data producer for: {', '.join(data_types)}")

        try:
            for data_type in data_types:
                if data_type not in self.data_types:
                    logger.error(f"Unknown data type: {data_type}")
                    continue

                logger.info(f"\n=== Processing {data_type.title()} Data ===")
                data = self.get_f1_data(data_type)
                self.process_data(data_type, data)

        except KeyboardInterrupt:
            logger.info("\nShutting down...")
        finally:
            if self.producer:
                self.producer.flush()  # Ensure all messages are delivered
                self.producer.close()
                logger.info("Producer closed")


if __name__ == "__main__":
    import argparse

    if __name__ == "__main__":
        parser = argparse.ArgumentParser(description='F1 Data Producer')
        parser.add_argument('--param1', required=True, help='Start time')
        parser.add_argument('--param2', required=True, help='End time')
        parser.add_argument('--session', required=True, help='Session key')
        parser.add_argument('--driver', required=True, help='Driver number')
        parser.add_argument('--data-types', nargs='+',
                            choices=['location', 'position', 'car', 'intervals'],
                            help='Data types to process')
        args = parser.parse_args()

        try:
            producer = F1SingleStoreProducer()
            producer.run(args.data_types)
        except Exception as e:
            logger.error(f"Fatal error: {e}")