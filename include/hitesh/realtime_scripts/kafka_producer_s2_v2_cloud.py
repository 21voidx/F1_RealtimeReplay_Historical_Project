from dotenv import load_dotenv
import os
import json
from confluent_kafka import Consumer, KafkaError
import singlestoredb as s2
from datetime import datetime
import logging
import socket
import time
from typing import Dict, List, Optional, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv('.env')


class F1SingleStorePipeline:
    def __init__(self, max_retries=3, retry_delay=5):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._load_config()
        self._init_data_types()
        self.driver_timestamps = {}
        self.conn = None

    def _init_data_types(self):
        """Initialize data type configurations"""
        self.data_types = {
            'location': {
                'topic': 'topic_0',
                'table': 'f1_location_data',
                'pipeline_name': 'f1_location_pipeline',
                'schema': """
                    CREATE TABLE IF NOT EXISTS f1_location_data (
                        id VARCHAR(255) PRIMARY KEY,
                        x FLOAT,
                        y FLOAT,
                        z FLOAT,
                        driver_number INT,
                        date DATETIME,
                        session_key INT,
                        meeting_key INT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """,
                'pipeline_fields': """
                       `id` <- payload::id,
                        `x` <-  payload::x,
                        `y` <-  payload::y,
                        `z` <-  payload::z,
                        `driver_number` <-  payload::driver_number,
                        `date` <- payload::date,
                        `session_key` <-  payload::session_key,
                        `meeting_key` <-  payload::meeting_key
                """
            },
            'position': {
                'topic': 'topic_3',
                'table': 'f1_position_data',
                'pipeline_name': 'f1_position_pipeline',
                'schema': """
                    CREATE TABLE IF NOT EXISTS f1_position_data (
                        id VARCHAR(255) PRIMARY KEY,
                        position INT,
                        driver_number INT,
                        date DATETIME,
                        session_key INT,
                        meeting_key INT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """,
                'pipeline_fields': """
                    `id` <- payload::id,
                    `position` <- payload::position,
                    `driver_number` <- payload::driver_number,
                    `date` <- payload::date,
                    `session_key` <- payload::session_key,
                    `meeting_key` <- payload::meeting_key
                """
            },
            'car': {
                'topic': 'topic_1',
                'table': 'f1_car_data',
                'pipeline_name': 'f1_car_pipeline',
                'schema': """
                    CREATE TABLE IF NOT EXISTS f1_car_data (
                        id VARCHAR(255) PRIMARY KEY,
                        brake INT,
                        drs INT,
                        n_gear INT,
                        rpm INT,
                        speed INT,
                        throttle INT,
                        driver_number INT,
                        date DATETIME,
                        session_key INT,
                        meeting_key INT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """,
                'pipeline_fields': """
                    `id` <- payload::id,
                    `brake` <- payload::brake,
                    `drs` <- payload::drs,
                    `n_gear` <- payload::n_gear,
                    `rpm` <- payload::rpm,
                    `speed` <- payload::speed,
                    `throttle` <- payload::throttle,
                    `driver_number` <- payload::driver_number,
                    `date` <- payload::date,
                    `session_key` <- payload::session_key,
                    `meeting_key` <- payload::meeting_key
                """
            },
            'intervals': {
                'topic': 'topic_2',
                'table': 'f1_intervals_data',
                'pipeline_name': 'f1_intervals_pipeline',
                'schema': """
                    CREATE TABLE IF NOT EXISTS f1_intervals_data (
                        id VARCHAR(255) PRIMARY KEY,
                        driver_number INT,
                        gap_to_leader FLOAT,
                        time_interval FLOAT,
                        date DATETIME,
                        session_key INT,
                        meeting_key INT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """,
                'pipeline_fields': """
                    `id` <- id,
                    `driver_number` <- payload::driver_number,
                    `gap_to_leader` <- payload::gap_to_leader,
                    `time_interval` <- payload::interval,
                    `date` <- payload::date,
                    `session_key` <- payload::session_key,
                    `meeting_key` <- payload::meeting_key
                """
            }
        }

    def setup_pipeline(self, data_type: str):
        """Set up SingleStore pipeline for a data type"""
        try:
            config = self.data_types[data_type]
            cursor = self.conn.cursor()

            # Drop existing pipeline
            drop_pipeline_query = f"DROP PIPELINE IF EXISTS {config['pipeline_name']}"
            cursor.execute(drop_pipeline_query)
            logger.info(f"Dropped existing pipeline {config['pipeline_name']}")

            # Create pipeline
            create_pipeline_query = f"""
            CREATE PIPELINE {config['pipeline_name']}
            AS LOAD DATA KAFKA '{self.kafka_config['bootstrap.servers']}/{config['topic']}'
            CONFIG '{{
                "client.id": "{self.kafka_config['client.id']}",
                "sasl.username": "{self.kafka_config['sasl.username']}",
                "sasl.mechanism": "PLAIN",
                "security.protocol": "SASL_SSL",
                "ssl.ca.location": "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"
            }}'
            CREDENTIALS '{{
                "sasl.password": "{self.kafka_config['sasl.password']}"
            }}'
            SKIP DUPLICATE KEY ERRORS
            INTO TABLE {config['table']}
            FORMAT JSON
            (
                {self._get_pipeline_fields(data_type)}
            )
            """
            print(create_pipeline_query)
            cursor.execute(create_pipeline_query)
            logger.info(f"Created pipeline {config['pipeline_name']}")

        except Exception as e:
            logger.error(f"Failed to setup pipeline for {data_type}: {e}")
            raise
    def start_pipeline(self, data_type: str):
        """Start a SingleStore pipeline"""
        try:
            cursor = self.conn.cursor()
            pipeline_name = self.data_types[data_type]['pipeline_name']
            cursor.execute(f"START PIPELINE {pipeline_name}")
            logger.info(f"Started pipeline {pipeline_name}")
        except Exception as e:
            logger.error(f"Failed to start pipeline {pipeline_name}: {e}")
            raise

    def monitor_pipeline(self, data_type: str):
        """Monitor pipeline status"""
        try:
            cursor = self.conn.cursor()
            pipeline_name = self.data_types[data_type]['pipeline_name']
            cursor.execute(f"""
                SELECT status, error, records_inserted, records_processed
                FROM information_schema.pipelines 
                WHERE pipeline_name = '{pipeline_name}'
            """)
            status = cursor.fetchone()
            if status:
                logger.info(f"Pipeline {pipeline_name}: Status={status[0]}, "
                          f"Records Inserted={status[2]}, Records Processed={status[3]}")
                if status[1]:  # Error
                    logger.error(f"Pipeline error: {status[1]}")
            return status
        except Exception as e:
            logger.error(f"Failed to monitor pipeline {pipeline_name}: {e}")
            raise
    def process_messages(self, data_types: List[str]):
        """Process messages using SingleStore pipelines"""
        try:
            invalid_types = set(data_types) - set(self.data_types.keys())
            if invalid_types:
                raise ValueError(f"Invalid data types: {invalid_types}")

            if not self.connect_db_with_retry():
                raise ConnectionError("Failed to establish SingleStore connection")

            # Create tables and setup pipelines
            for data_type in data_types:
                self.create_tables([data_type])
                self.setup_pipeline(data_type)
                self.start_pipeline(data_type)

            # Monitor pipelines
            while True:
                for data_type in data_types:
                    status = self.monitor_pipeline(data_type)
                    if status and status[1]:  # Error condition
                        raise Exception(f"Pipeline error for {data_type}: {status[1]}")
                time.sleep(10)  # Check status every 10 seconds

        except KeyboardInterrupt:
            logger.info("Shutting down pipelines gracefully...")
            self._cleanup(data_types)
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self._cleanup(data_types)
            raise

    def _cleanup(self, data_types: List[str]):
        """Clean up pipelines and connections"""
        if self.conn:
            try:
                cursor = self.conn.cursor()
                for data_type in data_types:
                    pipeline_name = self.data_types[data_type]['pipeline_name']
                    cursor.execute(f"STOP PIPELINE {pipeline_name}")
                    logger.info(f"Stopped pipeline {pipeline_name}")
                self.conn.close()
                logger.info("SingleStore connection closed")
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")

    def _load_config(self):
        """Load and validate configuration"""
        # Confluent Cloud configuration
        self.kafka_config = {
            'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': '7G3VW2EN5K6RSYO3',
            'sasl.password': 'lKDw9j5VJhg+QYsufy8x82HOUIy2K7Bl92/N3fMg1FNbUc/rb2oRjBRXQvGo/LAm',
            'group.id': 'f1_consumer_group',
            'auto.offset.reset': 'earliest',
            'session.timeout.ms': 45000,
            'client.id': 'ccloud-python-client-609ae157-eaef-4276-ae9a-a3a81742c6a2',
            # 'batch.size': 50000 , # Increase batch size
            # 'linger.ms': 50,  # Wait longer to batch messages
            # 'compression.type': 'snappy',  # Add compression
            # 'buffer.memory': 67108864,  # Increase buffer size
        }

        self.db_config = {
            'host': os.getenv('SINGLESTORE_HOST'),
            'user': os.getenv('SINGLESTORE_USER'),
            'password': os.getenv('SINGLESTORE_PASSWORD'),
            'database': os.getenv('SINGLESTORE_DATABASE'),
            'port': os.getenv('SINGLESTORE_PORT')

        }

        missing_configs = [k for k, v in self.db_config.items() if not v]
        if missing_configs:
            raise ValueError(f"Missing SingleStore configurations: {', '.join(missing_configs)}")

        logger.info("Confluent Cloud and SingleStore configurations loaded")

    def _verify_host_connection(self, host: str, port, timeout: int = 5) -> bool:
        """Verify if the host is reachable using database cursor"""
        # Create a connection to the database
        user = os.getenv('SINGLESTORE_USER')
        password =  os.getenv('SINGLESTORE_PASSWORD')
        print( f'{user}:{password}@{host}:{port}/')
        conn = s2.connect(
            f'{user}:{password}@{host}:{port}/')
        # Check if the connection is open
        with conn:
            with conn.cursor() as cur:
                flag = cur.is_connected()
                print(flag)

    def _drop_tables(self, data_types: List[str]):
        """Drop tables for specified data types"""
        cursor = self.conn.cursor()
        for data_type in data_types:
            try:
                # First stop and drop the pipeline if it exists
                pipeline_name = self.data_types[data_type]['pipeline_name']
                cursor.execute(
                    f"SELECT pipeline_name FROM information_schema.pipelines WHERE pipeline_name = '{pipeline_name}'")
                if cursor.fetchone():
                    cursor.execute(f"STOP PIPELINE {pipeline_name}")
                    cursor.execute(f"DROP PIPELINE {pipeline_name}")
                    logger.info(f"Pipeline {pipeline_name} stopped and dropped")

                # Now drop the table
                table_name = self.data_types[data_type]['table']
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.conn.commit()
                logger.info(f"Table {table_name} dropped successfully")
            except Exception as e:
                logger.error(f"Failed to drop table for {data_type}: {e}")
                raise

    def _cleanup(self, data_types: Optional[List[str]] = None):
        """Clean up pipelines and connections"""
        if self.conn:
            try:
                cursor = self.conn.cursor()
                if data_types:
                    for data_type in data_types:
                        pipeline_name = self.data_types[data_type]['pipeline_name']
                        # Check if pipeline exists before stopping/dropping
                        cursor.execute(
                            f"SELECT pipeline_name FROM information_schema.pipelines WHERE pipeline_name = '{pipeline_name}'")
                        if cursor.fetchone():
                            cursor.execute(f"STOP PIPELINE {pipeline_name}")
                            cursor.execute(f"DROP PIPELINE {pipeline_name}")
                            logger.info(f"Stopped and dropped pipeline {pipeline_name}")
                self.conn.close()
                logger.info("SingleStore connection closed")
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")

    def create_tables(self, data_types: List[str]):
        """Create tables for specified data types"""
        cursor = self.conn.cursor()
        self._drop_tables(data_types)  # Drop tables first
        for data_type in data_types:
            try:
                cursor.execute(self.data_types[data_type]['schema'])
                self.conn.commit()
                logger.info(f"Table {self.data_types[data_type]['table']} created/verified successfully")
            except Exception as e:
                logger.error(f"Failed to create table for {data_type}: {e}")
                raise

    def connect_db_with_retry(self) -> bool:
        """Connect to SingleStore with retry mechanism"""
        host = self.db_config['host']
        database =  self.db_config['database']
        port = self.db_config['port']  # Default port, you can make this configurable if needed
        logger.info(f"Attempting to connect to SingleStore at {host}")

        for attempt in range(self.max_retries):
            try:
                # Create connection string
                user = os.getenv('SINGLESTORE_USER')
                password = os.getenv('SINGLESTORE_PASSWORD')
                logger.info(f"Connecting with: {user}:****@{host}:{port}/{database}")

                # Establish connection
                self.conn = s2.connect(f'{user}:{password}@{host}:{port}/{database}')

                # Verify connection
                with self.conn.cursor() as cursor:
                    is_connected = cursor.is_connected()
                    if not is_connected:
                        raise ConnectionError("Connection test failed")

                logger.info("Successfully connected to SingleStore")
                return True

            except Exception as e:
                logger.error(f"Connection attempt {attempt + 1}/{self.max_retries} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error("Max retries reached. Could not connect to SingleStore")
                    raise

        return False

    def connect_kafka_with_retry(self, topics: List[str]) -> bool:
        """Connect to Confluent Cloud Kafka with retry mechanism"""
        for attempt in range(self.max_retries):
            try:
                logger.info("Attempting to connect to Confluent Cloud...")
                self.consumer = Consumer(self.kafka_config)
                self.consumer.subscribe(topics)

                # Test the connection
                msg = self.consumer.poll(timeout=5.0)
                logger.info("Successfully connected to Confluent Cloud")
                return True

            except Exception as e:
                logger.error(f"Kafka connection attempt {attempt + 1}/{self.max_retries} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error("Max retries reached. Could not connect to Kafka")
                    raise

        return False

    def should_process_message(self, message_timestamp: datetime, driver_number: int) -> bool:
        """Determine if a message should be processed based on its timestamp and driver number"""
        current_second = message_timestamp.strftime('%Y-%m-%d %H:%M:%S')

        if driver_number not in self.driver_timestamps:
            self.driver_timestamps[driver_number] = current_second
            return True

        if current_second != self.driver_timestamps[driver_number]:
            self.driver_timestamps[driver_number] = current_second
            return True

        return False

    def process_messages(self, data_types: List[str]):
        """Process messages using SingleStore pipelines"""
        try:
            invalid_types = set(data_types) - set(self.data_types.keys())
            if invalid_types:
                raise ValueError(f"Invalid data types: {invalid_types}")

            if not self.connect_db_with_retry():
                raise ConnectionError("Failed to establish SingleStore connection")

            # Create tables and setup pipelines
            for data_type in data_types:
                self.create_tables([data_type])
                self.setup_pipeline(data_type)
                self.start_pipeline(data_type)

            # Monitor pipelines
            while True:
                for data_type in data_types:
                    status = self.monitor_pipeline(data_type)
                    if status and status[1]:  # Error condition
                        raise Exception(f"Pipeline error for {data_type}: {status[1]}")
                time.sleep(10)  # Check status every 10 seconds

        except KeyboardInterrupt:
            logger.info("Shutting down pipelines gracefully...")
            self._cleanup(data_types)
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self._cleanup(data_types)
            raise

    def _process_message_loop(self, data_types: List[str], batch_size: int):
        """Main message processing loop"""
        cursor = self.conn.cursor()
        batches = {dt: [] for dt in data_types}
        processed_counts = {dt: 0 for dt in data_types}
        driver_counts = {dt: {} for dt in data_types}

        try:
            logger.info("Starting to process messages...")
            while True:
                try:
                    msg = self.consumer.poll(1.0)

                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            logger.error(f"Consumer error: {msg.error()}")
                            continue

                    # Parse the message value
                    message_value = json.loads(msg.value().decode('utf-8'))
                    data = message_value['payload']

                    # Determine data type based on message structure
                    data_type = next(
                        (dt for dt in data_types if all(
                            required_field in data
                            for required_field in self._get_required_fields(dt)
                        )),
                        None
                    )

                    if not data_type:
                        logger.warning(f"Could not determine data type for message: {data}")
                        continue

                    message_timestamp = datetime.strptime(data['date'], '%Y-%m-%d %H:%M:%S')
                    driver_number = data['driver_number']

                    if not self.should_process_message(message_timestamp, driver_number):
                        continue

                    if driver_number not in driver_counts[data_type]:
                        driver_counts[data_type][driver_number] = 0
                    driver_counts[data_type][driver_number] += 1

                    record = self.data_types[data_type]['process_record'](data)
                    batches[data_type].append(record)
                    processed_counts[data_type] += 1

                    if len(batches[data_type]) >= batch_size:
                        self._insert_batch(cursor, data_type, batches[data_type])
                        logger.info(f"Processed {processed_counts[data_type]} {data_type} messages")
                        logger.info(f"{data_type} messages per driver: " +
                                    ", ".join([f"Driver {d}: {c}" for d, c in driver_counts[data_type].items()]))
                        batches[data_type] = []

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue

        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            for dt in data_types:
                if batches[dt]:
                    self._insert_batch(cursor, dt, batches[dt])
                logger.info(f"Final statistics for {dt} - Processed: {processed_counts[dt]} messages")
                logger.info(f"Final {dt} messages per driver: " +
                            ", ".join([f"Driver {d}: {c}" for d, c in driver_counts[dt].items()]))


    def _insert_batch(self, cursor, data_type: str, batch: List[tuple]):
        """Insert a batch of records into the database"""
        try:
            config = self.data_types[data_type]
            placeholders = ', '.join(['%s'] * len(batch[0]))
            query = f"INSERT INTO {config['table']} {config['insert_fields']} VALUES ({placeholders})"
            cursor.executemany(query, batch)
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to insert batch for {data_type}: {e}")
            raise

    def _get_pipeline_fields(self, data_type: str) -> str:
        """Get field mappings for pipeline based on data type"""
        if data_type == 'location':
            return """
                `id` <- payload::id,
                `x` <- payload::x,
                `y` <- payload::y,
                `z` <- payload::z,
                `driver_number` <- payload::driver_number,
                `date` <- payload::date,
                `session_key` <- payload::session_key,
                `meeting_key` <- payload::meeting_key
            """
        elif data_type == 'position':
            return """
                `id` <- payload::id,
                `position` <- payload::position,
                `driver_number` <- payload::driver_number,
                `date` <- payload::date,
                `session_key` <- payload::session_key,
                `meeting_key` <- payload::meeting_key
            """
        elif data_type == 'car':
            return """
                `id` <- payload::id,
                `brake` <- payload::brake,
                `drs` <- payload::drs,
                `n_gear` <- payload::n_gear,
                `rpm` <- payload::rpm,
                `speed` <- payload::speed,
                `throttle` <- payload::throttle,
                `driver_number` <- payload::driver_number,
                `date` <- payload::date,
                `session_key` <- payload::session_key,
                `meeting_key` <- payload::meeting_key
            """
        elif data_type == 'intervals':
            return """
                `id` <- payload::id,
                `driver_number` <- payload::driver_number,
                `gap_to_leader` <- payload::gap_to_leader,
                `time_interval` <- payload::interval,
                `date` <- payload::date,
                `session_key` <- payload::session_key,
                `meeting_key` <- payload::meeting_key
            """

    def trigger_all_pipelines(self):
        """Create and trigger all data pipelines"""
        try:
            # Ensure database connection
            if not self.connect_db_with_retry():
                raise ConnectionError("Failed to establish SingleStore connection")

            # List of all data types
            all_data_types = ['location', 'position', 'car', 'intervals']

            logger.info("Starting pipeline creation and triggering process...")

            # First clean up any existing pipelines
            self._cleanup(all_data_types)

            # Then create all tables
            self.create_tables(all_data_types)
            logger.info("All tables created successfully")

            # Setup and start each pipeline
            for data_type in all_data_types:
                try:
                    logger.info(f"Setting up pipeline for {data_type}...")
                    self.setup_pipeline(data_type)
                    self.start_pipeline(data_type)
                    logger.info(f"Pipeline for {data_type} started successfully")
                except Exception as e:
                    logger.error(f"Failed to setup/start pipeline for {data_type}: {e}")
                    self._cleanup([data_type])
                    continue

            # Monitor all pipelines continuously
            logger.info("All pipelines started. Beginning monitoring...")
            return self.monitor_all_pipelines(all_data_types)

        except Exception as e:
            logger.error(f"Failed to trigger pipelines: {e}")
            self._cleanup(all_data_types)
            raise
        finally:
            # Close the connection only after everything is done
            if self.conn:
                try:
                    self.conn.close()
                    logger.info("SingleStore connection closed")
                except Exception as e:
                    logger.error(f"Error closing connection: {e}")

    def monitor_all_pipelines(self, data_types: List[str]):
        """Monitor all pipelines and report their status"""
        try:
            while True:
                all_status = {}
                for data_type in data_types:
                    try:
                        status = self.monitor_pipeline(data_type)
                        if status:
                            all_status[data_type] = {
                                'status': status[0],
                                'error': status[1],
                                'records_inserted': status[2],
                                'records_processed': status[3]
                            }

                            # Log any errors
                            if status[1]:
                                logger.error(f"Pipeline error for {data_type}: {status[1]}")

                    except Exception as e:
                        logger.error(f"Error monitoring {data_type} pipeline: {e}")
                        all_status[data_type] = {'status': 'ERROR', 'error': str(e)}

                # Print a status summary
                logger.info("\n=== Pipeline Status Summary ===")
                for dt, stats in all_status.items():
                    logger.info(f"{dt.upper()}: Status={stats['status']}, "
                                f"Processed={stats.get('records_processed', 'N/A')}, "
                                f"Inserted={stats.get('records_inserted', 'N/A')}")
                logger.info("===============================\n")

                time.sleep(10)  # Status update interval

        except KeyboardInterrupt:
            logger.info("Shutting down pipeline monitoring...")
            self._cleanup(data_types)
        except Exception as e:
            logger.error(f"Pipeline monitoring failed: {e}")
            self._cleanup(data_types)
            raise

    def _cleanup(self, data_types: Optional[List[str]] = None):
        """Clean up pipelines and connections"""
        if self.conn:
            try:
                cursor = self.conn.cursor()
                if data_types:
                    for data_type in data_types:
                        pipeline_name = self.data_types[data_type]['pipeline_name']
                        # Check if pipeline exists before stopping/dropping
                        cursor.execute(
                            f"SELECT pipeline_name FROM information_schema.pipelines WHERE pipeline_name = '{pipeline_name}'")
                        if cursor.fetchone():
                            cursor.execute(f"STOP PIPELINE {pipeline_name}")
                            cursor.execute(f"DROP PIPELINE {pipeline_name}")
                            logger.info(f"Stopped and dropped pipeline {pipeline_name}")
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='F1 Data Consumer Pipeline')
    parser.add_argument('--max-retries', type=int, default=3,
                        help='Maximum number of connection retry attempts')
    parser.add_argument('--retry-delay', type=int, default=5,
                        help='Delay between retry attempts in seconds')

    args = parser.parse_args()

    try:
        pipeline = F1SingleStorePipeline(
            max_retries=args.max_retries,
            retry_delay=args.retry_delay
        )

        # Trigger and monitor all pipelines
        pipeline.trigger_all_pipelines()
    except KeyboardInterrupt:
        logger.info("\nShutting down (interrupted by user)...")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        exit(1)