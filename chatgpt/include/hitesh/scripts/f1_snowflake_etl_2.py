# f1_snowflake_etl.py
import requests
import logging
from typing import Dict, List, Any, Optional
from snowflake.snowpark import Session
from datetime import datetime, timedelta
import time
class F1DataIngestion:
    BASE_URL = "https://api.openf1.org/v1"

    def __init__(self, snowpark_session: Session):
        self.session = snowpark_session
        self.schema = 'HITESH'
        self.execution_date = None

    def _make_request(self, endpoint: str, params: dict = None) -> List[Dict]:
        """Enhanced API request with better error handling and retries"""
        url = f'{self.BASE_URL}/{endpoint}'
        print(url)
        MAX_RETRIES = 2
        RETRY_DELAY = 5
        retry_count = 0

        while retry_count < MAX_RETRIES:
            try:
                logging.info(f"Making an API request for {url} with {params}")
                response = requests.get(url, params)
                response.raise_for_status()
                data = response.json()

                # Handle different response types
                if isinstance(data, dict):
                    return [data]  # Convert single dict to list
                elif isinstance(data, list):
                    return data
                else:
                    raise ValueError(f"Unexpected response type: {type(data)}")

            except requests.exceptions.ConnectionError as ce:
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    logging.warning(f"Connection error (attempt {retry_count}/{MAX_RETRIES}): {str(ce)}")
                    time.sleep(RETRY_DELAY)
                else:
                    logging.error(f"Failed to establish connection after {MAX_RETRIES} attempts: {str(ce)}")
                    raise

            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    logging.warning(f"Request failed (attempt {retry_count}/{MAX_RETRIES}): {str(e)}")
                    time.sleep(RETRY_DELAY)
                else:
                    logging.error(f"Request failed after {MAX_RETRIES} attempts: {str(e)}")
                    raise

    def get_recent_meeting(self) -> Dict:
        """Get most recent meeting data"""
        params = {'meeting_key': 'latest'}
        data = self._make_request('meetings', params)
        if not data:
            raise Exception("Meeting data wasn't found, check the url once!")
        return data[0]

    def get_session_data(self, meeting_key: int) -> List[Dict]:
        """Get session data for a meeting"""
        try:
            params = {'meeting_key': meeting_key}
            data = self._make_request('sessions', params)
            if data:
                return data
            return []
        except Exception as e:
            logging.error(f"Error getting session data for meeting {meeting_key}: {str(e)}")
            return []

    def get_session_endpoint_data(self, session: Dict, endpoint: str) -> Dict:
        """Get data for high-frequency endpoints with aggregation with adaptive window size"""
        params = {'session_key': session['session_key']}
        MAX_RETRIES = 3
        RETRY_DELAY = 5  # seconds
        INITIAL_WINDOW = timedelta(minutes=5)
        MIN_WINDOW = timedelta(minutes=2.5)

        if endpoint == 'car_data':
            start_time = datetime.fromisoformat(session['date_start'].replace('Z', '+00:00'))
            end_time = datetime.fromisoformat(session['date_end'].replace('Z', '+00:00'))
            total_records = 0

            if not self.table_exists('F1_CAR_DATA'):
                first_params = params.copy()
                first_params.update({
                    'date>': start_time.isoformat(),
                    'date<': (start_time + MIN_WINDOW).isoformat()
                })
                try:
                    sample_data = self._make_request(endpoint, first_params)
                    if isinstance(sample_data, list) and len(sample_data) > 0 and isinstance(sample_data[0], dict):
                        self.create_table('F1_CAR_DATA', sample_data[0])
                    else:
                        logging.error(f"Invalid sample data format received: {type(sample_data)}")
                        return {
                            'status': 'error',
                            'session_key': session['session_key'],
                            'records': 0,
                            'reason': 'invalid_sample_data'
                        }
                except Exception as e:
                    logging.error(f"Error creating table: {str(e)}")
                    return {
                        'status': 'error',
                        'session_key': session['session_key'],
                        'records': 0,
                        'reason': 'table_creation_failed'
                    }

            current_start = start_time
            success = True
            error_windows = []  # Track windows with errors

            while current_start < end_time:
                window_size = INITIAL_WINDOW
                window_success = False
                retry_count = 0

                while not window_success and retry_count < MAX_RETRIES:
                    current_end = min(current_start + window_size, end_time)
                    window_params = params.copy()
                    window_params.update({
                        'date>': current_start.isoformat(),
                        'date<': current_end.isoformat()
                    })

                    logging.info(
                        f"Fetching car_data for window: "
                        f"{current_start.strftime('%H:%M:%S')} - {current_end.strftime('%H:%M:%S')} "
                        f"(window size: {window_size.total_seconds() / 60:.1f} minutes)"
                    )

                    try:
                        chunk_data = self._make_request(endpoint, window_params)

                        # Handle different response types
                        if isinstance(chunk_data, dict):
                            chunk_data = [chunk_data]
                        elif not isinstance(chunk_data, list):
                            raise ValueError(f"Expected list or dict but got {type(chunk_data)}")

                        # Validate records
                        valid_records = []
                        for record in chunk_data:
                            if isinstance(record, dict):
                                required_fields = ['MEETING_KEY', 'SESSION_KEY', 'DRIVER_NUMBER', 'DATE']
                                missing_fields = [field for field in required_fields if field.upper() not in
                                                  {k.upper(): v for k, v in record.items()}]

                                if not missing_fields:
                                    valid_records.append(record)
                                else:
                                    logging.warning(f"Skipping record due to missing fields: {missing_fields}")

                        if valid_records:
                            self.load_data('F1_CAR_DATA', valid_records)
                            total_records += len(valid_records)
                            logging.info(f"Loaded {len(valid_records)} valid records for time window")
                        else:
                            logging.warning("No valid records in this time window, continuing to next window")

                        window_success = True  # Continue to next window even if no valid records

                    except Exception as e:
                        retry_count += 1
                        if window_size > MIN_WINDOW:
                            window_size = timedelta(minutes=window_size.total_seconds() / (60 * 2))
                            logging.warning(
                                f"Attempt {retry_count} failed. Reducing window size to "
                                f"{window_size.total_seconds() / 60:.1f} minutes. Error: {str(e)}"
                            )
                            time.sleep(RETRY_DELAY)
                        else:
                            if retry_count < MAX_RETRIES:
                                logging.warning(
                                    f"Attempt {retry_count} failed with minimum window size. "
                                    f"Error: {str(e)}. Retrying in {RETRY_DELAY} seconds..."
                                )
                                time.sleep(RETRY_DELAY)
                            else:
                                logging.error(
                                    f"All {MAX_RETRIES} attempts failed for window "
                                    f"{current_start} - {current_end}: {str(e)}"
                                )
                                error_windows.append((current_start, current_end))
                                success = False
                                window_success = True  # Move to next window

                if window_success:
                    current_start = current_end

            logging.info(f"Total car_data records processed: {total_records}")
            if error_windows:
                logging.warning(f"Failed windows: {error_windows}")

            # Return summary with error windows information
            return {
                'status': 'success' if success else 'partial',
                'session_key': session['session_key'],
                'records': total_records,
                'meeting_key': session.get('meeting_key'),
                'error_windows': error_windows if error_windows else None
            }

        else:
            retry_count = 0
            while retry_count < MAX_RETRIES:
                try:
                    data = self._make_request(endpoint, params)
                    return data  # For other endpoints, return the data directly
                except Exception as e:
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        logging.warning(
                            f"Attempt {retry_count} failed for endpoint {endpoint}: {str(e)}. "
                            f"Retrying in {RETRY_DELAY} seconds..."
                        )
                        time.sleep(RETRY_DELAY)
                    else:
                        logging.error(f"All {MAX_RETRIES} attempts failed for endpoint {endpoint}: {str(e)}")
                        return []

    def get_race_control_data(self, session_key: int) -> List[Dict]:
        """Get race control messages and flags for a session"""
        try:
            params = {'session_key': session_key}
            data = self._make_request('race_control', params)
            if data:
                if not self.table_exists('F1_RACE_CONTROL'):
                    self.create_table('F1_RACE_CONTROL', data[0])
                self.load_data('F1_RACE_CONTROL', data)
            return data
        except Exception as e:
            logging.error(f"Error getting race control data: {str(e)}")
            return []

    def cleanup_staging_tables(self) -> None:
        """Clean up all staging tables in the schema"""
        try:
            # Query to find all staging tables
            query = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{self.schema}'
            AND table_name LIKE '%STAGING%'
            """

            result = self.session.sql(query).collect()

            for row in result:
                staging_table = row[0]
                try:
                    drop_query = f"DROP TABLE IF EXISTS {self.schema}.{staging_table}"
                    self.session.sql(drop_query).collect()
                    logging.info(f"Successfully dropped staging table: {staging_table}")
                except Exception as e:
                    logging.error(f"Error dropping staging table {staging_table}: {str(e)}")

        except Exception as e:
            logging.error(f"Error during staging table cleanup: {str(e)}")

    def get_stints_data(self, session_key: int) -> List[Dict]:
        """Get tyre stint information for a session"""
        try:
            params = {'session_key': session_key}
            data = self._make_request('stints', params)
            if data:
                if not self.table_exists('F1_STINTS'):
                    self.create_table('F1_STINTS', data[0])
                self.load_data('F1_STINTS', data)
            return data
        except Exception as e:
            logging.error(f"Error getting stints data: {str(e)}")
            return []

    def get_pit_data(self, session_key: int) -> List[Dict]:
        """Get pit stop information for a session"""
        try:
            params = {'session_key': session_key}
            data = self._make_request('pit', params)
            if data:
                if not self.table_exists('F1_PIT'):
                    self.create_table('F1_PIT', data[0])
                self.load_data('F1_PIT', data)
            return data
        except Exception as e:
            logging.error(f"Error getting pit data: {str(e)}")
            return []

    def get_position_data(self, session_key: int) -> List[Dict]:
        """Get position information for a session"""
        try:
            params = {'session_key': session_key}
            data = self._make_request('position', params)
            if data:
                if not self.table_exists('F1_POSITION'):
                    self.create_table('F1_POSITION', data[0])
                self.load_data('F1_POSITION', data)
            return data
        except Exception as e:
            logging.error(f"Error getting position data: {str(e)}")
            return []

    def get_intervals_data(self, session_key: int) -> List[Dict]:
        """Get interval data between drivers for a session"""
        try:
            params = {'session_key': session_key}
            data = self._make_request('intervals', params)
            if data:
                if not self.table_exists('F1_INTERVALS'):
                    self.create_table('F1_INTERVALS', data[0])
                self.load_data('F1_INTERVALS', data)
            return data
        except Exception as e:
            logging.error(f"Error getting intervals data: {str(e)}")
            return []

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        try:
            result = self.session.sql(
                f"""SELECT count(*) FROM information_schema.tables
                   WHERE table_schema = '{self.schema}'
                   AND table_name = '{table_name}'"""
            ).collect()
            return result[0][0] > 0
        except Exception as e:
            logging.error(f"Error checking table existence: {str(e)}")
            return False

    def _get_table_columns(self, table_name: str) -> List[str]:
        """Get current columns in table"""
        try:
            query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{self.schema}' 
                AND table_name = '{table_name}'
            """
            result = self.session.sql(query).collect()
            return [row[0] for row in result]
        except Exception as e:
            logging.error(f"Error getting columns for {table_name}: {str(e)}")
            return []

    def record_exists(self, table_name: str, meeting_key: int, session_key: Optional[int] = None) -> bool:
        """Check if records exist for given meeting/session"""
        try:
            # First check if table exists
            if not self.table_exists(table_name):
                logging.info(f"Table {table_name} does not exist yet, returning False")
                return False

            query = f"""
                SELECT COUNT(*) 
                FROM {self.schema}.{table_name}
                WHERE meeting_key = {meeting_key}
            """
            if session_key:
                query += f" AND session_key = {session_key}"

            result = self.session.sql(query).collect()
            return result[0][0] > 0
        except Exception as e:
            logging.error(f"Error checking record existence: {str(e)}")
            return False

    def create_table(self, table_name: str, sample_data: Dict[str, Any]):
        """Create a new table based on sample data with specific handling for F1_LAPS"""
        if self.table_exists(table_name):
            logging.info(f"Table {table_name} already exists")
            return

        if table_name == 'F1_LAPS':
            # Use predefined schema for laps table
            columns = [
                "DATE_START TIMESTAMP",
                "DRIVER_NUMBER INTEGER",
                "MEETING_KEY INTEGER",
                "SESSION_KEY INTEGER",
                "LAP_NUMBER INTEGER",
                "DURATION_SECTOR_1 FLOAT",
                "DURATION_SECTOR_2 FLOAT",
                "DURATION_SECTOR_3 FLOAT",
                "LAP_DURATION FLOAT",
                "I1_SPEED INTEGER",
                "I2_SPEED INTEGER",
                "ST_SPEED INTEGER",
                "IS_PIT_OUT_LAP BOOLEAN",
                "SEGMENTS_SECTOR_1 VARIANT",
                "SEGMENTS_SECTOR_2 VARIANT",
                "SEGMENTS_SECTOR_3 VARIANT"
            ]
        elif table_name == 'F1_PIT':
            # Use predefined schema for PIT table
            columns = [
                "DATE TIMESTAMP",
                "DRIVER_NUMBER INTEGER",
                "MEETING_KEY INTEGER",
                "lap_number INTEGER",
                "pit_duration FLOAT",
                "session_key INTEGER",

            ]


        elif table_name == 'F1_INTERVALS':
            # Use predefined schema for intervals table
            columns = [
                "DATE TIMESTAMP",
                "DRIVER_NUMBER INTEGER",
                "session_key INTEGER",
                "meeting_key INTEGER",
                "GAP_TO_LEADER VARCHAR",  # Changed to VARCHAR to handle both numeric and string values
                "INTERVAL VARCHAR"  # Changed to VARCHAR to handle both numeric and string values
            ]
        elif table_name == 'F1_RACE_CONTROL':
            # Use predefined schema for intervals table
            columns = [
                "CATEGORY VARCHAR",
                "DATE TIMESTAMP",
                "driver_number INTEGER",
                "flag VARCHAR",
                "message VARCHAR",
                "lap_number INTEGER",
                "meeting_key VARCHAR",
                "scope VARCHAR",
                "sector VARCHAR",
                "session_key INTEGER"
            ]

        else:
            # Original logic for other tables
            columns = []
            for key, value in sample_data.items():
                if isinstance(value, str) and (key.endswith('date') or key.endswith('_at')):
                    columns.append(f"{key} TIMESTAMP")
                elif isinstance(value, (int, float)):
                    columns.append(f"{key} FLOAT")
                elif isinstance(value, str):
                    columns.append(f"{key} VARCHAR")
                elif isinstance(value, bool):
                    columns.append(f"{key} BOOLEAN")
                elif isinstance(value, list):
                    columns.append(f"{key} VARIANT")

        # Add audit columns
        additional_tracking_columns = [
            "PROCESSED_DATE DATE",
            "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()",
            "UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
        ]

        columns_str = ','.join(columns + additional_tracking_columns)
        create_ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} (
                {columns_str}
            )
        """

        try:
            self.session.sql(create_ddl).collect()

            # Add indexes for F1_LAPS table
            if table_name == 'F1_LAPS':
                index_ddls = [
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name.lower()}_session ON {self.schema}.{table_name}(SESSION_KEY, DRIVER_NUMBER)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name.lower()}_meeting ON {self.schema}.{table_name}(MEETING_KEY)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name.lower()}_driver ON {self.schema}.{table_name}(DRIVER_NUMBER)"
                ]
                for index_ddl in index_ddls:
                    self.session.sql(index_ddl).collect()

            logging.info(f"Created table {table_name}")
        except Exception as e:
            logging.error(f"Error creating table {table_name}: {str(e)}")
            raise

    def add_missing_columns(self, table_name: str):
        """Add missing audit columns to existing table"""
        try:
            existing_columns = self._get_table_columns(table_name)
            audit_columns = {
                'PROCESSED_DATE': 'DATE',
                'CREATED_AT': 'TIMESTAMP_NTZ',
                'UPDATED_AT': 'TIMESTAMP_NTZ'
            }

            for col_name, col_type in audit_columns.items():
                if col_name.upper() not in [col.upper() for col in existing_columns]:
                    alter_sql = f"""
                    ALTER TABLE {self.schema}.{table_name}
                    ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                    """
                    logging.info(f"Adding column with SQL: {alter_sql}")
                    self.session.sql(alter_sql).collect()

                    update_sql = f"""
                    UPDATE {self.schema}.{table_name}
                    SET {col_name} = CASE 
                        WHEN '{col_type}' LIKE '%TIMESTAMP%' THEN CURRENT_TIMESTAMP()
                        WHEN '{col_name}' = 'PROCESSED_DATE' THEN CURRENT_DATE()
                        ELSE NULL
                    END
                    WHERE {col_name} IS NULL
                    """
                    self.session.sql(update_sql).collect()
                    logging.info(f"Added and initialized column {col_name} in {table_name}")

        except Exception as e:
            logging.error(f"Error adding audit columns to {table_name}: {str(e)}")
            raise

    def load_data(self, table_name: str, data: List[Dict]):
        """Load data using optimized merge strategy with case-sensitive handling"""
        try:
            # Handle case where data is a single dictionary
            if isinstance(data, dict):
                data = [data]

            if not isinstance(data, list):
                logging.warning(f"Invalid data type for {table_name}. Expected List[Dict], got {type(data)}. Skipping.")
                return

            if not data:
                logging.warning(f"No data to load for {table_name}")
                return

            if not all(isinstance(record, dict) for record in data):
                invalid_records = [i for i, record in enumerate(data) if not isinstance(record, dict)]
                logging.warning(f"Invalid record types at indices {invalid_records}. Filtering out invalid records.")
                data = [record for record in data if isinstance(record, dict)]
                if not data:
                    logging.warning("No valid records remain after filtering.")
                    return

            staging_table = f"{table_name}_STAGING_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            try:
                self.add_missing_columns(table_name)

                # Standardize column names in the data to uppercase
                processed_data = []
                current_timestamp = datetime.now()

                for record in data:
                    try:
                        record_copy = {k.upper(): v for k, v in record.items()}
                        if table_name == 'F1_INTERVALS':
                            record_copy['GAP_TO_LEADER'] = str(record_copy.get('GAP_TO_LEADER', ''))
                            record_copy['INTERVAL'] = str(record_copy.get('INTERVAL', ''))

                        # Add additional validation for car_data records
                        if table_name == 'F1_CAR_DATA':
                            required_fields = ['MEETING_KEY', 'SESSION_KEY', 'DRIVER_NUMBER', 'DATE']
                            missing_fields = [field for field in required_fields if field not in record_copy]
                            if missing_fields:
                                logging.warning(f"Skipping record due to missing fields: {missing_fields}")
                                continue

                        record_copy['PROCESSED_DATE'] = self.execution_date
                        record_copy['CREATED_AT'] = current_timestamp
                        processed_data.append(record_copy)
                    except Exception as e:
                        logging.warning(f"Error processing record in {table_name}: {str(e)}, skipping record")
                        continue

                if not processed_data:
                    logging.warning(f"No valid records to process for {table_name}")
                    return

                # Create DataFrame with explicit schema validation
                try:
                    df = self.session.create_dataframe(processed_data)
                except Exception as e:
                    logging.error(f"Error creating DataFrame for {table_name}: {str(e)}")
                    return

                # Verify staging table doesn't exist before creating
                if self.table_exists(staging_table):
                    logging.warning(f"Staging table {staging_table} already exists, dropping it")
                    self.session.sql(f"DROP TABLE IF EXISTS {self.schema}.{staging_table}").collect()

                df.write.mode("overwrite").save_as_table(f"{self.schema}.{staging_table}")

                # Determine merge keys based on table type
                merge_keys = []
                if table_name == 'F1_MEETINGS':
                    merge_keys = ['MEETING_KEY']
                elif table_name == 'F1_SESSIONS':
                    merge_keys = ['MEETING_KEY', 'SESSION_KEY']
                elif table_name == 'F1_LAPS':
                    merge_keys = ["DATE_START", "DRIVER_NUMBER", "SESSION_KEY", "LAP_NUMBER"]
                elif table_name == 'F1_PIT':
                    merge_keys = ["DATE", "DRIVER_NUMBER", "SESSION_KEY", "LAP_NUMBER"]
                elif table_name == 'F1_RACE_CONTROL':
                    merge_keys = ["DATE", "CATEGORY", "DRIVER_NUMBER", "SESSION_KEY", "MEETING_KEY", "MESSAGE"]
                elif table_name == 'F1_CAR_DATA':
                    merge_keys = ['MEETING_KEY', 'SESSION_KEY', 'DRIVER_NUMBER', 'DATE']
                elif table_name == 'F1_INTERVALS':
                    merge_keys = ['MEETING_KEY', 'SESSION_KEY', 'DRIVER_NUMBER', 'DATE', 'GAP_TO_LEADER', 'INTERVAL']
                elif table_name == 'F1_POSITION':
                    merge_keys = ['MEETING_KEY', 'SESSION_KEY', 'DRIVER_NUMBER', 'DATE']
                else:
                    merge_keys = ['MEETING_KEY', 'SESSION_KEY']

                # Get and validate columns
                table_columns = [col.upper() for col in self._get_table_columns(table_name)]
                staging_columns = [col.upper() for col in self._get_table_columns(staging_table)]

                # Verify merge keys exist in both tables
                missing_target_keys = [key for key in merge_keys if key not in table_columns]
                missing_staging_keys = [key for key in merge_keys if key not in staging_columns]

                if missing_target_keys or missing_staging_keys:
                    logging.error(
                        f"Missing merge keys - Target: {missing_target_keys}, Staging: {missing_staging_keys}")
                    raise ValueError("Missing required merge keys")

                # Filter update columns based on existing columns
                update_columns = [
                    col for col in table_columns
                    if col not in merge_keys
                       and col not in ['CREATED_AT', 'UPDATED_AT']
                ]

                update_sets = [f"target.{col} = source.{col}" for col in update_columns]
                update_sets.append("target.UPDATED_AT = CURRENT_TIMESTAMP()")
                update_clause = ", ".join(update_sets)

                insert_columns = [col for col in table_columns if col != 'UPDATED_AT']
                source_values = [
                    f"source.{col}" if col != 'CREATED_AT' else 'CURRENT_TIMESTAMP()'
                    for col in insert_columns
                ]
                source_values.append('CURRENT_TIMESTAMP()')

                # Construct and execute merge SQL
                merge_sql = f"""
                    MERGE INTO {self.schema}.{table_name} target
                    USING {self.schema}.{staging_table} source
                    ON {" AND ".join([f"target.{key} = source.{key}" for key in merge_keys])}
                    WHEN MATCHED THEN UPDATE SET
                        {update_clause}
                    WHEN NOT MATCHED THEN INSERT
                        ({", ".join(insert_columns)}, UPDATED_AT)
                    VALUES
                        ({", ".join(source_values)})
                """

                logging.info(f"Executing merge for {table_name}")
                self.session.sql(merge_sql).collect()
                logging.info(f"Successfully merged {len(processed_data)} records into {table_name}")

            except Exception as e:
                logging.error(f"Error processing data for {table_name}: {str(e)}")
                raise
            finally:
                # Cleanup staging table
                try:
                    cleanup_sql = f"DROP TABLE IF EXISTS {self.schema}.{staging_table}"
                    self.session.sql(cleanup_sql).collect()
                    logging.info(f"Successfully dropped staging table: {staging_table}")
                except Exception as e:
                    logging.error(f"Error cleaning up staging table: {str(e)}")

        except Exception as e:
            logging.error(f"Top-level error in load_data for {table_name}: {str(e)}")
            return