import os
import sys
import logging
from datetime import datetime, timezone

from pyflink.table import (
    EnvironmentSettings, StreamTableEnvironment, DataTypes
)
from pyflink.table.expressions import col, lit, row, call
from pyflink.table.udf import udf

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS', 'localhost:9092')
SOURCE_TOPIC = os.environ.get('KAFKA_TOPIC', 'locations_topic')
SINK_WEBSOCKET_TOPIC = os.environ.get('KAFKA_WEBSOCKET_TOPIC', 'websocket_updates_topic')
KAFKA_CONSUMER_GROUP = 'flink_locations_processor_group'

JDBC_URL = os.environ.get('JDBC_URL', 'jdbc:postgresql://localhost:5432/mobilityTest')
JDBC_USERNAME = os.environ.get('JDBC_USERNAME', 'postgres')
JDBC_PASSWORD = os.environ.get('JDBC_PASSWORD', 'postgres')
JDBC_DRIVER = os.environ.get('JDBC_DRIVER', 'org.postgresql.Driver')

WATERMARK_DELAY_SECONDS = 10

@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def normalize_vehicle_id(vehicle_id) -> str:
    if vehicle_id is None:
        return None
    return str(vehicle_id).strip()

@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.BIGINT())
def normalize_timestamp_from_string(timestamp_value) -> int:
    if timestamp_value is None:
        return None

    def to_ms(v: int) -> int:
        if v < 1000000000000:        # sec -> ms
            return v * 1000
        if v < 1000000000000000:    # ms
            return v
        if v < 1000000000000000000:  # µs
            return v // 1000
        return v // 1000000             # ns

    if isinstance(timestamp_value, str):
        try:
            num = int(float(timestamp_value))
            return to_ms(num)
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    dt = datetime.strptime(timestamp_value, fmt)
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    pass
            return None
    else:
        try:
            num = int(timestamp_value)
            return to_ms(num)
        except Exception:
            return None

@udf(input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_type=DataTypes.BOOLEAN())
def validate_coordinates(latitude, longitude) -> bool:
    if latitude is None or longitude is None:
        return False
    try:
        lat = float(latitude); lon = float(longitude)
        return -90 <= lat <= 90 and -180 <= lon <= 180
    except Exception:
        return False

@udf(input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE(), DataTypes.BIGINT()], result_type=DataTypes.STRING())
def create_tgeogpoint_text(latitude, longitude, timestamp_ms) -> str:
    if latitude is None or longitude is None or timestamp_ms is None:
        return None
    try:
        lat = float(latitude); lon = float(longitude)
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return None
        ts = int(timestamp_ms)
        dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
        iso_ts = dt.strftime('%Y-%m-%d %H:%M:%S+00')
        return f"POINT({lon} {lat})@{iso_ts}"
    except Exception:
        return None

def process_locations():
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(environment_settings=settings)


    t_env.get_config().set("pipeline.auto-watermark-interval", "200 ms")
    t_env.get_config().set("table.exec.state.ttl", "1 h")

    t_env.create_temporary_function("NORMALIZE_VEHICLE_ID", normalize_vehicle_id)
    t_env.create_temporary_function("NORMALIZE_TIMESTAMP_FROM_STRING", normalize_timestamp_from_string)
    t_env.create_temporary_function("VALIDATE_COORDINATES", validate_coordinates)
    t_env.create_temporary_function("CREATE_TGEOGPOINT_TEXT", create_tgeogpoint_text)

    t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            vehicleID STRING,
            latitude  DOUBLE,
            longitude DOUBLE,
            `timestamp` STRING,
            source    STRING,
            proc_time AS PROCTIME(),
            event_time AS TO_TIMESTAMP_LTZ(NORMALIZE_TIMESTAMP_FROM_STRING(`timestamp`), 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '{WATERMARK_DELAY_SECONDS}' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
            'properties.group.id' = '{KAFKA_CONSUMER_GROUP}',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    t_env.execute_sql("""
        CREATE TEMPORARY VIEW base AS
        SELECT
            NORMALIZE_VEHICLE_ID(vehicleID)              AS vehicle_id,
            CAST(latitude AS DOUBLE)                     AS latitude,
            CAST(longitude AS DOUBLE)                    AS longitude,
            NORMALIZE_TIMESTAMP_FROM_STRING(`timestamp`) AS ts_ms,
            CAST(source AS STRING)                       AS source,
            event_time
        FROM kafka_source
    """)

    jdbc_sink = "jdbc_point_event_sink"
    t_env.execute_sql(f"""
        CREATE TABLE {jdbc_sink} (
            vehicle_id      STRING,
            `timestamp`     BIGINT,
            tgeogpoint_text STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{JDBC_URL}',
            'table-name' = 'vehicle_locations_temporal_insert_view',
            'username' = '{JDBC_USERNAME}',
            'password' = '{JDBC_PASSWORD}',
            'driver' = '{JDBC_DRIVER}',
            'sink.buffer-flush.max-rows' = '100',
            'sink.buffer-flush.interval' = '1 s',
            'sink.max-retries' = '3'
        )
    """)

    kafka_ws_sink = "kafka_sink_ws"
    t_env.execute_sql(f"""
        CREATE TABLE {kafka_ws_sink} (
            `type` STRING,
            `payload` ROW<
                vehicleID STRING,
                latitude  DOUBLE,
                longitude DOUBLE,
                `timestamp` BIGINT,
                source    STRING
            >
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SINK_WEBSOCKET_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
            'format' = 'json',
            'sink.partitioner' = 'round-robin'
        )
    """)

    stmt = t_env.create_statement_set()


    stmt.add_insert_sql("""
        INSERT INTO jdbc_point_event_sink
        SELECT
            vehicle_id,
            ts_ms AS `timestamp`,
            CREATE_TGEOGPOINT_TEXT(latitude, longitude, ts_ms) AS tgeogpoint_text
        FROM base
        WHERE
            vehicle_id IS NOT NULL
            AND ts_ms IS NOT NULL
            AND VALIDATE_COORDINATES(latitude, longitude) = TRUE
            AND CREATE_TGEOGPOINT_TEXT(latitude, longitude, ts_ms) IS NOT NULL
    """)


    stmt.add_insert_sql("""
        INSERT INTO kafka_sink_ws
        SELECT
            'UPDATE' AS `type`,
            ROW(
                CAST(vehicle_id AS STRING),
                CAST(latitude AS DOUBLE),
                CAST(longitude AS DOUBLE),
                CAST(ts_ms AS BIGINT),
                CAST(source AS STRING)
            ) AS `payload`
        FROM base
        WHERE
            vehicle_id IS NOT NULL
            AND ts_ms IS NOT NULL
            AND VALIDATE_COORDINATES(latitude, longitude) = TRUE
    """)

    table_result = stmt.execute()
    job_client = table_result.get_job_client()
    job_id = job_client.get_job_id() if job_client is not None else "unknown"

    logger.info("=" * 60)
    logger.info("Flink job started.")
    logger.info("Job ID: %s", job_id)
    logger.info("Source topic: %s", SOURCE_TOPIC)
    logger.info("WebSocket sink topic: %s", SINK_WEBSOCKET_TOPIC)
    logger.info("DB sink table: vehicle_locations_temporal_insert_view")
    logger.info("Watermark delay: %s seconds", WATERMARK_DELAY_SECONDS)
    logger.info("=" * 60)



if __name__ == '__main__':
    try:
        process_locations()
        logger.info("Flink pipeline configured and started.")
    except Exception as ex:
        logger.error("Error starting Flink pipeline: %s", ex, exc_info=True)
        sys.exit(1)
