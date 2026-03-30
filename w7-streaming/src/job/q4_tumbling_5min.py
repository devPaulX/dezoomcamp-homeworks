from pyflink.datastream import StreamExecutionEnvironment # type: ignore
from pyflink.table import EnvironmentSettings, StreamTableEnvironment # type: ignore


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    t_env.get_config().set("parallelism.default", "1")

    t_env.execute_sql("""
        CREATE TABLE green_trips (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INT,
            DOLocationID INT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS CAST(lpep_pickup_datetime AS TIMESTAMP(3)),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'properties.group.id' = 'q4-clean',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE q4_tumbling_5min (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'q4_tumbling_5min',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    stmt = t_env.execute_sql("""
        INSERT INTO q4_tumbling_5min
        SELECT
            window_start,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
        )
        GROUP BY window_start, PULocationID
    """)

    stmt.wait()


if __name__ == "__main__":
    main()