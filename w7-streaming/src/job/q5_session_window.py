from pyflink.datastream import StreamExecutionEnvironment # type: ignore
from pyflink.table import EnvironmentSettings, StreamTableEnvironment # type: ignore


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    t_env.get_config().get_configuration().set_string("parallelism.default", "1")

    t_env.execute_sql("""
        CREATE TABLE green_trips (
            PULocationID INT,
            lpep_pickup_datetime STRING,
            event_timestamp AS CAST(lpep_pickup_datetime AS TIMESTAMP(3)),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'q5-session-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE q5_session_window (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'q5_session_window',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    stmt = t_env.execute_sql("""
        INSERT INTO q5_session_window
        SELECT
            SESSION_START(event_timestamp, INTERVAL '5' MINUTE) AS window_start,
            SESSION_END(event_timestamp, INTERVAL '5' MINUTE) AS window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM green_trips
        GROUP BY
            PULocationID,
            SESSION(event_timestamp, INTERVAL '5' MINUTE)
    """)

    stmt.wait()


if __name__ == "__main__":
    main()