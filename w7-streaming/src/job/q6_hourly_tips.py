from pyflink.datastream import StreamExecutionEnvironment # type: ignore
from pyflink.table import EnvironmentSettings, StreamTableEnvironment # type: ignore


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    t_env.get_config().get_configuration().set_string("parallelism.default", "1")

    # Source table: Kafka / Redpanda
    t_env.execute_sql("""
        CREATE TABLE green_trips (
            tip_amount DOUBLE,
            lpep_pickup_datetime STRING,
            event_timestamp AS CAST(lpep_pickup_datetime AS TIMESTAMP(3)),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'q6-hourly-tips',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Sink table: Postgres
    t_env.execute_sql("""
        CREATE TABLE q6_hourly_tips (
            window_start TIMESTAMP(3),
            total_tip_amount DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'q6_hourly_tips',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # Insert hourly tip sums into Postgres
    stmt = t_env.execute_sql("""
        INSERT INTO q6_hourly_tips
        SELECT
            TUMBLE_START(event_timestamp, INTERVAL '1' HOUR) AS window_start,
            SUM(tip_amount) AS total_tip_amount
        FROM green_trips
        GROUP BY
            TUMBLE(event_timestamp, INTERVAL '1' HOUR)
    """)

    stmt.wait()


if __name__ == "__main__":
    main()