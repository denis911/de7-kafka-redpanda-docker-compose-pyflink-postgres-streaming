# this is 5 min tumble window job for green taxi dataset
# - source and sink tables are different now

# Postgres table for 5 min tumble in green trips data
#   CREATE TABLE five_min_tumble (
#       window_start TIMESTAMP, 
#       PULocationID INTEGER,
#       num_trips INTEGER
#       );

# STEP 1 - create source from kafka:
def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime BIGINT,
            PULocationID INTEGER,
            event_timestamp AS TO_TIMESTAMP_LTZ(lpep_pickup_datetime, 3),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'json.fail-on-missing-field' = 'false'
        );
    """
    
    # 'json.ignore-parse-errors' = 'true'
    # 'scan.startup.mode' = 'earliest-offset' VS 'scan.startup.mode' = 'latest-offset'
    # 'properties.auto.offset.reset' = 'latest'
    t_env.execute_sql(source_ddl)
    return table_name

# STEP 2 - create sink to postgres:
def create_events_aggregated_sink(t_env):
    table_name = 'five_min_tumble'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP_LTZ(3), 
            PULocationID INTEGER,
            num_trips INTEGER,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """

    t_env.execute_sql(sink_ddl)
    return table_name

# STEP 3 - run:
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1) # it used to be 3 in the pass through, but with 3 job never runs...
    # With higher parallelism, idle consumer subtasks prevent the watermark from advancing.

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Create Kafka table
    source_table = create_events_source_kafka(t_env)
    aggregated_table = create_events_aggregated_sink(t_env)

    t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT
            window_start,
            PULocationID,
            CAST(COUNT(*) AS INT) AS num_trips
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID;

        """).wait()


if __name__ == '__main__':
    log_processing()
    

