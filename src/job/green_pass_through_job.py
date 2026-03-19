# this is simple pass through for green taxi dataset
# - source and sink tables are identical, no transformations:

# Postgres table for green trips data
# CREATE TABLE processed_events (
#     lpep_pickup_datetime TIMESTAMP,
#     lpep_dropoff_datetime TIMESTAMP,    
#     PULocationID INTEGER,
#     DOLocationID INTEGER,
#     passenger_count INTEGER,   
#     trip_distance DOUBLE PRECISION,
#     tip_amount DOUBLE PRECISION,
#     total_amount DOUBLE PRECISION
# );

# STEP 1 - create source from kafka:
def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE
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
def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE
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
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # checkpoint every 10 seconds
    # enable_checkpointing(10 * 1000) tells Flink to take a snapshot of the job's 
    # state every 10 seconds. A checkpoint captures the Kafka offsets (how far Flink has read) 
    # and any in-flight data. If the job crashes, it resumes from the last checkpoint 
    # instead of starting from the beginning.

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    # Streaming mode here - the job runs continuously, waiting for new data
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_events_source_kafka(t_env)
    postgres_sink = create_processed_events_sink_postgres(t_env)

    # The INSERT INTO ... SELECT is the pipeline - read from Kafka and write to PostgreSQL
    t_env.execute_sql(
        f"""
        INSERT INTO {postgres_sink}
        SELECT
            TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss.SSS'),
            TO_TIMESTAMP(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss.SSS'),
            PULocationID,
            DOLocationID,
            passenger_count,
            trip_distance,
            tip_amount,
            total_amount
        FROM {source_table}
        """
    ).wait()

if __name__ == '__main__':
    log_processing()
    

