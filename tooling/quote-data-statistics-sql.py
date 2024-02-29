from pyflink.table.table_environment import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment

# Create a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
# Create a TableEnvironment
t_env = StreamTableEnvironment.create(env)

# Create a summary table
t_env.execute_sql("""
    CREATE TABLE summary_table (
        `recordId` INT,
        `tobBid` DOUBLE,
        `tobAsk` DOUBLE,
        `uniqueBidProviders` ARRAY<INT>,
        `uniqueAskProviders` ARRAY<INT>,
        `sumAllBidSizes` INT,
        `sumAllAskSizes` INT,
        `processing_time` AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'quote-data-summary',
        'scan.startup.mode' = 'earliest-offset',
        'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
        'format' = 'avro-confluent',
        'avro-confluent.url' = 'http://schema-registry:8081'
    );
""")

t_env.execute_sql("""
    CREATE TABLE statistics_table (
        `recordId` INT,
        `avgTobBid` DOUBLE,
        `avgTobAsk` DOUBLE,
        `maxTobBid` DOUBLE,
        `minTobAsk` DOUBLE,
        `maxSumBidAmount` INT,
        `maxSumAskAmount` INT,
        `minSumBidAmount` INT,
        `minSumAskAmount` INT,
        PRIMARY KEY (`recordId`) NOT ENFORCED
    ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'quote-data-statistics',
        'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
        'key.format' = 'avro-confluent',
        'key.avro-confluent.url' = 'http://schema-registry:8081',
        'value.format' = 'avro-confluent',
        'value.avro-confluent.url' = 'http://schema-registry:8081'
    );
""")

statistics_insert_query = """
    INSERT INTO statistics_table
    SELECT
        recordId, 
        AVG(tobBid) AS avgTobBid, 
        AVG(tobAsk) AS avgTobAsk, 
        MAX(tobBid) AS maxTobBid,
        MIN(tobAsk) AS minTobAsk,
        MAX(sumAllBidSizes) AS maxSumBidAmount,
        MAX(sumAllAskSizes) AS maxSumAskAmount,
        MIN(sumAllBidSizes) AS minSumBidAmount,
        MIN(sumAllAskSizes) AS minSumAskAmount
    FROM TABLE(
        HOP(TABLE summary_table, DESCRIPTOR(processing_time), INTERVAL '10' SECOND, INTERVAL '5' SECOND))
    GROUP BY recordId;
"""

# Execute the summary INSERT INTO query
t_env.execute_sql(statistics_insert_query).wait()
