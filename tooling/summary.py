from pyflink.table.table_environment import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment

# Create a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
# Create a TableEnvironment
t_env = StreamTableEnvironment.create(env)

t_env.execute_sql("""
    CREATE TABLE enriched_table (
        `recordId` INT,
        `time` BIGINT,
        `arrivalTime` BIGINT,
        `bid0Price` DOUBLE,
        `bid0Ccy` STRING,
        `bid0Size` INT,
        `bid0DataSource` INT,
        `bid0AgeOffsetMsecs` INT,
        `bid1Price` DOUBLE,
        `bid1Ccy` STRING,
        `bid1Size` INT,
        `bid1DataSource` INT,
        `bid1AgeOffsetMsecs` INT,
        `bid2Price` DOUBLE,
        `bid2Ccy` STRING,
        `bid2Size` INT,
        `bid2DataSource` INT,
        `bid2AgeOffsetMsecs` INT,
        `bid3Price` DOUBLE,
        `bid3Ccy` STRING,
        `bid3Size` INT,
        `bid3DataSource` INT,
        `bid3AgeOffsetMsecs` INT,
        `bid4Price` DOUBLE,
        `bid4Ccy` STRING,
        `bid4Size` INT,
        `bid4DataSource` INT,
        `bid4AgeOffsetMsecs` INT,
        `ask0Price` DOUBLE,
        `ask0Ccy` STRING,
        `ask0Size` INT,
        `ask0DataSource` INT,
        `ask0AgeOffsetMsecs` INT,
        `ask1Price` DOUBLE,
        `ask1Ccy` STRING,
        `ask1Size` INT,
        `ask1DataSource` INT,
        `ask1AgeOffsetMsecs` INT,
        `ask2Price` DOUBLE,
        `ask2Ccy` STRING,
        `ask2Size` INT,
        `ask2DataSource` INT,
        `ask2AgeOffsetMsecs` INT,
        `ask3Price` DOUBLE,
        `ask3Ccy` STRING,
        `ask3Size` INT,
        `ask3DataSource` INT,
        `ask3AgeOffsetMsecs` INT,
        `ask4Price` DOUBLE,
        `ask4Ccy` STRING,
        `ask4Size` INT,
        `ask4DataSource` INT,
        `ask4AgeOffsetMsecs` INT,
        `recordSymbol` STRING,
        `recordName` STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'quote-data-enriched',
        'scan.startup.mode' = 'earliest-offset',
        'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
        'format' = 'avro-confluent',
        'avro-confluent.url' = 'http://schema-registry:8081'
    );
""")

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

# Insert query for the summary table
summary_insert_query = """
    INSERT INTO summary_table
    SELECT
        `recordId`,
        `bid0Price` AS `tobBid`,
        `ask0Price` AS `tobAsk`,
        ARRAY[`bid0DataSource`, `bid1DataSource`, `bid2DataSource`, `bid3DataSource`, `bid4DataSource`] AS `uniqueBidProviders`,
        ARRAY[`ask0DataSource`, `ask1DataSource`, `ask2DataSource`, `ask3DataSource`, `ask4DataSource`] AS `uniqueAskProviders`,
        `bid0Size` + `bid1Size` + `bid2Size` + `bid3Size` + `bid4Size` AS `sumAllBidSizes`,
        `ask0Size` + `ask1Size` + `ask2Size` + `ask3Size` + `ask4Size` AS `sumAllAskSizes`
    FROM `enriched_table`;
"""

# Execute the summary INSERT INTO query
t_env.execute_sql(summary_insert_query).wait()
