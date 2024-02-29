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
  TUMBLE(TABLE summary_table, DESCRIPTOR(processing_time), INTERVAL '10' SECOND))
GROUP BY recordId;


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
  HOP(TABLE summary_table, DESCRIPTOR(processing_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
GROUP BY recordId;


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
  CUMULATE(TABLE summary_table, DESCRIPTOR(processing_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
GROUP BY recordId;


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
SELECT * FROM statistics_table;