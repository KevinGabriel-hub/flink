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