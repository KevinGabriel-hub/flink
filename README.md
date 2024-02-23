# Running the Practice environment
## Pre-reqs
Make sure to have Docker installed.
Make sure you're in the right directory (`flink-env`).

Before running docker-compose, we need to build three images:
```bash
docker build -t tooling -f tooling.Dockerfile .

docker build -t my-kafka-connect:latest -f kafka-connect.Dockerfile .

docker build -t my-flink -f flink.Dockerfile .
```

## Running the environment
Simply run docker-compose for deploying the environment:
```bash
docker-compose -f env-compose.yml up -d
```

You can access Confluent Control Center at [localhost:9021](http://localhost:9021)

TODO - You can access Hazelcast Management Center at [localhost:8080](http://localhost:8080). The cluster is setup in developer mode (i.e.: no security), so you will need to click the "Enable" dev mode button before you can access the cluster data.

You can tear down your environment by running
```bash
docker-compose -f env-compose.yml down
```

## Environment Overview
This environment has been initially set up with data flowing into Kafka.
![image](resources/flink.png)

Reference data is present in a PostgreSQL database, and being sourced to Kafka through a Kafka Connector. This is then made available under a `postgresql-quote_data_symbols` topic. There should be [50 symbols total](tooling/sql-data.sql). The [schema is available here](tooling/schema-postgresql-quote_data_symbols-value-v1.avsc).

Streaming Data is being sent through a simple Data Generator application, written into a `quote-data-raw` topic. Data is generated every 25~75ms. The [schema is available here](tooling/QuoteData.avsc).

You can access [Control Center](http://localhost:9021) to check topic data under port 9021.

## Lab
The objective of this lab is to apply a series of transformations through the help of Hazelcast. Each stage should write the results back into Kafka. The data samples have been randomised, although they do resemble real use cases we have worked on before.

### Step 1: Enrich and clean dataset
Build a new topic based on the data being streamed to the `quote-data-raw`. It should be enriched with the data present in the `postgresql-quote_data_symbols`. The `recordId` from `quote-data-raw` can be matched with the `key` from `postgresql-quote_data_symbols` topic. Essentially, enrich the stream with two new fields: the `recordSymbol` and `recordName` fields.

Besides the enrichment, we want to clean up any quote data entries whose Top of the Book prices are 0 or null, to create a stream of clean quote data. For the purposes of the exercise, you can assume that if either ask0price or bid0price are 0 or null, you can filter out the record. 

> Hint: consider filtering before enriching for improved performance. 

Keep the key as it comes from the original source: `{recordId}`

The output topic should be named `quote-data-enriched`. It should contain all the fields from `quote-data-raw`, as well as the two additional fields from `postgresql-quote_data_symbols` (i.e.: `recordSymbol` and `recordName`).

### Step 2: Summary dataset
The purpose of this step is to publish a Summary derived from the enriched stream of data coming out from the `quote-data-enriched` topic. There is a one-to-one correspondence between an enriched quote data message and its summary (i.e. no aggregations are performed). As thus, an enriched quote data message and its corresponding quote data summary share the same key.

The output topic should be named `quote-data-summary` and have the expected Message fields as defined in the table below:

| Field              | Correspondence to QuoteDataEnriched                                                 |
|--------------------|-----------------------------------------------------------------------------|
| recordId           | recordId                                                                    |
| tobBid             | bid0Price                                                                   |
| tobAsk             | ask0Price                                                                   |
| uniqueBidProviders | Mathematical set containing all the unique bidNDataSource, where n E 0...5 |
| uniqueAskProviders | Mathematical set containing all the unique askNDataSource, where n E 0...5 |
| sumAllBidSizes     | Sum of all bidNSize, where n E 0...5                                       |
| sumAllAskSizes     | Sum of all askNSize, where n E 0...5                                       |


### Step 3: Statistics dataset
This step should generated statistics from the `quote-data-summary` topic.
In summary, what this stream processor does is:
1. Consume messages from input quote `quote-data-summary` topic;
2. Group messages by recordId and create a hopping time window;
3. Calculate statistics for each time-windowed aggregation;
4. Generate a stream of quote data statistics using the intermediary objects created in the previous steps and publish to an output topic named `quote-data-statistics`

The key for `quote-data-statistics` is composed of the `recordId` and the window start time and end. Like so,
```
[recordId@window_start/window_end]
```

We expect the message fields to look like the below table:

| Field           | Correspondence to QuoteDataSummary               |
|-----------------|--------------------------------------------------|
| recordId        | recordId                                         |
| avgTobBid       | Average of all tobBid prices in the time window  |
| avgTobAsk       | Average of all tobAsk prices in the time window  |
| maxTobBid       | Maximum of all tobBid prices in the time window  |
| minTobAsk       | Minimum of all tobAsk prices in the time window  |
| maxSumBidAmount | Maximum of all sumAllBidSizes in the time window |
| maxSumAskAmount | Maximum of all sumAllAskSizes in the time window |
| minSumBidAmount | Minimum of all sumAllBidSizes in the time window |
| minSumAskAmount | Minimum of all sumAllAskSizes in the time window |

> Note: Min tobBid and max tobAsk are indeed meant to not be included as fields for statistics;

### Step 4: Output/Visualise results
The final step is to visualise the data generated in steps 3 and 4. Any tooling can be used to the effect, from printing to the console, to visualisations in tooling like Grafana, VictoriaMetrics, or anything the reader thinks can convey the data best.

# Troubleshooting
Q: I can't see the `postgresql-quote_data_symbols` topic?
A: Sometimes Connect is available, but rejects some curl commands. If this is the case, simply re-execute the docker-compose command to deploy the connector, e.g.: `docker-compose -f env-compose.yml up register-connector`
