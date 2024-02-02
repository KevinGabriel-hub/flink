from pyflink.table.expressions import col

# Create the StreamExecutionEnvironment & TableEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# Create Source for User Interactions
user_interactions_table = """
    CREATE TABLE user_interactions (
        user_id INT,
        page_id STRING,
        action STRING,
        timestamp STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'user_interactions',
        'scan.startup.mode' = 'earliest-offset',
        'properties.bootstrap.servers' = 'kafka-kafka-bootstrap:9092',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    );
"""
table_env.execute_sql(user_interactions_table)

# Create Source for Product Purchases
product_purchases_table = """
    CREATE TABLE product_purchases (
        user_id INT,
        product_id STRING,
        quantity INT,
        timestamp STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'product_purchases',
        'scan.startup.mode' = 'earliest-offset',
        'properties.bootstrap.servers' = 'kafka-kafka-bootstrap:9092',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    );
"""
table_env.execute_sql(product_purchases_table)

# Conversion between Table and DataStream
datastream_interactions = table_env.to_append_stream(table_env.from_path('user_interactions'))
datastream_purchases = table_env.to_append_stream(table_env.from_path('product_purchases'))

# Define a transformation function for the data
def transform_interactions(data):
    return data.select(col("user_id"), col("page_id"), col("action"), col("timestamp"))

def transform_purchases(data):
    # Perform your own transformation logic here
    # For example, select only specific columns and rename them
    return data.select(col("user_id").alias("customer_id"), col("product_id"), col("quantity"))

# Apply transformations
datastream_interactions_transformed = datastream_interactions.map(transform_interactions)
datastream_purchases_transformed = datastream_purchases.map(transform_purchases)

# Combine the two streams
datastream_combined = datastream_interactions_transformed.union(datastream_purchases_transformed)

# Interpret the combined DataStream as a Table
combined_table = table_env.from_data_stream(datastream_combined, ['customer_id', 'page_id', 'action', 'product_id', 'quantity'])

table_env.create_temporary_view("combined_view", combined_table)

# Define the sinks
create_sink_high_value_customers = """
    CREATE TABLE sink_high_value_customers (
        customer_id INT,
        action STRING,
        total_quantity INT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'high_value_customers',
        'scan.startup.mode' = 'earliest-offset',
        'properties.bootstrap.servers' = 'kafka-kafka-bootstrap:9095',
        'format' = 'json'
    );
"""

create_sink_all_data = """
    CREATE TABLE sink_all_data (
        customer_id INT,
        page_id STRING,
        action STRING,
        product_id STRING,
        quantity INT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'all_data',
        'scan.startup.mode' = 'earliest-offset',
        'properties.bootstrap.servers' = 'kafka-kafka-bootstrap:9095',
        'format' = 'json'
    );
"""

# Execute CREATE TABLE statements
table_env.execute_sql(create_sink_high_value_customers)
table_env.execute_sql(create_sink_all_data)

stmt_set = table_env.create_statement_set()

# Insert data into sinks
stmt_set.add_insert_sql("INSERT INTO sink_high_value_customers SELECT customer_id, action, SUM(quantity) as total_quantity FROM combined_view WHERE quantity > 5 GROUP BY customer_id, action")
stmt_set.add_insert_sql("INSERT INTO sink_all_data SELECT customer_id, page_id, action, product_id, quantity FROM combined_view")

# Execute the job
stmt_set.execute()