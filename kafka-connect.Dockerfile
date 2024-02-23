# Extend from the Confluent Kafka Connect Docker image
FROM confluentinc/cp-kafka-connect:latest

# Install the Confluent JDBC Connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest

# Add the PostgreSQL JDBC driver
ADD https://jdbc.postgresql.org/download/postgresql-42.2.5.jar /usr/share/java/kafka-connect-jdbc/
