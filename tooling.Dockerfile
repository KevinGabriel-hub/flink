# Dockerfile
FROM python:3.8-slim

WORKDIR /app

# Update the package lists for upgrades and new packages
RUN apt-get update

# Install curl
RUN apt-get install -y curl

# Clean up the package lists
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Install requests library
RUN pip install requests fastavro confluent-kafka avro-python3

# Copy the script and schema to the Docker image
# COPY register_schema.py ./
COPY tooling/QuoteData.avsc ./
COPY tooling/data-producer.py ./

# Copy the script into the image
COPY tooling/wait-for-it.sh /usr/local/bin/wait-for-it.sh
# Make the script executable
RUN chmod +x /usr/local/bin/wait-for-it.sh

CMD ["python", "./data-producer.py"]