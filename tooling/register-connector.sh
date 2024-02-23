#!/bin/bash

echo "registering connector..."

max_attempts=30
attempt=0
sleep_time=15 # in seconds

until [ $attempt -ge $max_attempts ]
do
  curl -X POST -H "Content-Type: application/json" --data @kafka-connect-refdata.json http://connect:8083/connectors && break
  attempt=$[$attempt+1]
  echo "Attempt $attempt failed! Trying again in $sleep_time seconds..."
  sleep $sleep_time
done

if [ $attempt -eq $max_attempts ]; then
  echo "Script failed after $max_attempts attempts."
fi