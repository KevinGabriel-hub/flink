import json
import requests

# Load the schema from the file
with open('QuoteData.avsc', 'r') as file:
    schema = json.load(file)

# Define the schema in the format required by the Schema Registry
schema_to_register = {
    "schema": json.dumps(schema)
}

# Send a POST request to the Schema Registry
response = requests.post('http://schema-registry:8081/subjects/QuoteData/versions', json=schema_to_register)

# Check the response
if response.status_code == 200:
    print("Schema registered successfully")
else:
    print("Failed to register schema:", response.content)