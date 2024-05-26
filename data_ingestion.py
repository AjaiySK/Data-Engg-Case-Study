from kafka import KafkaProducer
import json
import csv
import avro.schema
import avro.io
import io

# Kafka setup
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Ingest JSON data (Ad Impressions)
def ingest_json_data(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            data = json.loads(line)
            producer.send('ad_impressions', value=json.dumps(data).encode('utf-8'))

# Ingest CSV data (Clicks and Conversions)
def ingest_csv_data(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            producer.send('clicks_conversions', value=json.dumps(row).encode('utf-8'))

# Ingest Avro data (Bid Requests)
def ingest_avro_data(file_path, schema_path):
    schema = avro.schema.Parse(open(schema_path, "rb").read())
    with open(file_path, 'rb') as file:
        while file.peek():
            reader = avro.io.DatumReader(schema)
            decoder = avro.io.BinaryDecoder(file)
            data = reader.read(decoder)
            producer.send('bid_requests', value=json.dumps(data).encode('utf-8'))

# Example usage
ingest_json_data('ad_impressions.json')
ingest_csv_data('clicks_conversions.csv')
ingest_avro_data('bid_requests.avro', 'schema.avsc')
