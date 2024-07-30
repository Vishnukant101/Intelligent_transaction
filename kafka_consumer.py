import json

from kafka import KafkaConsumer

consumer = KafkaConsumer('transactions', bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    transaction_data = message.value
    # Here you can process the transaction data or save it to a database
    print(transaction_data)