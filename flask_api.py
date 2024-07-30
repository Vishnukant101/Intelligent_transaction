from flask import Flask, jsonify, request
from kafka import KafkaProducer
import json

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

@app.route('/transaction', methods=['POST'])
def create_transaction():
    transaction_data = request.json
    producer.send('transactions', value=transaction_data)
    return jsonify({"message": "Transaction data received and published to Kafka"}), 200

if __name__ == '_main_':
    app.run(debug=True, port=5000)