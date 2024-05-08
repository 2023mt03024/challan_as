"""Module consumes and stores challan messages."""
import os
import uuid
import json
from threading import Thread
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError
import socket
from flask import Flask, render_template, request, url_for, flash, redirect
from werkzeug.exceptions import abort
import requests

# Create Flask app
app = Flask(__name__)

# For kubenetes health check
@app.route('/health')
def health_check():
    # Return success
    return 'OK', 200
      
# Get vehicle owner
def get_vehicle_owner(vehicle_number):
    try:
        # Prepare query params
        payload = {'vehicle_number': vehicle_number}

        # Send the request
        r = requests.get('http://{}:{}/vehicle'.format(os.getenv("VEHICLE_SERVICE_HOST"),
                        os.getenv("VEHICLE_SERVICE_PORT")), params=payload)

        # Get owner
        return r.text
    except Exception as error:
        # Return failure
        return "Not Available"

# Process the messages
def msg_process(msg):
    # Get payload
	body = json.loads(msg)
     
    # Get owner name
	ownerName = get_vehicle_owner(body['Vehicle Number'])
	
	try:
        # Insert a challan into Challans table
		tableChallans.put_item(
			Item={
                'Echallan No': uuid.uuid4().hex,
                'Vehicle Number': body['Vehicle Number'],
                'Owner Name': ownerName,
                'Unit Name': body['Unit Name'],
                'Date': body['Date'],
                'Time': body['Time'],
                'Place of Violation': body['Place of Violation'],
                'PS Limits' : body['PS Limits'],
                'Violation': body['Violation'],
                'Fine Amount': int(body['Fine Amount']),
                'User Charges': 35,
                'Total Fine': int(body['Fine Amount']) + 35
			}
		)
          
	except Exception as error:
        # Return failure
		print(error)

# Consume kafka messages
def consume_messages(consumer):
    try:
        # Subscribe for Challan messages
        consumer.subscribe(['Challan'])

        # Keep on receive messages
        while True:
            # Poll for message for every 1 second
            msg = consumer.poll(timeout = 1.0)

            # Time out expired, poll again
            if msg is None: 
                continue

            # Check for error
            if msg.error():
                # Check for f error is not EOF
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("ERROR: {}".format(msg.error()))
            else:
                # process the challan message
                msg_process(msg.value().decode('utf-8'))
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

# Starts from here
if __name__ == "__main__": 
    # Get endpoint url
	endpointUrl = 'http://{}:{}'.format( 
                              os.getenv('DYNAMODB_SERVICE_HOST'),
                              os.getenv('DYNAMODB_SERVICE_PORT'))
      
    # Get dynamodb resource
	dynamodb = boto3.resource('dynamodb',  aws_access_key_id="anything", aws_secret_access_key="anything",
                          region_name="us-west-2", endpoint_url=endpointUrl)
          
    # Get Challans table
	tableChallans = dynamodb.Table('Challans')
     
    # Get status
	try:
		tableStatus = tableChallans.table_status
	except ClientError:
        # Create table as it is not exist
		tableChallans = dynamodb.create_table(  TableName='Challans',
                                                KeySchema=[
                                                        {
                                                            'AttributeName': 'Echallan No',
                                                            'KeyType': 'HASH'  #Partition_key
                                                        }
                                                ],
                                                AttributeDefinitions=[
                                                        {
                                                            'AttributeName': 'Echallan No',
                                                            'AttributeType': 'S'
                                                        }
                                                ],
                                                ProvisionedThroughput={
                                                    'ReadCapacityUnits': 10,
                                                    'WriteCapacityUnits': 10
                                                }
                                            )
     
    # Initialize kafka configuration
	conf = {'bootstrap.servers': os.getenv('KAFKA_SERVICE_HOST') + ':' + os.getenv('KAFKA_SERVICE_PORT'),
            'group.id': 'challan',
            'auto.offset.reset': 'smallest',
            'client.id': socket.gethostname()}

    # Create  a consumer
	consumer = Consumer(conf)
     
    # Start consume messages in a separate thread            
	consume_thread = Thread(target=consume_messages, args=(consumer,), daemon=True)
	consume_thread.start()
     
    # Set the configuration
	app.config[os.getenv("FLASK_KEY")] = os.getenv("FLASK_KEY_VALUE")

    # Run the application
	app.run(host='0.0.0.0', port=8002)
