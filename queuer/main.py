#!/usr/bin/env python3
import os
import sys
import pika

if len(sys.argv) == 1:
    print("Please include a directory to process. Exiting.")
    sys.exit()

root_dir = sys.argv[1]

rabbit_host = os.environ.get('RABBITMQ_HOST')
rabbit_user = os.environ.get("RABBIT_USER")
rabbit_password = os.environ.get("RABBIT_PASSWORD")

if not rabbit_host:
    print("RabbitMQ host not set. Exiting.")
    sys.exit()
if not rabbit_user:
    print("RabbitMQ user not set. Exiting.")
    sys.exit()
if not rabbit_password:
    print("RabbitMQ password not set. Exiting.")
    sys.exit()

credentials = pika.PlainCredentials(rabbit_user, rabbit_password)
connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host, 5672, '/', credentials))
channel = connection.channel()
channel.queue_declare(queue='dedupe')


def scan(directory):
    print("Scanning directory %s" % directory)
    for filename in os.scandir(directory):
        if filename.is_dir():
            scan(filename)
        else:
            file_path = os.path.join(root_dir, filename)
            channel.basic_publish(exchange='',
                                  routing_key='dedupe',
                                  body=file_path)
            print('Queued ' + file_path)

scan(root_dir)

connection.close()
