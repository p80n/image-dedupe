#!/usr/bin/env python3
import hashlib
import os
import sys
import pika
import threading
import functools
from sqlalchemy import create_engine
from sqlalchemy import Column, String, UnicodeText
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm import sessionmaker


db_string = "postgresql://dedupe:dedupe@10.43.161.3/image_dedupe"

Base = declarative_base()
engine = create_engine(db_string)

class File(Base):
    __tablename__ = 'files'
    id = Column(String(32), primary_key=True)
    path = Column(UnicodeText, index=True)
    duplicates = Column(MutableList.as_mutable(JSONB))

    def __repr__(self):
        return "<File(path='%s', md5='%s')>" % (self.path, self.id)

Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)
session = Session()


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
connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host, 5672, '/', credentials, heartbeat=0))
channel = connection.channel()
queue = channel.queue_declare(queue='dedupe')

bytes_deleted = 0
deleted_files = []
threads = []


def process_queue():

    def process_file(channel, method, path):
        global bytes_deleted
        global deleted_files
        global connection
        file = open(path, "rb")
        content = file.read()
        md5 = hashlib.md5()
        md5.update(content)
        digest = md5.hexdigest()

        file = session.query(File).get(digest)
        if file is None:
            print("Adding %r to database" % path);
            file = File(id=digest, path=path)
            session.add(file)
            session.commit()
        else:
            if path != file.path:
                deleted_files.append(os.path.basename(path))
                if file.duplicates:
                    if path not in file.duplicates:
                        file.duplicates.append(path)
                        flag_modified(file, "duplicates")
                        session.add(file)
                        session.commit()

                else:
                    file.duplicates = [path]

                file_size = os.path.getsize(path)
                bytes_deleted += file_size
                print("Deleting duplicate file %s" % path)
                os.remove(path)

        cb = functools.partial(channel.basic_ack, method.delivery_tag)
        connection.add_callback_threadsafe(cb)


    def on_message(ch, method, properties, body):
        path = body.decode()
        print("Checking %r" % path)
        if not os.path.exists(path):
            channel.basic_ack(delivery_tag = method.delivery_tag)
            return

        t = threading.Thread(target=process_file, args=(channel, method, path))
        t.start()
        threads.append(t)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='dedupe', on_message_callback=on_message)
    channel.start_consuming()


try:
    process_queue()
except KeyboardInterrupt:
    print('Exiting. %s mib freed' % round(bytes_deleted/1024/1024))
    print(",".join(deleted_files))
    channel.stop_consuming()
    # Wait for all to complete
    for thread in threads:
        thread.join()
    connection.close()
    sys.exit(0)




