import logging

from kafka import KafkaConsumer

from Mongo.mongo import MongoJob
from Mongo.client import clientMongo


class ConsumerClass:

    def __init__(self):

        self.client = clientMongo()

    def consumer_kafka(self):
        logging.info("\n\nRUNNING CONSUMER \n\n")

        mongo_client = self.client.mongoClient

        try:
            consumer = KafkaConsumer('government', 'wiki', 'health', 'economy', 'environment', 'education', 'business',
                                     'sport', 'entertainment', group_id='my-group',
                                     bootstrap_servers=['localhost:9092'])

            obj = []
            for message in consumer:

                #
                # For each message join in table - mongo db
                #
                # create new thread
                thread_obj = MongoJob(mongo_client, message)
                obj.append(thread_obj)
                # start thread --  insert ARTICLE in table Mongo DB
                thread_obj.start()

        except Exception as e:
            logging.warning("\n\nDetails of the Exception:" + str(e.args) + "\n\n")
            raise e

        # JOIN THREADS ---------
        try:
            for obj in obj:
                obj.join()

        except Exception as e:
            logging.warning("\n\nDetails of the Exception:" + str(e.args) + "\n\n")
            raise e
