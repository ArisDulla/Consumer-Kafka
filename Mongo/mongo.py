import logging

import json
import threading


class MongoJob(threading.Thread):

    def __init__(self, client, message):
        threading.Thread.__init__(self)

        self.exc = None
        self.cluster = client
        self.message = message

    def insert(self):

        topic = self.message.topic
        my_str = self.message.value
        print(topic)
        res = json.loads(my_str)

        db = self.cluster["Collections"]

        collection = db[topic]

        logging.info(" join in column = " + topic)
        # JOIN
        try:
            if "wiki" == topic:
                collection.insert_one(res)

            else:
                collection.insert_many(res)

        except Exception as a:
            logging.warning("Exception" + str(a))
            print(a)
            raise a

    #
    def run(self):

        try:
            self.insert()
        except BaseException as e:
            self.exc = e

    def join(self):
        threading.Thread.join(self)
        if self.exc:
            raise self.exc
