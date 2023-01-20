import logging

from Kafka.consumer import ConsumerClass

# logging
# logging.basicConfig(filename="stdLoggerFile.log",
#                    filemode='a',
#                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
#                    datefmt='%H:%M:%S',
#                    level=logging.DEBUG)

#
#
# START CONSUMERS
#
x = ConsumerClass()
x.consumer_kafka()
