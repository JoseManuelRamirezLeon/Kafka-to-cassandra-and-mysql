import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import multiprocessing
import threading, logging, time

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, SimpleClient
from kafka import KafkaConsumer

#from atomiclong import AtomicLong

#import mysql.connector
#from cassandra.cluster import Cluster

# These access token and secret are obtained when applying for a twitter api:
access_token = "WRITE YOUR OWN"
access_token_secret =  "WRITE YOUR OWN"
consumer_key =  "WRITE YOUR OWN"
consumer_secret =  "WRITE YOUR OWN"


# This is a class to create a streamlistener, which we will use later to create a twitter stream:
class StdOutListener(StreamListener):
    # This methods sends data from the twitter stream into the kafka topic 'twitter'
    def on_data(self, data):
        kafka = SimpleClient("localhost:9092")
        producer = SimpleProducer(kafka)
        producer.send_messages("twitter", data.encode('utf-8'))
        #print(data)
        return True

    def on_error(self, status):

        print('ERROR!!!! --------------------------->')


# This function creates a stream listener and starts a stream which shows all messages that contain the word 'python'
def prod():
    # A stream listener object is created
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # A tweepy stream object is created by using the stream listener and the api credentials
    stream = Stream(auth, l)
    # Finally, a twitter stream which contains all tweets that have the word 'python' is created
    stream.filter(track="puigdemont")


# This class consumes messages from kafka and inserts them into cassandra and mysql
# It inherits from the multiprocessing.Process class in order to be able to start several consumers
class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        os.environ[
            'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 pyspark-shell'
        sc = SparkContext()
        sc.setLogLevel('ERROR')
        #Streaming window 1s
        ssc = StreamingContext(sc, 1)
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092')

        # Subscribe to kafka topic named 'twitter'
        consumer.subscribe(['twitter'])

        kafkaParams = {"bootstrap.servers": "localhost:9092"}

        tweet_DStream = KafkaUtils.createDirectStream(ssc, ['twitter'], kafkaParams)

        tweet_strings = tweet_DStream.map(lambda line: line[1])
        inicio_texto = ',"text":'
        fin_texto = '","source":'
        tweet_strings = tweet_strings.map(lambda tweet_string: tweet_string[tweet_string.find(inicio_texto) + 9:])
        tweet_strings = tweet_strings.map(lambda tweet_string: tweet_string[:tweet_string.find(fin_texto)])
        tweet_string_length = tweet_strings.map(lambda tweet: len(tweet))

        def find_average(rdd):
            all_chars = rdd.reduce(lambda x, y: x + y)
            all_tweets = rdd.count()
            return all_chars/all_tweets

        tweet_string_length.foreachRDD(lambda rdd: print(find_average(rdd)))

        # start the streaming context
        ssc.start()
        ssc.awaitTermination()
        ssc.start()
        ssc.awaitTermination()

        consumer.close()


# This function is used to start a kafka consumer - which will, in turn, send the tweets to cassandra and mysql
def cons():
    tasks = [
        Consumer()
    ]

    for t in tasks:
        t.start()


# Finally, we use 2 threads to be able to simultaneously send and consume messages from kafka
t1 = threading.Thread(target=prod)
t1.start()

t2 = threading.Thread(target=cons)
t2.start()
