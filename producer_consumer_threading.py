import threading

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

import mysql.connector
import threading, logging, time
import multiprocessing
from cassandra.cluster import Cluster

access_token = "insert your own access token"
access_token_secret =  "insert your own token secret"
consumer_key =  "insert your own consumer key"
consumer_secret =  "insert your own consumer secret"


class StdOutListener(StreamListener):
    def on_data(self, data):
        kafka = KafkaClient("localhost:9092")
        producer = SimpleProducer(kafka)
        producer.send_messages("twitter", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

def prod():

    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track="trump")


from kafka import KafkaConsumer, KafkaProducer

class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()


    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['twitter'])



        while True:
            for message in consumer:
                tweet_string = str(message)


                #Insert values into cassandra

                cluster = Cluster()
                session = cluster.connect('twitter')
                preparedTweetInsert = session.prepare(
                    """
                    INSERT INTO twitter_cassandra2 (id, tweet)
                    VALUES (?,?)
                    """
                )
                #mensaje = tweet_string[inicio:fin+1]
                #print(mensaje)
                session.execute(preparedTweetInsert, [tweet_string[152:207],tweet_string[208:405]])

                #Insert values into mysql
                mydb = mysql.connector.connect(host = "localhost", user = "root",
                                               passwd = "abc123",database = "twitter")

                mycursor = mydb.cursor()

                sql = "INSERT INTO tweets2 (id, tweet) VALUES ('{}', '{}') ON DUPLICATE KEY UPDATE ID=ID;"
                #val = (tweet_string[152:207],tweet_string[208:405])
                mycursor.execute(sql.format(tweet_string[152:207],tweet_string[219:405].replace('\\','').replace('\'', '')))

                mydb.commit()

                # if self.stop_event.is_set():
                #     break

        consumer.close()


def cons():

    tasks = [
        Consumer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10)

    for task in tasks:
        task.stop()



t1 = threading.Thread(target=prod)
t1.start()

t2 = threading.Thread(target=cons)
t2.start()
