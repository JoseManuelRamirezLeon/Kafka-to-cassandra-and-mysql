import multiprocessing
import threading, logging, time

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, SimpleClient
from kafka import KafkaConsumer


import mysql.connector
from cassandra.cluster import Cluster

access_token = "insert your own"
access_token_secret =  "insert your own"
consumer_key =  "insert your own"
consumer_secret =  "insert your own"


class StdOutListener(StreamListener):
    def on_data(self, data):
        kafka = SimpleClient("localhost:9092")
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
        
        #Subscribe to kafka topic named 'twitter', which, unsurprisingly, carries twitter messages 
        consumer.subscribe(['twitter'])
        
        #Continuously insert tweets into cassandra and mysql 
        while True:
            for message in consumer:
                tweet_string = str(message)

                #Insert values into cassandra

                cluster = Cluster()
                session = cluster.connect('twitter')
                preparedTweetInsert = session.prepare(
                    """
                    INSERT INTO twitter_cassandra (id, tweet)
                    VALUES (?,?)
                    """
                )

                #tweet id is within characters 152 through 207, whereas the message is contained 
                #within characters 208 through 405
                session.execute(preparedTweetInsert, [tweet_string[152:207],tweet_string[208:405]])

                #use your mysql database credentials
                mydb = mysql.connector.connect(host = "localhost", user = "user",
                                               passwd = "password",database = "twitter")

                mycursor = mydb.cursor()
                
                #mysql table is called tweets
                #sql statement is a basic insert statement
                sql = "INSERT INTO tweets (id, tweet) VALUES ('{}', '{}') ON DUPLICATE KEY UPDATE ID=ID;"
                
                ##tweet id is within characters 152 through 207, whereas the message is contained 
                #within characters 208 through 405
                #sql statment is executed, replacing backslashes in the tweet so that mysql does not get confused
                mycursor.execute(sql.format(tweet_string[152:207],tweet_string[219:405].replace('\\','').replace('\'', '')))
                #sql statment is commited
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
