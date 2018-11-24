import multiprocessing
import threading, logging, time

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, SimpleClient
from kafka import KafkaConsumer


import mysql.connector
from cassandra.cluster import Cluster

# These access token and secret are obtained when applying for a twitter api:
access_token = "insert your own"
access_token_secret =  "insert your own"
consumer_key =  "insert your own"
consumer_secret =  "insert your own"

#This is a class to create a streamlistener, which we will use later to create a twitter stream:
class StdOutListener(StreamListener):
    #This methods sends data from the twitter stream into the kafka topic 'twitter'
    def on_data(self, data):
        kafka = SimpleClient("localhost:9092")
        producer = SimpleProducer(kafka)
        producer.send_messages("twitter", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

# This function creates a stream listener and starts a stream which shows all messages that contain the word 'python'
def prod():
    #A stream listener object is created
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    #A tweepy stream object is created by using the stream listener and the api credentials
    stream = Stream(auth, l)
    #Finally, a twitter stream which contains all tweets that have the word 'python' is created
    stream.filter(track="python")


#This class consumes messages from kafka and inserts them into cassandra and mysql
#It inherits from the multiprocessing.Process class in order to be able to start several consumers
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
        
        #Subscribe to kafka topic named 'twitter'
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
                sql = "INSERT INTO tweets (id, tweet) VALUES ('{}', '{}') ON DUPLICATE KEY UPDATE ID=ID;"
                
                #tweet id is within characters 152 through 207, message within characters 208 through 405
                #sql statment is executed, replacing backslashes in the tweet so that mysql does not get confused
                mycursor.execute(sql.format(tweet_string[152:207],tweet_string[219:405].replace('\\','').replace('\'', '')))
                #sql statment is commited
                mydb.commit()


        consumer.close()

# This function is used to just start a kafka consumer - which will, in turn, send the tweets to cassandra and mysql 
def cons():

    tasks = [
        Consumer()
    ]

    for t in tasks:
        t.start()


#Finally, we use 2 threads to be able to simultaneously send and consume messages from kafka
t1 = threading.Thread(target=prod)
t1.start()

t2 = threading.Thread(target=cons)
t2.start()
