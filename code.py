'''
Sentiment Analysis by streaming real-time tweets
Live data on tweets were streamed using Apache Kafka and processed using spark and python. A basic
sentimental analysis on twitter streams was performed to find out how many tweets are positive and how
many are negative in that instant.
'''
#getting data into kafka servers
#twitter_to_kafka.py
import json
from kafka import SimpleProducer, KafkaClient
import tweepy
import configparser
# Note: Some of the imports are external python libraries. They are installed on the current machine.
# If you are running multinode cluster, you have to make sure that these libraries
# and currect version of Python is installed on all the worker nodes.
class TweeterStreamListener(tweepy.StreamListener):

""" A class to read the twiiter stream and push it to Kafka"""
def __init__(self, api):
self.api = api
super(tweepy.StreamListener, self).__init__()
client = KafkaClient("localhost:9092")
self.producer = SimpleProducer(client, async = True,
batch_send_every_n = 1000,
batch_send_every_t = 10)
def on_status(self, status):

""" This method is called whenever new data arrives from live stream.
We asynchronously push this data to kafka queue"""
msg = status.text.encode('utf-8')
#print(msg)
try:
self.producer.send_messages(b'twitterstream', msg)
except Exception as e:
print(e)
return False
return True
def on_error(self, status_code):
print("Error received in kafka producer")
return True # Don't kill the stream
def on_timeout(self):
return True # Don't kill the stream

if __name__ == '__main__':
# Read the credententials from 'twitter.txt' file
config = configparser.ConfigParser()
config.read('twitter.txt')
consumer_key = config['DEFAULT']['consumerKey']
consumer_secret = config['DEFAULT']['consumerSecret']
access_key = config['DEFAULT']['accessToken']
access_secret = config['DEFAULT']['accessTokenSecret']

# Create Auth object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)
File: /home/vivek/Untitled Document 2 Page 2 of 3

# Create stream and bind the listener to it
stream = tweepy.Stream(auth, listener = TweeterStreamListener(api))

#Custom Filter rules pull all traffic for those filters in real time.
#stream.filter(track = ['love', 'hate'], languages = ['en'])
stream.filter(locations=[-180,-90,180,90], languages = ['en'])

#Now, twitter data has come into Kafka clusters
##Sentiment analysis
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
def main():
conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10) # Create a streaming context with batch interval of 10 sec
ssc.checkpoint("checkpoint")
pwords = load_wordlist("positive.txt")
nwords = load_wordlist("negative.txt")
counts = stream(ssc, pwords, nwords, 100)
make_plot(counts)
def make_plot(counts):

"""
Plot the counts for the positive and negative words for each timestep.
Use plt.show() so that the plot will popup.
"""
pos = []
neg = []
for i in range(len(counts)) :
if len(counts[i]) > 0 :
pos.append(counts[i][0][1])
neg.append(counts[i][1][1])
plt.plot(pos, marker="o",color="b")
plt.plot(neg, marker="o",color="g")
plt.xlabel("Time step")
plt.ylabel("Word count")
plt.legend((plt.Line2D([1], [1], marker="o",color="b"),plt.Line2D([1], [1], marker="o",color="g")),
("Positive","Negative"),numpoints=2, loc=2)
plt.show()
def load_wordlist(filename):

"""
This function should return a list or set of words from the given filename.
"""
f = open(filename)
text = f.read()
return text.split('\n')
def convert_word(a, pwords, nwords):
tup = ('positive', 0)
File: /home/vivek/Untitled Document 2 Page 3 of 3
if nwords.count(a) > 0 :
tup = ('negative', 1)
elif pwords.count(a) > 0 :
tup = ('positive', 1)
return tup
def update_function(new_value, count):
if count is None:
count = 0
return sum(new_value, count)
def stream(ssc, pwords, nwords, duration):
kstream = KafkaUtils.createDirectStream(
ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

# Each element of tweets will be the text of a tweet.
# You need to find the count of all the positive and negative words in these tweets.
# Keep track of a running total counts and print this at every time step (use the pprint function).
words = tweets.flatMap(lambda line : line.split(" "))
word_count = words.map(lambda word : convert_word(word, pwords, nwords))
word_counts = word_count.reduceByKey(lambda a, b : a + b)
running_total = word_counts.updateStateByKey(update_function)
running_total.pprint()

# Let the counts variable hold the word counts for all time steps
# You will need to use the foreachRDD function.
# For our implementation, counts looked like:
# [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
counts = []

# YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
word_counts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
ssc.start() # Start the computation
ssc.awaitTerminationOrTimeout(duration)
ssc.stop(stopGraceFully=True)
return counts
if __name__=="__main__":
main()
