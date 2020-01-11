from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = set(load_wordlist("positive.txt"))
    nwords = set(load_wordlist("negative.txt"))
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    positivearr=[]
    negativearr=[]
    for i in counts:
        for j in i:
            if j[0]=="positive":
                 positivearr.append(j[1])
            else:
                 negativearr.append(j[1])

    value=list(range(len(negativearr)))

    plt.plot(value,positivearr,'b-',marker='o')
    plt.plot(value,negativearr,'g-',marker='o')
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.gca().legend(('positive','negative'))
    plt.show()


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE


    txtfile = open(filename, 'r')

    worldlist = [line.strip('\n') for line in txtfile.readlines()]

    return worldlist

def updateFunction(newValues, runningCount):

    if runningCount is None:

        runningCount = 0

    return sum(newValues, runningCount)


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    
    tweetwords=tweets.flatMap(lambda line: line.split(" "))

    words=tweetwords.filter(lambda x: (x in pwords) or (x in nwords))

    mapwords=words.map(lambda word: ("positive",1) if word in pwords else ("negative",1) )

    dstreamTotal=mapwords.reduceByKey(lambda x,y:x+y)

    runningTotal=dstreamTotal.updateStateByKey(updateFunction)

    runningTotal.pprint()    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    dstreamTotal.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
