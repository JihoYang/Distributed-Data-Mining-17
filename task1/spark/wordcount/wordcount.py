from pyspark import SparkConf, SparkContext

APP_NAME = "word_count"

def tokenize(text):
    return text.split()

def main(sc):

    from operator import add
    
    text = sc.textFile("/root/Distributed-Data-Mining-17/task1/spark/wordcount/data/war_and_peace.txt")

    words = text.flatMap(tokenize)

    wc = words.map(lambda x: (x,1))

    counts = wc.reduceByKey(add)
    counts.saveAsTextFile("wc")

if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)


