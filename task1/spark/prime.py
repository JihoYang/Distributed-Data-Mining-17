## Imports
from pyspark import SparkConf, SparkContext

## Module Constants
APP_NAME = "Prime"

## isprime finds prime numbers
def isprime(n):

    """
    check if integer n is a prime
    """

    # make sure n is a positive integer
    n = abs(int(n))

    # 0 and 1 are not primes
    if n < 2:
        return False

    # 2 is the only even prime number
    if n == 2:
        return True

    # all other even numbers are not primes
    if not n & 1:
        return False

    # range starts with 3 and only needs to go up the square root of n
    # for all odd numbers
    for x in range(3, int(n**0.5)+1, 2):
        if n % x == 0:
            return False
    return True

## tokenize splits the numbers in the input file into seperate numbers (as strings)
def tokenize(numbers_raw):
    return numbers_raw.split()

## checks the number of workers
def workername():

    import socket
    return str(socket.gethostname())
    anrdd=sc.parallelize(['',''])
    namesRDD = anrdd.flatMap(lambda e: (1,workername()))
    namesRDD.count()

## Main function
def main(sc):

    # Import the input file, convert it to RDD, and distribute them to worker nodes
    numbers_raw = sc.textFile("/root/Distributed-Data-Mining-17/task1/spark/data/input_numbers.txt")

    print "input file imported and RDD created"

    #print sc.getExecutorMemoryStatus


    # tokenize the input file into seperate numbers
    nums = numbers_raw.flatMap(tokenize)

    print "input file tokenized"
    print "list of workers: %s" %workername

    # count number of prime numbers
    prime_num = nums.filter(isprime).count()

    # print the result
    print "Number of prime numbers = %i" %prime_num

    # save the output file - this isn't working
    #prime_num.saveAsTextFile("task1_output")

if  __name__ == "__main__":

    ## Configure Spark
    conf = SparkConf().setAppName(APP_NAME)

    #conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    #Execute Main function
    main(sc)

