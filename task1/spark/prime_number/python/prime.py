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
    if not n % 2:
        return False

    # range starts with 3 and only needs to go up the square root of n
    # for all odd numbers
    for x in range(3, int(n**0.5)+1, 2):
        if n % x == 0:
            return False
    return True

## Tokenize
def tokenize(text):
    return text.split()


## Main function
def main(sc):

    from operator import add
    
    numbers_raw = sc.textFile("/root/Distributed-Data-Mining-17/task1/spark/prime_number/python/data/input_numbers.txt")
    nums = numbers_raw.flatMap(tokenize)

    #print nums.filter(isprime).count()
    prime = nums.map(isprime)

    #for x in prime.collect():
    #    print x
    print prime.reduceByKey(add)


if  __name__ == "__main__":

    ## Configure Spark
    conf = SparkConf().setAppName(APP_NAME)

    #conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    #Execute Main function
    main(sc)

