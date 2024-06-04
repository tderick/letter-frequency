import argparse
from pyspark import SparkConf, SparkContext

""" initialize SparkConf """
conf = SparkConf()

""" set the application name """
conf.setAppName("Letter Frequency Count")

""" Initialize a new SparkContext with Spark configuration """
sc = SparkContext(conf=conf)

""" Argument parser to handle command line inputs """
parser = argparse.ArgumentParser(description='Calculate character frequency.')
parser.add_argument('input_folder', type=str, help='Path to the input folder containing text files')
parser.add_argument('output_folder', type=str, help='Path to the output folder to save results')

args = parser.parse_args()
input_folder = args.input_folder
output_folder = args.output_folder

""" Create an RDD from text files in the input folder """
rdd = sc.textFile(input_folder)

""" Convert all characters to lowercase
    *map* transforms an RDD of length n into another RDD of length n.
    So at the end of the map transformation, we will an RDD of the same size but with the input line in lowercase
"""
rdd = rdd.map(lambda line: line.lower())

"""Filter out non-alphabetic characters and flatten the RDD
   *map* cannot be used here because the out RDD is not the same size as the input one.
   *flatMap* return 0, 1 or more element after the transformation. It is the right fit here
   because at the end of the transformation, the output will have different size than the input
"""
rdd = rdd.flatMap(lambda line: [char for char in line if 'a' <= char <= 'z'])

""" Map each character to a tuple (character, 1) """
rdd = rdd.map(lambda char: (char, 1))

""" reduceByKey count the occurrences of each character to have the total number of character in the input document.
    *reduceByKey* combines values with the same key. It is like the reduce function in Map-Reduce
"""
character_count_rdd = rdd.reduceByKey(lambda a, b: a + b)

""" We need the total count of all the character present in the input documents to calculate the percentage
    of apparition of each character. For this purpose, we use *count*.
    *count* returns the number of elements in the dataset. As we have already processed our input documents, we 
    can count the number of element in the RDD before the *reduceByKey* transformation to have the total number of character we are interested in.
"""
total_character_count = rdd.count()

""" We use *mapValues* to calculate the percentage of apparition of every character.
    *mapValues* is a transformation operation that is available on a Pair RDD (i.e., an RDD of key-value pairs). It applies a transformation function 
    to the values of each key-value pair in the RDD while keeping the key unchanged. It takes a function as an argument, which is applied to the value 
    part of each key-value pair. The function is applied independently to each partition of the RDD, in parallel.
    See: `https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.mapValues.html`

    In this case, we divide the count of the caracter by the total count, multiply by 100 and only take 2 decimal digit.

"""
character_frequency_rdd = character_count_rdd.mapValues(lambda count: round((count / total_character_count)*100,2))

""" Save the result to the output folder """
character_frequency_rdd.saveAsTextFile(output_folder)

""" Stop the Spark session """
sc.stop()
