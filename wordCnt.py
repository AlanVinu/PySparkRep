from pyspark import SparkConf, SparkContext

#Starting spark contextand running the application locally
#Passing config information to spark context

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# reading in text file and storing it 
file = sc.textFile("D:/Spark/SherlockHolmes.txt")

# splitting the text using lambda function
words = file.flatMap(lambda line: line.split(" "))

# mapping values to each word
wordsOneCnt = words.map(lambda word: (word, 1))

# summing the values by using reduceByKey
wordsCnt = wordsOneCnt.reduceByKey(lambda a, b: a + b)

# viewing the ocuurances of the words
cntWords = wordsCnt.map(lambda wc: (wc[1], wc[0]))

# Sorting the key-value pairs by frequency of occurrence
wordsCntSort = cntWords.sortByKey()

wordsCntSort.saveAsTextFile("D:/Spark/code.SH/wrdcnt")
