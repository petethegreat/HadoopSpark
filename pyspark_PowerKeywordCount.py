#!/usr/local/spark/spark-2.2.0-bin-hadoop2.7/bin/pyspark


from pyspark import SparkConf, SparkContext

def flatMapLines(x):
    '''
    Function to map a keyword line into multiple keys
    append classname_level to the start of each keyword
    add a "total" keyword, to track the total number of powers for this classname_level
    return a list of "classname_level,keyword"
    '''
    words = x.split(' ')
    key = words[0]
    vals = [ key+',total']
    for kw in words[1:len(words)]:
        vals.append(key+','+kw)
    return(vals)
    

conf = SparkConf()
conf.setMaster("spark://ravage:7077")
conf.setAppName("My application")
conf.set("spark.executor.memory", "512m")
conf.set("sparkHome","/usr/local/spark/spark-2.2.0-bin-hadoop2.7")
sc = SparkContext(conf = conf)


keywordsFile = 'hdfs://localhost:54310/user/hduser/powerKeywords.txt'
outfile = 'hdfs://localhost:54310/user/hduser/output/powerKeywordCount.txt'

# sc = SparkContext('local','test app')

data = sc.textFile(keywordsFile) # rdd of lines


keyworded = data.flatMap(flatMapLines).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).coalesce(1).map(
    lambda x: "'{classlevel}','{kw}',{count}".format(
        classlevel=x[0].split(',')[0].encode('utf-8'),
        kw=x[0].split(',')[1].encode('utf-8'),
        count=x[1]))

# flatmap - split each line into a list of "classname_level,keyword"
# map - for each keyword, form a tuple (kw,count)
# reduceby key - sum the keyword counts
# coalesce - merge the RDD partitions
# map - form a string "classname_level, keyword, count" from each tuple

keyworded.saveAsTextFile(outfile)
# write the strings to a file




