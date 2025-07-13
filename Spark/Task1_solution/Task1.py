from pyspark import SparkContext, SparkConf
import re

config = (SparkConf()
          .setAppName("Task1")
          .setMaster("yarn")
          .set("spark.hadoop.fs.defaultFS", "mipt-master.atp-fivt.org:8020")
          )

sc = SparkContext(conf=config)

stop_words = set(sc.textFile("/data/wiki/stop_words_en-xpo6.txt").flatMap(lambda x: x.split()).collect())
broadcast_stop_words = sc.broadcast(stop_words)

articles = sc.textFile("/data/wiki/en_articles_part")

def process_article(line):
    try:
        _, text = line.split('\t', 1)
    except:
        return []

    text = re.sub("[^a-zA-Z]", " ", text).lower().split()
    words = [word for word in text if word not in broadcast_stop_words.value and len(word) > 0]

    return [(words[i], words[i+1]) for i in range(len(words)-1)] if len(words) > 1 else []

bigrams = articles.flatMap(process_article)
target_bigrams = bigrams.filter(lambda x: x[0] == "narodnaya")

result = target_bigrams.map(lambda x: ("{}_{}".format(x[0], x[1]), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortByKey()

output = result.collect()
for (bigram, count) in output:
    print("{}\t{}".format(bigram, count))

sc.stop()