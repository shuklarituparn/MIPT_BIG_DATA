# Task 5. Spark

* Soft deadline: May 7 23.59
* Hard deadline: May 14 23.59

Problems 1 and 2 may be solved using either RDD or DF API.

## Problem 1

#### Source data

* `/data/wiki/en_articles` - Wikipedia articles (full dataset).
* `/data/wiki/en_articles_part` - sample. Please use this for debugging in order not to overload the cluster.

Data format
```
article ID <tab> article text
```
* `/data/wiki/stop_words_en-xpo6.txt` - list of stop words that should be filtered out.

Data format: one stop word per line.
```
...
wherein
whereupon
wherever
...
```
#### Problem statement
Find all pairs of adjacent words (bigrams) where the first one is «narodnaya». For each bigram, count the total number of occurrences in Wikipedia articles. Print them all in lexicographic order like this: `word_pair <tab> count`. Example:

```
red_apple 100500
crazy_zoo 42
```

Note that the bigrams are in lower case and should be joined by an underscore.

#### Technical details
When parsing, filter out all symbols that are not Latin letters:
```
text = re.sub("^\W+|\W+$", "", text)
```

## Problem 2

#### Source data
* Full dataset: `/data/twitter/twitter_sample.txt` (use this dataset when committing your code)
* Sample: `/data/twitter/twitter_sample_small.txt`

**Data format:**
```
user_id \t follower_id
```
#### Problem statement
You're given an oriented graph. Using the breadth-first search algorithm, find the shortest path length between vertices 12 and 34.
Note that you can stop the search before general algorithm finishes, since we're only interested in a single path.
Output format: a sequence of vertices (including start and finish), comma-separated, without spaces. E.g. the path «12 -> 42 -> 34» should be printed as 12,42,34.

#### Additional comments
Since the dataset isn't large, you might give in to the temptation to call `take()` or `collect()` and convert RDD into a regular Python object. It's easier to work with regular objects, however they don't meet the criteria of being distributed and highly available, so such a solution will not be graded.
Avoid writing UDFs when possible and use the module `pyspark.sql.functions` instead.

#### Basic snippet
You can start with this code. It's inefficient and won't work in the testing system, but it might give you some initial ideas on how to solve the problem.
```python
def parse_edge(s):
  user, follower = s.split("\t")
  return (int(user), int(follower))

def step(item):
  prev_v, prev_d, next_v = item[0], item[1][0], item[1][1]
  return (next_v, prev_d + 1)

def complete(item):
  v, old_d, new_d = item[0], item[1][0], item[1][1]
  return (v, old_d if old_d is not None else new_d)

n = 400  # number of partitions
edges = sc.textFile("/data/twitter/twitter_sample_small.txt").map(parse_edge).cache()
forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(n).persist()

x = 12
d = 0
distances = sc.parallelize([(x, d)]).partitionBy(n)
while True:
  candidates = distances.join(forward_edges, n).map(step)
  new_distances = distances.fullOuterJoin(candidates, n).map(complete, True).persist()
  count = new_distances.filter(lambda i: i[1] == d + 1).count()
  if count > 0:
    d += 1
    distances = new_distances
  else:
    break
```
SparkContext creation:
```python
from pyspark import SparkContext, SparkConf

config = SparkConf().setAppName("my_super_app").setMaster("local[3]")  # config; specify application name and execution mode (local[*] to run the app locally, yarn for YARN)
sc = SparkContext(conf=config)  # create a context using our config
```



