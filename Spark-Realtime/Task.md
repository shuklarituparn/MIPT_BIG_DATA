# Homework on Realtime

* Deadline: 26.05, 23:59 MSK.

#### Comments 

* Task should be solved using Spark Streaming.

## Task 1. Spark Streaming
#### Data
Input: `/data/realtime/uids`

Data format:
```
...
seg_firefox 4176
...
```

#### Task

A segment is a set of users defined by a certain attribute. When a user visits a web service from his device, this event is logged on the web service side in the following format ` 'user_id <tab> user_agent'. For example:
```
f78366c2cbed009e1febc060b832dbe4	Mozilla/5.0 (Linux; Android 4.4.2; T1-701u Build/HuaweiMediaPad) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.73 Safari/537.36
62af689829bd5def3d4ca35b10127bc5	Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36
```
Portions of web logs are received at the input in the described format. It is required to divide the audience (users) in these logs into the following segments:
1. Users who work on the Internet from under the iPhone.
2. Users, cat. they use the Firefox browser.
3. Users, cat. they use Windows.

Do not worry if some users do not fall into any of these segments, because in real life, data that is difficult to classify often comes across. We simply do not include such users in the sample.

Also, the segments may intersect (after all, it is possible that the user uses Windows, on which Firefox is installed). In order to select segments, you can use the following heuristics (or come up with your own):

|Segment|Heuristics|
|----|----|
|seg_iphone|`parsed_ua['device']['family'] like '%iPhone%'`|
|seg_firefox|`parsed_ua['user_agent']['family'] like '%Firefox%'`|
|seg_windows|`parsed_ua['os']['family'] like '%Windows%'`|

Estimate the number of unique users in each segment using the algorithm [HyperLogLog](https://github.com/svpcom/hyperloglog) (set `error_rate` = 1%).
As a result, output the segments and the number of users in the following format ` 'segment_name <tab> count'. Sort the result by the number of users in descending order.

#### Code for generating batches
In the task, use it without changes because it is critical for the verification system.
```python
from hdfs import Config
import subprocess

client = Config().get_client()
nn_address = subprocess.check_output('hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode("utf-8")

sc = SparkContext(master='yarn-client')

# Preparing base RDD with the input data
DATA_PATH = "/data/realtime/uids"

batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path])) for path in client.list(DATA_PATH)[:30]]

# Creating QueueStream to emulate realtime data generating
BATCH_TIMEOUT = 2 # Timeout between batch generation
ssc = StreamingContext(sc, BATCH_TIMEOUT)
dstream = ssc.queueStream(rdds=batches)
```
