import os
import subprocess
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from ua_parser import user_agent_parser
import hyperloglog
import time
from hdfs import Config


def get_segments(user_agent_str):
    try:
        parsed = user_agent_parser.Parse(user_agent_str)
        segments = []
        if 'iPhone' in parsed['device']['family']:
            segments.append('seg_iphone')
        if 'Firefox' in parsed['user_agent']['family']:
            segments.append('seg_firefox')
        if 'Windows' in parsed['os']['family']:
            segments.append('seg_windows')
        return segments
    except:
        return []


def parse_line(line):
    parts = line.split('\t')
    if len(parts) != 2:
        return []
    user_id, ua = parts
    return [(segment, user_id) for segment in get_segments(ua)]


def update_hll(new_vals, old_hll):
    hll = old_hll or hyperloglog.HyperLogLog(0.01)
    for user_id in new_vals:
        hll.add(user_id)
    return hll


def collect_result(rdd):
    global FINAL_RESULTS
    FINAL_RESULTS = rdd.collect()


def set_ending_flag(rdd):
    global RDD_FINISHED
    if rdd.isEmpty():
        RDD_FINISHED = True


RDD_FINISHED = False
FINAL_RESULTS = []

client = Config().get_client()
nn_address = subprocess.check_output(
    'hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode("utf-8")
sc = SparkContext(master='yarn-client', appName='RealtimeSegments')
ssc = StreamingContext(sc, 2)
ssc.checkpoint(
    './checkpoint{}'.format(time.strftime("%Y_%m_%d_%H_%M_%s", time.gmtime())))
DATA_PATH = "/data/realtime/uids"
batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path]))
           for path in client.list(DATA_PATH)[:30]]
dstream = ssc.queueStream(rdds=batches)
dstream.foreachRDD(set_ending_flag)
segmented_users = dstream.flatMap(parse_line)
segmented_users = segmented_users.updateStateByKey(update_hll)
segmented_users.foreachRDD(collect_result)

ssc.start()

while not RDD_FINISHED:
    pass

ssc.stop()

for segment, hll in FINAL_RESULTS:
    print("{}\t{}".format(segment, len(hll)))