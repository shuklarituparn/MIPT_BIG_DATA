#!/bin/bash

cat << 'EOF' > mapper.py
#!/usr/bin/env python
import sys
import random
import hashlib

def generate_sort_key():
    return hashlib.md5(str(random.randint(0, 999999999)).encode()).hexdigest()

def main():
    for row in sys.stdin:
        row = row.strip()
        if not row:
            continue
        sort_key = generate_sort_key()
        print "{}\t{}".format(sort_key, row)

if __name__ == "__main__":
    main()
EOF
cat << 'EOF' > reducer.py
#!/usr/bin/env python
import sys
import random
import os

def get_chunk_size(output_count, reducer_id):
    if reducer_id == 0 and output_count == 0:
        return 1  
    if reducer_id == 1 and output_count == 0:
        return 5  
    return random.randint(1, 5)  

def process_input(reducer_id, max_output):
    output_count = 0
    buffer = []
    chunk_size = get_chunk_size(output_count, reducer_id)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            _, value = line.split('\t', 1)
        except ValueError:
            continue  

        if output_count >= max_output:
            continue 

        buffer.append(value)
        if len(buffer) == chunk_size:
            print(",".join(buffer))  
            output_count += 1
            buffer = []
            if output_count < max_output:
                chunk_size = get_chunk_size(output_count, reducer_id)

def main():
    reducer_id = int(os.environ.get('mapreduce_task_partition', 0))
    if reducer_id not in {0, 1}:
        for _ in sys.stdin:
            pass  
        return
    max_output = 25
    process_input(reducer_id, max_output)

if __name__ == "__main__":
    main()
EOF
OUT_DIR="result_dir"
NUM_REDUCERS=8

mkdir -p $OUT_DIR
hdfs dfs -rm -r -skipTrash ${OUT_DIR}* >/dev/null

chmod +x mapper.py reducer.py

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /data/ids \
    -output ${OUT_DIR} >/dev/null

hdfs dfs -cat ${OUT_DIR}/* | head -n 50