#!/bin/bash

OUT_DIR="stopwords_count_top10"
NUM_REDUCERS=8
UBER_FLAG=false

cat << 'EOF' > mapper.py
#!/usr/bin/env python
import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')

STOP_WORDS_FILE = 'stop_words_en.txt'
stop_words = set()

with open(STOP_WORDS_FILE) as f:
    for line in f:
        stop_words.add(line.strip().lower())

word_pattern = re.compile(r'\b[a-z]+\b', re.IGNORECASE)

for line in sys.stdin:
    doc_id, text = line.strip().split('\t', 1)
    words_in_doc = set(word.lower() for word in word_pattern.findall(text))
    for word in words_in_doc:
        if word in stop_words:
            print "%s\t%s" % (word, doc_id)
EOF
chmod +x mapper.py

cat << 'EOF' > reducer.py
#!/usr/bin/env python
import sys

current_word = None
current_docs = set()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t', 1)
    if len(parts) != 2:
        continue

    word, doc_id = parts
    if current_word is None:
        current_word = word
        current_docs.add(doc_id)
        continue

    if word == current_word:
        current_docs.add(doc_id)
    else:
        print("%s\t%d" % (current_word, len(current_docs)))
        current_word = word
        current_docs = {doc_id}

if current_word is not None:
    print("%s\t%d" % (current_word, len(current_docs)))
EOF
chmod +x reducer.py

hdfs dfs -rm -r -skipTrash $OUT_DIR >/dev/null 2>&1

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="CountStopWordsTop10" \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -D mapreduce.job.ubertask.enable=$UBER_FLAG \
    -files /datasets/stop_words_en.txt,mapper.py,reducer.py \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /data/wiki/en_articles \
    -output $OUT_DIR > /dev/null

hdfs dfs -cat ${OUT_DIR}/part-* \
| sort -k2,2nr -k1,1 \
| head -10