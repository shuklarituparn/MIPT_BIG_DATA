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