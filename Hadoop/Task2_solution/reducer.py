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