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