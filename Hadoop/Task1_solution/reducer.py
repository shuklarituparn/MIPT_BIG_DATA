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