from pyspark import SparkContext, SparkConf

config = SparkConf().setAppName("Task2").setMaster("yarn")
sc = SparkContext(conf=config)  

def parse_edge(s):
    user, follower = s.split("\t")
    return (int(user), int(follower))

def step(item):
    prev_v, paths, next_v = item[0], item[1][0], item[1][1]
    new_paths = [path + [str(next_v)] for path in paths]
    return (next_v, new_paths)

n = 400 
edges = sc.textFile("/data/twitter/twitter_sample.txt").map(parse_edge)
forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(n).persist()

start = 12
target = 34
paths = sc.parallelize([(start, [[str(start)]])]).partitionBy(n)

found_paths = []
current_distance = 0
found = False

while not found:
    new_paths = paths.join(forward_edges, n).map(step)
    combined_paths = new_paths.reduceByKey(lambda a, b: a + b)
    target_paths = combined_paths.filter(lambda x: x[0] == target).take(1)
    if target_paths:
        found_paths = target_paths[0][1]
        found = True
    else:
        paths = combined_paths
        current_distance += 1
        
        
        if current_distance > 100:  
            break

if found_paths:
    for path in found_paths:
        print(','.join(path))
else:
    print("No path exists between", start, "and", target)