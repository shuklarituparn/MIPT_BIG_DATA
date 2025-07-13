## Task 3. Hadoop MapReduce
 You're to solve 2 problems in this homework, using either [Hadoop Java API](http://hadoop.apache.org/docs/r2.6.1/api) or Hadoop Streaming. Try to optimize your algorithm:
 - use as few jobs as possible
 - use more than 1 reducer (a single reducer is only allowed in final job for sorting the output)

1. [1 point]. Subtask 110 is mandatory for everyone.
2. [2 points]. One of the remaining must be selected according to your username on **gitlab.atp-fivt.org**. Pass the username as an argument to [this](/variant_counters/get_mr_variant.py) script.

Example

```
$ ./get_mr_variant.py velkerr
116
```

Commit your code to the corresponding repository on gitlab.atp-fivt.org. Push the first subtask to branch mapreducetask1 and the second one to branch mapreducetask2.

<h3> Deadlines </h3>

| Soft deadline | Hard deadline |
|  ------  |------|
| March 23, 23:59| April 6, 23:59|

You can fix issues within a month after the code review.

* 50% of the score is lost after the soft deadline
* 75% after the hard deadline

### How to submit your code

For each hometask, you've got a repo with branch `master` and a branch for each subtask. To hand in completed work, you must do the following in each branch:
1. Put a file named `.gitlab-ci.yml` with the text below into the root directory:
```yml
job1:
  except:
    - master
  script:
    - date
    - if [ `grep -v '#' ${CI_COMMIT_REF_NAME}/run.sh | wc -l` -gt 0 ]; then (cd ~/code; ./gitlab_ci_runner.py); else exit 1; fi
```
(you can just download it [here](http://gitlab.atp-fivt.org/root/demos/blob/ci_files/.gitlab-ci.yml))

2. Create a directory with the same name as the branch. E.g. directory `mapreducetask1` in branch `mapreducetask1`.

3. This directory should contain a file named `run.sh`, which will serve as the entry point into your program and will be executed by the testing system. `run.sh` can either contain the entire solution or call other files.

> Even if you develop on python, there should be a `run.sh` containing, for instance, just a call to the necessary python script.
>
> 	#!/usr/bin/env bash
> 	
> 	python my_python_script.py $*
>
> Here $* forwards parameters with which `run.sh` was called to `my_python_script.py`.

4. If some command produces output in stdin, you can mute it using redirection to 1>/dev/null, for example: `hadoop fs -rm -r -skipTrash $OUT_DIR* 1>/dev/null`

### Input data

wikipedia:
* location on cluster: whole dataset --- `/data/wiki/en_articles`, sample --- `/data/wiki/en_articles_part`
* format: text, each line looks as follows:
          `article_id <tab> article_text`

list of identifiers:
* location on cluster: whole dataset --- `/data/ids`, sample --- `/data/ids_part`
* format: text, one id per line

### Subtasks

1. (110) Perform a random shuffle of the id list. Then write a random number (1 to 5) of comma-separated ids in each line.
* input: id list
* output format: id1, id2, ...
* print: first 50 lines

For shuffling you could e.g. append a random number to each record, sort the entire list by this new component and then discard it.

Output example:
```
1cf54b530128257d72,4cdf3efa01036a9a48,8c3e7fb30261aaf9cf
4cfe6230016553c3ed,76e1b8690176f801bb,e7409c39013c9db7b4,a5f1519c02b22550e6
83a119ef02346d0879
```

_A real industry case actually. A new database server had to be stress-tested. For that, one needed to generate some realistic-looking queries. The possible set of keys for this DB was known, and also that one query contained up to five keys._


3.(112) (“stop words”) Find words met in the greatest number of documents. Filter out punctuation marks. Sort by number of documents (break ties lexicographically).
* input: wikipedia
* output format: `word <tab> num of documents`
* print: top 10 words

Output example:
```
and	4053
in	4048
to	4045
```



### Hints
1. In all subtasks involving wikipedia articles (111-116), convert the words to lower case before computations.
2. Use the regular expression `[^A-Za-z\\s]` to filter out garbage.
3. By spaces we understand the standard whitespace characters `[ \t\n\r\f\v]` and their contiguous sequences (see the python docs for `string.split()`).
4. The testing system reads the answer from `stdout`, so it shouldn't contain any irrelevant information. `stderr` should be left intact.
