# Task 4. Hive

<h3>Deadlines</h3>

| Soft deadline | Hard deadline |
|  ------  |------|
| April 23, 23:59 | April 30, 23:59|

You can fix issues within a month after the code review.

* 50% of the score is lost after the soft deadline
* 75% after the hard deadline

You're to solve six problems in this homework.
1. Task 1 is mandatory for everyone.
2. For tasks 2, 3 and 4, use [this script](variant_counters/get_hive_variant.py) to determine your variant.

### Apache HUE

Apache HUE running on our cluster may be used. To access it, forward port 8888 when logging in: `ssh <USER>@mipt-client -L 8888:mipt-node03.atp-fivt.org:8888`. The password coincides with the username, so please change it on first visit.

## Source data

### I. User logs

The dataset is located at `/data/user_logs/*_M`. There're three parts, each in its own subdirectory. Each part has a different set of columns, separated with tabs ('\t') or spaces.

#### А. Logs of users' queries to news messages (user_logs).
1. IP address which sent the query;
2. Query timestamp (TIMESTAMP or INT);
3. Http request body (STRING);
4. Size of the response (SMALLINT);
5. Http status code (SMALLINT);
6. Info about the client app, including web browser description (STRING).

**Important:** info about browser is located at the beginning of the 6'th field (characters from 0'th index to the first whitespace character), the rest of the string doesn't determine the user's browser. IP and timestamp are separated with three tabs.

#### B. user_data
1. IP address (STRING);
2. Web browser (STRING);
3. Gender (STRING); // male, female
4. Age (TINYINT).

#### С. Info about the users' physical locations (ip_data).
1. IP address (STRING);
2. Region (STRING).

### II. Subnets

Located at `/data/subnets`. Choose one of three datasets in the directory (`/data/subnets/variant[1-3]`) according to your variant. They all have identical formats.

1. IP address (STRING);
2. Subnet mask (STRING).

The meaning of the first field differs between datasets.
* 1. Network address.
* 2. Address of an arbitary host in the network.
* 3. Broadcast address.

Samples can be found at `/data/subnets_S`. While full datasets contain 5000 records, samples only have 20.

## Tasks

**Task 1 (411)**. Create external tables from the source data. There should be 4 of them: user logs, user data, ip data and subnets. From the first one (user logs), make another with the same data, but partitioned by dates. All the queries in the following tasks must use this new partitioned table, not the original one. Other three tables may be left as they are.

Serialization and deserialization must be performed using regular expressions (see `org.apache.hadoop.hive.contrib.serde2.RegexSerDe`, `org.apache.hadoop.hive.serde2.RegexSerDe`).

You can check whether the tables have been created correctly with simple queries like `SELECT * FROM <table> LIMIT 10`. Add them to your script.

**Additional requirements:**
1. The name of your database should coincide with your username on **GitLab.atp-fivt.org** (for example, **ivchenkoon**).
2. Don't push DDL to GitLab; the databases have already been created for you, and the testing system has no permissions to re-create them.
3. The tables should be named like this:
    * Logs - partitioned table with logs.
    * Users - user data.
    * IPRegions - IP data.

*Example output:*
```
33.49.147.163	http://lenta.ru/4303000	1189	451	Chrome/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0)n	20140101
75.208.40.166	http://newsru.com/3330815	60	306	Safari/5.0 (Windows; U; MSIE 9.0; Windows NT 8.1; Trident/5.0; .NET4.0E; en-AU)n	20140101
```

**Task 2.1. (421)**.
Count visits on each day and sort the results in descending order.

*Example output:*
```
20140308	96
20140409	96
20140318	96
```

**Task 3.1. (441)**. Count visits by males and females for each region.

*Example output:*
```
Tver	66968157	29097223
Voronezh	60445347	26333509
```


Hint for task 3: use the conditional function [IF](https://www.folkstalk.com/2011/11/conditional-functions-in-hive.html).

**Task 5.1. (471)**.
Imagine that all news websites have moved to the DNS zone `.com`. You've been requested to update the logs database so that the logs would point to new domain names. For instance, http://news.rambler.ru/8744806 should now be replaced with http://news.rambler.com/8744806. Use streaming in Hive SQL queries (hint: use `awk` and `sed` utilities). Print top 10 records without sorting.

*Example output:*
```
49.203.96.67	20140102	http://lenta.com/2296722	716	499	Safari/5.0
33.49.147.163	20140102	http://news.yandex.com/5605690	850	300	Safari/5.0
```

Literature
1. Tom White. Hadoop: The Definitive Guide, 3rd edition. O’Reilly, 2012, chapter 12.
2. Edward Capriolo, Dean Wampler, Jason Rutherglen. Programming Hive. O’Reilly, 2012.
3. [UDTF examples](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-Built-inTable-GeneratingFunctions%28UDTF%29) from Hive wiki.

Here are the points you earn for solving each task.

|Task | Branch name | Points
|:--|:--|:--|
|1|hivetask1|0.4|
|2|hivetask2|0.2|
|3|hivetask3|0.3|
|5|hivetask5|0.3|

**Notes**.
1. In some tasks, you're asked to print top $`n`$ records. This just means using `LIMIT N`, sorting the results is unnecessary. Sorting is only required in 2.x.
2. Please submit the 1st task before all others, since they are tested on the tables that are created in task 1.
3. If your personal Hive database is corrupted, please contact one of the instructors.

**Concerning HUE**
1. HUE has got its own users, so if you use HUE, create the database there.
2. You need a separate database to work in terminal (e.g. to test UDFs).
3. There's an additional database for testing purposes, whose name conincides with git username. Don't touch it until you've pushed the code to GitLab.
