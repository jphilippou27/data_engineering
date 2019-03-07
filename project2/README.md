# A summary
I spun up a cluster with kafka, zookeeper, spark, presto, cloudera/hadoop and the mids container. I added a fancier flask api to collect user info (web session etc.). Then I logged the events to kafka with no end point so the terminal window stayed open and active throughout. Next I used spark to pull events from kafka and filter and flatten and transform them before writing them to  HDFS storage.  Finally I used presto to query the events stored on parquet files. I attempted to get the streaming code working but it failed due to memory shortages that
I couldn't clear up with limited funds/being under a deadline.

 

   ## setting up (make directory, move to directory, & copying of yml)
    
    ```
    git pull in ~/w205/course-content
    mkdir ~/w205/full-stack2/
    cd ~/w205/full-stack2
    cp ~/w205/course-content/13-Understanding-Data/docker-compose.yml .
    docker-compose pull
    cp ~/w205/spark-from-files/readme.md . ## bring over file from last ps as a starting point
    cp ~/w205/course-content/13-Understanding-Data/*.py .
    docker kill $(docker ps -q)
    docker-compose ps
    ```
    
 Output should be an empty docker list, otherwise ports get fudged
    
   ## here is a copy of the envir. details
   ## The `docker-compose.yml` 

        ```
        ---
        version: '2'
        services:
          zookeeper:
            image: confluentinc/cp-zookeeper:latest
            environment:
              ZOOKEEPER_CLIENT_PORT: 32181
              ZOOKEEPER_TICK_TIME: 2000
            expose:
              - "2181"
              - "2888"
              - "32181"
              - "3888"
            extra_hosts:
              - "moby:127.0.0.1"

          kafka:
            image: confluentinc/cp-kafka:latest
            depends_on:
              - zookeeper
            environment:
              KAFKA_BROKER_ID: 1
              KAFKA_ZOOKEEPER_CONNECT: zookeeper:32181
              KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092
              KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
            expose:
              - "9092"
              - "29092"
            extra_hosts:
              - "moby:127.0.0.1"

          cloudera:
            image: midsw205/hadoop:0.0.2
            hostname: cloudera
            expose:
              - "8020" # nn
              - "8888" # hue
              - "9083" # hive thrift
              - "10000" # hive jdbc
              - "50070" # nn http
            ports:
              - "8888:8888"
            extra_hosts:
              - "moby:127.0.0.1"

          spark:
            image: midsw205/spark-python:0.0.6
            stdin_open: true
            tty: true
            volumes:
              - ~/w205:/w205
            expose:
              - "8888"
            #ports:
            #  - "8888:8888"
            depends_on:
              - cloudera
            environment:
              HADOOP_NAMENODE: cloudera
              HIVE_THRIFTSERVER: cloudera:9083
            extra_hosts:
              - "moby:127.0.0.1"
            command: bash

          presto:
            image: midsw205/presto:0.0.1
            hostname: presto
            volumes:
              - ~/w205:/w205
            expose:
              - "8080"
            environment:
              HIVE_THRIFTSERVER: cloudera:9083
            extra_hosts:
              - "moby:127.0.0.1"

          mids:
            image: midsw205/base:0.1.9
            stdin_open: true
            tty: true
            volumes:
              - ~/w205:/w205
            expose:
              - "5000"
            ports:
              - "5000:5000"
            extra_hosts:
              - "moby:127.0.0.1"
        ```

    
  # running cluster
  ``` 
  docker-compose up -d 
  ````


  # creating web application to collect data
    
    first pass at api (simple) - remembered to 'add guild', same app as before. Takes user events and creates temporary placeholder
  
    ```python
    #!/usr/bin/env python
    import json
    from kafka import KafkaProducer
    from flask import Flask, request

    app = Flask(__name__)
    producer = KafkaProducer(bootstrap_servers='kafka:29092')


    def log_to_kafka(topic, event):
        event.update(request.headers)
        producer.send(topic, json.dumps(event).encode())


    @app.route("/")
    def default_response():
        default_event = {'event_type': 'default'}
        log_to_kafka('events', default_event)
        return "This is the default response!\n"


    @app.route("/purchase_a_sword")
    def purchase_a_sword():
        purchase_sword_event = {'event_type': 'purchase_sword'}
        log_to_kafka('events', purchase_sword_event)
        return "Sword Purchased!\n"

    @app.route("/join_a_guild")
    def join_a_guild():
        join_guild_event = {'event_type': 'join_guild'}
        log_to_kafka('events', join_guild_event)
        return "Guild joined!\n"
    ```
    
    
    
  ## run the flask API you just made; creating route in pyflask
  ``` 
  docker-compose exec mids \
  env FLASK_APP=/w205/spark-from-files/game_api.py \
  flask run --host 0.0.0.0
  ```
  
  Output should read: 
  ```
   Serving Flask app "game_api"
   * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
  ```

  
  ## Run Kafkacat 
  ### On a new terminal (that can be left running to catch streaming of events). Notice you drop the e, because there is no end since we want it to be continuous
  ```
  docker-compose exec mids kafkacat -C -b kafka:29092 -t events -o beginning
  ```
  
  
  ## make some calls to your API
  ### aka use curl to make purchases on another terminal; rinse and repeat (make  a few more events so its interesting)
  ```
  docker-compose exec mids \
  ab \
    -n 10 \
    -H "Host: user1.comcast.com" \
    http://localhost:5000/
  ```
 
  ```
  docker-compose exec mids \
  ab \
    -n 10 \
    -H "Host: user1.comcast.com" \
    http://localhost:5000/purchase_a_sword
  ```
  
  ```
  docker-compose exec mids \
  ab \
    -n 10 \
    -H "Host: user2.att.com" \
    http://localhost:5000/
  ```
  
  ```
  docker-compose exec mids \
  ab \
    -n 10 \
    -H "Host: user2.att.com" \
    http://localhost:5000/purchase_a_sword
  ```
   
  ```
  docker-compose exec mids \
  ab \
    -n 10 \
    -H "Host: user2.att.com" \
    http://localhost:5000/join_a_guild
  ```
  
  ```
  docker-compose exec mids \
  ab \
    -n 10 \
    -H "Host: user2.att.com" \
    http://localhost:5000/join_a_guild
  ```
  
  
  should output something like this (after each event run):
  
        ```
        science@w205s1-martin-12:~/w205/full-stack2$ docker-compose exec mids \
        >   ab \
        >     -n 10 \
        >     -H "Host: user1.comcast.com" \
        >     http://localhost:5000/purchase_a_sword
        This is ApacheBench, Version 2.3 <$Revision: 1706008 $>
        Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
        Licensed to The Apache Software Foundation, http://www.apache.org/

        Benchmarking localhost (be patient).....done


        Server Software:        Werkzeug/0.14.1
        Server Hostname:        localhost
        Server Port:            5000

        Document Path:          /purchase_a_sword
        Document Length:        17 bytes

        Concurrency Level:      1
        Time taken for tests:   0.078 seconds
        Complete requests:      10
        Failed requests:        0
        Total transferred:      1720 bytes
        HTML transferred:       170 bytes
        Requests per second:    128.18 [#/sec] (mean)
        Time per request:       7.802 [ms] (mean)
        Time per request:       7.802 [ms] (mean, across all concurrent requests)
        Transfer rate:          21.53 [Kbytes/sec] received

        Connection Times (ms)
                      min  mean[+/-sd] median   max
        Connect:        0    0   0.8      0       2
        Processing:     4    7   2.6      7      11
        Waiting:        2    5   2.2      6       9
        Total:          4    8   3.1      7      13

        Percentage of the requests served within a certain time (ms)
          50%      7
          66%      9
          75%     11
          80%     11
          90%     13
          95%     13
          98%     13
          99%     13
         100%     13 (longest request)
        ```


## use spark to write out events
### pull events from kafka into more permenant storage in hdfs; same as before
       ```
            #Capture our pyspark code in a file this time
            #!/usr/bin/env python
            """Extract events from kafka and write them to hdfs
            """
            import json
            from pyspark.sql import SparkSession, Row
            from pyspark.sql.functions import udf

            #create user defined function
            @udf('boolean')
            def is_purchase(event_as_json):
                event = json.loads(event_as_json)
                if event['event_type'] == 'purchase_sword':
                    return True
                return False


            def main():
                """main
                """
                spark = SparkSession \
                    .builder \
                    .appName("ExtractEventsJob") \
                    .getOrCreate()

                raw_events = spark \
                    .read \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:29092") \
                    .option("subscribe", "events") \
                    .option("startingOffsets", "earliest") \
                    .option("endingOffsets", "latest") \
                    .load()

                purchase_events = raw_events \
                    .select(raw_events.value.cast('string').alias('raw'),
                            raw_events.timestamp.cast('string')) \
                    .filter(is_purchase('raw'))

                extracted_purchase_events = purchase_events \
                    .rdd \
                    .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
                    .toDF()
                extracted_purchase_events.printSchema()
                extracted_purchase_events.show()

                extracted_purchase_events \
                    .write \
                    .mode('overwrite') \
                    .parquet('/tmp/purchases')


            if __name__ == "__main__":
                main()
    
            ````
   ### run code above
    
    ```
    docker-compose exec spark \
      spark-submit \
    /w205/full-stack2/filtered_writes.py
    ```
    
   Output: spark generates a whole bunch of noise & the table below confirms the spark code above filters to just purchases
     
    
    +------+-----------------+---------------+--------------+--------------------+
    |Accept|             Host|     User-Agent|    event_type|           timestamp|
    +------+-----------------+---------------+--------------+--------------------+
    |   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:46:...|
    |   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:46:...|
    |   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:46:...|
    |   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:46:...|
    |   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:46:...|
    |   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:46:...|
    |   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:46:...|
    |   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:46:...|
    |   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:46:...|
    |   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:46:...|
    |   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:49:...|
    |   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:49:...|
    |   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:49:...|
    |   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:49:...|
    |   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:49:...|
    |   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:49:...|
    |   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:49:...|
    |   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:49:...|
    |   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:49:...|
    |   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-08-12 21:49:...|
    +------+-----------------+---------------+--------------+--------------------+

   ## write out join guild
   ```
            #Capture our pyspark code in a file this time
            #!/usr/bin/env python
            """Extract events from kafka and write them to hdfs
            """
            import json
            from pyspark.sql import SparkSession, Row
            from pyspark.sql.functions import udf

            #create user defined function
            @udf('boolean')
            def is_join(event_as_json):
                event = json.loads(event_as_json)
                if event['event_type'] == 'join_guild':
                    return True
                return False


            def main():
                """main
                """
                spark = SparkSession \
                    .builder \
                    .appName("ExtractEventsJob") \
                    .getOrCreate()

                raw_events = spark \
                    .read \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:29092") \
                    .option("subscribe", "events") \
                    .option("startingOffsets", "earliest") \
                    .option("endingOffsets", "latest") \
                    .load()

                join_events = raw_events \
                    .select(raw_events.value.cast('string').alias('raw'),
                            raw_events.timestamp.cast('string')) \
                    .filter(is_join('raw'))

                extracted_join_events = join_events \
                    .rdd \
                    .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
                    .toDF()
                extracted_join_events.printSchema()
                extracted_join_events.show()

                extracted_join_events \
                    .write \
                    .mode('overwrite') \
                    .parquet('/tmp/joins')


            if __name__ == "__main__":
                main() 
 ```
     
     
 ## call code above       
    
    ```
    docker-compose exec spark \
      spark-submit \
    /w205/full-stack2/filtered_join_guild.py
    ```

Guild code above outputs: 
     
         Accept|         Host|     User-Agent|event_type|           timestamp|
        +------+-------------+---------------+----------+--------------------+
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        |   */*|user2.att.com|ApacheBench/2.3|join_guild|2018-08-13 02:04:...|
        +------+-------------+---------------+----------+--------------------+

     
   
   ## Look at results in hadoop

    ```
    docker-compose exec cloudera hadoop fs -ls /tmp/
    ```
    
   outputs: 
   
    ```
    Found 5 items
    drwxrwxrwt   - mapred mapred              0 2016-04-06 02:26 /tmp/hadoop-yarn
    drwx-wx-wx   - hive   supergroup          0 2018-08-13 01:58 /tmp/hive
    drwxr-xr-x   - root   supergroup          0 2018-08-13 02:10 /tmp/joins
    drwxrwxrwt   - mapred hadoop              0 2016-04-06 02:28 /tmp/logs
    drwxr-xr-x   - root   supergroup          0 2018-08-13 02:04 /tmp/purchases
    ```
    
   we can also checkout the events stored in parquet files (shown below):
   
   ```
    b-bd67-f49eb3f2e4ae/pyspark-e7a88240-577f-447a-aa19-dd20e3746d88
    science@w205s1-martin-12:~/w205/full-stack2$  docker-compose exec cloudera hadoop fs -ls /tmp/purchases/
    Found 2 items
    -rw-r--r--   1 root supergroup          0 2018-08-12 22:50 /tmp/purchases/_SUCCESS
    -rw-r--r--   1 root supergroup       1654 2018-08-12 22:50 /tmp/purchases/part-00000-d6d3c4c5-52e9-43c1-a329-5742f95a7f77-c000.snappy.parquet
   ```

    
   ## run spark 
    #### which will auto create the hive metastore and create an external table of purchase events stored in parquet files 
    
    ```
    #!/usr/bin/env python
        """Extract events from kafka and write them to hdfs
        """
        import json
        from pyspark.sql import SparkSession, Row
        from pyspark.sql.functions import udf


        @udf('boolean')
        def is_purchase(event_as_json):
            event = json.loads(event_as_json)
            if event['event_type'] == 'purchase_sword':
                return True
            return False


        def main():
            """main
            """
            spark = SparkSession \
                .builder \
                .appName("ExtractEventsJob") \
                .enableHiveSupport() \
                .getOrCreate()

            raw_events = spark \
                .read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("subscribe", "events") \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest") \
                .load()

            purchase_events = raw_events \
                .select(raw_events.value.cast('string').alias('raw'),
                        raw_events.timestamp.cast('string')) \
                .filter(is_purchase('raw'))

            extracted_purchase_events = purchase_events \
                .rdd \
                .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
                .toDF()
            extracted_purchase_events.printSchema()
            extracted_purchase_events.show()
            
            #Creates temporary table 
            extracted_purchase_events.registerTempTable("extracted_purchase_events")

            spark.sql("""
                create external table purchases
                stored as parquet
                location '/tmp/purchases'
                as
                select * from extracted_purchase_events
            """)


        if __name__ == "__main__":
            main()
    ```
    
    
   ## run spark code above
   
   ```
   docker-compose exec spark spark-submit /w205/full-stack2/write_hive_table.py
   ```
   
   ## run guild version
   
   ```
    docker-compose exec spark spark-submit /w205/full-stack2/write_hive_join_table.py
   ```
   
   outputs a whole bunch of junk and a table that looks exactly the same as the one above
    
  ## Check to see if it ran in hdfs
  
  ```
  docker-compose exec cloudera hadoop fs -ls /tmp/
  ```
  
  Outputs the following results, indicating a /tmp/purchases folder (Its a little weird the size is 0 though....)
  ```
    Found 5 items
    drwxrwxrwt   - mapred mapred              0 2016-04-06 02:26 /tmp/hadoop-yarn
    drwx-wx-wx   - hive   supergroup          0 2018-08-13 00:06 /tmp/hive
    drwxrwxrwt   - mapred hadoop              0 2016-04-06 02:28 /tmp/joins
    drwxrwxrwt   - mapred hadoop              0 2016-04-06 02:28 /tmp/logs
    drwxr-xr-x   - root   supergroup          0 2018-08-13 00:06 /tmp/purchases
   ```
   
   ## Running a few presto queries
   ### getting the data from hdfs
   
   ```
   docker-compose exec presto presto --server presto:8080 --catalog hive --schema default
   ```
   
   should output: 'presto:default>'
   
   ## Type command to look at the tables:
   
   ```
   show tables;
   ```
   
   Output:
   ```
   8080 --catalog hive --schema default
    presto:default> show tables;
       Table
    -----------
     joins
     purchases
    (2 rows)
   ```
    
  ## Describe tables (cmd & output)
   ### also a little weird that I've got 0 rows

    ```
    presto:default> describe purchases ;
       Column   |  Type   | Comment
    ------------+---------+---------
     accept     | varchar |
     host       | varchar |
     user-agent | varchar |
     event_type | varchar |
     timestamp  | varchar |
    (5 rows)

    Query 20180813_001652_00003_w6tf5, FINISHED, 1 node
    Splits: 2 total, 0 done (0.00%)
    0:01 [0 rows, 0B] [0 rows/s, 0B/s]
    ```
   #### table 2
    ```
    presto:default> describe joins;
    Column   |  Type   | Comment
    ------------+---------+---------
     accept     | varchar |
     host       | varchar |
     user-agent | varchar |
     event_type | varchar |
     timestamp  | varchar |
    (5 rows)

    Query 20180813_023001_00004_4r7bb, FINISHED, 1 node
    Splits: 2 total, 0 done (0.00%)
    0:01 [0 rows, 0B] [0 rows/s, 0B/s]
    ```
    
    
   ## Query purchases table
   ### This looks like it ran alright, despite there not being rows...
    
    ```
    presto:default> select * from purchases;
    ```
    
   Output
   
     ```
      accept |       host        |   user-agent    |   event_type   |        timestamp
     --------+-------------------+-----------------+----------------+-------------------------
      */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:46:58.728
      */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:46:58.742
      */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:46:58.751
      */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:46:58.759
      */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:46:58.762
      */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:46:58.769
      */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:46:58.776
      */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:46:58.783
      */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:46:58.792
      */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:46:58.803
      */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:49:01.958
      */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:49:01.973
      */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:49:01.981
      */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:49:01.989
      */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:49:01.996
      */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:49:02.007
      */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:49:02.012
      */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:49:02.017
      */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:49:02.019
      */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-08-12 21:49:02.026
     (20 rows)
     Query 20180813_001727_00004_w6tf5, FINISHED, 1 node
     Splits: 2 total, 0 done (0.00%)
     0:02 [0 rows, 0B] [0 rows/s, 0B/s]
     ```
    
   
   ## Streaming (?!); almost, not just yet
   
   ```
       #!/usr/bin/env python
    """Extract events from kafka and write them to hdfs
    """
    import json
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import udf, from_json
    from pyspark.sql.types import StructType, StructField, StringType

    # not infering the schema any more
    def purchase_sword_event_schema():
        """
        root
        |-- Accept: string (nullable = true)
        |-- Host: string (nullable = true)
        |-- User-Agent: string (nullable = true)
        |-- event_type: string (nullable = true)
        |-- timestamp: string (nullable = true)
        """
        return StructType([
            StructField("Accept", StringType(), True),
            StructField("Host", StringType(), True),
            StructField("User-Agent", StringType(), True),
            StructField("event_type", StringType(), True),
        ])


    @udf('boolean')
    def is_sword_purchase(event_as_json):
        """udf for filtering events
        """
        event = json.loads(event_as_json)
        if event['event_type'] == 'purchase_sword':
            return True
        return False


    def main():
        """main
        """
        spark = SparkSession \
            .builder \
            .appName("ExtractEventsJob") \
            .getOrCreate()

        raw_events = spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "events") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()

        sword_purchases = raw_events \
            .filter(is_sword_purchase(raw_events.value.cast('string'))) \
            .select(raw_events.value.cast('string').alias('raw_event'),
                    raw_events.timestamp.cast('string'),
                    from_json(raw_events.value.cast('string'),
                              purchase_sword_event_schema()).alias('json')) \
            .select('raw_event', 'timestamp', 'json.*')

        sword_purchases.printSchema()
        sword_purchases.show(100)


    if __name__ == "__main__":
        main()
   ```
    
  ## Run spark job
  
  ```
  docker-compose exec spark spark-submit /w205/full-stack2/filter_swords_batch.py
  ```
  
  Output is the same ln 455 (the first spark job)
  
  ## Actual streaming!
  
  ```
  #!/usr/bin/env python
        """Extract events from kafka and write them to hdfs
        """
        import json
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import udf, from_json
        from pyspark.sql.types import StructType, StructField, StringType


        def purchase_sword_event_schema():
            """
            root
            |-- Accept: string (nullable = true)
            |-- Host: string (nullable = true)
            |-- User-Agent: string (nullable = true)
            |-- event_type: string (nullable = true)
            |-- timestamp: string (nullable = true)
            """
            return StructType([
                StructField("Accept", StringType(), True),
                StructField("Host", StringType(), True),
                StructField("User-Agent", StringType(), True),
                StructField("event_type", StringType(), True),
            ])


        @udf('boolean')
        def is_sword_purchase(event_as_json):
            """udf for filtering events
            """
            event = json.loads(event_as_json)
            if event['event_type'] == 'purchase_sword':
                return True
            return False


        def main():
            """main
            """
            spark = SparkSession \
                .builder \
                .appName("ExtractEventsJob") \
                .getOrCreate()
            
            
            raw_events = spark \
                .readStream \ ##this is where the streaming magic happens
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("subscribe", "events") \
                .load()

            sword_purchases = raw_events \
                .filter(is_sword_purchase(raw_events.value.cast('string'))) \
                .select(raw_events.value.cast('string').alias('raw_event'),
                        raw_events.timestamp.cast('string'),
                        from_json(raw_events.value.cast('string'),
                                  purchase_sword_event_schema()).alias('json')) \
                .select('raw_event', 'timestamp', 'json.*')

            query = sword_purchases \
                .writeStream \
                .format("console") \
                .start()

            query.awaitTermination()  ## also new; streaming adjustment


        if __name__ == "__main__":
            main()
  ```
      
  ## Run streaming code above
      
      ```
      docker-compose exec spark spark-submit /w205/full-stack2/filter_swords_stream.py
      ```
   Empty output:
   
    +---------+---------+------+----+----------+----------+
    |raw_event|timestamp|Accept|Host|User-Agent|event_type|
    +---------+---------+------+----+----------+----------+
    +---------+---------+------+----+----------+----------+
    
    
   Try 2:
   
   
    +--------------------+--------------------+------+-----------------+---------------+--------------+
    |           raw_event|           timestamp|Accept|             Host|     User-Agent|    event_type|
    +--------------------+--------------------+------+-----------------+---------------+--------------+
    |{"Host": "user1.c...|2018-08-13 02:47:...|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|
    |{"Host": "user1.c...|2018-08-13 02:47:...|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|
    |{"Host": "user1.c...|2018-08-13 02:47:...|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|
    |{"Host": "user1.c...|2018-08-13 02:47:...|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|
    |{"Host": "user1.c...|2018-08-13 02:47:...|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|
    |{"Host": "user1.c...|2018-08-13 02:47:...|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|
    |{"Host": "user1.c...|2018-08-13 02:47:...|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|
    |{"Host": "user1.c...|2018-08-13 02:47:...|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|
    +--------------------+--------------------+------+-----------------+---------------+--------------+

    
    

   ## Generate some more events
     
      ```
      docker-compose exec mids \
      ab \
        -n 10 \
        -H "Host: user1.comcast.com" \
        http://localhost:5000/
      ```


      ```
      docker-compose exec mids \
      ab \
        -n 10 \
        -H "Host: user1.comcast.com" \
        http://localhost:5000/purchase_a_sword
      ```

      ```
      docker-compose exec mids \
      ab \
        -n 10 \
        -H "Host: user2.att.com" \
        http://localhost:5000/
      ```

      ```
      docker-compose exec mids \
      ab \
        -n 10 \
        -H "Host: user2.att.com" \
        http://localhost:5000/purchase_a_sword
      ```

      ```
      docker-compose exec mids \
      ab \
        -n 10 \
        -H "Host: user2.att.com" \
        http://localhost:5000/join_a_guild
      ```

      ```
      docker-compose exec mids \
      ab \
        -n 10 \
        -H "Host: user2.att.com" \
        http://localhost:5000/join_a_guild
      ```
  
  ## Pull from the stream
  
      ```
          Write from a stream
        #!/usr/bin/env python
        """Extract events from kafka and write them to hdfs
        """
        import json
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import udf, from_json
        from pyspark.sql.types import StructType, StructField, StringType


        def purchase_sword_event_schema():
            """
            root
            |-- Accept: string (nullable = true)
            |-- Host: string (nullable = true)
            |-- User-Agent: string (nullable = true)
            |-- event_type: string (nullable = true)
            |-- timestamp: string (nullable = true)
            """
            return StructType([
                StructField("Accept", StringType(), True),
                StructField("Host", StringType(), True),
                StructField("User-Agent", StringType(), True),
                StructField("event_type", StringType(), True),
            ])


        @udf('boolean')
        def is_sword_purchase(event_as_json):
            """udf for filtering events
            """
            event = json.loads(event_as_json)
            if event['event_type'] == 'purchase_sword':
                return True
            return False


        def main():
            """main
            """
            spark = SparkSession \
                .builder \
                .appName("ExtractEventsJob") \
                .getOrCreate()

            raw_events = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("subscribe", "events") \
                .load()

            sword_purchases = raw_events \
                .filter(is_sword_purchase(raw_events.value.cast('string'))) \
                .select(raw_events.value.cast('string').alias('raw_event'),
                        raw_events.timestamp.cast('string'),
                        from_json(raw_events.value.cast('string'),
                                  purchase_sword_event_schema()).alias('json')) \
                .select('raw_event', 'timestamp', 'json.*')

            sink = sword_purchases \
                .writeStream \
                .format("parquet") \
                .option("checkpointLocation", "/tmp/checkpoints_for_sword_purchases") \
                .option("path", "/tmp/sword_purchases") \
                .trigger(processingTime="10 seconds") \
                .start()

            sink.awaitTermination()


        if __name__ == "__main__":
            main()
    ```
    
## Run it
    ```
    docker-compose exec spark spark-submit /w205/full-stack2/write_swords_stream.py
    ```
  
  Sadly this failed with the following output:
   
   ```
    Traceback (most recent call last):
      File "/w205/full-stack2/write_swords_stream.py", line 72, in <module>
        main()
      File "/w205/full-stack2/write_swords_stream.py", line 68, in main
        sink.awaitTermination()
      File "/spark-2.2.0-bin-hadoop2.6/python/lib/pyspark.zip/pyspark/sql/streaming.py", line 106, in awaitTermination
      File "/spark-2.2.0-bin-hadoop2.6/python/lib/py4j-0.10.4-src.zip/py4j/java_gateway.py", line 1133, in __call__
      File "/spark-2.2.0-bin-hadoop2.6/python/lib/pyspark.zip/pyspark/sql/utils.py", line 75, in deco
    pyspark.sql.utils.StreamingQueryException: 'Failed to construct kafka consumer\n=== Streaming Query ===\nIdentifier: [id = 521fce47-266c-473c-835f-2aec43a60c3c, runId = f65d488f-d68b-422d-b1e0-c34c2b8a9379]\nCurrent Committed Offsets: {}\nCurrent Available Offsets: {}\n\nCurrent State: INITIALIZING\nThread State: RUNNABLE'
    18/08/13 01:25:01 INFO SparkContext: Invoking stop() from shutdown hook
    18/08/13 01:25:01 INFO SparkUI: Stopped Spark web UI at http://172.22.0.7:4041
    18/08/13 01:25:01 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
    18/08/13 01:25:01 INFO MemoryStore: MemoryStore cleared
    18/08/13 01:25:01 INFO BlockManager: BlockManager stopped
    18/08/13 01:25:01 INFO BlockManagerMaster: BlockManagerMaster stopped
    18/08/13 01:25:01 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
    18/08/13 01:25:01 INFO SparkContext: Successfully stopped SparkContext
    18/08/13 01:25:01 INFO ShutdownHookManager: Shutdown hook called
    18/08/13 01:25:01 INFO ShutdownHookManager: Deleting directory /tmp
   ```
    
  So I tried it again and seemingly got further/better results on round II overall, but still got the soul crushing message:
  
    ```
    #
    # There is insufficient memory for the Java Runtime Environment to continue.
    # Native memory allocation (mmap) failed to map 95944704 bytes for committing reserved memory.
    # An error report file with more information is saved as:
    # /spark-2.2.0-bin-hadoop2.6/hs_err_pid1230.log
    ```
  
