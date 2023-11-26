# Assignment 3

## Q1 Spark Streaming with Real Time Data and Kafka

- Start Kafka
```bash
cd kafka
docker-compose up -d
```

- You can monitor the kafka cluster at http://localhost:9021/clusters

- Start ELK Stack
```bash
cd elk
docker-compose up -d
```

- You can login to kibana at http://localhost:5601/app/home using username=elastic and password=changeme

Start pyspark
```bash
pyspark
```
- Add Reddit API client ID and client secret to Reddit.ipynb

- Run reddit.ipynb. Wait for data to be streamed to topic1

- Run Spark.ipynb. Data will be streamed to topic2

- Logstash is configured to read from topic2 and write to elasticsearch

- Add index pattern in kibana for logstash-*

- Visualization chart screenshots are in q1/screenshots folder

## Q2 Analyzing Social Networks using GraphX/GraphFrame

Following command to run social_network.py with input and output file

```bash
python social_network.py musae_facebook_edges.csv output.csv
```

If facing any issues with graphframes package, follow the below steps
- Add graphframes-0.8.3-spark3.5-s_2.12.jar to spark jars
- Run pyspark with graphframes package to download dependencies

```bash
pyspark --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12
```

- Run SocialNetwork.ipynb
