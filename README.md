# Assignment 3

## Q1 Spark Streaming with Real Time Data and Kafka

- Start Kafka
```bash
cd kafka
docker-compose up -d
```

- Start ELK Stack
```bash
cd elk
docker-compose up -d
```

Start pyspark
```bash
pyspark
```

- Run Reddit.ipynb. Wait for topic1 streaming

- Run Spark.ipynb. Data will be streamed to topic2

- Logstash is configured to read from topic2 and write to elasticsearch

- Kibana visualization is available at http://localhost:5601

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
