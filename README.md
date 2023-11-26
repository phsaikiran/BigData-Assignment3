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

- Start Reddit.ipynb
Wait for topic1 streaming

- Start Spark.ipynb
Data will be streamed to topic2

- Logstash is configured to read from topic2 and write to elasticsearch
- Kibana visualization is available at http://localhost:5601
