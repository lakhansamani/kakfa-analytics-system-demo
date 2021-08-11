# kakfa-analytics-system-demo

Codebase for the the following article:
[Create page view analytics system using Kafka, Go, Postgres & GraphQL in 5 steps](https://dev.to/lakhansamani/create-page-view-analytics-system-using-kafka-go-postgres-graphql-in-5-steps-1hc1)

## Getting started

🚨 **Pre-requisites**

- Make sure you have [`Postgres DB`](https://www.postgresql.org/) running on your machine.

**Starting Kafka Server, Producer & Consumer**

- Clone Repo `git clone https://github.com/lakhansamani/kakfa-analytics-system-demo.git analytics-demo`
- Change dir `cd analytics-demo`
- Start Kafka Server `docker-compose up`
- Create another terminal session and start producer `cd producer && go build && ./producer`
- Create another terminal session and start consumer `cd consumer && go build && ./consumer`
