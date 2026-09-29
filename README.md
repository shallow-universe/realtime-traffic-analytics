# 🚦 Real-Time Traffic Analytics

An end-to-end **real-time traffic data engineering pipeline** built using **Apache Kafka, PySpark Structured Streaming, Delta Lake, Hive Metastore, Spark SQL, and Power BI**.

The project demonstrates how continuously generated traffic data can be ingested, processed, cleaned, transformed, stored using a **Medallion Architecture**, and exposed for analytics through a BI dashboard.

---

## 🏗️ Architecture

```text
                 ┌─────────────────────┐
                 │   Traffic Producer  │
                 │   Python + Faker    │
                 └──────────┬──────────┘
                            │
                            ▼
                    ┌───────────────┐
                    │ Apache Kafka  │
                    │ traffic-topic │
                    └───────┬───────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │   Bronze Layer    │
                  │ Raw Streaming Data│
                  └─────────┬─────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │   Silver Layer    │
                  │ Clean & Validated │
                  │      Data         │
                  └─────────┬─────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │    Gold Layer     │
                  │ Analytics-Ready   │
                  │    Data Model     │
                  └─────────┬─────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   Delta Lake +      │
                 │   Hive Metastore    │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │  Spark Thrift       │
                 │     Server          │
                 └──────────┬──────────┘
                            │
                            ▼
                    ┌───────────────┐
                    │   Power BI    │
                    │   Dashboard   │
                    └───────────────┘
```

---

## ✨ Features

* ⚡ Real-time data ingestion using **Apache Kafka**
* 🔄 Stream processing using **PySpark Structured Streaming**
* 🥉🥈🥇 **3-layer Medallion Architecture**
* 🧹 Data cleaning and validation
* ♻️ Duplicate detection and removal
* 🔧 Feature engineering for traffic analytics
* 🗄️ Delta Lake-based storage
* 🐝 Hive Metastore integration
* ⭐ Star-schema analytics model
* 📊 Power BI dashboard integration
* 🐳 Docker-based development environment
* 🛠️ Spark SQL access through Spark Thrift Server

---

## 🧰 Technology Stack

| Component           | Technology                   |
| ------------------- | ---------------------------- |
| Data Generation     | Python, Faker                |
| Message Broker      | Apache Kafka                 |
| Stream Processing   | PySpark Structured Streaming |
| Storage             | Delta Lake                   |
| Metadata Management | Hive Metastore               |
| Query Engine        | Apache Spark SQL             |
| SQL Access          | Spark Thrift Server          |
| Visualization       | Power BI                     |
| Containerization    | Docker / Docker Compose      |
| Development         | VS Code                      |

---

## 🔄 Data Pipeline

### 1. Data Generation

A Python producer continuously generates simulated traffic events containing information such as:

* Vehicle ID
* Road ID
* City zone
* Vehicle speed
* Congestion level
* Event timestamp
* Weather
* Other traffic attributes

The generated events are published to the Kafka topic:

```text
traffic-topic
```

### 2. Bronze Layer

The Bronze layer stores the incoming streaming data in its raw form.

```text
Kafka → Bronze
```

This layer preserves the original event data for downstream processing.

### 3. Silver Layer

The Silver layer transforms raw records into reliable datasets through:

* Data type conversion
* Null/missing-value handling
* Data validation
* Duplicate removal
* Data cleaning
* Timestamp processing

```text
Bronze → Silver
```

### 4. Gold Layer

The Gold layer contains analytics-ready datasets organized using a **star schema**.

```text
                 ┌─────────────┐
                 │  dim_zone   │
                 └──────┬──────┘
                        │
                        │
┌─────────────┐   ┌─────▼──────┐ 
│  dim_road   │──▶│fact_traffic│
└─────────────┘   └────────────┘
```

### Gold Tables

#### `fact_traffic`

Contains measurable traffic events and derived attributes.

#### `dim_zone`

Contains city-zone information such as:

* Zone type
* Traffic risk

#### `dim_road`

Contains road-level information such as:

* Road type
* Speed limit

---

## 🗃️ BI Layer

The Gold tables are exposed through BI views:

```text
bi_fact_traffic
bi_dim_zone
bi_dim_road
```

Spark Thrift Server provides SQL access to these datasets, which are then consumed by **Power BI** for visualization and analysis.

The dashboard can be used to analyze:

* Traffic volume
* Congestion levels
* Average speed
* Traffic by zone
* Traffic by road
* Peak-hour patterns
* Weather vs. traffic conditions

---

## 🐳 Running the Project

### Prerequisites

Install:

* Docker Desktop
* Git
* Power BI Desktop
* Python 3.x

Clone the repository:

```bash
git clone https://github.com/shallow-universe/realtime-traffic-analytics.git

cd realtime-traffic-analytics
```

Start the infrastructure:

```bash
docker compose up -d
```

Check running containers:

```bash
docker ps
```

---

## 📡 Create Kafka Topic

Create the traffic topic:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic traffic-topic \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1
```

---

## 🚀 Run the Pipeline

- Start streaming data to Kafka:

```bash
python producer/traffic_dirty_producer.py
```

- Submit the Bronze, Silver, and Gold Spark applications:

```bash
docker exec -it spark-worker \
  /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_bronze.py
```

```bash
docker exec -it spark-worker \
  /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_silver.py
```

```bash
docker exec -it spark-worker \
  /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_gold.py
```

---

## 🔎 Data Model

The project uses a **3-table star schema**:

```text
                  dim_zone
                     │
                     │
                     ▼
dim_road ───────▶ fact_traffic
```

This separates:

* **Facts:** traffic measurements and events
* **Dimensions:** road and geographical attributes

making the Gold layer suitable for analytical workloads.

---

## 📊 Power BI

The final analytics layer can be connected to Power BI through **Spark Thrift Server**.

The repository includes the Power BI dashboard:

```text
Live Traffic Analytics Dashboard.pbix
```

The dashboard provides an analytical view of traffic conditions and congestion patterns.

---

## 📁 Project Structure

```text
realtime-traffic-analytics/
│
├── apps/
│   ├── traffic_bronze.py
│   ├── traffic_silver.py
│   └── traffic_gold.py
│
├── hive-conf/
│   └── hive-site.xml
│
├── producer/
│   └── traffic_dirty_producer.py
│
├── docker-compose.yml
├── SQL.txt
├── commands.txt
├── Live Traffic Analytics Dashboard.pbix
├── LICENSE
└── README.md
```

---

## 🔑 Key Concepts Demonstrated

* Real-time data engineering
* Event-driven architecture
* Kafka-based streaming
* Spark Structured Streaming
* Medallion Architecture
* Data quality engineering
* Delta Lake
* Data warehousing
* Star schema
* Hive Metastore
* Spark SQL
* BI integration
* Containerized data platforms

---

## 📌 Project Highlights

**3-layer** Medallion Architecture
**3-table** Gold star schema
**5+** data-quality and transformation operations
**3-partition** Kafka topic
**3 BI views** for analytics consumption

---
## Dashboard
<img width="4135" height="3279" alt="Live Traffic Analytics Dashboard_page-0001" src="https://github.com/user-attachments/assets/1e3629af-ebed-463b-bd4f-cbca945ff4b4" />


---

## 📄 License

This project is licensed under the **MIT License**.
