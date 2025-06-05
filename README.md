# E-commerce Analytics Platform

A complete data engineering pipeline with batch and real-time components for e-commerce analytics.

## Project Components

### 1. Batch Data Pipeline
- Historical data ingestion (simulated)
- Data Lake storage (emulated with local directory)
- Batch processing with Apache Spark
- Data Warehouse using PostgreSQL
- ETL orchestration with Apache Airflow

### 2. Real-time Data Pipeline
- Real-time event streaming with Apache Kafka
- Kafka dependency with Apache Zookeeper
- Real-time data storage with Redis
- Real-time user interaction simulation script (Kafka Producer)
- Real-time Kafka Consumer Service

### 3. Web Interface
- Web framework using Django MVT
- Real-time updates with Django Channels

### 4. Containerization
- Multi-stage Docker build for all services
- Docker Compose for orchestration
- Health checks for critical services
- Persistent volumes for data

## Project Structure

```
e-commerce-analytics/
├── airflow/                  # Airflow configuration and DAGs
│   └── dags/                 # Airflow DAG definitions
│       └── ecommerce_etl_dag.py  # ETL workflow definition
├── data/                     # Data storage (mounted in containers)
│   └── lake/                 # Data lake (simulated S3)
├── django_app/               # Django web application
│   ├── analytics/            # Django analytics app
│   ├── ecommerce_analytics/  # Django project settings
│   ├── templates/            # HTML templates
│   └── manage.py             # Django management script
├── kafka_scripts/            # Kafka producers and consumers
│   ├── consumer.py           # Real-time event consumer
│   └── producer.py           # Event simulation producer
├── spark/                    # Spark jobs
│   └── jobs/                 # Spark job definitions
│       └── batch_processing.py  # Batch ETL job
├── docker-compose.yml        # Docker Compose configuration
├── Dockerfile                # Multi-stage Dockerfile for all services
├── requirements.txt          # Python dependencies for all services 
└── README.md                 # Project documentation
```

## Docker Architecture

The platform uses a multi-stage Dockerfile approach:

1. **Base Image**: Common Python environment for all services
2. **Builder**: Installs all dependencies in one layer
3. **Service-specific stages**:
   - Django web application
   - Kafka consumer for processing events
   - Kafka producer for simulating events
   - Spark for batch processing
   - Airflow for workflow orchestration

This approach:
- Reduces duplication
- Optimizes layer caching
- Minimizes image sizes
- Standardizes the build process

## Setup and Running

1. Make sure Docker and Docker Compose are installed on your system.
2. Clone the repository:
   ```
   git clone https://github.com/yourusername/e-commerce-analytics.git
   cd e-commerce-analytics
   ```
3. Start the services:
   ```
   docker-compose up -d
   ```
4. Access the components:
   - Django Web Application: http://localhost:8000
   - Airflow Web UI: http://localhost:8080
   - Spark Web UI: http://localhost:4040

## Data Flow

1. **Batch Pipeline**:
   - Airflow triggers a daily ETL process
   - Python script generates/ingests data into the data lake
   - Spark job processes the data and loads it to PostgreSQL
   - Data is available for analysis in the Django web app

2. **Real-time Pipeline**:
   - Simulation script generates user events and sends them to Kafka
   - Kafka consumer processes events and updates metrics in Redis
   - Django Channels WebSocket connections push real-time updates to the dashboard

## Technologies Used

- **Apache Airflow**: Workflow orchestration
- **Apache Spark**: Batch data processing
- **Apache Kafka**: Event streaming
- **PostgreSQL**: Data warehouse
- **Redis**: Real-time data store
- **Django**: Web framework
- **Django Channels**: WebSocket support
- **Docker & Docker Compose**: Containerization
- **JavaScript/Chart.js**: Dashboard visualizations 