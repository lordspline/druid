# Apache Druid Repository Explainer

## Overview

Apache Druid is a high-performance real-time analytics database designed to deliver sub-second queries on streaming and batch data at scale. This repository contains the complete source code for Apache Druid, an open-source distributed data store.

## What is Apache Druid?

Druid is a real-time analytics database that combines:
- **Stream ingestion** - Process millions of events per second
- **Batch ingestion** - Load large historical datasets
- **Sub-second queries** - Interactive analytics at scale
- **High availability** - Distributed architecture with no single point of failure
- **Horizontal scalability** - Add more servers to handle more data

### Key Use Cases
- Real-time analytics dashboards
- Application performance monitoring
- Digital marketing analytics
- Network telemetry analysis
- Supply chain analytics
- Risk/fraud analysis

## Repository Structure

### Core Components

#### `/server`
Contains the core server implementation including:
- Query processing engine
- Storage management
- Cluster coordination logic

#### `/processing`
The data processing engine that handles:
- Data ingestion pipelines
- Query execution
- Aggregation operations
- Data indexing

#### `/sql`
SQL query layer that provides:
- SQL parser and planner
- SQL to native query translation
- JDBC driver support
- SQL functions and operators

#### `/services`
Core services implementation for the distributed system

#### `/indexing-service`
Manages data ingestion tasks and coordination

#### `/indexing-hadoop`
Hadoop-based batch ingestion support

### Extensions

#### `/extensions-core`
Built-in extensions that provide essential functionality:
- **Data Formats**: Parquet, ORC, Avro, Protobuf support
- **Cloud Storage**: S3, Azure, Google Cloud Storage
- **Streaming**: Kafka, Kinesis ingestion
- **Databases**: PostgreSQL, MySQL metadata storage
- **Security**: Kerberos, Basic authentication, PAC4J
- **Processing**: DataSketches, Bloom filters, Histograms
- **Multi-Stage Query**: Distributed SQL query engine

#### `/extensions-contrib`
Community-contributed extensions:
- **Time Series**: Time-based aggregations
- **Storage**: Cassandra, InfluxDB integrations
- **Monitoring**: Prometheus, Graphite, StatsD emitters
- **Advanced Analytics**: Moving averages, TDigest sketches
- **Data Lakes**: Delta Lake, Iceberg support

### Web Console (`/web-console`)
A React-based web UI that provides:
- Visual data loading wizards
- Query workbench with SQL editor
- Cluster management interface
- Real-time monitoring dashboards
- Task and supervisor management

### Testing & Quality

#### `/integration-tests` & `/integration-tests-ex`
Comprehensive integration test suites covering:
- End-to-end ingestion scenarios
- Query correctness
- Cluster operations
- Fault tolerance

#### `/benchmarks`
Performance benchmarking suite for:
- Query performance
- Ingestion throughput
- Compression efficiency
- Index operations

### Documentation (`/docs`)
Comprehensive documentation covering:
- Architecture and design
- Getting started tutorials
- API references
- Operations guides
- SQL reference
- Configuration options

### Development Tools

#### `/dev`
Development utilities and scripts

#### `/codestyle`
Code style configurations and checks

#### `/.github`
GitHub Actions workflows for:
- Continuous Integration
- Code quality checks
- Security scanning
- Automated testing

## Architecture

### Distributed System Design

Druid follows a microservices architecture with specialized node types:

#### Master Server Components
- **Coordinator**: Manages data availability and segment distribution
- **Overlord**: Controls data ingestion workloads

#### Query Server Components
- **Broker**: Routes queries and merges results
- **Router**: API gateway and web console host

#### Data Server Components
- **Historical**: Stores and queries historical data
- **MiddleManager/Peon**: Executes ingestion tasks
- **Indexer**: Alternative ingestion system (experimental)

### External Dependencies
- **Deep Storage**: S3, HDFS, or filesystem for data persistence
- **Metadata Storage**: PostgreSQL or MySQL for cluster metadata
- **ZooKeeper**: Service discovery and coordination

## Key Technologies

### Languages
- **Java**: Primary implementation language (JDK 11 or 17 required)
- **JavaScript/TypeScript**: Web console implementation
- **Python**: Testing and utility scripts

### Build System
- **Maven**: Primary build tool
- **npm/yarn**: Web console build

### Major Dependencies
- Apache Calcite (SQL parsing)
- Jackson (JSON processing)
- Netty (Network communication)
- Apache Curator (ZooKeeper client)

## Data Ingestion

### Streaming Ingestion
- **Kafka**: Exactly-once ingestion from Kafka topics
- **Kinesis**: Amazon Kinesis stream processing

### Batch Ingestion
- **Native Batch**: Parallel ingestion from various sources
- **SQL-based**: Using INSERT/REPLACE statements
- **Hadoop-based**: MapReduce jobs for large datasets

### Supported Data Formats
- JSON, CSV, TSV
- Avro, Parquet, ORC
- Protobuf
- Custom formats via extensions

## Query Capabilities

### SQL Support
- ANSI SQL with extensions
- Window functions
- Joins (including cross-datasource)
- Subqueries
- PIVOT/UNPIVOT operations

### Native Queries
- JSON-based query language
- More flexibility than SQL
- Direct access to all features

### Query Types
- Timeseries aggregations
- TopN queries
- GroupBy queries
- Scan queries
- Search queries

## Development Workflow

### Building from Source
```bash
# Clone the repository
git clone https://github.com/apache/druid.git

# Build with Maven
mvn clean package -DskipTests

# Build specific module
mvn clean package -pl :druid-processing
```

### Running Locally
- Use the quickstart scripts in `/examples`
- Docker compose configurations available
- Development configurations in `/dev`

### Contributing
- Follow Apache contribution guidelines
- Code style enforcement via checkstyle
- Comprehensive test coverage required
- Pull request CI/CD validation

## Community and Support

- **Mailing Lists**: dev@druid.apache.org, druid-user Google Group
- **Slack**: Active community channels
- **Documentation**: https://druid.apache.org/docs/latest/
- **Issues**: GitHub issue tracker

## License

Apache License 2.0 - Fully open source with commercial-friendly licensing

## Summary

This repository represents a mature, production-ready analytics database that powers real-time analytics at many of the world's largest companies. The codebase is well-organized with clear separation between core functionality and extensions, comprehensive testing, and extensive documentation. The active community and Apache governance ensure long-term sustainability and continuous improvement.