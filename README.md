# Elasticsearch Client

Java 21 client application for connecting to Elasticsearch 7.14.1 cluster with full CRUD operations.

## Features

- Connects to Elasticsearch cluster (localhost:9201, localhost:9202, localhost:9203)
- Full CRUD operations (Create, Read, Update, Delete)
- Automatic index initialization with sample data
- Comprehensive exception handling and resource management
- Logging with SLF4J and Logback
- Unit tests with JUnit 5
- Maven build support

## Prerequisites

- Java 21
- Maven 3.6+
- Elasticsearch 7.14.1 cluster running on localhost:9201, localhost:9202, localhost:9203

## Dependencies

- Elasticsearch High Level REST Client 7.14.1
- Lombok
- SLF4J API
- Logback Classic
- Jackson Databind
- Jackson JavaTime Module
- JUnit 5 (for testing)

## Project Structure

```
src/
├── main/
│   ├── java/
│   │   └── org/daodao/elasticsearch/
│   │       ├── model/
│   │       │   └── SampleData.java
│   │       ├── service/
│   │       │   └── ElasticsearchService.java
│   │       ├── Constants.java
│   │       ├── ElasticsearchClientConfig.java
│   │       └── ElasticsearchApplication.java
│   └── resources/
│       ├── application.properties
│       └── logback.xml
└── test/
    └── java/
        └── org/daodao/elasticsearch/service/
            ├── ElasticsearchServiceTest.java
            ├── ElasticsearchServiceAdvancedTest.java
            └── TestSuite.java
```

## Configuration

The application reads configuration from `src/main/resources/application.properties`:

```
elasticsearch.hosts=localhost:9201,localhost:9202,localhost:9203
elasticsearch.scheme=http
elasticsearch.connectTimeout=5000
elasticsearch.socketTimeout=60000
elasticsearch.connectionRequestTimeout=5000
```

## Building and Running

1. Build the project:
   ```bash
   mvn clean install
   ```

2. Run the application:
   ```bash
   mvn exec:java -Dexec.mainClass="org.daodao.elasticsearch.ElasticsearchApplication"
   ```

   Or run the jar file:
   ```bash
   java -jar target/elasticsearch-client-1.0-SNAPSHOT.jar
   ```

## Running Tests

Execute unit tests with Maven:
```bash
mvn test
```

Or run the test suite directly:
```bash
mvn test -Dtest=org.daodao.elasticsearch.service.TestSuite
```

## Usage

The application automatically initializes an Elasticsearch index named `sample_data` with sample documents if it doesn't exist. It then demonstrates all CRUD operations:

1. Creates a new document
2. Retrieves the document
3. Updates the document
4. Searches for documents
5. Lists all documents
6. Deletes the document

The application handles all exceptions gracefully and ensures proper resource cleanup.
