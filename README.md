# Library Management System

## Introduction

This Library Management System is designed to manage and streamline library operations by integrating various technologies for data handling and API services. The system features an ETL pipeline for data extraction, transformation, and loading, leverages Microsoft SQL for robust data storage, and provides a FastAPI interface for easy and efficient access to library data.

## Architecture

### Components

- **ETL Pipeline**: Automated scripts that generate, transform, and load data using Python.
- **Python Scripts**: Utilize libraries like Faker to create realistic but fictitious data sets.
- **MSSQL Database**: Stores all library data, ensuring robust data management.
- **FastAPI**: Provides a RESTful API for frontend interactions and external integrations.
- **Docker**: Containers ensure a consistent and isolated environment across different deployment scenarios.
- **Apache Airflow**: Manages and schedules the ETL pipeline tasks efficiently.

### Workflow

1. **Data Generation**:
   - Python scripts use the Faker library to generate fake data for books, members, and transactions.
   - Pandas and NumPy are employed to manipulate data structures and perform data transformations.
2. **ETL Process**:
   - Apache Airflow orchestrates the workflow where data is extracted (generated), transformed for consistency using Pandas, and loaded into the MSSQL database.
3. **API Interface**:
   - FastAPI provides endpoints for querying the database and managing library operations through HTTP requests.
4. **Deployment**:
   - Docker containers encapsulate the environment, ensuring that the Python environment, libraries, and dependencies are consistent and the application is scalable.

## Getting Started

### Prerequisites

- Docker
- Docker Compose
- Git (for version control)

### Installation

1. **Clone the repository**:
- ` git clone https://github.com/kurekhombre/Library.git`
- ` cd Library`
2. **Build and Run Docker Containers**:
- ` docker-compose up --build`


### Usage

- Access the FastAPI documentation at `*docs` to view and interact with the API endpoints.
- Monitor Airflow workflows by accessing the Airflow web interface at `*`.

###  Tests
- Execute tests:
`docker exec -it <container_name> pytest`
___
### TODO
- TODO :)


