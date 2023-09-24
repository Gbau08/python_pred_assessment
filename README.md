# Air Quality Data Processing Tool

This application retrieves air quality data, processes the gathered information, and commits the processed results to an Azure SQL Server database. The application has been containerized as a Docker image, facilitating seamless deployment on Azure Container Instance (ACI) and scheduling via Azure Logic App.

## Prerequisites:
- Git: Ensure you have Git installed on your machine.
- Docker install Docker on your machine.
- Python: This project requires Python. Download and install from Python's official site.


## Application Overview

### **Python Code**:
    
- **DataFetcher**:
    - `download_data`: Downloads data from the given URL with retries.
    - `extract_data_from_zip`: Extracts .ndjson data and a countries.csv file from the downloaded ZIP archive.
    
- **DataProcessor**:
    - `filter_countries`: Filters data to only include entries present in the countries.csv.
    - `filter_parameters_hourly_obs`: Filters data for specific pollutants and their hourly observations.
    - `compute_rolling_average`: Calculates a 24-hour rolling average.
    - `calculate_aqi_formula`: Determines the Air Quality Index (AQI) based on pollutant concentrations and provided breakpoints.
    - `compute_aqi`: Computes AQI values for PM2.5 and PM10 pollutants based on the 24-hour rolling average.
    
- **DatabaseManager**:
    - `create_tables_if_not_exists`: Initializes AQI_TABLE and ROLLING_AVG_TABLE if they don't exist.
    - `batch_insert_data`: Populates the tables with data.
    - `drop_table_if_exists`: Deletes a table if it exists.

- **Main Function**: Orchestrates the entire data processing flow.
    - Data fetching using `DataFetcher`.
    - Data processing using `DataProcessor`.
    - Database operations using `DatabaseManager`.
    
- **Logging**: The application employs the logging module to keep track of info, errors, and significant events.

## Azure Data Pipeline Overview

### 1. **Docker**: 
Build the image that containerizes the Python application.

### 2. **Azure Container Registry (ACR)**: 
Holds the Docker image, enabling Azure services to retrieve and run the image.

### 3. **Azure Container Instance (ACI)**: 
Provides a serverless Azure environment for executing the Docker containers, facilitating the running of the Python app and its connection to Azure SQL DB.

### 4. **Azure SQL DB**: 
A fully managed relational database where the processed air quality data is stored.

### 5. **Logic App**: 
Automates workflows and integrates services, apps, and data, and schedules the ACI container's execution.

## Data Flow & Connections

1. **Scheduled Execution**: The Logic App triggers the ACI container at set intervals.
2. **Data Processing**: The container runs the Python app, which fetches and processes air quality data.
3. **Data Storage**: Processed data is stored in the Azure SQL DB.
4. **Connection Flow**:
    - ACI retrieves the Docker image from ACR.
    - The Python application connects with Azure SQL DB using given credentials.
    - Logic App manages the scheduled execution of the ACI container.

The entire architecture provides a powerful, scalable, and automated solution for managing air quality data pipelines.

## **Improvements**:
  - **Incremental Logic**: The current architecture overwrites data with each run. An incremental update mechanism is recommended.
  - **Architecture Suggestions**: Consider using a cloud storage shared with the client for raw data uploads, and utilize tools like DBT for ELT processes instead of Python applications.
  - **Code Modularization**: To improve reusability, consider further modularizing the code.
  - **DevOps Integration**: Implement a CI/CD pipeline, use terraform for infrastructure provisioning, and consider multi-environment deployments.

## **Running the Project: Local Setup Instructions **:

### 1. **Clone the Repository**: 
```
git clone git@github.com:Gbau08/python_pred_assessment.git
cd python_pred_assessment
```

### 2. **Build the Docker Image**: 
```
docker build -t my-python-pred-app .
```

### 3. **Run the Application Using Docker**: 
```
docker run -e DATABASE_HOST=AskMeTheServer.database.windows.net -e DATABASE_USER=AskMeTheUser -e DATABASE_PASSWORD=AskMeThePassword my-python-pred-app
```