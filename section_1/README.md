# Section 1: API Integration and Data Pipeline

## Overview
This project builds a modular pipeline to fetch and store financial data from sources like Polygon.io and Frankfurter. Configurable collectors access these APIs, parse the data, and write it to a SQL database using SQLAlchemy. In production, such collectors could run daily (using tools like Airflow) to keep the data up-to-date.

After storage, the database lets you query financial details easily—for instance, retrieving any stock’s price on any date. If needed, the system also converts prices between currencies using real-time exchange rates.


## Project Structure

- **models.py**  
  Contains the "SourceConfig" configuration that used to defines data source - this configuration is based on the sources table in our sql DB.  

- **database.py**  
  Implements the SQLAlchemy models and manages database interactions.  
  It contains classes to handle database sessions, insert or update operations,  
  and manages the mapping between source types and target tables.

- **utils.py**  
  Provides utility functions such as token decryption for consistent and centralized usage  
  across the project.

- **main.py**  
  Serves as the entry point of the application.  
  It enables the addition of new source configurations and triggers data scraping processes.

- **collectors/**  
  - **collectors.py**  
    Maps source types to their corresponding collector classes along with their default target tables.
    
  - **base_data_collector.py**  
    Defines an abstract base class that standardizes API request handling, error management,  
    and the overall data collection workflow.
    
  - **exchange_rate_collector.py**  
    Contains the API integration and processing logic for retrieving exchange rate data.
    
  - **polygon_collector.py**  
    Contains the API integration and processing logic for retrieving stock data from Polygon.io.
    
  - **data_classes.py**  
    Defines data classes that structure the scraped data into plain, structured objects.

## How to Run

1. **Create the Virtual Environment and Install Dependencies:**  
   Open a terminal and execute the following commands:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Run the Main Application:**  
   With the virtual environment active, run the main script with:
   ```bash
   python main.py
   ```
   This will initiate the API data scraping process and start the data pipeline.

