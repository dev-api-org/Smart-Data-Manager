# Smart Data Manager – ETL Pipeline

## Project Overview
The **Smart Data Manager** project demonstrates a Python-based ETL pipeline for managing and processing business data.  
This script extracts data from a SQL Server database, cleans and transforms it, and loads aggregated summary tables back into the database.  
These summary tables can be used for reporting, analytics, or integration with Power Apps and Power BI.

---

## ETL Pipeline Features
- **Extract**: Reads data from multiple SQL Server tables:
  - `Programs`
  - `Projects`
  - `Progress`
  - `Teams`
  - `Team_Members`
  - `Members`
- **Transform**: Cleans and standardizes data:
  - Converts dates and numeric fields
  - Handles missing columns safely
  - Aggregates data for program, team, and member-level summaries
- **Load**: Writes processed summary tables to SQL Server:
  - `Program_Summary_Report`
  - `Team_Performance_Report`
  - `Member_Progress_Report`
- **Logging**: Tracks ETL process in `etl_pipeline.log`.

---

## Requirements
- Python 3.13+
- Libraries:
  - `pandas`
  - `SQLAlchemy`
  - `pyodbc`
- Access to SQL Server with the following environment variables:
  - `SQL_SERVER`
  - `SQL_DB`
  - `SQL_USER`
  - `SQL_PASSWORD`
  - `ODBC_DRIVER` (e.g., "ODBC Driver 17 for SQL Server")

---

## Setup Instructions

1. **Clone the repository**  
   ```bash
   git clone <your-repo-url>
   cd <repo-folder>

   ## Data Flow

The following diagram illustrates the flow of data in the Smart Data Manager project:


