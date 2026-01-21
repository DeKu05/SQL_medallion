#  SQL Medallion Architecture Pipeline
# SQL Medallion Architecture Pipeline

A complete implementation of the Medallion Architecture (Bronze-Silver-Gold) data pipeline using MySQL, demonstrating data engineering best practices for structured data transformation and aggregation.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Pipeline Execution](#pipeline-execution)
- [Results](#results)
- [Screenshots](#screenshots)

## Overview

This project implements a three-layer medallion architecture to process e-commerce order data from Indian cities. The pipeline ingests raw JSON data, transforms it into structured format, and generates business intelligence metrics.

**Use Case:** Sales Analysis across major Indian cities (Mumbai, Delhi, Bangalore, Hyderabad, Pune, Chennai, Kolkata)

## Architecture

The Medallion Architecture consists of three layers:

### Bronze Layer (Raw Data)
- **Purpose:** Data ingestion and storage
- **Format:** Raw JSON documents
- **Characteristics:**
  - No transformations applied
  - Complete historical data
  - Source of truth for all downstream processes
  - Includes metadata (ingestion timestamp)

### Silver Layer (Cleaned Data)
- **Purpose:** Data transformation and standardization
- **Format:** Structured relational tables
- **Transformations:**
  - JSON parsing and extraction
  - Data type conversions
  - Schema validation
  - Field standardization

### Gold Layer (Business Metrics)
- **Purpose:** Business-level aggregations
- **Format:** Aggregated tables ready for analytics
- **Metrics:**
  - City-wise total sales
  - Revenue analysis
  - Business KPIs

## Technologies Used

- **Database:** MySQL 8.0+
- **IDE:** MySQL Workbench
- **Language:** SQL
- **Data Format:** JSON (NDJSON - Newline Delimited JSON)

## Project Structure

```
SQL_medallion/
├── data/
│   └── orders.json           # Sample order data
├── scripts/
│   └── medallion_pipeline.sql # Complete pipeline script
├── screenshots/
|
└── README.md
```

##  Setup Instructions

### Prerequisites
- MySQL Server 8.0 or higher
- MySQL Workbench (recommended)
- Basic understanding of SQL

### Step 1: Enable Local Data Loading

**In MySQL Configuration:**
```sql
SET GLOBAL local_infile = 1;
```

**In MySQL Workbench:**
1. Go to `Edit` → `Preferences` → `SQL Editor`
2. Enable `OPT_LOCAL_INFILE=1`
3. Restart MySQL Workbench

### Step 2: Clone Repository
```bash
git clone <your-repo-url>
cd SQL_medallion
```

### Step 3: Prepare Data File
Ensure `orders.json` is placed in the `data/` directory with the correct path:
```
local path for the json files
```

## Pipeline Execution

### Run Complete Pipeline
```sql
-- Execute the entire medallion_pipeline.sql script
source : local_folder;
```

## Results

### Sample Output - Gold Layer (City-wise Sales) for orders

| City       | Total Sales (₹) |
|------------|-----------------|
| Bangalore  | 8,426.50        |
| Mumbai     | 7,701.75        |
| Delhi      | 5,465.75        |
| Hyderabad  | 4,700.25        |
| Chennai    | 4,651.00        |
| Pune       | 3,175.00        |
| Kolkata    | 980.50          |

### Data Lineage Summary

| Layer          | Record Count |
|----------------|--------------|
| Bronze Layer   | 20           |
| Silver Layer   | 20           |
| Gold Layer     | 7            |

## Screenshots

### 1. Database Creation

*Creating the medallion_db database*

![alt text](1-1.png)

### 2. Bronze Layer - Table Creation

*Bronze layer table structure with JSON column*

![alt text](2.png)

### 3. Enabling Local Infile

*Configuring MySQL to allow local file loading*

![alt text](3.png)

### 4. Bronze Layer - Data Loaded

*Raw JSON data successfully ingested into bronze layer*

![alt text](4-1.png)

![alt text](5-1.png)

### 5. Bronze Layer - Data validation
*filter out invalid data successfully*

for correct orders :
![alt text](11.png)

for faluty data :
![alt text](13.png)
### 6. Silver Layer - Transformed Data

*Parsed and structured data in silver layer*

![alt text](6-1.png)

![alt text](7.png)


### 7. Gold Layer - Aggregated Results

*Final business metrics showing city-wise sales*

![alt text](8.png)

### 8. Data Lineage Verification

*Complete pipeline verification across all three layers*
old verification method :

![alt text](9.png) 

![alt text](11-1.png)

new verification of data validity :

![alt text](17.png)

### 9. TOP and BOTTOM performers
*TOP and BOTTOM performing cities*
top performers :
![alt text](14-1.png)

bottom performers :
![alt text](15-1.png)

### 10. Invalid records filtering 
*table to see the invalid records*

![alt text](16-1.png)

## 11. Complete successfull run of the SQL pipeline 

![alt text](20.png)


##  Key Learnings

1. **Data Lakehouse Pattern:** Implementing medallion architecture for incremental data refinement
2. **JSON Handling:** Working with semi-structured data in SQL databases
3. **ETL Pipeline:** Building end-to-end data transformation pipelines
4. **Data Quality:** Maintaining data lineage and transformation history
5. **SQL Best Practices:** Using proper data types, constraints, and aggregations



