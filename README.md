# Data Warehouse and Analytics Project

Building a modern **data warehouse using SQL Server**, including **ETL processes**, **data modeling (Star Schema)**, and **SQL-based analytics** for reporting and decision-making.

---

## Project Overview

This project consolidates sales-related data from multiple source systems into a centralized, analytics-ready data warehouse. It follows a structured workflow similar to real-world data engineering projects:

- Ingest raw data from source systems (CSV files)
- Clean and standardize the data (data quality fixes and validation)
- Model the data into business-friendly dimensions and facts (Gold layer)
- Enable analytics and reporting through SQL queries

---

## Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using **SQL Server** to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (**ERP** and **CRM**) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis (missing values, inconsistent formats, duplicates, etc.).
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the **latest dataset only**; historization is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

## Data Architecture (Medallion Pattern)

This project uses a layered architecture:

- **Bronze Layer** → Raw ingestion (data loaded as-is from CSV files)
- **Silver Layer** → Cleaned and standardized data (quality fixes + transformations)
- **Gold Layer** → Analytics-ready model (Star Schema: dimensions + fact tables)

---

## Data Model (Gold Layer)

The Gold layer follows a **Star Schema** design optimized for analytics.

### Dimension Tables
- **`gold.dim_customers`**  
  Stores customer master data and descriptive attributes such as name, country, gender, marital status, and create date.

- **`gold.dim_products`**  
  Stores product information and descriptive attributes such as category, subcategory, product line, and cost.

### Fact Table
- **`gold.fact_sales`**  
  Stores sales transactions and measurable metrics such as sales amount, quantity, and price.  
  The fact table connects to dimensions using **surrogate keys** (e.g., `customer_key`, `product_key`) to ensure consistent joins and analytics performance.

---

## BI: Analytics & Reporting (Data Analytics)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:

- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.

Example analytics include:
- Top customers by revenue
- Best-selling products by quantity and sales
- Monthly/weekly sales trends
- Sales performance by category and country

---
## Data Architecture
![Data Architecture](./data_architecture.png)

## Tools & Technologies Used

- **SQL Server**
- **T-SQL**
- **ETL Concepts**
- **Data Cleansing & Validation**
- **Star Schema Data Modeling**

---

## License

This project is licensed under the **MIT License**. You are free to use, modify, and share this project with proper attribution.  
See the [MIT License](LICENSE) for more details.

---

## About Me

Hi, I’m **John**, an aspiring **Data Engineer** passionate about building modern data platforms and turning raw data into meaningful insights. I’m currently learning and improving my skills in **SQL Server**, **ETL development**, **data modeling**, and **analytics reporting** by working on hands-on projects like this data warehouse and analytics solution.
