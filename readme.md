# Weather Analytics Platform
- [Assumption](#Assumption)
- [Architecture](#Architecture)
- [Data Process](#Data-Process)
- [Data Model](#Data-Model)
- [Data Quality](#Data-Quality)
- [Consideration](#Consideration)
- [Data Source](#Data-Source)

This project aims to build an end-to-end data solution to allow users to resolve enquries around weather data using Bureau of Meteorology (BOM) weather data. <br>
An efficient database design and sufficient data governance over data quality are considered to ensure a reliable and trustworthy data product.

## Assumption
- The weather dataset from BOM is assumed to be updated every month.
- Scope of the project is limited to the weather measured from 2012 onwards.

## Architecture
<img src="./images/architecture diagram.JPG" width="800">

The solution is built in a way that the dataset gets processed, validated and loaded into the database schemas for consumption.
Technologies and methodologies were selected based on their robustness and scalability in handling large data volume.

- **Python**: Python is selected as a scripting language due to its versatility that allows 
easy integrations with external systems. Python loads and processes datasets with the object
storage and the database.
- **MinIO**: MinIO is used as a S3-compatible object storage to hold the raw compressed BOM weather
dataset. The object storage is chosen as a landing platform of the raw data given its scalability and cheap storage.
- **Snowflake**: OLAP database is used for the optimised analytical query performance. And Snowflake among many OLAP databases is selected as it automatically manages statistics and
data partitions, taking engineering burdens away. Additionally, it provides scalability with
different compute resources so-called "warehouse" to cope with large data volume and usage.
- **dbt**: dbt is used to transform data models within the database. And this tool is chosen to
leverage Snowflake's distributed computing power, and its own comprehensive testing framework.
- **Airflow**: Airflow orchestrates Python and dbt processes. And it is used for its robustness in
scheduling and orachestrating data processes.
- **Docker**: Docker is used to host both Airflow and MinIO object storage.

## Data Process
<img src="./images/airflow dag.png" width="800">

As shown on the Airflow dag diagram above, there are 5 data processes defined.
- **land_file**: This process fetches the compressed BOM weather dataset from the BOM
server via FTP and loads into the MinIO object storage.
- **stage_data**: This process extracts weather and station datasets from the
compressed BOM dataset file in the object storage. And this pre-processes and
loads the datasets into the Snowflake staging schema.
- **generate_dbt_model**: Based on the available years in the preprocessed weather dataset
from the Snowflake staging schema, this process generates yearly partitioned tables for
weather measurements (e.g., rain_2023) with respective dbt data model & schema files (e.g., rain_2023.sql & rain_2023.yml). 
This script allows the dataset to grow incrementally without having to manually create new table nor dbt data model scripts.
- **incremental_data_load**: This loads data to the data models incrementally, and refreshes
the aggregated data model.
- **reconcile_data**: This process reconciles the row counts between staging schema and
weather measurement schemas in Snowflake to ensure data integrity.

Scripts can be found at:
- Airflow dag script: [airflow/dags](https://github.com/TravisH0301/weather_analytics_platform/tree/main/airflow/dags)
- Python scripts: [airflow/dags/scripts](https://github.com/TravisH0301/weather_analytics_platform/tree/main/airflow/dags/scripts)
- dbt scripts: [airflow/dags/dbt](https://github.com/TravisH0301/weather_analytics_platform/tree/main/airflow/dags/dbt)

## Data Model
<img src="./images/data model.png" width="800">


## Data Quality


## Consideration


## Data Source
[Bureau of Meteorology FTP Public Products](http://www.bom.gov.au/catalogue/anon-ftp.shtml)