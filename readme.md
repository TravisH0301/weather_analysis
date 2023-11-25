# Weather Analytics Platform
1. [Assumption](#Assumption)
2. [Architecture](#Architecture)
3. [Data Process](#Data-Process)
4. [Data Model](#Data-Model)
5. [Data Quality](#Data-Quality)
6. [CI/CD](#CICD)
7. [Consideration](#Consideration)
8. [Data Source](#Data-Source)

This project aims to build an end-to-end data solution to allow users to resolve enquries around weather data using Bureau of Meteorology (BOM) weather data. <br>
An efficient database design and sufficient data governance over data quality are considered to ensure a reliable and trustworthy data product.

## Assumption
- The weather dataset from BOM is assumed to be updated every month.
- Scope of the project is limited to the weather measured from 2012 onwards.

## Architecture
<img src="./images/architecture diagram.png" width="800">

The solution is built in a way that the dataset gets processed, validated and loaded into the database schemas for consumption.
Technologies and methodologies were selected based on their robustness and scalability in handling large data volume.

- **Python** <br>
Python is selected as a scripting language due to its versatility that allows 
easy integrations with external systems. Python loads and processes datasets with the object
storage and the database.
- **MinIO** <br>
MinIO is used as a S3-compatible object storage to hold the raw compressed BOM weather
dataset. The object storage is chosen as a landing platform of the raw data given its scalability and cheap storage.
- **Snowflake** <br>
OLAP database is used for the optimised analytical query performance. And Snowflake among many OLAP databases is selected as it automatically manages statistics and
data partitions, taking engineering burdens away. Additionally, it provides scalability with
different compute resources so-called "warehouse" to cope with large data volume and usage.
- **dbt** <br>
dbt is used to transform data models within the database. And this tool is chosen to
leverage its own comprehensive testing framework and Snowflake's distributed computing power.
- **Airflow** <br>
Airflow orchestrates Python and dbt processes. And it is used for its robustness in
scheduling and orachestrating data processes.
- **Docker** <br>
Docker is used to host Airflow, MinIO object storage and Buildkite pipeline agent.
- **Buildkite** <br>
Buildkite is used for building CI/CD pipeline, and is selected given its hybrid CI/CD SaaS model that provides security and efficiency by self-hosting the pipeline compute.

## Data Process
<img src="./images/airflow dag.png" width="800">

As shown on the Airflow dag diagram above, there are 5 data processes defined.
1. **land_file** <br>
This process fetches the compressed BOM weather dataset from the BOM
server via FTP and loads into the MinIO object storage.
2. **stage_data** <br>
This process extracts weather and station datasets from the
compressed BOM dataset file in the object storage. And this pre-processes and
loads the datasets into the Snowflake staging schema.
3. **generate_dbt_model** <br>
Based on the available years in the preprocessed weather dataset
from the Snowflake staging schema, this process generates yearly partitioned tables for
weather measurements (e.g., rain_2023) with respective dbt data model & schema files (e.g., rain_2023.sql & rain_2023.yml). 
This script allows the dataset to grow incrementally without having to manually create new table nor dbt data model scripts.
4. **incremental_data_load** <br>
This loads data to the data models incrementally, and refreshes
the aggregated data model.
5. **reconcile_data** <br>
This process reconciles the row counts between staging schema and
weather measurement schemas in Snowflake to ensure data integrity.

In executing the above tasks, Airflow dag is designed to send email alerts upon success or failure of the task to assist job monitoring.

Scripts can be found at:
- Airflow dag script: [airflow/dags/](https://github.com/TravisH0301/weather_analytics_platform/tree/main/airflow/dags)
- Python scripts: [airflow/dags/scripts/](https://github.com/TravisH0301/weather_analytics_platform/tree/main/airflow/dags/scripts)
- dbt scripts: [airflow/dags/dbt/](https://github.com/TravisH0301/weather_analytics_platform/tree/main/airflow/dags/dbt)

## Data Model
<img src="./images/data model.png" width="800">

In Snowflake, there are 3 types of schemas.

- **Staging** <br>
The staging schema holds the preprocessed weather and station datasets. The preprocessed station dataset is not used in other schemas due to its incompleteness with missing station information. Ideally, the `station_id` from this table would be concatenated with `date` from the weather table to create a synthetic key, uniquely identifying records in the weather tables in weather measurement schemas, as well as acting as a join key between weather measurement schemas. However, due to the missing stations in the station dataset, `station_name` was used instead, leading to a varchar key rather than a numeric key.

- **Weather Measurements** <br>
Each weather measurement schema holds individual weather measurement. For example, `RAIN` schema holds tables with `rain` measurement column. And the weather data in weather schemas are partitioned into separate tables by year. For example `RAIN` weather schema holds partitioned tables such as `RAIN_2023`, `RAIN_2022`, `RAIN_2021` and so on. This is to enhance cost and performance efficiencies by skipping yearly data that is not required. And the tables among weather schemas can be joined by using the join key `record_id`, which is consisted of `station_name` and `date` that uniquely identifies daily weather measurement records.

- **Aggregated** <br>
The aggregated schema contains monthly average weather measurements where all parititoned tables across different weather measurement schemas are joined and grouped into a single table. This table is created as a physical table, refreshing every month, and it aims to provide monthly weather insights without having to recompute aggregations.

In addition, 2 columns; `STATE` and `LOAD_DATE` are added to the tables to allow consumers to filter records by location and to allow engineers to track load history.

## Data Quality
In discovering the BOM weather dataset, several data quality issues were identified.

1. Missing station information in the station datset that exist in the weather dataset.
2. Duplicated weather datasets across multiple state folders. (e.g., Weather dataset measured at Albury Airport exists in both NSW and VIC folders)
3. Measurement errors in weather measurement columns. (e.g., Maximum temperature being larger than minimum temperature)

To address above issues and ensure data quality, multiple validation layers have been implemented in the data workflow.

- **stage_data** <br>
In the stage data step, dataset records are deduplicated and records with measurement errors are disregarded.
- **generate_dbt_model** <br>
dbt schema files are generated with respective test cases for weather tables.
- **incremental_data_load** <br>
dbt test cases are conducted for table columns to ensure their values are valid. 
- **reconcile_data** <br>
Row counts between staging schema and weather schemas are compared.

Likewise, data integrity has been achieved by having adequate data validation layers in the workflow.

## CI/CD
[![Build status](https://badge.buildkite.com/0175b2e6dff7302d0c6758615c301b240ae19df549d603864b.svg)](https://buildkite.com/personal-176/weather-analytics-platform)

A CI/CD pipeline is built on Buildkite to conduct unit tests on the preprocessing steps for the weather and station datasets. When the pipeline build is executed, the following steps are taken in the local agent deployed on Docker:

1. Read pipeline.yml from the repository (by default the repository is cloned in the agent).
2. Install dependencies via pip.
3. Conduct unit tests using test datasets

Scripts can be found at:
- Buildkite dependencies: [buildkite/](https://github.com/TravisH0301/weather_analytics_platform/tree/main/buildkite)
- Unit test dependencies: [tests/](https://github.com/TravisH0301/weather_analytics_platform/tree/main/tests)


## Consideration
- **Object storage lifecycle** <br>
Lifecycle can be set up in the object storage by either archiving or deleting old contents in the bucket to reserve storage cost. 

- **Query optimisation** <br>
When the data volume grows, clustering in Snowflake can be considered to enhance query performance. Clustering allows records to be co-located within micro-partitions and enables worker nodes to skip irrelevant records. Columns that are often used for filtering or join are good candidates for the clustering key. On the other hand, appropriate compute resource size or "warehouse" should be considered when usage and data volume grow.

- **DevOps** <br>
The scope of the CI/CD only covers a unit test. For future improvement, different test types, such as integration test or regression test, can be considered to ensure robust continuous integration of the data product.

## Data Source
[Bureau of Meteorology FTP Public Products](http://www.bom.gov.au/catalogue/anon-ftp.shtml)