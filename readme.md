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
leverage Snowflake's distributed computing power, and its own comprehensive testing framework.
- **Airflow** <br>
Airflow orchestrates Python and dbt processes. And it is used for its robustness in
scheduling and orachestrating data processes.
- **Docker** <br>
Docker is used to host both Airflow and MinIO object storage.

## Data Process
<img src="./images/airflow dag.png" width="800">

As shown on the Airflow dag diagram above, there are 5 data processes defined.
- **land_file** <br>
This process fetches the compressed BOM weather dataset from the BOM
server via FTP and loads into the MinIO object storage.
- **stage_data** <br>
This process extracts weather and station datasets from the
compressed BOM dataset file in the object storage. And this pre-processes and
loads the datasets into the Snowflake staging schema.
- **generate_dbt_model** <br>
Based on the available years in the preprocessed weather dataset
from the Snowflake staging schema, this process generates yearly partitioned tables for
weather measurements (e.g., rain_2023) with respective dbt data model & schema files (e.g., rain_2023.sql & rain_2023.yml). 
This script allows the dataset to grow incrementally without having to manually create new table nor dbt data model scripts.
- **incremental_data_load** <br>
This loads data to the data models incrementally, and refreshes
the aggregated data model.
- **reconcile_data** <br>
This process reconciles the row counts between staging schema and
weather measurement schemas in Snowflake to ensure data integrity.

Scripts can be found at:
- Airflow dag script: [airflow/dags](https://github.com/TravisH0301/weather_analytics_platform/tree/main/airflow/dags)
- Python scripts: [airflow/dags/scripts](https://github.com/TravisH0301/weather_analytics_platform/tree/main/airflow/dags/scripts)
- dbt scripts: [airflow/dags/dbt](https://github.com/TravisH0301/weather_analytics_platform/tree/main/airflow/dags/dbt)

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


## Consideration


## Data Source
[Bureau of Meteorology FTP Public Products](http://www.bom.gov.au/catalogue/anon-ftp.shtml)