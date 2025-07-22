# Food Prices Monitoring Pipeline

## Overview
This project automates the collection, transformation, storage, and visualization of food prices data from Kenyan open data sources and World Bank’s Microdata API. The pipeline is orchestrated using Apache Airflow hosted on an AWS EC2 instance, with PostgreSQL for data warehousing and Grafana for real-time visualization.

### a. Architecture

The snapshot below shows the project architecture.

![Project Architecture Diagram](https://res.cloudinary.com/depbmpoam/image/upload/v1753192160/Screenshot_2025-07-21_160237_yxyiav.png)

### b. Project File Structure

```graphql
FoodPricesMonitoring/  # Root directory
│
├── airflow/                
│   ├── dags/
│   └── airflow.cfg
|   └── airflow.db
└── README.md                 
```

### c. Schema Diagram

The data warehouse follows a star schema, the database structure is as shown below

![Database Diagram](https://res.cloudinary.com/depbmpoam/image/upload/v1753192803/Screenshot_2025-07-21_160903_srdqtw.png)

**Fact Table:** fact_prices. Holds all PRIMARY KEYs and prices

**Dimension tables:** dim_date, dim_market, dim_commodity, dim_pricetype

## Project Setup
### 1. Clone the project
To clone this project, run the following commands to do so.

> NOTE: For Airflow, run `airflow db init` to initialize a new instance of `airflow.cfg`, Airflow's config file and `airflow.db` a SQLite database instance. This project doesn't have these files as they have private information.

```bash
git clone https://github.com/dkkinyua/FoodPricesMonitoring
cd FoodMonitoringPipeline
```

### 2. Install and activate a virtual environment 
To install required dependencies, run the following command in your terminal

> NOTE: For Windows users, run Airflow on Windows Subsystem for Linux (WSL), as Airflow doesn't natively support Windows.

```bash
python3 -m venv myenv
source myenv/bin/activate # MacOS/Linux
myenv\Scripts\activate # Windows

pip install -r requirements.txt
```

### 3. Setup Airflow
Run the following commands to setup Airflow for this project

#### a. Set `AIRFLOW_HOME`.

Run the following command to set `AIRFLOW_HOME` where our Airflow configuration will reside at 

```bash
export AIRFLOW_HOME=/path/to/FoodMonitoringPipeline/airflow
```

#### Initialize Airflow config files
Run the following command to initialize these files into `/path/to/FoodMonitoringPipeline/airflow`

```bash
airflow db init
```

#### b. Create Admin user 
Run the following command to create an Admin user to access the Airflow UI.

```bash
airflow users create --firstname first --lastname last --username user_name --email email@example.com --role Admin
```

#### c. Set settings in `airflow.cfg`
Set the following settings by running:

```bash
nano airflow/airflow.cfg
```
The settings are:

```ini
load_settings=False # default True

[database]
sqlalchemy_conn_url = POSTGRES_URL # default SQLite Conn string 

```
Save the file and migrate the changes to the new connected PostgreSQL database

#### d. Migrate changes to the database
Run the following command to save these changes and also update the migrated database to Postgres.

```bash
airflow db migrate
```

#### e. Run the webserver and scheduler

Run the following commands to initialize Airflow's webserver and scheduler

```bash
airflow webserver & airflow scheduler
```

The Airflow UI will start on port `8080`, go to `localhost:8080` to access.

The snapshot below shows a successful DAG run.
![DAG Run Snapshot](https://res.cloudinary.com/depbmpoam/image/upload/v1753216531/Screenshot_2025-07-18_153448_zqwilo.png)

