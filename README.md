# Autompg-Pipeline
## Overview
This project is an end-to-end ETL pipeline that takes cleaned CSV data and loads it into a PostgreSQL database in an automated, reliable and configurable way.
The pipeline ensures that:

* Data is always clean and structured before entering the database.

* PostgreSQL tables remain consistent with the CSV source.

* Database operations are secure and logged for transparency.

This assignment simulates a real-world data engineering workflow where raw datasets are cleaned, transformed and stored in a relational database for analysis, reporting, or further processing.

## Tech Stack

Programming Language: Python (pandas, logging, dotenv)

Database: PostgreSQL

ORM/DB Connector: SQLAlchemy + psycopg2

Tools:
* pgAdmin → To query and validate data

* dotenv → To securely manage credentials and configs
