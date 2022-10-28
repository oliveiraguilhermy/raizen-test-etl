# Data Engineer Test
This repository was created to show my solution for the ETL test proposed by raizen.

## Arquitetura:
Airflow was used to orchestrate the pipline and BigQuery for storage.


To run this project you will need:
- Project on Google Cloud
- Enable BigQuery API
- Create a dataset in BigQuery
- Create a service account in IAM with BigQuery User permissions
- Configure an environment variable in airflow with the service account key

## Dag:
![model](/src/dag.png)



## Sales of oil derivative fuels table schema and preview

![model](/src/fuels.png) ![model](/src/fuels_preview.png)

## Sales of diesel table schema and preview

![model](/src/diesel.png) ![model](/src/diesel_preview.png)