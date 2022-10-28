#!/usr/bin/env bash
airflow db init

airflow users create --username admin --password admin --firstname admin --lastname admin --role Admin -e admin@admin.com
