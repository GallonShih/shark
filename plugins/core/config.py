# -*- coding: utf-8 -*-

"""
Define all configurations of DAGs.

If you need to create a new dag, please follow below format,
and import your config to main dag file.

The following three parameters should be contained in configs:

1. dag_owner_name: The owner of the DAG.
2. dag_id: The dag name shown in frontend. The naming of this colums MUST follow the rule:
    a. Title: Start with `ETL`, `TAG`, `CLEAN`, or `OP`. If this dag will contain two or more tasks,
       use dot to separate tasks. (e.g. ETL.TAG)
    b. Content: Explain briefly and concisely what this dag has done.
    c. Use `-` to separate Title and Content. (e.g. ETL.TAG-do_something_task)
3. schedule_interval: cronjob format of executing frequency

History:
2022/09/05 Created by Gallon
"""

### Operation Related Definition

AIRFLOW_LOG_CLEANUP = {
    "dag_owner_name": "operations",
    "dag_id": "OP.CLEAN-airflow_log_cleanup",
    "schedule_interval": "@daily"
}

AIRFLOW_DB_CLEANUP = {
    "dag_owner_name": "operations",
    "dag_id": "OP.CLEAN-airflow_db_cleanup",
    "schedule_interval": "@daily"
}

### ONLY ETL Related Definition

### ETL and TAG Related Definition