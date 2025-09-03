from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

# Cron expression
# https://en.wikipedia.org/wiki/Cron#CRON_expression
# * * * * *
# minutes hour day_of_the_month month day_of_the_week
# examples:
# tous les jours à minuit: 0 0 * * * (@daily)
# tous les semaines les jeudi à 14:48: 48 14 * * 4
# tous les 2 de chaque mois à 5:39: 39 5 2 * *

with DAG("scheduling_every_15_minutes", schedule="*/15 * * * *") as dag:
    task_minute = EmptyOperator(task_id="every_15_minutes")

# be careful of timezone
with DAG("scheduling_twice_per_day", schedule="15 10,15 * * *") as dag:
    task_5_minutes = EmptyOperator(task_id="twice_per_day")

with DAG("scheduling_every_hour", schedule="30 * * * *") as dag:
    task_5_minutes = EmptyOperator(task_id="every_hour")

with DAG(
    "scheduling_first_day_of_the_month", schedule="10 0 1 * *", catchup=False
) as dag:
    task_first_day = EmptyOperator(task_id="first_day")

with DAG("scheduling_first_wednesday_of_the_month", schedule="10 10 * * WED#1") as dag:
    task_first_wed = EmptyOperator(task_id="first_wednesday")

with DAG("scheduling_first_monday_of_the_month", schedule="0 0 * * MON#1") as dag:
    task_first_mon = EmptyOperator(task_id="first_monday")
