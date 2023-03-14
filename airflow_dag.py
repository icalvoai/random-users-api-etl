from airflow import DAG
from airflow.decorators import task, dag
from pendulum import datetime
import requests

@dag(
    start_date=datetime(2022,11,1),
    schedule="@daily",
    catchup=False
)

def docs_example_dag():

    @task()
    def tell_me_what_to_do():
        response = requests.get("https://www.boredapi.com/api/activity")
        return response.json()["activity"]

    tell_me_what_to_do()

docs_example_dag()