from airflow.models import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Ivan Calvo',
    'start_date': days_ago(1) # make start date in the past
}

#defining the dag object
dag = DAG(
    dag_id='random-us-people',
    default_args=args,
    schedule_interval='@daily' #to make this workflow happen every day
)

#assigning the task for our dag to do
with dag:

    # scripts folder
    SCRIPTS_FOLDER = '/home/icalvo/Documents/random-users-api-etl'
    

    # EXTRACT ------
    RESULTS = 100
    URL = "https://randomuser.me/api/"
    EXTRACT_OUTPUT_FOLDER = SCRIPTS_FOLDER + "/data/raw"
    
    extract_command = f"python '{SCRIPTS_FOLDER}/1-extract.py' --url {URL} --n_results {RESULTS} --output_folder {EXTRACT_OUTPUT_FOLDER}"
    extract = BashOperator(
        task_id="extract_data",
        bash_command=extract_command
    )
    

    # TRANSFORM ------
    TRANSFORM_INPUT_FOLDER = EXTRACT_OUTPUT_FOLDER
    TRANSFORM_OUTPUT_FOLDER = SCRIPTS_FOLDER + "/data/stagging"

    transform_command = f"python '{TRANSFORM_INPUT_FOLDER}/2-transform.py' --input_folder {TRANSFORM_INPUT_FOLDER} --output_folder {TRANSFORM_OUTPUT_FOLDER}"
    transform = BashOperator(
        task_id="transform_data",
        bash_command=transform_command,
        trigger_rule='all_success'
    )

    # LOAD ------
    LOAD_INPUT_FOLDER = TRANSFORM_OUTPUT_FOLDER
    CREDENTIALS_FILE = SCRIPTS_FOLDER + "/credentials.json"
    TARGET_TABLE = "random_people"

    load_command = f"python '{SCRIPTS_FOLDER}/1-extract.py' --input_folder {LOAD_INPUT_FOLDER} --credentials_file {CREDENTIALS_FILE} --target_table {TARGET_TABLE}"
    load = BashOperator(
        task_id="load_data",
        bash_command=load_command,
        trigger_rule='all_success'

    )

    extract >> transform >> load