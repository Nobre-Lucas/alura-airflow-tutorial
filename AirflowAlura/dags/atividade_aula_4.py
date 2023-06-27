from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def cumprimentos():
    print("Boas-vindas ao Airflow")


with DAG(
    dag_id="atividade_aula_4", start_date=days_ago(1), schedule_interval="@daily"
) as dag:
    cumprimentar = PythonOperator(task_id="cumprimentar", python_callable=cumprimentos)
    responder = BashOperator(task_id="responder", bash_command='echo "Muito obrigado!"')

    cumprimentar >> responder
