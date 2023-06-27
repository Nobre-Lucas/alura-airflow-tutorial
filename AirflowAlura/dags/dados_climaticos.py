from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add
import pendulum

from os.path import join
import pandas as pd

with DAG(
    "dados_climaticos",
    start_date=pendulum.datetime(2023, 5, 29, tz="UTC"),
    schedule_interval="0 0 * * 1",
) as dag:
    cria_pasta = BashOperator(
        task_id="cria_pasta",
        bash_command='mkdir -p "/home/NobreLucas/Documents/TutoriaisCursos/Alura/FormacaoApacheAirflow/AirflowAlura/semana={{data_interval_end.strftime("%Y-%m-%d")}}"',
    )

    def extrai_dados(data_interval_end):
        city = "Boston"
        key = "2EW9SPRT5SSUSPTGMCFX82LLN"

        URL = join(
            "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
            f"{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv",
        )

        dados = pd.read_csv(URL)

        file_path = f"/home/NobreLucas/Documents/TutoriaisCursos/Alura/FormacaoApacheAirflow/AirflowAlura/semana={data_interval_end}/"

        dados.to_csv(file_path + "dados_brutos.csv")
        dados[["datetime", "tempmin", "temp", "tempmax"]].to_csv(
            file_path + "temperaturas.csv"
        )
        dados[["datetime", "description", "icon"]].to_csv(file_path + "condicoes.csv")

    extrai_dados = PythonOperator(
        task_id="extrai_dados",
        python_callable=extrai_dados,
        op_kwargs={"data_interval_end": '{{data_interval_end.strftime("%Y-%m-%d")}}'},
    )
    
    cria_pasta >> extrai_dados
