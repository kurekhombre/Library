from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator

def run_stored_procedure():
    hook = MsSqlHook(mssql_conn_id="airflow_mssql")
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("EXEC MoveOldRentsToHistory")
        conn.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
    finally:
        cursor.close()
        conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'run_sql_server_procedure',
    default_args=default_args,
    description='DAG to run a SQL Server stored procedure',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='run_mssql_stored_procedure',
        python_callable=run_stored_procedure,
    )

t1
