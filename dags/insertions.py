import random
from datetime import datetime, timedelta
import sys
import os

from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from data_generators import UserDataGenerator, AuthorDataGenerator, BookDataGenerator, PublisherDataGenerator, CategoryDataGenerator, RatingDataGenerator, RentalDataGenerator
from db_fetcher import DatabaseFetcher


### ---
#  Connection AIRFLOW_MSSQL
from airflow import settings
from airflow.models import Connection
conn = Connection(
        conn_id='airflow_mssql',
        conn_type='mssql',
        host='db',
        login='sa',
        password='YourStrongPassw0rd',
        port='1433'
) 
session = settings.Session() 
session.add(conn)
session.commit() 
### ---
    
dag_params = {
    'dag_id': 'test',
    'start_date': datetime(2024,5,5),
    'schedule_interval': '@daily',
    'default_args': {
        'owner': 'Karol',
        'retries': 0,
        'retry_delay': timedelta(minutes=1)
    }

}


with DAG(**dag_params) as dag:
    @dag.task(task_id="insert_book_data_mssql_task")
    def insert_book_data_mssql_task():
        fetcher = DatabaseFetcher(mssql_conn_id="airflow_mssql", schema="projects")

        author_ids = fetcher.fetch_ids("library_db.Authors", "ID")
        publisher_ids = fetcher.fetch_ids("library_db.Publishers", "ID")
        category_ids = fetcher.fetch_ids("library_db.Categories", "ID")

        book_generator = BookDataGenerator(locale='pl_PL', rows_numb=100, 
                                        author_ids=author_ids, 
                                        publisher_ids=publisher_ids, 
                                        category_ids=category_ids)

        rows = book_generator.get_data_as_tuples()

        target_fields = book_generator.get_target_fields()

        mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="projects")
        mssql_hook.insert_rows(table="library_db.Books", rows=rows, target_fields=target_fields)

    @dag.task(task_id="insert_user_data_mssql_task")
    def insert_user_data_mssql_task():
        mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="projects")
        user_generator = UserDataGenerator(locale='pl_PL', rows_numb=100)
        rows = user_generator.get_data_as_tuples()
        target_fields = user_generator.get_target_fields()
        mssql_hook.insert_rows(table="library_db.Users", rows=rows, target_fields=target_fields)

    @dag.task(task_id="insert_author_data_mssql_task")
    def insert_author_data_mssql_task():
        mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="projects")
        author_generator = AuthorDataGenerator(locale='pl_PL', rows_numb=100)
        rows = author_generator.get_data_as_tuples()
        target_fields = author_generator.get_target_fields()
        mssql_hook.insert_rows(table="library_db.Authors", rows=rows, target_fields=target_fields)

    @dag.task(task_id="insert_publisher_data_mssql_task")
    def insert_publisher_data_mssql_task():
        mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="projects")
        publisher_generator = PublisherDataGenerator(locale='pl_PL', rows_numb=100)
        rows = publisher_generator.get_data_as_tuples()
        target_fields = publisher_generator.get_target_fields()
        mssql_hook.insert_rows(table="library_db.Publishers", rows=rows, target_fields=target_fields)

    @dag.task(task_id="insert_category_data_mssql_task")
    def insert_category_data_mssql_task():
        mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="projects")
        category_generator = CategoryDataGenerator(locale='pl_PL', rows_numb=100)
        rows = category_generator.get_data_as_tuples()
        target_fields = category_generator.get_target_fields()
        mssql_hook.insert_rows(table="library_db.Categories", rows=rows, target_fields=target_fields)

    @dag.task(task_id="insert_rating_data_mssql_task")
    def insert_rating_data_mssql_task():
        fetcher = DatabaseFetcher(mssql_conn_id="airflow_mssql", schema="projects")

        user_ids = fetcher.fetch_ids("library_db.Users", "ID")
        book_ids = fetcher.fetch_ids("library_db.Books", "ID")

        rating_generator = RatingDataGenerator(locale='pl_PL', rows_numb=100, 
                                        user_ids=user_ids, 
                                        book_ids=book_ids)

        rows = rating_generator.get_data_as_tuples()

        target_fields = rating_generator.get_target_fields()

        mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="projects")
        mssql_hook.insert_rows(table="library_db.Ratings", rows=rows, target_fields=target_fields)

    @dag.task(task_id="insert_rental_data_mssql_task")
    def insert_rental_data_mssql_task():
        fetcher = DatabaseFetcher(mssql_conn_id="airflow_mssql", schema="projects")

        user_ids = fetcher.fetch_ids("library_db.Users", "ID")
        book_ids = fetcher.fetch_ids("library_db.Books", "ID")

        rental_generator = RentalDataGenerator(locale='pl_PL', rows_numb=100, 
                                        user_ids=user_ids, 
                                        book_ids=book_ids)

        rows = rental_generator.get_data_as_tuples()

        target_fields = rental_generator.get_target_fields()

        mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="projects")
        mssql_hook.insert_rows(table="library_db.Rents", rows=rows, target_fields=target_fields)

    insert_user_data_mssql_task() >> insert_author_data_mssql_task() >> insert_publisher_data_mssql_task() >> insert_category_data_mssql_task() >> insert_book_data_mssql_task() >> insert_rating_data_mssql_task() >> insert_rental_data_mssql_task()