import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from typing import List, Tuple
import random
import pandas as pd
import numpy as np
from faker import Faker

class DataGenerator:
    def __init__(self, lang="PL-pl", rows_numb=5) -> None:
        self.fake = Faker(lang)
        self.rows_numb = rows_numb
        self.data = None

    def generate_data(self):
        raise NotImplementedError("Subclasses must implement `generate_data` method!")
    
    def get_data_as_tuples(self, df: pd.DataFrame = None) -> List[Tuple]:
        """
        Convert a Pandas DataFrame into a list of tuples.
        """
        if df is None:
            df = self.generate_data()
        self.data = df
        return [tuple(x) for x in df.to_records(index=False)]
    
    def get_target_fields(self) -> List[str]:
        return list(self.data.columns)

class UserDataGenerator(DataGenerator):
    def generate_data(self):
        users_data = {
            # 'ID': np.arange(1, self.rows_numb + 1),
            'Name': [self.fake.name().replace('pan ', '').replace('pani ', '') for _ in range(self.rows_numb)],
            'Address': [self.fake.address().replace('\n', ' - ') for _ in range(self.rows_numb)]
        }
        return pd.DataFrame(users_data)

class BookDataGenerator(DataGenerator):
    def __init__(self, locale='pl_PL', rows_numb=5, author_ids=None, publisher_ids=None, category_ids=None):
        super().__init__(locale, rows_numb)
        self.author_ids = author_ids or []
        self.publisher_ids = publisher_ids or []
        self.category_ids = category_ids or []

    def generate_data(self):
        books_data = {
            'ISBN': [self.fake.isbn13().replace('-', '') for _ in range(self.rows_numb)],
            'Title': [' '.join(random.sample(self.fake.words(), 2)).capitalize() for _ in range(self.rows_numb)],
            # 'Author': np.random.choice(authors_data['ID'], rows_numb),
            # 'Publisher': np.random.choice(publishers_data['ID'], rows_numb),
            # 'Category': np.random.choice(categories_df['ID'], rows_numb)
            'Author': [self.fake.name() for _ in range(self.rows_numb)],
            'Publisher': [self.fake.name() for _ in range(self.rows_numb)],
            'Category': [self.fake.name() for _ in range(self.rows_numb)]
        }
        return pd.DataFrame(books_data)

###---
class DatabaseFetcher:
    def __init__(self, mssql_conn_id="airflow_mssql", schema="projects"):
        self.mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema=schema)

    def ensure_table_exists(self, table_name, create_table_sql):
        """Ensures that the specified table exists."""
        check_sql = f"IF OBJECT_ID('{table_name}', 'U') IS NULL {create_table_sql}"
        self.mssql_hook.run(check_sql)

    def fetch_ids(self, table_name, column_name):
        """Fetches all IDs from the specified table and column."""
        sql = f"SELECT {column_name} FROM {table_name}"
        return [row[0] for row in self.mssql_hook.get_records(sql)]
    
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
        mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="projects")
        book_generator = BookDataGenerator()
        fetcher = DatabaseFetcher()

        author_ids = fetcher.fetch_ids("library_db.Authors", "ID")
        publisher_ids = fetcher.fetch_ids("library_db.Publishers", "ID")
        category_ids = fetcher.fetch_ids("library_db.Categories", "ID")
        print("AUTHOR IDS")
        print(author_ids)
        print("AUTHOR IDS ENDS")
        rows = book_generator.get_data_as_tuples()
        print(rows)
        # INSERT INTO library_db.Books ("ISBN", "Title", "Author", "Publisher", "Category") VALUES (<rows>);

        # target_fields = ["ISBN", "Title", "Author", "Publisher", "Category"]
        target_fields = book_generator.get_target_fields()
        mssql_hook.insert_rows(table="library_db.Books", rows=rows, target_fields=target_fields)

    @dag.task(task_id="insert_user_data_mssql_task")
    def insert_user_data_mssql_task():
        mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="projects")
        user_generator = UserDataGenerator()
        rows = user_generator.get_data_as_tuples()
        print(rows)

        # target_fields = ["Name", "Address"]
        target_fields = user_generator.get_target_fields()
        mssql_hook.insert_rows(table="library_db.Users", rows=rows, target_fields=target_fields)


    insert_book_data_mssql_task() >> insert_user_data_mssql_task()