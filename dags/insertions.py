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
    def __init__(self, locale="PL-pl", rows_numb=5) -> None:
        self.fake = Faker(locale)
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
            'Name': [self.fake.name().replace('pan ', '').replace('pani ', '') for _ in range(self.rows_numb)],
            'Address': [self.fake.address().replace('\n', ' - ') for _ in range(self.rows_numb)]
        }
        return pd.DataFrame(users_data)
    
class AuthorDataGenerator(DataGenerator):
    def generate_data(self):
        authors_data = {
            'Name': [self.fake.name().replace('pan ', '').replace('pani ', '') for _ in range(self.rows_numb)],
            'Address': [self.fake.address().replace('\n', ' - ') for _ in range(self.rows_numb)]
        }
        return pd.DataFrame(authors_data)

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
            'Author': [np.random.choice(self.author_ids) if self.author_ids else self.fake.name() for _ in range(self.rows_numb)],
            'Publisher': [np.random.choice(self.publisher_ids) if self.publisher_ids else self.fake.name() for _ in range(self.rows_numb)],
            'Category': [np.random.choice(self.category_ids) if self.category_ids else self.fake.name() for _ in range(self.rows_numb)]
        }
        return pd.DataFrame(books_data)

class PublisherDataGenerator(DataGenerator):
    def generate_data(self):
        publishers_data = {
            'Name': [' '.join(random.sample(self.fake.words(), 2)).capitalize() for _ in range(self.rows_numb)],
        }
        return pd.DataFrame(publishers_data)

class CategoryDataGenerator(DataGenerator):
    def generate_data(self):
        categories_data = {
            'Name': [' '.join(random.sample(self.fake.words(), 2)).capitalize() for _ in range(self.rows_numb)],
        }
        return pd.DataFrame(categories_data)

class RatingDataGenerator(DataGenerator):
    def __init__(self, locale='pl_PL', rows_numb=5, user_ids=None, book_ids=None):
        super().__init__(locale, rows_numb)
        self.user_ids = user_ids or []
        self.book_ids = book_ids or []

    def generate_data(self):
        ratings_data = {
            'UserID': [np.random.choice(self.user_ids) if self.user_ids else self.fake.name() for _ in range(self.rows_numb)],
            'BookID': [np.random.choice(self.book_ids) if self.book_ids else self.fake.name() for _ in range(self.rows_numb)],
            'Rate': [random.randint(1,5) for _ in range(self.rows_numb)],
            'Comment': [' '.join(random.sample(self.fake.words(), 2)).capitalize() for _ in range(self.rows_numb)]
        }
        return pd.DataFrame(ratings_data)

class RentalDataGenerator(DataGenerator):
    def __init__(self, locale='en_US', rows_numb=5, user_ids=None, book_ids=None):
        super().__init__(locale, rows_numb)
        self.user_ids = user_ids or []
        self.book_ids = book_ids or []

    def generate_data(self):
        rental_dates = [self.fake.date_between(start_date='-2y', end_date='today') for _ in range(self.rows_numb)]
        return_date = [self.calculate_return_date(rd) for rd in rental_dates]

        rentals_data = {
            'UserID': [np.random.choice(self.user_ids) for _ in range(self.rows_numb)],
            'BookID': [np.random.choice(self.book_ids) for _ in range(self.rows_numb)],
            'RentalDate': rental_dates,
            'ReturnDate': return_date
        }
        return pd.DataFrame(rentals_data)

    def calculate_return_date(self, rental_date):
        duration = random.choice([14, 21, 28])
        return rental_date + timedelta(days=duration)
    
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