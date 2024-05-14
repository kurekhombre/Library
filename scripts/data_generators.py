from typing import List, Tuple
from datetime import timedelta

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
    