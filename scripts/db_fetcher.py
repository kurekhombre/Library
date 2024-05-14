from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

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