import time
from functools import wraps
import os
import pytest

try:
    from pandas_tosql_dbx_fix.main import to_sql_dbx as dbx
except ModuleNotFoundError:
    from src.pandas_tosql_dbx_fix.main import to_sql_dbx as dbx

# My environment variables are declared in a .env file in the package's root directory
# so VS Code can read them, and in my ~/.bashrc file so pytest can access them from here.
server = os.getenv("DATABRICKS_SERVER_HOSTNAME", "False")
hpath = os.getenv("DATABRICKS_HTTP_PATH", "False")
catalog = os.getenv("CATALOG", "False")
schema = os.getenv("SCHEMA", "False")
token = os.getenv("DATABRICKS_TOKEN", "False")
table_name = "to_sql_table"
# Extra arguments are passed untouched to databricks-sql-connector
# See src/databricks/sql/thrift_backend.py for complete list
extra_connect_args = {
    "user_agent_entry": "Tarek's workaround to avoid the _user_agent_entry warning message",
}


# Timer decorator
def timer(func):
    """A decorator to measure the execution time of a function."""

    @wraps(func)
    def wrapper_timer(*args, **kwargs):
        tic = time.perf_counter()
        result = func(*args, **kwargs)
        toc = time.perf_counter()
        elapsed_time = toc - tic
        print(
            f"Function {func.__name__!r} wrote {kwargs['n_rows']:,.0f} rows to {catalog}.{schema}.{table_name} in {elapsed_time:,.1f} seconds"
        )
        return result

    return wrapper_timer


@timer
@pytest.mark.parametrize("n_rows", [1, 100, 1000000])
def test_push_df_pat(n_rows: int):
    db_con = dbx.connect_to_dbx_pat(
        server, hpath, catalog, schema, token, extra_connect_args
    )
    df = dbx.create_test_dataframe(n_rows)
    assert (
        dbx.to_sql_dbx(
            df,
            db_con,
            f"{catalog}.{schema}.{table_name}",
            if_exists="append",
        )
        == -1
    )
