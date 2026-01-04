import time
from functools import wraps
import pandas as pd
import sqlalchemy
import os
import pytest

try:
    from pandas_tosql_dbx_fix.main import to_sql_dbx
except ModuleNotFoundError:
    from src.pandas_tosql_dbx_fix.main import to_sql_dbx

# My environment variables are declared in a .env file in the package's root directory
# so VS Code can read them, and in my ~/.bashrc file so pytest can access them from here.
server = os.getenv("DATABRICKS_SERVER_HOSTNAME", "False")
hpath = os.getenv("DATABRICKS_HTTP_PATH", "False")
catalog = os.getenv("CATALOG", "False")
schema = os.getenv("SCHEMA", "False")
token = os.getenv("DATABRICKS_TOKEN", "False")
table = "to_sql_table"
# Extra arguments are passed untouched to databricks-sql-connector
# See src/databricks/sql/thrift_backend.py for complete list
# You can change the 'auth_type' to 'pat' if you want to use personal access tokens, but you will need to change the connection string to add the token there.
# Ex: https://github.com/databricks/databricks-sqlalchemy/blob/6b80531e9ff008b59edc5d53bc2e4466f3fa5489/sqlalchemy_example.py#L61
extra_connect_args = {
    "user_agent_entry": "Tarek's workaround to avoid the _user_agent_entry warning message",
}
# print("Yes")


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
            f"Function {func.__name__!r} wrote {kwargs['n_rows']:,.0f} rows to {catalog}.{schema}.{table} in {elapsed_time:,.1f} seconds"
        )
        return result

    return wrapper_timer


def create_test_dataframe(n_rows: int):
    data = {
        "RangeColumn1": range(0, n_rows),
    }
    return pd.DataFrame(data)


def connect_to_dbx_oauth():
    extra_connect_args["auth_type"] = "databricks-oauth"

    return sqlalchemy.create_engine(
        url=f"databricks://{server}?"
        + f"http_path={hpath}&catalog={catalog}&schema={schema}",
        connect_args=extra_connect_args,
    )


def connect_to_dbx_pat():
    extra_connect_args["auth_type"] = "pat"

    return sqlalchemy.create_engine(
        url=f"databricks://token:{token}@{server}?"
        + f"http_path={hpath}&catalog={catalog}&schema={schema}",
        connect_args=extra_connect_args,
    )


@timer
@pytest.mark.parametrize("n_rows", [1, 100, 1000000])
def test_push_df_pat(n_rows: int):
    db_con = connect_to_dbx_pat()
    df = create_test_dataframe(n_rows)
    assert (
        to_sql_dbx(
            df,
            db_con,
            f"{catalog}.{schema}.{table}",
            if_exists="append",
        )
        == -1
    )
