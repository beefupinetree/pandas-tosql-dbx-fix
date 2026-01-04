import warnings
import pandas as pd
from typing import Literal
from databricks.sqlalchemy.base import DatabricksDialect
from pandas._typing import DtypeArg, IndexLabel
import sqlalchemy
import time

# Fix for the Pandas to_sql() dataframe method that fails when we try pushing more than 256 values.
# TODO: Try packaging it instead


def to_sql_dbx(
    frame,
    sql_connection,
    name: str,
    schema: str | None = None,
    if_exists: Literal["fail", "replace", "append"] = "fail",
    index: bool = False,
    index_label: IndexLabel | None = None,
    chunksize: int | None = None,
    dtype: DtypeArg | None = None,
) -> int | None:
    """Insert data directly into a Databricks delta table from on-prem Windows or Linux.

    Args:
        frame (Dataframe): Pandas dataframe
        sql_connection (Engine): A SQL Alchemy Engine conected to Databricks
        name (str): Full name of the Databricks table. Must include the names of: catalog.schema.table
        schema (str | None, optional): Name of SQL schema in database to write to (if database flavor supports this). If None, use default schema (default).
        if_exists (str | [fail , replace, append], optional): Action to take if the table we're writing to already exists. Defaults to "fail".
        index (bool, optional): Isn't supported by Databricks. Should the dataframe index be included when inserting the data into Databricks. Defaults to False.
        index_label (IndexLabel | None, optional): Isn't supported by Databricks. Defaults to None.
        chunksize (int | None, optional): Number of rows to be inserted at a time. Defaults to None. Limited to 900,000 per insert statement
        dtype (DtypeArg | None, optional): _description_. Defaults to None.

    Returns:
        None: Returns -1 on success.
    """

    if if_exists not in ("fail", "replace", "append"):
        raise ValueError(f"'{if_exists}' is not valid for if_exists")

    if isinstance(frame, pd.Series):
        frame = frame.to_frame()
    elif not isinstance(frame, pd.DataFrame):
        raise NotImplementedError(
            "'frame' argument should be either a Series or a DataFrame"
        )

    # Check that the table name includes a catalog and schema in the format: catalog.schema.table
    name_parts = name.split(".")
    if len(name_parts) != 3:
        raise ValueError(
            f"The name argument you've provided in invalid '{name}', it needs to include the catalog, schema, and table name. Ex: catalog.schema.table_name"
        )

    # check whether the SQL Alchemy connection includes a catalog and schema
    if (
        sql_connection.dialect.catalog != name_parts[0]
        or sql_connection.dialect.schema != name_parts[1]
    ):
        _catalog = name_parts[0]
        _schema = name_parts[1]

    pandas_sql = pd.io.sql.pandasSQL_builder(
        sql_connection, schema=schema, need_transaction=True
    )

    table = pd.io.sql.SQLTable(
        name_parts[2],
        frame=frame,
        pandas_sql_engine=pandas_sql,
        index=index,
        if_exists=if_exists,
        index_label=index_label,
        dtype=dtype,
    )
    # The create() method handles all 3 options for the 'if_exists' argument
    table.create()

    keys, data_list = table.insert_data()
    nrows = len(table.frame)

    if nrows == 0:
        warnings.warn(
            "Your dataframe is empty. No actions were taken.",
            stacklevel=4,
        )
        return 0

    # Tarek: Based on what I've seen, the query fails when we try to insert around 940,000 data points at once.
    # I set the limit to 900,000 to be safe
    datapoint_limit = 900000
    ncols = len(table.frame.columns)
    ndatapoints = nrows * ncols

    # Change the number of chunks and their size if we are pushing more than 900,000 data points.
    # The data will be divided into chunks where each chunk contains 900,000 data points at most.
    if chunksize is None:
        if ndatapoints >= datapoint_limit:
            chunks = (ndatapoints // datapoint_limit) + 1
            chunksize = datapoint_limit // ncols
        else:
            chunksize = nrows
            chunks = 1
    elif chunksize == 0:
        raise ValueError("chunksize argument should be non-zero")
    elif chunksize < 0:
        raise ValueError("chunksize argument should be greater than zero")
    # Manual override of my 900,000 limit to the chunk-size if the user chooses their own chunksize argument.
    else:
        if (chunksize * ncols) > datapoint_limit:
            warnings.warn(
                f"The chunk size you have selected {chunksize:,} might not work. Set it to 'None' if you run into issues, and let the code handle the size.",
                stacklevel=4,
            )
        chunks = (nrows // chunksize) + 1

    with table.pd_sql.run_transaction() as conn:
        for i in range(chunks):
            start_i = i * chunksize
            end_i = min((i + 1) * chunksize, nrows)
            if start_i >= end_i:
                break
            chunk_iter = zip(*(arr[start_i:end_i] for arr in data_list))
            data = [dict(zip(keys, row)) for row in chunk_iter]
            stmt = sqlalchemy.insert(table.table).values(data)
            # Using the databricks dialect here is essential to compile the query string in a way that avoids syntax errors.
            compiled_stmt = stmt.compile(
                compile_kwargs={"literal_binds": True}, dialect=DatabricksDialect()
            )
            # the standard process is to execute the stmt directly without compiling it
            _ = conn.execute(compiled_stmt)


if __name__ == "__main__":
    server = "YOUR OWN DATABRICKS SERVER"
    hpath = "YOUR OWN WAREHOUSE"
    catalog = "YOUR CATALOG NAME"
    schema = "YOUR SCHEMA NAME"
    table = "to_sql_table"

    # Extra arguments are passed untouched to databricks-sql-connector
    # See src/databricks/sql/thrift_backend.py for complete list
    # You can change the 'auth_type' to 'pat' if you want to use personal access tokens, but you will need to change the connection string to add the token there.
    # Ex: https://github.com/databricks/databricks-sqlalchemy/blob/6b80531e9ff008b59edc5d53bc2e4466f3fa5489/sqlalchemy_example.py#L61
    extra_connect_args = {
        "user_agent_entry": "Tarek's workaround to avoid the _user_agent_entry warning message",
        "auth_type": "databricks-oauth",
    }

    db_con = sqlalchemy.create_engine(
        url=f"databricks://{server}?"
        + f"http_path={hpath}&catalog={catalog}&schema={schema}",
        connect_args=extra_connect_args,
    )

    # Create a test dataframe from a dictionary with a column name as the key and a range as the value
    # Change the number of rows in the test dataframe by changing the variable 'n'
    n = 1000
    data = {
        "RangeColumn1": range(0, n),
    }

    # Create the DataFrame
    df = pd.DataFrame(data)

    tic = time.time()
    # Push the dataframe to Databricks
    to_sql_dbx(
        df,
        db_con,
        f"{catalog}.{schema}.{table}",
        if_exists="append",
    )
    toc = time.time()
    print(f"Wrote {n} rows to {catalog}.{schema}.{table} in {toc - tic:,.1f} seconds")
