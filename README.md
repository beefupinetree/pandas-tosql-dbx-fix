# Pandas to_sql() method fix for Databricks
[![version](https://img.shields.io/badge/version-1.0.2-blue)](#)
[![PyPI version](https://badge.fury.io/py/pandas-tosql-dbx-fix.svg)](https://badge.fury.io/py/pandas-tosql-dbx-fix)
[![MIT License](https://img.shields.io/badge/License-MIT-gold.svg)](https://choosealicense.com/licenses/mit/)

Fix for the Pandas `to_sql()` dataframe method that fails when we try pushing more than 255 values to a Databricks table.

**Table of Contents**

- [Installation](#installation)
- [Execution / Usage](#execution--usage)
- [Background and core issue](#background-and-core-issue)
- [What this package does differently](#what-this-package-does-differently)
- [Contributing](#contributing)
- [License](#license)

## Installation

```sh
python -m pip install pandas-tosql-dbx-fix
```

## Execution / Usage

Once the package in installed, you can use the code here to get started with the pandas-tosql-dbx-fix library:

```python
import os
import pandas_tosql_dbx_fix as pdx

# Use your own values for the following variables
server = "YOUR_DATABRICKS_SERVER_HOSTNAME"
hpath = "YOUR_DATABRICKS_HTTP_PATH"
catalog = "YOUR_CATALOG_NAME"
schema = "YOUR_SCHEMA_NAME"
# token = "YOUR_DATABRICKS_TOKEN" # if needed

table_name = "to_sql_table"
test_table_rows = 1000

df = pdx.create_test_dataframe(test_table_rows)

# You can also connect to Databricks using a token with the pdx.connect_to_dbx_pat() function,
# or by creating your own SQLAlchemy engine.
db_con = pdx.connect_to_dbx_oauth(
        server, hpath, catalog, schema
    )

# The function takes the same arguments as the to_sql() method in Pandas
pdx.to_sql_dbx(
            df,
            db_con,
            f"{catalog}.{schema}.{table_name}",
            if_exists="append",
        )
```

## Background and core issue
The Pandas `to_sql()` function worked well with Databricks up until November 2023 with the [3.0.0 release](https://github.com/databricks/databricks-sql-python/releases/tag/v3.0.0) of the *Databricks SQL Connector* for Python. This release introduced [Native Parameters](https://github.com/databricks/databricks-sql-python/blob/main/docs/parameters.md#using-native-parameters) which replaced a value in a SQL query to a parameter. Here is an example of the same `to_sql()` command that writes one row into a table, and what the query looks like when it's sent to Databricks before and after implementing native parameters:

Before 3.0.0 and native parameters:
>`INSERT INTO table (col_1, col_2) VALUES (100, 250)`

After 3.0.0 and native parameters:
>`INSERT INTO table (col_1, col_2) VALUES (:value1, :value2)`
---
The issue with the 'Native Parameter' change is that these parameters are [limited to 255 in a single query execution](https://github.com/databricks/databricks-sql-python/blob/main/docs/parameters.md#when-to-use-inline-parameters). Which ultimately means that the Pandas `to_sql()` function will fail when trying to insert more than 255 values into a Databricks table. Not 255 rows, but 255 *values*. This practically renders the `to_sql()`function unusable.

## What this package does differently
The `pandas-tosql-dbx-fix` package isolates the Pandas code responsible for building and executing the SQL queries when `to_sql()` is called. The main difference is that instead of building the SQL query and sending it to Databricks, the package compiles the query using the Databricks dialect first, *and then* sends it out to a Databricks SQL warehouse. Compiling the query replaces the parameters (ex: :value1, :value2) with their real values. This removes parameters from the query entirely and bypasses the 255 parameter limit. You can find this [here](https://github.com/beefupinetree/pandas-tosql-dbx-fix/blob/e6adbb80d58aca38ccf42327611e22b945c8d336/src/pandas_tosql_dbx_fix/main.py#L174) in the code.

I also changed the way the source dataframe gets broken up into chunks, since I started running into issues once a single SQL query tried to insert more than 950,000 values at once. I added some logic [here](https://github.com/beefupinetree/pandas-tosql-dbx-fix/blob/e6adbb80d58aca38ccf42327611e22b945c8d336/src/pandas_tosql_dbx_fix/main.py#L144) that limits each SQL query to 900,000 values. Each chunk now contains `n_cols * n_rows <= 900,000`.

## Contributing

To contribute to the development of pandas-tosql-dbx-fix, follow the steps below:

1. Fork pandas-tosql-dbx-fix from <https://github.com/beefupinetree/pandas-tosql-dbx-fix>
2. Create your feature branch (`git checkout -b feature-new`)
3. Make your changes
4. Commit your changes (`git commit -am 'Add some new feature'`)
5. Push to the branch (`git push origin feature-new`)
6. Create a new pull request

## License

pandas-tosql-dbx-fix is distributed under the MIT license. See [`LICENSE`](LICENSE.md) for more details.
