# Pandas tosql() method fix for Databricks

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=fff)](#)
[![version](https://img.shields.io/badge/version-1.0.0-blue)](#)
[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/)

Fix for the Pandas to_sql() dataframe method that fails when we try pushing more than 256 values to a Databricks table.

**Table of Contents**

- [Installation](#installation)
- [Execution / Usage](#execution--usage)
- [Contributing](#contributing)
- [License](#license)

## Installation

```sh
python -m pip install pandas-tosql-dbx-fix
```

## Execution / Usage

Once the package in installed, you can use the code here to get started with the pandas-tosql-dbx-fix library in your code:

```python
import os
import pandas_tosql_dbx_fix as pdx

# Use your own values for the following variables
server = os.getenv("DATABRICKS_SERVER_HOSTNAME", "False")
hpath = os.getenv("DATABRICKS_HTTP_PATH", "False")
catalog = os.getenv("CATALOG", "False")
schema = os.getenv("SCHEMA", "False")
token = os.getenv("DATABRICKS_TOKEN", "False")
table_name = "to_sql_table"
test_table_rows = 100

extra_connect_args = {
    "user_agent_entry": "Tarek's workaround to avoid the _user_agent_entry warning message",
}

df = pdx.create_test_dataframe(test_table_rows)

# You can also connect to Databricks using a token with the pdx.connect_to_dbx_pat() function, or by creating your  own SQLAlchemy engine.
db_con = pdx.connect_to_dbx_oauth(
        server, hpath, catalog, schema, extra_connect_args
    )

pdx.to_sql_dbx(
            df,
            db_con,
            f"{catalog}.{schema}.{table_name}",
            if_exists="append",
        )
```

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