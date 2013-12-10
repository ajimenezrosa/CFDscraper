CFDscraper
==========

Scrapes financial web tables and writes them to a MySQL database.
Written with SQLalchemy-core to help ensure database portability.

Algorithm:

Create a database connection using initial parameters.
Output a metadata and connection object.

Using bootstrap_table as a guide, create tables in the db if they are missing.

Using bootstrap_table as a guide, fill list_of_rows A from the database:
    Iterate bootstrap table:
    If a table is empty in the database:
        Fill list of rows with an empty placeholder (None).
    If a table has data, fill list_of_rows A with last data from the database.
        Where there are tables with no rows, enter NONE into list_of_rows A.

Loop:
    Fill list_of_rows B from the web using the bootstrap table as guide.
    Make list_of_rows C by comparing list_of_rows A and list_of_rows B.
    Write list_of_rows C to the database.
    list_of_rows A = list_of_rows B (as a copy?)
    Wait some time, check for interrupt.


Data Structures (These are not classes, only examples):

data_row:
    This contains the data loaded from the db, or from the web. It can be one
    of three different kinds which are indistinguishable: Old, news or changed.
    Form: [table_name_string, column_name_1, column_value_1, ...
        column_name_n, column_value_n ]
    Example: ["US_10_Year_Bond", "time", "12-1-13-12:00:00", "price", 2.425]

list_of_rows:
    Simply a list of data_row entries.

table_df:
    This is a disposable table scraped from the web and turned into a pandas
    Dataframe. I use it because being able to look up data by column and row
    makes things easy for loading into a list of rows. Its exact form is not
    important because it only exists inside of one function. (probably)

bootstrap_table:
    A list of tuples to specify everything needed to create tables and scrape
    the web. By convention, I will always be making the first column the index
    in the database and the first column the one that is checked for changes.
    This will probably always be datetime. The dtypes are sql dtypes, not
    python dtypes.

*   bootstrap = (db_table_name,
                ((column1_name, column1_dtype, web_row_string, web_col_string),
                (column2_name, column2_dtype, web_row_string, web_col_string)))

*   The columns are always in bootstrap_table[][1].
    The total number of columns is given by len(bootstrap_table[x][1])

Some rules:
    Use no ORMs. They might be too slow. SQLAlchemy core only.
    Speed test everything.
    Always rely on the bootstrap_table to unambiguously determine table form.
    Thus, table reflection is off limits.
    So as not to incure costs of converting from SQL datetime, consider an
    INT time format such as SQL UNIX_TIMESTAMP. (Will this work with pandas?)
    Important: is SQL UNIX_TIMESTAMP the same as the dreaded TIMESTAMP that
    corrects the time depending on what the system time zone is?
    Utilize executemany where ever possible. It might be faster than individual
    inserts.

Regarding Python3 and MySQL support:
http://simon04.net/2013/03/python3-mysql/
pymysql is drop in replacement?
https://pypi.python.org/pypi/PyMySQL/0.6.1
Did: conda install pymysql

Because of a hang problem, installing the head from github:
https://github.com/PyMySQL/PyMySQL/issues/136
pip install --upgrade https://github.com/PyMySQL/PyMySQL/tarball/master
this will no longer be needed after 0.6.1

Create n-column tables via a parameterized SQLalchemy-core method:
http://stackoverflow.com/questions/2574105/
sqlalchemy-dynamic-mapping/2575016#2575016

http://stackoverflow.com/questions/2580497
/database-on-the-fly-with-scripting-languages/2580543#2580543

