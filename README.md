# Adressing-Consistency-on-Pandas-PySpark-Polars
This repository is created to expose a method to ensure consistency on data:
  - Data consistency is the accuracy, completeness, and correctness of data stored in a database. The same data across all related systems, applications, and databases is when we say that data is consistent. Inconsistent data can lead to incorrect analysis, decision-making, and outcomes.

SQL use ACID rules in order to mantain clean and integral data, but, when using Pandas or PySpark or Polars, how can we imitate those great properties that SQL have? Well, look at these functions:
- Pandas: assert_frame_equal() (https://pandas.pydata.org/docs/reference/api/pandas.testing.assert_frame_equal.html#pandas.testing.assert_frame_equal)
- Polars: assert_frame_equal() (https://docs.pola.rs/docs/python/version/0.18/reference/api/polars.testing.assert_frame_equal.html)
- Spark version >=3.5.0 : assertDataFrameEqual() (https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.testing.assertDataFrameEqual.html)
##### These functions were created to make unit tests.

### Basically, what these functions do is compare two dataframes and check:
- Index class,
- dtypes / columns class,
- DataFrame class,
- values.

So, these tools can help us a lot to confirm that our pipelines are working correectly. These kind of tests should be included before any ingestion of data on production databases.

### Explanation of this excercise
a) Read a dataframe and extract only some columns:
- str, int, double and bool values were selected.

b) Create some dummy transformations for every column:
- Multiply all numeric columns * 2.
- Delete the letter "e" from all str columns.
- Set all bool variables to True.
Create 3 extra numeric columns:
- Mean of purchases.
- Median of age.
- Mean of credit_score.

c) Create a function that pipeline these past transformations and make an assertion.
- For this, is necesary to create a result or expected dataframe.
- Tip: only generate assertions on sample, otherwise, its impossible to generate expected dataframe results for every pipeline you want to unit test.

### Extra
Data were stored in a local PostgreSQL database and then read it with Python.
