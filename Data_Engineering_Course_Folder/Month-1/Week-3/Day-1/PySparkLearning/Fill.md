PySpark fillna() & fill():

In PySpark, DataFrame.fillna() or DataFrameNaFunctions.fill() is used to replace NULL/None values on all or selected multiple DataFrame columns with either zero(0), empty string, space, or any constant literal values.

While working on PySpark DataFrame we often need to replace null values since certain operations on null values return errors. Hence, we need to graciously handle nulls as the first step before processing. Also, while writing to a file, it’s always best practice to replace null values, not doing this results in nulls on the output file.

As part of the cleanup, sometimes you may need to Drop Rows with NULL/None Values in PySpark DataFrame and Filter Rows by checking IS NULL/NOT NULL conditions.

PySpark fillna() & fill() Syntax
PySpark provides DataFrame.fillna() and DataFrameNaFunctions.fill() to replace NULL/None values. These two are aliases of each other and returns the same results.

fillna(value, subset=None)
fill(value, subset=None)

value – Value should be the data type of int, long, float, string, or dict. Value specified here will be replaced with NULL/None values.
subset – This is optional, when used it should be the subset of the column names where you wanted to replace NULL/None values.

PySpark Replace NULL/None Values with Zero (0)
PySpark fill(value:Long) signatures that are available in DataFrameNaFunctions is used to replace NULL/None values with numeric values either zero(0) or any constant value for all integer and long datatype columns of PySpark DataFrame or Dataset.
