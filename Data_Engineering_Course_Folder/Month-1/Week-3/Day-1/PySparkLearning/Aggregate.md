PySpark Aggregate Functions:

PySpark provides built-in standard Aggregate functions defined in DataFrame API, these come in handy when we need to make aggregate operations on DataFrame columns. Aggregate functions operate on a group of rows and calculate a single return value for every group.

All these aggregate functions accept input as, Column type or column name in a string and several other arguments based on the function and return Column type.

When possible try to leverage standard library as they are little bit more compile-time safety, handles null and perform better when compared to UDF’s. If your application is critical on performance try to avoid using custom UDF at all costs as these are not guarantee on performance.

PySpark Aggregate Functions
PySpark SQL Aggregate functions are grouped as “agg_funcs” in Pyspark. Below is a list of functions defined under this group.

approx_count_distinct Aggregate Function
In PySpark approx_count_distinct() function returns the count of distinct items in a group.

avg (average) Aggregate Function
avg() function returns the average of values in the input column.

collect_list Aggregate Function
collect_list() function returns all values from an input column with duplicates.

collect_set Aggregate Function
collect_set() function returns all values from an input column with duplicate values eliminated.

countDistinct Aggregate Function
countDistinct() function returns the number of distinct elements in a columns

count function
count() function returns number of elements in a column.

grouping function
grouping() Indicates whether a given input column is aggregated or not. returns 1 for aggregated or 0 for not aggregated in the result. If you try grouping directly on the salary column you will get below error.

first function
first() function returns the first element in a column when ignoreNulls is set to true, it returns the first non-null element.

last function
last() function returns the last element in a column. when ignoreNulls is set to true, it returns the last non-null element.

kurtosis function
kurtosis() function returns the kurtosis of the values in a group.

max function
max() function returns the maximum value in a column.