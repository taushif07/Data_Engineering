PySpark apply Function to Column:

How to apply a function to a column in PySpark? By using withColumn(), sql(), select() you can apply a built-in function or custom function to a column. In order to apply a custom function, first you need to create a function and register the function as a UDF. Recent versions of PySpark provide a way to use Pandas API hence, you can also use pyspark.pandas.DataFrame.apply().
