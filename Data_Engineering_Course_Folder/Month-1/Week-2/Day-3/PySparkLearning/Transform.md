PySpark transform():

PySpark provides two transform() functions one with DataFrame and another in pyspark.sql.functions.

pyspark.sql.DataFrame.transform() – Available since Spark 3.0
pyspark.sql.functions.transform()

Following is the syntax of the pyspark.sql.DataFrame.transform() function

# Syntax

DataFrame.transform(func: Callable[[…], DataFrame], \*args: Any, \*\*kwargs: Any) → pyspark.sql.dataframe.DataFrame
The following are the parameters:

func – Custom function to call.
*args – Arguments to pass to func.
*kwargs – Keyword arguments to pass to func.

I have created the three custom transformations to be applied to the DataFrame. These transformations are nothing but Python functions that take the DataFrame apply some changes and return the new DataFrame.
Ezoic

to_upper_str_columns() – This function converts the CourseName column to upper case and updates the same column.
reduce_price() – This function takes the argument and reduces the value from the fee and creates a new column.
apply_discount() – This creates a new column with the discounted fee.
