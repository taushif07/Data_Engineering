PySpark withColumnRenamed to Rename Column on DataFrame

Use PySpark withColumnRenamed() to rename a DataFrame column, we often need to rename one column or multiple (or all) columns on PySpark DataFrame, you can do this in several ways. When columns are nested it becomes complicated.
Since DataFrame’s are an immutable collection, you can’t rename or update a column instead when using withColumnRenamed() it creates a new DataFrame with updated column names

PySpark withColumnRenamed – To rename DataFrame column name
PySpark has a withColumnRenamed() function on DataFrame to change a column name. This is the most straight forward approach; this function takes two parameters; the first is your existing column name and the second is the new column name you wish for

PySpark withColumnRenamed() Syntax:

withColumnRenamed(existingName, newNam)