PySpark Union and UnionAll:

PySpark union() and unionAll() transformations are used to merge two or more DataFrame’s of the same schema or structure.
Dataframe union() – union() method of the DataFrame is used to merge two DataFrame’s of the same structure/schema. The output includes all rows from both DataFrames and duplicates are retained. If schemas are not the same it returns an error. To deal with the DataFrames of different schemas we need to use unionByName() transformation.

Syntax

dataFrame1.union(dataFrame2)

DataFrame unionAll() – unionAll() is deprecated since Spark “2.0.0” version and replaced with union().

Syntax

dataFrame1.unionAll(dataFrame2)

PySpark unionByName():

The pyspark.sql.DataFrame.unionByName() to merge/union two DataFrames with column names. In PySpark you can easily achieve this using unionByName() transformation, this function also takes param allowMissingColumns with the value True if you have a different number of columns on two DataFrames.

Following is the syntax of the unionByName()

# unionByName() Syntax

unionByName(df, allowMissingColumns=True)

Difference between PySpark uionByName() vs union()
The difference between unionByName() function and union() is that this function
resolves columns by name (not by position). In other words, unionByName() is used to merge two DataFrames by column names instead of by position.

unionByName() also provides an argument allowMissingColumns to specify if you have a different column counts.
