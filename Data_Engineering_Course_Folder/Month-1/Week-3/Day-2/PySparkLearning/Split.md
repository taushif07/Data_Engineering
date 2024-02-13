PySpark Convert String to Array Column:

PySpark SQL provides split() function to convert delimiter separated String to an Array (StringType to ArrayType) column on DataFrame. This can be done by splitting a string column based on a delimiter like space, comma, pipe e.t.c, and converting it into ArrayType.

Split() function syntax
PySpark SQL split() is grouped under Array Functions in PySpark SQL Functions class with the below syntax.

pyspark.sql.functions.split(str, pattern, limit=-1)
The split() function takes the first argument as the DataFrame column of type String and the second argument string delimiter that you want to split on. You can also use the pattern as a delimiter. This function returns pyspark.sql.Column of type Array.
