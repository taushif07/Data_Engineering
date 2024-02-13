PySpark lit() – Add Literal or Constant to DataFrame:

PySpark SQL functions lit() and typedLit() are used to add a new column to DataFrame by assigning a literal or constant value. Both these functions return Column type as return type. typedLit() provides a way to be explicit about the data type of the constant value being added to a DataFrame, helping to ensure data consistency and type correctness of PySpark workflows.

Both of these are available in PySpark by importing pyspark.sql.functions

lit() Function to Add Constant Column
PySpark lit() function is used to add constant or literal value as a new column to the DataFrame.

Creates a [[Column]] of literal value. The passed in object is returned directly if it is already a [[Column]]. If the object is a Scala Symbol, it is converted into a [[Column]] also. Otherwise, a new [[Column]] is created to represent the literal value
