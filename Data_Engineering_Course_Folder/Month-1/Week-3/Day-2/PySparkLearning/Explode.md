PySpark – explode nested array into rows:

Problem: How to explode & flatten nested array (Array of Array) DataFrame columns into rows using PySpark.

Solution: PySpark explode function can be used to explode an Array of Array (nested Array) ArrayType(ArrayType(StringType)) columns to rows on PySpark DataFrame using python example.

Now, let’s explode “subjects” array column to array rows. after exploding, it creates a new column ‘col’ with rows represents an array.
If you want to flatten the arrays, use flatten function which converts array of array columns to a single array on DataFrame.
