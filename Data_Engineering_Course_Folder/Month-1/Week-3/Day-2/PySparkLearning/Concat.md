PySpark – Convert array column to a String:

In this PySpark article, I will explain how to convert an array of String column on DataFrame to a String column (separated or concatenated with a comma, space, or any delimiter character) using PySpark function concat_ws() (translates to concat with separator), and with SQL expression using Scala example.

When curating data on DataFrame we may want to convert the Dataframe with complex struct datatypes, arrays and maps to a flat structure. here we will see how to convert array type to string type.

Convert an array of String to String column using concat_ws()
In order to convert array to a string, PySpark SQL provides a built-in function concat_ws() which takes delimiter of your choice as a first argument and array column (type Column) as the second argument.

Syntax

concat_ws(sep, \*cols)
Usage

In order to use concat_ws() function, you need to import it using pyspark.sql.functions.concat_ws . Since this function takes the Column type as a second argument, you need to use col().
