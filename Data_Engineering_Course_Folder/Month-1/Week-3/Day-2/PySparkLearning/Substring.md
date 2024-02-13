Pyspark – Get substring() from a column:

In PySpark, the substring() function is used to extract the substring from a DataFrame string column by providing the position and length of the string you wanted to extract.

Using SQL function substring()
Using the substring() function of pyspark.sql.functions module we can extract a substring or slice of a string from the DataFrame column by providing the position and length of the string you wanted to slice.

Syntax
substring(str, pos, len)
Note: Please note that the position is not zero based, but 1 based index.