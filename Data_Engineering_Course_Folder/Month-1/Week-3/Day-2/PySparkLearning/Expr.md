PySpark SQL expr():

PySpark expr() is a SQL function to execute SQL-like expressions and to use an existing DataFrame column value as an expression argument to Pyspark built-in functions. Most of the commonly used SQL functions are either part of the PySpark Column class or built-in pyspark.sql.functions API, besides these PySpark also supports many other SQL functions, so in order to use these, you have to use expr() function.

Below are 2 use cases of PySpark expr() funcion.

First, allowing to use of SQL-like functions that are not present in PySpark Column type & pyspark.sql.functions API. for example CASE WHEN, regr_count().
Second, it extends the PySpark SQL Functions by allowing to use DataFrame columns in functions for expression.

1. PySpark expr() Syntax
   Following is syntax of the expr() function.

expr(str)
expr() function takes SQL expression as a string argument, executes the expression, and returns a PySpark Column type. Expressions provided with this function are not a compile-time safety like DataFrame operations.

2. PySpark SQL expr() Function Examples
   Below are some of the examples of using expr() SQL function.

2.1 Concatenate Columns using || (similar to SQL)
If you have SQL background, you pretty much familiar using || to concatenate values from two string columns, you can use expr() expression to do exactly same.
