PySpark Count Distinct from DataFrame:

In PySpark, you can use distinct().count() of DataFrame or countDistinct() SQL function to get the count distinct.

distinct() eliminates duplicate records(matching all columns of a Row) from DataFrame, count() returns the count of records on DataFrame. By chaining these you can get the count distinct of PySpark DataFrame.

countDistinct() is a SQL function that could be used to get the count distinct of the selected multiple columns.

1. Using DataFrame distinct() and count()
On the above DataFrame, we have a total of 10 rows and one row with all values duplicated, performing distinct counts ( distinct().count() ) on this DataFrame should get us 9.

2. Using countDistinct() SQL Function
DataFrame distinct() returns a new DataFrame after eliminating duplicate rows (distinct on all columns). if you want to get count distinct on selected multiple columns, use the PySpark SQL function countDistinct(). This function returns the number of distinct elements in a group.