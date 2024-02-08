PySpark Groupby:

Similar to SQL GROUP BY clause, PySpark groupBy() function is used to collect the identical data into groups on DataFrame and perform count, sum, avg, min, max functions on the grouped data.

GroupBy() Syntax & Usage

Following is the syntax of the groupby

# Syntax

DataFrame.groupBy(*cols)
#or
DataFrame.groupby(*cols)

When we perform groupBy() on PySpark Dataframe, it returns GroupedData object which contains below aggregate functions.

FUNCTION DEFINITION
count() Use groupBy() count() to return the number of rows for each group.
mean() Returns the mean of values for each group.
max() Returns the maximum of values for each group.
min() Returns the minimum of values for each group.
sum() Returns the total for values for each group.
avg() Returns the average for values for each group.
agg() Using groupBy() agg() function, we can calculate more than one aggregate at a time.
pivot() This function is used to Pivot the DataFrame, which I will not cover in this article as I already have a dedicated article for Pivot & Unpivot DataFrame.
