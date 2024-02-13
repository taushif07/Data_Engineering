PySpark Window Functions:

PySpark Window functions are used to calculate results, such as the rank, row number, etc., over a range of input rows.

Window Functions
PySpark Window functions operate on a group of rows (like frame, partition) and return a single value for every input row. PySpark SQL supports three kinds of window functions:

ranking functions
analytic functions
aggregate functions

The table below defines Ranking and Analytic functions; for aggregate functions, we can use any existing aggregate functions as a window function.

To operate on a group, first, we need to partition the data using Window.partitionBy() , and for row number and rank function, we need to additionally order by on partition data using orderBy clause.

WINDOW FUNCTIONS USAGE & SYNTAX PYSPARK WINDOW FUNCTIONS DESCRIPTION
row_number() Returns a sequential number starting from 1 within a window partition
rank() Returns the rank of rows within a window partition, with gaps.
percent_rank() Returns the percentile rank of rows within a window partition.
dense_rank() Returns the rank of rows within a window partition without any gaps. Where as Rank() returns rank with gaps.
ntile(n) Returns the ntile id in a window partition
cume_dist() Returns the cumulative distribution of values within a window partition
lag(e, offset)
lag(columnname, offset)
lag(columnname, offset, defaultvalue) returns the value that is `offset` rows before the current row, and `null` if there is less than `offset` rows before the current row.
lead(columnname, offset)
lead(columnname, offset)
lead(columnname, offset, defaultvalue) returns the value that is `offset` rows after the current row, and `null` if there is less than `offset` rows after the current row.

PySpark Window Ranking functions
PySpark’s Window Ranking functions, like row_number(), rank(), and dense_rank(), assign sequential numbers to DataFrame rows based on specified criteria within defined partitions. These functions enable sorting and ranking operations, identifying row positions in partitions based on specific orderings.

The row_number() assigns unique sequential numbers, rank() provides the ranking with gaps, and dense_rank() offers ranking without gaps. They’re valuable in selecting top elements within groups and bottom elements within groups, facilitating analysis of data distributions, and identifying the highest or lowest values within partitions in PySpark DataFrames.

2. PySpark Window Ranking functions
   PySpark’s Window Ranking functions, like row_number(), rank(), and dense_rank(), assign sequential numbers to DataFrame rows based on specified criteria within defined partitions. These functions enable sorting and ranking operations, identifying row positions in partitions based on specific orderings.

2.1
row_number Window Function
row_number() window function gives the sequential row number starting from 1 to the result of each window partition.

2.2 rank Window Function
rank() window function provides a rank to the result within a window partition. This function leaves gaps in rank when there are ties.

2.3 dense_rank Window Function
dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps. This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.

2.4 percent_rank Window Function

2.5 ntile Window Function
ntile() window function returns the relative rank of result rows within a window partition. In the below example we have used 2 as an argument to ntile hence it returns ranking between 2 values (1 and 2)

3. PySpark Window Analytic Functions

   3.1 cume_dist Window Function
   cume_dist(): This function computes the cumulative distribution of a value within a window partition. It calculates the relative rank of a value within the partition. The result ranges from 0 to 1, where a value of 0 indicates the lowest value in the partition, and 1 indicates the highest. It’s useful for understanding the distribution of values compared to others within the same partition.

3.2 lag Window Function
This is the same as the LAG function in SQL. The lag() function allows you to access a previous row’s value within the partition based on a specified offset. It retrieves the column value from the previous row, which can be helpful for comparative analysis or calculating differences between consecutive rows.

3.3 lead Window Function
This is the same as the LEAD function in SQL. Similar to lag(), the lead() function retrieves the column value from the following row within the partition based on a specified offset. It helps in accessing subsequent row values for comparison or predictive analysis.

4. PySpark Window Aggregate Functions
   PySpark’s window aggregate functions, such as sum(), avg(), and min(), compute aggregated values within specified window partitions. These functions perform calculations across rows related to each row in the partition, enabling cumulative or comparative analyses within specific groupings.

They allow computation of running totals, averages, minimums, or other aggregations over defined windows, assisting in tasks like calculating moving averages, cumulative sums, or identifying extreme values within subsets of data in PySpark DataFrames. These functions offer insights into data trends, patterns, and statistical summaries within specified groupings or orderings.
