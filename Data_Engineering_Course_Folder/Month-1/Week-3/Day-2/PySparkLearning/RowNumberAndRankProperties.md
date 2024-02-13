WINDOW FUNCTIONS USAGE & SYNTAX PYSPARK WINDOW FUNCTIONS DESCRIPTION
row_number() Returns a sequential number starting from 1 within a window partition
rank() Returns the rank of rows within a window partition, with gaps.
percent_rank() Returns the percentile rank of rows within a window partition.
dense_rank() Returns the rank of rows within a window partition without any gaps. Where as Rank() returns rank with gaps.

2. PySpark Window Ranking functions
   PySpark’s Window Ranking functions, like row_number(), rank(), and dense_rank(), assign sequential numbers to DataFrame rows based on specified criteria within defined partitions. These functions enable sorting and ranking operations, identifying row positions in partitions based on specific orderings.

The row_number() assigns unique sequential numbers, rank() provides the ranking with gaps, and dense_rank() offers ranking without gaps. They’re valuable in selecting top elements within groups and bottom elements within groups, facilitating analysis of data distributions, and identifying the highest or lowest values within partitions in PySpark DataFrames.

2.1 row_number Window Function
row_number() window function gives the sequential row number starting from 1 to the result of each window partition.

2.2 rank Window Function
rank() window function provides a rank to the result within a window partition. This function leaves gaps in rank when there are ties.

2.3 dense_rank Window Function
dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps. This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.

2.4 percent_rank Window Function
