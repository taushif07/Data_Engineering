PySpark foreach():

PySpark foreach() is an action operation that is available in RDD, DataFram to iterate/loop over each element in the DataFrmae, It is similar to for with advanced concepts. This is different than other actions as foreach() function doesn’t return a value instead it executes the input function on each element of an RDD, DataFrame

Following is the syntax of the foreach() function

# Syntax

DataFrame.foreach(f)

PySpark foreach() Usage

When foreach() applied on PySpark DataFrame, it executes a function specified in for each element of DataFrame. This operation is mainly used if you wanted to manipulate accumulators, save the DataFrame results to RDBMS tables, Kafka topics, and other external sources.

Syntax

# Syntax
RDD.foreach(f: Callable[[T], None]) → None