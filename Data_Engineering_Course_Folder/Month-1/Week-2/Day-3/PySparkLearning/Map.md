PySpark map() Transformation:

PySpark map (map()) is an RDD transformation that is used to apply the transformation function (lambda) on every element of RDD/DataFrame and returns a new RDD.

RDD map() transformation is used to apply any complex operations like adding a column, updating a column, or transforming the data, etc; the output of map transformations would always have the same number of records as the input.

Note1: DataFrame doesn’t have map() transformation to use with DataFrame; hence, you need to convert DataFrame to RDD first.
Note 2: If you have a heavy initialization, use PySpark mapPartitions() transformation instead of map(); as with mapPartitions(), heavy initialization executes only once for each partition instead of every record.

# Syntax

map(f, preservesPartitioning=False)
