PySpark Random Sample:

PySpark provides a pyspark.sql.DataFrame.sample(), pyspark.sql.DataFrame.sampleBy(), RDD.sample(), and RDD.takeSample() methods to get the random sampling subset from the large dataset

PySpark SQL sample() Usage & Examples
PySpark sampling (pyspark.sql.DataFrame.sample()) is a mechanism to get random sample records from the dataset, this is helpful when you have a larger dataset and wanted to analyze/test a subset of the data for example 10% of the original file.

Below is the syntax of the sample() function.

sample(withReplacement, fraction, seed=None)

fraction – Fraction of rows to generate, range [0.0, 1.0]. Note that it doesn’t guarantee to provide the exact number of the fraction of records.

seed – Seed for sampling (default a random seed). Used to reproduce the same random sampling.

withReplacement – Sample with replacement or not (default False).

Stratified sampling in PySpark
You can get Stratified sampling in PySpark without replacement by using sampleBy() method. It returns a sampling fraction for each stratum. If a stratum is not specified, it takes zero as the default.

sampleBy() Syntax

sampleBy(col, fractions, seed=None)
col – column name from DataFrame

fractions – It’s Dictionary type takes key and value.

PySpark RDD Sample
PySpark RDD also provides sample() function to get a random sampling, it also has another signature takeSample() that returns an Array[T].

RDD sample() Syntax & Example

PySpark RDD sample() function returns the random sampling similar to DataFrame and takes a similar types of parameters but in a different order.

sample() of RDD returns a new RDD by selecting random sampling. Below is a syntax.

sample(self, withReplacement, fraction, seed=None)

RDD takeSample() Syntax & Example

RDD takeSample() is an action hence you need to careful when you use this function as it returns the selected sample records to driver memory. Returning too much data results in an out-of-memory error similar to collect().

Syntax of RDD takeSample() .

takeSample(self, withReplacement, num, seed=None)
