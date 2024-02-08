In RDD, you can create parallelism at the time of the creation of an RDD using parallelize(), textFile() and wholeTextFiles().

<-------------------------------------------------------------------------------------------->
PySpark Broadcast Variables:

In PySpark RDD and DataFrame, Broadcast variables are read-only shared variables that are cached and available on all nodes in a cluster in-order to access or use by the tasks. Instead of sending this data along with every task, PySpark distributes broadcast variables to the workers using efficient broadcast algorithms to reduce communication costs.

Use case
Let me explain with an example when to use broadcast variables, assume you are getting a two-letter country state code in a file and you wanted to transform it to full state name, (for example CA to California, NY to New York e.t.c) by doing a lookup to reference mapping. In some instances, this data could be large and you may have many such lookups (like zip code e.t.c).

Instead of distributing this information along with each task over the network (overhead and time consuming), we can use the broadcast variable to cache this lookup info on each machine and tasks use this cached info while executing the transformations.

How does PySpark Broadcast work?
Broadcast variables are used in the same way for RDD, DataFrame.
When you run a PySpark RDD, DataFrame applications that have the Broadcast variables defined and used, PySpark does the following.

PySpark breaks the job into stages that have distributed shuffling and actions are executed with in the stage.
Later Stages are also broken into tasks
Spark broadcasts the common data (reusable) needed by tasks within each stage.
The broadcasted data is cache in serialized format and deserialized before executing each task.

You should be creating and using broadcast variables for data that shared across multiple stages and tasks.

Note that broadcast variables are not sent to executors with sc.broadcast(variable) call instead, they will be sent to executors when they are first used.

How to create Broadcast variable
The PySpark Broadcast is created using the broadcast(v) method of the SparkContext class. This method takes the argument v that you want to broadcast.

In PySpark shell

broadcastVar = sc.broadcast(Array(0, 1, 2, 3))
broadcastVar.value
<------------------------------------------------------->

PySpark Accumulator:

The PySpark Accumulator is a shared variable that is used with RDD and DataFrame to perform sum and counter operations similar to Map-reduce counters. These variables are shared by all executors to update and add information through aggregation or computative operations.

What is PySpark Accumulator?
Accumulators are write-only and initialize once variables where only tasks that are running on workers are allowed to update and updates from the workers get propagated automatically to the driver program. But, only the driver program is allowed to access the Accumulator variable using the value property.

How to create Accumulator variable in PySpark?
Using accumulator() from SparkContext class we can create an Accumulator in PySpark programming. Users can also create Accumulators for custom types using AccumulatorParam class of PySpark.

Some points to note..

sparkContext.accumulator() is used to define accumulator variables.
add() function is used to add/update a value in accumulator
value property on the accumulator variable is used to retrieve the value from the accumulator.
We can create Accumulators in PySpark for primitive types int and float. Users can also create Accumulators for custom types using AccumulatorParam class of PySpark.