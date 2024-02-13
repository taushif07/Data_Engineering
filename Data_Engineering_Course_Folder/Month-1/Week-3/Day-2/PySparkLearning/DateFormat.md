PySpark date_format() – Convert Date to String format:

In PySpark use date_format() function to convert the DataFrame column from Date to String format. In this tutorial, we will show you a Spark SQL example of how to convert Date to String format using date_format() function on DataFrame.

date_format() – function formats Date to String format. This function supports all Java Date formats specified in DateTimeFormatter.

Following are Syntax and Example of date_format() Function:

Syntax: date_format(column,format)
Example: date_format(current_timestamp(),"yyyy MM dd").alias("date_format")
