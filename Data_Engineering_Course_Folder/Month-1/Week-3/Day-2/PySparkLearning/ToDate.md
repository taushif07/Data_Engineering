PySpark to_date() – Convert Timestamp to Date:

PySpark functions provide to_date() function to convert timestamp to date (DateType), this ideally achieved by just truncating the time part from the Timestamp column. In this tutorial, I will show you a PySpark example of how to convert timestamp to date on DataFrame & SQL.

to_date() – function formats Timestamp to Date.

Syntax: to_date(timestamp_column)
Syntax: to_date(timestamp_column,format)
PySpark timestamp (TimestampType) consists of value in the format yyyy-MM-dd HH:mm:ss.SSSS and Date (DateType) format would be yyyy-MM-dd. Use to_date() function to truncate time from Timestamp or to convert the timestamp to date on DataFrame column.
