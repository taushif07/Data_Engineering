PySpark to_timestamp():

Use to_timestamp() function to convert String to Timestamp (TimestampType) in PySpark. The converted time would be in a default format of MM-dd-yyyy HH:mm:ss.SSS. It is commonly used when you have a DataFrame with a column that contains date or timestamp values in string format and you want to convert them into a proper timestamp or date data type for further analysis or operations. Make sure to adjust the timestamp_format to match the format of the timestamp strings in your data. The format should follow the SimpleDateFormat pattern for date and time formats. I will explain how to use this function with a few

Syntax – to_timestamp()

Syntax: to_timestamp(timestampString:Column)
Syntax: to_timestamp(timestampString:Column,format:String)

This function has the above two signatures that are defined in PySpark SQL Date & Timestamp Functions, the first syntax takes just one argument and the argument should be in Timestamp format ‘MM-dd-yyyy HH:mm:ss.SSS‘, when the format is not in this format, it returns null.

The second signature takes an additional String argument to specify the format of the input Timestamp; this supports formats specified in SimeDateFormat. Using this additional argument, you can cast String from any format to Timestamp type in PySpark.
