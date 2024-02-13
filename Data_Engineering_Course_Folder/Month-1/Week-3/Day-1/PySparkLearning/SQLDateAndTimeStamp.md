PySpark SQL Date and Timestamp Functions:

PySpark Date and Timestamp Functions are supported on DataFrame and SQL queries and they work similarly to traditional SQL, Date and Time are very important if you are using PySpark for ETL. Most of all these functions accept input as, Date type, Timestamp type, or String. If a String used, it should be in a default format that can be cast to date.

DateType default format is yyyy-MM-dd
TimestampType default format is yyyy-MM-dd HH:mm:ss.SSSS
Returns null if the input is a string that can not be cast to Date or Timestamp.

PySpark SQL provides several Date & Timestamp functions hence keep an eye on and understand these. Always you should choose these functions instead of writing your own functions (UDF) as these functions are compile-time safe, handles null, and perform better when compared to PySpark UDF. If your PySpark application is critical on performance try to avoid using custom UDF at all costs as these are not guarantee performance.

For readable purposes, I’ve grouped these functions into the following groups.

Date Functions
Timestamp Functions
Date and Timestamp Window Functions

The default format of the PySpark Date is yyyy-MM-dd.

PYSPARK DATE FUNCTION DATE FUNCTION DESCRIPTION
current_date() Returns the current date as a date column.
date_format(dateExpr,format) Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument.
to_date() Converts the column into `DateType` by casting rules to `DateType`.
to_date(column, fmt) Converts the column into a `DateType` with a specified format
add_months(Column, numMonths) Returns the date that is `numMonths` after `startDate`.
date_add(column, days)
date_sub(column, days) Returns the date that is `days` days after `start`
datediff(end, start) Returns the number of days from `start` to `end`.
months_between(end, start) Returns number of months between dates `start` and `end`. A whole number is returned if both inputs have the same day of month or both are the last day of their respective months. Otherwise, the difference is calculated assuming 31 days per month.
months_between(end, start, roundOff) Returns number of months between dates `end` and `start`. If `roundOff` is set to true, the result is rounded off to 8 digits; it is not rounded otherwise.
next_day(column, dayOfWeek) Returns the first date which is later than the value of the `date` column that is on the specified day of the week.
For example, `next_day(‘2015-07-27’, “Sunday”)` returns 2015-08-02 because that is the first Sunday after 2015-07-27.
trunc(column, format) Returns date truncated to the unit specified by the format.
For example, `trunc(“2018-11-19 12:01:19”, “year”)` returns 2018-01-01
format: ‘year’, ‘yyyy’, ‘yy’ to truncate by year,
‘month’, ‘mon’, ‘mm’ to truncate by month
date_trunc(format, timestamp) Returns timestamp truncated to the unit specified by the format.
For example, `date_trunc(“year”, “2018-11-19 12:01:19”)` returns 2018-01-01 00:00:00
format: ‘year’, ‘yyyy’, ‘yy’ to truncate by year,
‘month’, ‘mon’, ‘mm’ to truncate by month,
‘day’, ‘dd’ to truncate by day,
Other options are: ‘second’, ‘minute’, ‘hour’, ‘week’, ‘month’, ‘quarter’
year(column) Extracts the year as an integer from a given date/timestamp/string
quarter(column) Extracts the quarter as an integer from a given date/timestamp/string.
month(column) Extracts the month as an integer from a given date/timestamp/string
dayofweek(column) Extracts the day of the week as an integer from a given date/timestamp/string. Ranges from 1 for a Sunday through to 7 for a Saturday
dayofmonth(column) Extracts the day of the month as an integer from a given date/timestamp/string.
dayofyear(column) Extracts the day of the year as an integer from a given date/timestamp/string.
weekofyear(column) Extracts the week number as an integer from a given date/timestamp/string. A week is considered to start on a Monday and week 1 is the first week with more than 3 days, as defined by ISO 8601
last_day(column) Returns the last day of the month which the given date belongs to. For example, input “2015-07-27” returns “2015-07-31” since July 31 is the last day of the month in July 2015.
from_unixtime(column) Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the yyyy-MM-dd HH:mm:ss format.
from_unixtime(column, f) Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the given format.
unix_timestamp() Returns the current Unix timestamp (in seconds) as a long
unix_timestamp(column) Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds), using the default timezone and the default locale.
unix_timestamp(column, p) Converts time string with given pattern to Unix timestamp (in seconds).

PYSPARK TIMESTAMP FUNCTION SIGNATURE TIMESTAMP FUNCTION DESCRIPTION
current_timestamp () Returns the current timestamp as a timestamp column
hour(column) Extracts the hours as an integer from a given date/timestamp/string.
minute(column) Extracts the minutes as an integer from a given date/timestamp/string.
second(column) Extracts the seconds as an integer from a given date/timestamp/string.
to_timestamp(column) Converts to a timestamp by casting rules to `TimestampType`.
to_timestamp(column, fmt) Converts time string with the given pattern to timestamp

Date and Timestamp Window Functions
Below are PySpark Data and Timestamp window functions.

DATE & TIME WINDOW FUNCTION SYNTAX DATE & TIME WINDOW FUNCTION DESCRIPTION
window(timeColumn, windowDuration,
slideDuration, startTime) Bucketize rows into one or more time windows given a timestamp specifying column. Window starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in the order of months are not supported.
window(timeColumn, windowDuration, slideDuration) Bucketize rows into one or more time windows given a timestamp specifying column. Window starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in the order of months are not supported. The windows start beginning at 1970-01-01 00:00:00 UTC
window(timeColumn, windowDuration) Generates tumbling time windows given a timestamp specifying column. Window starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in the order of months are not supported. The windows start beginning at 1970-01-01 00:00:00 UTC.
