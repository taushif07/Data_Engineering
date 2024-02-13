PySpark Replace Column Values in DataFrame:

You can replace column values of PySpark DataFrame by using SQL string functions regexp_replace(), translate(), and overlay() with Python examples.

1. PySpark Replace String Column Values
   By using PySpark SQL function regexp_replace() you can replace a column value with a string for another string/substring. regexp_replace() uses Java regex for matching, if the regex does not match it returns an empty string,
2. Replace Column Values Conditionally
   In the above example, we just replaced Rd with Road, but not replaced St and Ave values, let’s see how to replace column values conditionally in PySpark Dataframe by using when().otherwise() SQL condition function.

3. Replace Column Value with Dictionary (map)
   You can also replace column values from the python dictionary (map). In the below example, we replace the string value of the state column with the full abbreviated name from a dictionary key-value pair, in order to do so I use PySpark map() transformation to loop through each row of DataFrame.
4. Replace Column Value Character by Character
   By using translate() string function you can replace character by character of DataFrame column value. In the below example, every character of 1 is replaced with A, 2 replaced with B, and 3 replaced with C on the address column.
5. Replace Column with Another Column Value
   By using expr() and regexp_replace() you can replace column value with a value from another DataFrame column. In the below example, we match the value from col2 in col1 and replace with col3 to create new_column. Use expr() to provide SQL like expressions and is used to refer to another column to perform operations.
6. Replace All or Multiple Column Values
   If you want to replace values on all or selected DataFrame columns, refer to How to Replace NULL/None values on all column in PySpark or How to replace empty string with NULL/None value

7. Using overlay() Function
   Replace column value with a string value from another column.
