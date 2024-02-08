PySpark Column Class | Operators & Functions

pyspark.sql.Column class provides several functions to work with DataFrame to manipulate the Column values, evaluate the boolean expression to filter rows, retrieve a value or part of a value from a DataFrame column, and to work with list, map & struct columns.

Key Points:

PySpark Column class represents a single Column in a DataFrame.
It provides functions that are most used to manipulate DataFrame Columns & Rows.
Some of these Column functions evaluate a Boolean expression that can be used with filter() transformation to filter the DataFrame Rows.

pyspark.sql.Column class provides several functions to work with DataFrame to manipulate the Column values, evaluate the boolean expression to filter rows, retrieve a value or part of a value from a DataFrame column, and to work with list, map & struct columns.

Note: Most of the pyspark.sql.functions return Column type hence it is very important to know the operation you can perform with Column type.

COLUMN FUNCTION FUNCTION DESCRIPTION
alias(*alias, \*\*kwargs)
name(*alias, \**kwargs) Provides alias to the column or expressions
name() returns same as alias().
asc()
asc_nulls_first()
asc_nulls_last() Returns ascending order of the column.
asc_nulls_first() Returns null values first then non-null values.
asc_nulls_last() – Returns null values after non-null values.
astype(dataType)
cast(dataType) Used to cast the data type to another type.
astype() returns same as cast().
between(lowerBound, upperBound) Checks if the columns values are between lower and upper bound. Returns boolean value.
bitwiseAND(other)
bitwiseOR(other)
bitwiseXOR(other) Compute bitwise AND, OR & XOR of this expression with another expression respectively.
contains(other) Check if String contains in another string.
desc()
desc_nulls_first()
desc_nulls_last() Returns descending order of the column.
desc_nulls_first() -null values appear before non-null values.
desc_nulls_last() – null values appear after non-null values.
startswith(other)
endswith(other) String starts with. Returns boolean expression
String ends with. Returns boolean expression
eqNullSafe(other) Equality test that is safe for null values.
getField(name) Returns a field by name in a StructField and by key in Map.
getItem(key) Returns a values from Map/Key at the provided position.
isNotNull()
isNull() isNotNull() – Returns True if the current expression is NOT null.
isNull() – Returns True if the current expression is null.
isin(*cols) A boolean expression that is evaluated to true if the value of this expression is contained by the evaluated values of the arguments.
like(other)
rlike(other) Similar to SQL like expression.
Similar to SQL RLIKE expression (LIKE with Regex).
over(window) Used with window column
substr(startPos, length) Return a Column which is a substring of the column.
when(condition, value)
otherwise(value) Similar to SQL CASE WHEN, Executes a list of conditions and returns one of multiple possible result expressions.
dropFields(\*fieldNames) Used to drops fields in StructType by name.
withField(fieldName, col) An expression that adds/replaces a field in StructType by name.
