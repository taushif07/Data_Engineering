PySpark When Otherwise | SQL Case When Usage:

PySpark When Otherwise and SQL Case When on DataFrame with Examples – Similar to SQL and programming languages, PySpark supports a way to check multiple conditions in sequence and returns a value when the first condition met by using SQL like case when and when().otherwise() expressions, these works similar to “Switch" and "if then else" statements.

Using “When Otherwise” on DataFrame.
PySpark SQL “Case When” on DataFrame.
Using Multiple Conditions With & (And) | (OR) operators
PySpark When Otherwise – when() is a SQL function that returns a Column type and otherwise() is a function of Column, if otherwise() is not used, it returns a None/NULL value.

PySpark SQL Case When – This is similar to SQL expression, Usage: CASE WHEN cond1 THEN result WHEN cond2 THEN result... ELSE result END.

1. Using when() otherwise() on PySpark DataFrame.
   PySpark when() is SQL function, in order to use this first you should import and this returns a Column type, otherwise() is a function of Column, when otherwise() not used and none of the conditions met it assigns None (Null) value. Usage would be like when(condition).otherwise(default).

when() function take 2 parameters, first param takes a condition and second takes a literal value or Column, if condition evaluates to true then it returns a value from second param.

The below code snippet replaces the value of gender with a new derived value, when conditions not matched, we are assigning “Unknown” as value, for null assigning empty.
