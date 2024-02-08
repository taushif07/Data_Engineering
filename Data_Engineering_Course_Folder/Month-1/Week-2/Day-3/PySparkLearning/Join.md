PySpark Join is used to combine two DataFrames and by chaining these you can join multiple DataFrames; it supports all basic join type operations available in traditional SQL like INNER, LEFT OUTER, RIGHT OUTER, LEFT ANTI, LEFT SEMI, CROSS, SELF JOIN. PySpark Joins are wider transformations that involve data shuffling across the network.

PySpark SQL Joins comes with more optimization by default (thanks to DataFrames) however still there would be some performance issues to consider while using. I would recommend reading through the PySpark Tutorial where I explained several insights of performance issues.

PySpark Join Syntax
PySpark SQL join has a below syntax and it can be accessed directly from DataFrame.

# Syntax

join(self, other, on=None, how=None)

join() operation takes parameters as below and returns DataFrame.

param other: Right side of the join
param on: a string for the join column name
param how: default inner. Must be one of inner, cross, outer,full, full_outer, left, left_outer, right, right_outer,left_semi, and left_anti.
