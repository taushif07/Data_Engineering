PySpark MapType (Dict) Usage with Examples:

PySpark MapType (also called map type) is a data type to represent Python Dictionary (dict) to store key-value pair, a MapType object comprises three fields, keyType (a DataType), valueType (a DataType) and valueContainsNull (a BooleanType).

What is PySpark MapType
PySpark MapType is used to represent map key-value pair similar to python Dictionary (Dict), it extends DataType class which is a superclass of all types in PySpark and takes two mandatory arguments keyType and valueType of type DataType and one optional boolean argument valueContainsNull. keyType and valueType can be any type that extends the DataType class. for e.g StringType, IntegerType, ArrayType, MapType, StructType (struct) e.t.c.

MapType Key Points:

The First param keyType is used to specify the type of the key in the map
The Second param valueType is used to specify the type of the value in the map.
Third parm valueContainsNull is an optional boolean type that is used to specify if the value of the second param can accept Null/None values.
The key of the map won’t accept None/Null values.
PySpark provides several SQL functions to work with MapType.
