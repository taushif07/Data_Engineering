PySpark Read CSV file into DataFrame:

PySpark provides csv("path") on DataFrameReader to read a CSV file into PySpark DataFrame and dataframeObj.write.csv("path") to save or write to the CSV file.

PySpark supports reading a CSV file with a pipe, comma, tab, space, or any other delimiter/separator files.

Note: PySpark out of the box supports reading files in CSV, JSON, and many more file formats into PySpark DataFrame.

1. PySpark Read CSV File into DataFrame
   Using csv("path") or format("csv").load("path") of DataFrameReader, you can read a CSV file into a PySpark DataFrame, These methods take a file path to read from as an argument. When you use format("csv") method, you can also specify the Data sources by their fully qualified name, but for built-in sources, you can simply use their short names (csv,json, parquet, jdbc, text e.t.c).
