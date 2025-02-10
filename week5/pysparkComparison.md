## Pyspark vs. DuckDB
When analyzing data with both DuckDB and PySpark, I found the PySpark implementation to be very straightforward. 
After doing the DuckDB implementation first and understanding the necessary queries, the PySpark version felt intuitive. 
I also liked the support for functions like .intersect() and .count() which made the code highly readable. I feel that PySpark's support for
writing queries like this would make more complicated queries, like ones with lots of with statements, much more readable and manageable. 
However, I noticed that breaking these functions into separate operations could make writing the initial query more challenging. 
Since I’m used to thinking in terms of SQL queries, structuring queries in PySpark’s DataFrame API felt like I had a lot more flexibility in how I wrote the query.
