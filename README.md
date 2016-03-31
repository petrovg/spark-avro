# Avro Data Source for Spark 2.0

A cut down Avro data source for Spark 2.0. The purpose of this is to be minimal. It only supports what it supports, which is not a lot.

## How to use

Save a dataset as a table

    Seq(1, 2, 3, 4).toDS.write.format("org.apache.spark.avro").saveAsTable("numbers")

