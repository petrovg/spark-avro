# Avro Data Source for Spark 2.0

A cut down Avro data source for Spark 2.0. The purpose of this is to be minimal. It only supports Spark 2.0, which seems to have changed the API's for implementing data sources quite significantly.

This is a first cut and currently untested.

Most of the code comes from Databricks's spark-avro.

## How to use

Save a dataset as a table

    Seq(1, 2, 3, 4).toDS.write.format("org.apache.spark.avro").saveAsTable("numbers")

