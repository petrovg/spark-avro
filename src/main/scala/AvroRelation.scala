package org.apache.spark.avro

import java.io.FileNotFoundException

import org.apache.avro.SchemaBuilder
import org.apache.avro.file.{DataFileReader, FileReader}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.FsInput
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet
import org.apache.spark.sql.avro.AvroOutputWriterFactory

import scala.collection.Iterator
import scala.collection.JavaConversions._


/**
  * Created by petrovg on 31/03/2016.
  */
class DefaultSource extends FileFormat with DataSourceRegister {

  import AvroRelation._

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    val avroSchema = files match {
      case Seq(head, _*) => newReader(head.getPath.toString, sparkSession)(_.getSchema)
      case Seq() =>
        throw new java.io.FileNotFoundException("Cannot infer the schema when no files are present.")
    }
    Some(SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType])
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    val recordName = options.getOrElse("recordName", "topLevelRecord")
    val recordNamespace = options.getOrElse("recordNamespace", "")

    val build = SchemaBuilder.record(recordName).namespace(recordNamespace)
    val outputAvroSchema = SchemaConverters.convertStructToAvro(dataSchema, build, recordNamespace)
    AvroJob.setOutputKeySchema(job, outputAvroSchema)

    new AvroOutputWriterFactory(dataSchema, recordName, recordNamespace)
  }

  override def shortName(): String = "avro"
}


object AvroRelation {

  /**
    * Opens up the location to for reading. Takes in a function to run on the schema and returns the
    * result of this function. This takes in a function so that the caller does not have to worry
    * about cleaning up and closing the reader.
    * @param location the location in the filesystem to read from
    * @param fun the function that is called on when the reader has been initialized
    * @tparam T the return type of the function given
    */
  def newReader[T](location: String, sparkSession: SparkSession)(fun: FileReader[GenericRecord] => T): T = {
    val path = new Path(location)
    val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(path.toUri, hadoopConfiguration)

    println(path)
    println(location)
    println(hadoopConfiguration)
    println(fs)

    val statuses = fs.globStatus(path) match {
      case null => throw new FileNotFoundException(s"The path ($location) is invalid.")
      case globStatus => globStatus.toStream.map(_.getPath).flatMap(getAllFiles(fs, _))
    }

    val IgnoreFilesWithoutExtensionProperty = "avro.mapred.ignore.inputs.without.extension"

    val singleFile =
      (if (hadoopConfiguration.getBoolean(IgnoreFilesWithoutExtensionProperty, true)) {
        statuses.find(_.getName.endsWith("avro"))
      } else {
        statuses.headOption
      }).getOrElse(throw new FileNotFoundException(s"No avro files present at ${path.toString}"))

    val reader = DataFileReader.openReader(new FsInput(singleFile, hadoopConfiguration),
      new GenericDatumReader[GenericRecord]())
    val result = fun(reader)
    reader.close()
    result
  }

  private def getAllFiles(fs: FileSystem, path: Path): Stream[Path] = {
    if (fs.isDirectory(path)) {
      fs.listStatus(path).toStream.map(_.getPath).flatMap(getAllFiles(fs, _))
    } else {
      Stream(path)
    }
  }
}