import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.sources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

class AvroOutputWriterFactory(schema: StructType,
                              recordName: String,
                              recordNamespace: String) extends OutputWriterFactory {





//  override def newInstance(path: String,
//                           dataSchema: StructType,
//                           context: TaskAttemptContext): OutputWriter =
//    new AvroOutputWriter(path, context, schema, recordName, recordNamespace)
  override private[sql] def newInstance(path: String,
                                        bucketId: Option[Int],
                                        dataSchema: StructType,
                                        context: TaskAttemptContext): OutputWriter = {
      val config = context.getConfiguration
      val recordName = config.getStrings("recordName", "topLevelRecord").head
      val recordNamespace = config.getStrings("recordNamespace", "").head
      new AvroOutputWriter(path, context, dataSchema, recordName, recordNamespace)
  }
}


import java.io.{IOException, OutputStream}
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util.HashMap

import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext, TaskAttemptID}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.OutputWriter
import org.apache.spark.sql.types._

import scala.collection.immutable.Map

class AvroOutputWriter(path: String,
                       context: TaskAttemptContext,
                       schema: StructType,
                       recordName: String,
                       recordNamespace: String) extends OutputWriter  {

  private lazy val converter = createConverterToAvro(schema, recordName, recordNamespace)

  /**
    * Overrides the couple of methods responsible for generating the output streams / files so
    * that the data can be correctly partitioned
    */
  private val recordWriter: RecordWriter[AvroKey[GenericRecord], NullWritable] =
    new AvroKeyOutputFormat[GenericRecord]() {

      private def getConfigurationFromContext(context: TaskAttemptContext): Configuration = {
        // Use reflection to get the Configuration. This is necessary because TaskAttemptContext
        // is a class in Hadoop 1.x and an interface in Hadoop 2.x.
        val method = context.getClass.getMethod("getConfiguration")
        method.invoke(context).asInstanceOf[Configuration]
      }

      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        val uniqueWriteJobId =
          getConfigurationFromContext(context).get("spark.sql.sources.writeJobUUID")
        val taskAttemptId: TaskAttemptID = {
          // Use reflection to get the TaskAttemptID. This is necessary because TaskAttemptContext
          // is a class in Hadoop 1.x and an interface in Hadoop 2.x.
          val method = context.getClass.getMethod("getTaskAttemptID")
          method.invoke(context).asInstanceOf[TaskAttemptID]
        }
        val split = taskAttemptId.getTaskID.getId
        new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$extension")
      }

      @throws(classOf[IOException])
      override def getAvroFileOutputStream(c: TaskAttemptContext): OutputStream = {
        val path = getDefaultWorkFile(context, ".avro")
        path.getFileSystem(getConfigurationFromContext(context)).create(path)
      }

    }.getRecordWriter(context)

  override def write(row: Row): Unit = {
    val key = new AvroKey(converter(row).asInstanceOf[GenericRecord])
    recordWriter.write(key, NullWritable.get())
  }

  override def close(): Unit = recordWriter.close(context)

  /**
    * This function constructs converter function for a given sparkSQL datatype. This is used in
    * writing Avro records out to disk
    */
  private def createConverterToAvro(
                                     dataType: DataType,
                                     structName: String,
                                     recordNamespace: String): (Any) => Any = {
    dataType match {
      case BinaryType => (item: Any) => item match {
        case null => null
        case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
      }
      case ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | StringType | BooleanType => identity
      case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      case TimestampType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Timestamp].getTime
      case ArrayType(elementType, _) =>
        val elementConverter = createConverterToAvro(elementType, structName, recordNamespace)
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val sourceArray = item.asInstanceOf[Seq[Any]]
            val sourceArraySize = sourceArray.size
            val targetArray = new Array[Any](sourceArraySize)
            var idx = 0
            while (idx < sourceArraySize) {
              targetArray(idx) = elementConverter(sourceArray(idx))
              idx += 1
            }
            targetArray
          }
        }
      case MapType(StringType, valueType, _) =>
        val valueConverter = createConverterToAvro(valueType, structName, recordNamespace)
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val javaMap = new HashMap[String, Any]()
            item.asInstanceOf[Map[String, Any]].foreach { case (key, value) =>
              javaMap.put(key, valueConverter(value))
            }
            javaMap
          }
        }
      case structType: StructType =>
        val builder = SchemaBuilder.record(structName).namespace(recordNamespace)
        val schema: Schema = SchemaConverters.convertStructToAvro(
          structType, builder, recordNamespace)
        val fieldConverters = structType.fields.map(field =>
          createConverterToAvro(field.dataType, field.name, recordNamespace))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val record = new Record(schema)
            val convertersIterator = fieldConverters.iterator
            val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
            val rowIterator = item.asInstanceOf[Row].toSeq.iterator

            while (convertersIterator.hasNext) {
              val converter = convertersIterator.next()
              record.put(fieldNamesIterator.next(), converter(rowIterator.next()))
            }
            record
          }
        }
    }
  }
}