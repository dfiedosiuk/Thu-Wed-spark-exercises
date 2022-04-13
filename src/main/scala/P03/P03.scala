package P03

import org.apache.spark.sql.ForeachWriter

import java.sql.Timestamp
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

object P03 extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

//  val path = if (args.length > 0) args(0) else "/mnt/c/projects/MondaySpark/data"

//  val mySchema = StructType {
//    StructField("id", LongType, nullable = false) ::
//      StructField("name", StringType, nullable = false) :: Nil
//  }
  import scala.concurrent.duration._


  val myWriter = new ForeachWriter[(Timestamp, Long)] {
    // epoch = batch
    // partition = core
    val directory = "tmp/streaming"

    var pw: PrintWriter = _

    override def open(partitionId: Long, epochId: Long): Boolean = {
      pw = new PrintWriter(new File(directory + s"/batch_${partitionId}_${epochId}" ))
      println(s"> partitionId: ${partitionId} epochId: ${epochId}")
      true
    }

    override def process(value: (Timestamp, Long)): Unit = {
      pw.write(s"${value._2}>>>${value._1}\n")
      pw.flush()
      println(s">> value: ${value}")
    }

    override def close(errorOrNull: Throwable): Unit = {
      pw.close()
      println(s">> errorOrNull: ${errorOrNull}")
      println("BREAK")
    }
  }



  val inputs = spark
    .readStream
    .format("rate")
    .load
    .as[(Timestamp, Long)]
    .writeStream
    .trigger(Trigger.ProcessingTime(5.seconds))
    .foreach(myWriter)
    .start.awaitTermination
}
