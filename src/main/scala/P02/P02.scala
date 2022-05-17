package P02

object P02 extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val data = Seq(
    (None, 0),
    (None, 1),
    (Some(2), 0),
    (None, 1),
    (Some(4), 1)).toDF("id", "group")

    data
      .na
      .drop
      .show

}
