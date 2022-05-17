package P01

import org.apache.spark.sql.functions._

object P01 extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    (1, "Mr"),
    (1, "Mme"),
    (1, "Mr"),
    (1, null),
    (1, null),
    (1, null),
    (2, "Mr"),
    (3, null)).toDF("UNIQUE_GUEST_ID", "PREFIX")

//  input.groupBy($"UNIQUE_GUEST_ID").agg(max($"PREFIX")).show
  input.groupBy($"UNIQUE_GUEST_ID").agg(count($"PREFIX")).as("PREFIX")

  input.groupBy($"UNIQUE_GUEST_ID", $"PREFIX").agg(count($"PREFIX"))

  input
    .groupBy($"UNIQUE_GUEST_ID", $"PREFIX")
    .agg(count($"PREFIX"))
    .groupBy($"UNIQUE_GUEST_ID")
    .agg(max(struct($"count(PREFIX)",$"PREFIX"))).show

  input
    .groupBy($"UNIQUE_GUEST_ID", $"PREFIX")
    .agg(
      count($"PREFIX").as("Occ"))
    .groupBy($"PREFIX")
    .agg(max($"Occ"))
    .show
}
