import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame


object agg {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(getClass.getName)

    logger.warn("Start program")

    val spark = SparkSession.builder()
      .master("yarn")
      .appName("Lab04b Andrey Blednykh")
      .getOrCreate()

    import spark.implicits._

    val input_topic_name = "andrey_blednykh2"

    val kafkaInputParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "failOnDataLoss" -> "false",
      "subscribe" -> input_topic_name,
      "startingOffsets" -> """earliest"""
    )

    val sdf_kafka = spark
      .readStream
      .format("kafka")
      .options(kafkaInputParams)
      .load

    def foreachBatchFunction(dataFrame: DataFrame, batchId: Long): Unit = {

      val kafka_values = dataFrame
        .select(col("value").cast("string"))

      val schema = StructType(Seq(
        StructField("event_type", StringType, true),
        StructField("category", StringType, true),
        StructField("item_id", StringType, true),
        StructField("item_price", IntegerType, true),
        StructField("uid", StringType, true),
        StructField("timestamp", LongType, true)
      ))

      val json_data = kafka_values
        .withColumn("value", from_json(col("value"), schema))
        .select(col("value.*"))

      val parsed_data = json_data
        .withColumn("gregShortLeft", date_format(to_timestamp((col("timestamp")/1000).cast("long")),"yyyyMMddHH"))
        .withColumn("unixLeft", unix_timestamp(col("gregShortLeft"),"yyyyMMddHH"))
        .withColumn("gregLeft", from_unixtime(col("unixLeft")))
        .withColumn("gregRight", col("gregLeft") + expr("INTERVAL 1 HOURS"))
        .withColumn("gregShortRight", date_format(col("gregRight"), "yyyyMMddHH"))
        .withColumn("unixRight", unix_timestamp(col("gregShortRight"),"yyyyMMddHH"))
        //.drop(col("gregShortLeft"))
        .drop(col("gregLeft"))
        .drop(col("gregRight"))
        .drop(col("gregShortRight"))
        .drop(col("category"))
        .drop(col("item_id"))

      val visitors = parsed_data
        .na.drop(Seq("uid"))
        .groupBy(col("unixLeft").as("start_ts"), col("unixRight").as("end_ts"))
        .agg(count(lit(1)).as("visitors"))

      val agg_kafka = parsed_data
        .filter("event_type = 'buy'")
        .groupBy(col("unixLeft").as("start_ts"), col("unixRight").as("end_ts"))
        .agg(sum(col("item_price")).as("revenue"), count(lit(1)).as("purchases"), avg(col("item_price")).as("aov"))

      val join_data = visitors
        .join(agg_kafka, Seq("start_ts", "end_ts"), "left")
        .select("start_ts", "end_ts", "revenue", "visitors", "purchases", "aov")
        .orderBy(asc("start_ts"))

      val output_topic_name = "andrey_blednykh2_lab04b_out"

      val kafkaOutputParams = Map(
        "kafka.bootstrap.servers" -> "spark-master-1:6667",
        "topic" -> output_topic_name,
        "truncate" -> "false"
      )

      sdf_kafka
        .select(col("start_ts").cast("string").alias("key"),
          to_json(struct("start_ts", "end_ts", "revenue", "visitors", "purchases", "aov")).alias("value"))
        .write
        .format("kafka")
        .options(kafkaOutputParams)
        .mode("update")
        .save()
    }

    sdf_kafka
      .writeStream
      .foreachBatch(foreachBatchFunction _)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()

  }
}
