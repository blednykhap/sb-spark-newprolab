import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger


object agg {
  def main(args: Array[String]): Unit = {
      println("hello world")

    val logger = Logger.getLogger(getClass.getName)
    val output_topic_name = "andrey_blednykh2_lab04b_out"

    logger.warn("Hello world!")

    val spark = SparkSession.builder()
      .master("yarn")
      .appName("")
      .getOrCreate()

    import spark.implicits._

    val input_topic_name = "andrey_blednykh2"

    val kafkaInputParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "failOnDataLoss" -> "false",
      "subscribe" -> input_topic_name,
      "startingOffsets" -> """earliest"""
      //"endingOffsets" -> """latest"""
      //"maxOffsetsPerTrigger" -> "5"
    )

    val sdf_kafka = spark
      .readStream
      .format("kafka")
      .options(kafkaInputParams)
      .load

    val kafka_values = sdf_kafka
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

    val kafkaOutputParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "topic" -> output_topic_name,
      "checkpointLocation" -> "streaming/checkpoint/",
      "truncate" -> "false"
    )

    join_data
      .select(col("start_ts").cast("string").alias("key"),
        to_json(struct("start_ts", "end_ts", "revenue", "visitors", "purchases", "aov")).alias("value"))
      .writeStream
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .format("kafka")
      .options(kafkaOutputParams)
      .outputMode("update")
      .start()
      .awaitTermination()

  }
}
