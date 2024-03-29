import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}

object filter {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(getClass.getName)

    logger.warn("***| Start application");

    val spark = SparkSession
      .builder()
      .appName("Kafka Lab04a Andrey Blednykh")
      .getOrCreate()

    import spark.implicits._

    val topic_name = spark.conf.get("spark.filter.topic_name") // lab04_input_data
    var offset = spark.conf.get("spark.filter.offset")  // earliest
    val output_dir_prefix = spark.conf.get("spark.filter.output_dir_prefix")

    if (offset != "earliest") {
      offset = s"""{"$topic_name":{"0":$offset}}"""
    }

    logger.warn("***| Get variables")
    logger.warn(s"***| Topic_name: $topic_name")
    logger.warn(s"***| Offset: $offset")
    logger.warn(s"***| Output_dir_prefix: $output_dir_prefix")

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> topic_name,
      "startingOffsets" -> offset,
      "endingOffsets" -> "latest"
    )

    val kafka_df = spark.read
      .format("kafka")
      .options(kafkaParams)
      .load

    val parsed_df = kafka_df
      .select('key, 'value.cast("string"), 'topic, 'partition, 'offset, 'timestamp, 'timestampType)

    val schema = StructType(Seq(
      StructField("event_type", StringType, true),
      StructField("category", StringType, true),
      StructField("item_id", StringType, true),
      StructField("item_price", IntegerType, true),
      StructField("uid", StringType, true),
      //StructField("timestamp", StringType, true)
      //StructField("timestamp", TimestampType, true)
      StructField("timestamp", LongType, true)
    ))

    val parsed_value = parsed_df.select('value.cast("string"))

    val parsed_json = parsed_value
      .withColumn("value", from_json($"value", schema))
      .select($"value.*")

    val extra_json = parsed_json
      .withColumn("date",
        trim(date_format(to_timestamp((col("timestamp")/1000).cast("long")),"yyyyMMdd")))
      .withColumn("p_date",
        trim(date_format(to_timestamp((col("timestamp")/1000).cast("long")),"yyyyMMdd")))

    val k_view_data = extra_json
      .select("category","event_type","item_id","item_price","timestamp","uid","date","p_date")
      .filter("event_type = 'view'")

    val k_buy_date = extra_json
      .select("category","event_type","item_id","item_price","timestamp","uid","date","p_date")
      .filter("event_type = 'buy'")

    k_view_data
      .write
      .mode("overwrite")
      .partitionBy("p_date")
      .json(s"$output_dir_prefix/view/")

    k_buy_date
      .write
      .mode("overwrite")
      .partitionBy("p_date")
      .json(s"$output_dir_prefix/buy/")

    logger.warn("***| Finish job")

  }
}
