import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object filter {
  def main(args: Array[String]): Unit = {

    println("************** start job **************")
    println()

    val spark = SparkSession
      .builder()
      .appName("Kafka Lab04a Andrey Blednykh")
      .master("yarn")
      .getOrCreate()

    import spark.implicits._

    val topic_name = spark.conf.get("spark.filter.topic_name") // lab04_input_data
    val offset = spark.conf.get("spark.filter.offset")  // earliest
    val output_dir_prefix = spark.conf.get("spark.filter.output_dir_prefix")

    println("******* get variables *******")
    println(s"topic_name: $topic_name")
    println(s"offset: $offset")
    println(s"output_dir_prefix: $output_dir_prefix")
    println()

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> topic_name,
      "startingOffsets" -> offset
    )

    val kafka_df = spark.read
      .format("kafka")
      .options(kafkaParams)
      .load

    val parsed_df = kafka_df
      .select('key, 'value.cast("string"), 'topic, 'partition, 'offset, 'timestamp, 'timestampType)

    println("******* parsed_df *******")
    parsed_df.show(2, 200, true)
    println()

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

    println("******* parsed_value *******")
    parsed_value.show(2, 200, true)
    println()

    val parsed_json = parsed_value
      .withColumn("value", from_json($"value", schema))
      .select($"value.*")

    println("******* parsed_json *******")
    parsed_json.show(2, 200, true)
    println()

    val extra_json = parsed_json
      .withColumn("date",
        trim(date_format(to_timestamp((col("timestamp")/1000).cast("long")),"yyyyMMdd")))
      .withColumn("p_date",
        trim(date_format(to_timestamp((col("timestamp")/1000).cast("long")),"yyyyMMdd")))

    println("******* extra_json *******")
    extra_json.show(2, 200, true)
    println()

    val k_view_data = extra_json
      .select("category","event_type","item_id","item_price","timestamp","uid","date","p_date")
      .filter("event_type = 'view'")

    println("******* k_view_data *******")
    k_view_data.show(2, 200, true)
    println()

    val k_buy_date = extra_json
      .select("category","event_type","item_id","item_price","timestamp","uid","date","p_date")
      .filter("event_type = 'buy'")

    println("******* k_buy_date *******")
    k_buy_date.show(2, 200, true)
    println()

    k_view_data
      .repartition($"p_date")
      .write
      .partitionBy("p_date")
      .json(s"$output_dir_prefix/view/")

    println("******* write k_view_data finished *******")
    println()

    k_buy_date
      .repartition($"p_date")
      .write
      .partitionBy("p_date")
      .json(s"$output_dir_prefix/buy/")

    println("******* write k_buy_date finished *******")
    println()

    println("************** finish job **************")

  }
}
