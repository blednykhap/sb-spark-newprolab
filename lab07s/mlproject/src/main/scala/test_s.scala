import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}

object test_s {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Lab 07 TestPart")
      .master("yarn")
      //.config("spark.submit.deployMode", "cluster")
      .config("spark.driver.memory", "9g")
      .config("spark.driver.cores", "3")
      .config("spark.executor.instances", "6")
      .config("spark.executor.memory", "9g")
      .config("spark.executor.cores", "3")
      //.config("spark.sql.shuffle.partitions", "81")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    import spark.implicits._

    val modelPath = "/user/andrey.blednykh2/model"

    val kafkaInputParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "andrey_blednykh2",
      "startingOffsets" -> """earliest"""
    )

    val kafkaWeblogs = spark
      .readStream
      .format("kafka")
      .options(kafkaInputParams)
      .load

    val schema = StructType(Seq(
      StructField("uid", StringType, true),
      StructField("visits", StringType, true)
    ))

    val schemaVisit = ArrayType(
      StructType(Seq(
        StructField("url", StringType, true),
        StructField("timestamp", StringType, true)
      ))
    )

 /*   val kafkaValues = kafkaData
      .select(col("value").cast("string"))

    val kafkaWeblogs = kafkaValues
      .withColumn("value", from_json(col("value"), schema))
      .select(col("value.*")) */

    val testing = kafkaWeblogs
      .select(col("value").cast("string"))
      .withColumn("value", from_json(col("value"), schema))
      .select(col("value.*"))
      .withColumn("parsedVisits", from_json(col("visits"), schemaVisit))
      .withColumn("explodeVisits", explode(col("parsedVisits")))
      .withColumn("timestamp", col("explodeVisits.timestamp"))
      .withColumn("urlRaw", col("explodeVisits.url"))
      //.withColumn("host", lower(callUDF("parse_url", $"urlRaw", lit("HOST")))) // a log of bags... http, https, NULL domains... for partial correct url
      .withColumn("cleaning1", regexp_replace(col("urlRaw"), "https://", "http://"))
      .withColumn("cleaning2", regexp_replace(col("cleaning1"), "http://http://", "http://"))
      .withColumn("host", regexp_extract($"cleaning2","^(([^:\\/?#]+):)?(\\/\\/([^\\/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?", 4))
      .withColumn("cleaning3", regexp_replace($"host", "^www.", ""))
      .withColumn("domain", regexp_replace($"cleaning3", "^\\.", ""))  // special for .kasparov (with www1.)
      .select("uid", "domain")
      .groupBy("uid")
      .agg(collect_list("domain").alias("domains"))

    val model = PipelineModel.load(modelPath)

    val logPrediction = model.transform(testing)

    val resultForKafka = logPrediction
      .select(col("uid"), col("predictedLabel").alias("gender_age"))

    val kafkaOutputParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "topic" -> "andrey_blednykh2_lab07_out",
      "checkpointLocation" -> "streaming/checkpoint/",
      "truncate" -> "false"
    )

    resultForKafka
      .select(col("uid").cast("string").alias("key"),
        to_json(struct("uid", "gender_age")).alias("value"))
      .writeStream
      .format("kafka")
      .options(kafkaOutputParams)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .start()
      .awaitTermination()

  }
}
