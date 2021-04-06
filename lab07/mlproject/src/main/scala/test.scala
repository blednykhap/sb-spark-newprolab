import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object test {
  def main(args: Array[String]): Unit = {

    val modelPath = "/user/andrey.blednykh2/model"

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("Lab 07 TestPart")
      .getOrCreate()

    import spark.implicits._

    val kafkaInputParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "andrey.blednykh2",
      "startingOffsets" -> """earliest"""
    )

    val kafkaWeblogs = spark
      .readStream
      .format("kafka")
      .options(kafkaInputParams)
      .load

    val testing = kafkaWeblogs
      .withColumn("parsedVisits", explode(col("visits")))
      .withColumn("timestamp", col("parsedVisits.timestamp"))
      .withColumn("urlRaw", col("parsedVisits.url"))
      //.withColumn("host", lower(callUDF("parse_url", $"urlRaw", lit("HOST")))) // a log of bags... http, https, NULL domains... for partial correct url
      .withColumn("cleaning1", regexp_replace(col("urlRaw"), "https://", "http://"))
      .withColumn("cleaning2", regexp_replace(col("cleaning1"), "http://http://", "http://"))
      .withColumn("host", regexp_extract($"cleaning2","^(([^:\\/?#]+):)?(\\/\\/([^\\/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?", 4))
      .withColumn("cleaning3", regexp_replace($"host", "^www.", ""))
      .withColumn("domain", regexp_replace($"cleaning3", "^\\.", ""))  // special for kasparov with www1.
      .select("uid", "domain")
      .groupBy("uid")
      .agg(collect_list("domain").alias("domains"))

    val model = PipelineModel.load(modelPath)

    val logPrediction = model.transform(testing)

    val resultForKafka = logPrediction
      .select(col("uid"), col("predictedLabel").alias("gender_age"))

    val kafkaOutputParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "topic" -> "andrey_blednykh2_lab04b_out",
      "checkpointLocation" -> "streaming/checkpoint/",
      "truncate" -> "false"
    )

    resultForKafka
      .select(col("uid").cast("string").alias("key"), to_json(struct("uid", "gender_age")).alias("value"))
      .writeStream
      .format("kafka")
      //.format("console")
      .options(kafkaOutputParams)
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("update")
      .start()
      .awaitTermination()

    spark.stop()

  }
}
