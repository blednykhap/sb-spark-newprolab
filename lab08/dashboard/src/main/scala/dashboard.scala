import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{PipelineModel}

object dashboard {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Dashboard Lab08 DE")
      //.master("yarn")
      //.config("spark.submit.deployMode", "cluster")
      .config("spark.driver.memory", "30g")
      .config("spark.driver.cores", "6")
      .config("spark.executor.instances", "9")
      .config("spark.executor.memory", "9g")
      .config("spark.executor.cores", "3")
      //.config("spark.sql.shuffle.partitions", "81")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    import spark.implicits._

    val rawLogs = spark.read.json("/labs/laba08/laba08.json")

    val parseLogs = rawLogs
      .select(col("date"), col("uid"), explode(col("visits")).alias("visits"))
      .select(col("date"), col("uid"), col("visits.timestamp").alias("timestamp"), col("visits.url").alias("url"))
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .select(col("date"), col("uid"), col("domain"))
      .groupBy(col("date"), col("uid"))
      .agg(collect_list("domain").alias("domains"))

    val modelPath = "/user/andrey.blednykh2/model"

    val model = PipelineModel.load(modelPath)

    val logPrediction = model.transform(parseLogs)

    val result = logPrediction
      .select(col("predictedLabel").alias("gender_age"), col("uid"), col("date"))

    val esOptions =
      Map(
        "es.nodes" -> "10.0.0.5:9200",
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true",
        "es.net.http.auth.user" -> "andrey.blednykh2",
        "es.net.http.auth.pass" -> "AkRjOhAK"
      )

    result.write
      .mode("append")
      .format("org.elasticsearch.spark.sql")
      .options(esOptions)
      .save("andrey_blednykh2_lab08/_doc")

    spark.stop()

  }
}
