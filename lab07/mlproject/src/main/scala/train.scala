import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object train {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Lab 07 TrainPart")
      .master("yarn")
      .config("spark.driver.memory", "9g")
      .config("spark.driver.cores", "3")
      .config("spark.executor.instances", "6")
      .config("spark.executor.memory", "9g")
      .config("spark.executor.cores", "3")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    import spark.implicits._

    val trainDir = "/labs/laba07/laba07.json"
    val modelPath = "/user/andrey.blednykh2/model"

    val jsonWeblogs = spark.read.json(trainDir)

    val training = jsonWeblogs
      .withColumn("parsedVisits", explode(col("visits")))
      .withColumn("timestamp", col("parsedVisits.timestamp"))
      .withColumn("urlRaw", col("parsedVisits.url"))
      //.withColumn("host", lower(callUDF("parse_url", $"urlRaw", lit("HOST")))) // a log of bags... http, https, NULL domains... for partial correct url
      .withColumn("cleaning1", regexp_replace(col("urlRaw"), "https://", "http://"))
      .withColumn("cleaning2", regexp_replace(col("cleaning1"), "http://http://", "http://"))
      .withColumn("host", regexp_extract($"cleaning2","^(([^:\\/?#]+):)?(\\/\\/([^\\/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?", 4))
      .withColumn("cleaning3", regexp_replace($"host", "^www.", ""))
      .withColumn("domain", regexp_replace($"cleaning3", "^\\.", ""))  // special for kasparov with www1.
      .select("uid", "gender_age", "domain")
      .groupBy("uid", "gender_age")
      .agg(collect_list("domain").alias("domains"))

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
      .fit(training);

    val lr = new LogisticRegression()
      .setMaxIter(1000)
      .setRegParam(0.001)

    val lc = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(indexer.labels);

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, lc))

    val model = pipeline.fit(training)

    model.write.overwrite().save(modelPath)

    spark.stop()
  }
}
