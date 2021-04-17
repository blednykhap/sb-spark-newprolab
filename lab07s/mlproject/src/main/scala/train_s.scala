import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.estimator.Url2DomainTransformer
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object train_s {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Lab 07 TrainPart")
 //     .master("yarn")
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

    val trainDir = "/labs/laba07/laba07.json"
    val modelPath = "/user/andrey.blednykh2/model"

    val jsonWeblogs = spark.read.json(trainDir)

    val training = jsonWeblogs
      .withColumn("parsedVisits", explode(col("visits")))
      .withColumn("timestamp", col("parsedVisits.timestamp"))
      .withColumn("url", col("parsedVisits.url"))
      .drop("visits", "parsedVisits", "timestamp")
      .select("uid", "gender_age", "url")

    val u2d = new Url2DomainTransformer()
      .setInputCol("url")
      .setOutputCol("domains")

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
      .fit(training);

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val lc = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(indexer.labels);

    val pipeline = new Pipeline()
      .setStages(Array(u2d, cv, indexer, lr, lc))

    val model = pipeline.fit(training)

    model.write.overwrite().save(modelPath)

    spark.stop()
  }
}
