import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

object users_items {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("UsersItems Lab05 DE")
      /*.master("yarn")
      .config("spark.submit.deployMode", "cluster")
      .config("spark.driver.memory", "6g")
      .config("spark.driver.cores", "3")
      .config("spark.executor.instances", "8")
      .config("spark.executor.memory", "6g")
      .config("spark.executor.cores", "3") */
      .getOrCreate()

    val itemsUpdate = spark.conf.get("spark.users_items.update")
    val outputDir = spark.conf.get("spark.users_items.output_dir")
    val inputDir = spark.conf.get("spark.users_items.input_dir")

    val events = spark.read.json(s"$inputDir/*/*/*.json")

    val new_max_date = events
      .agg(max(col("date").cast("integer")))
      .take(1)(0).getInt(0)

    def get_old_max_date(path: String) : String = {

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      try {
        val dirs = fs.listStatus(new Path(path))
          .filter(_.isDir)
          .map(_.getPath.getName.toInt)

        if (dirs.size >= 1) {
          dirs.reduceLeft(_ max _).toString
        } else {
          ""
        }
      } catch { case _: Throwable => ""}

    }

    val old_max_date = get_old_max_date(s"$outputDir")

    val new_users_items = events
      .na.drop(Seq("uid"))
      .withColumn("norm_column", regexp_replace(lower(col("item_id")), "[-| ]", "_"))
      .withColumn("new_column",
        when(col("event_type") === "buy", concat(lit("buy_"), col("norm_column")))
          .otherwise(concat(lit("view_"), col("norm_column"))))
      .groupBy(col("uid"))
      .pivot(col("new_column"))
      .agg(count(lit(1)))
      .na.fill(0)

    if ((itemsUpdate == 1) && (old_max_date != "") && (new_max_date > old_max_date.toInt)) {
      val old_users_items = spark.read.parquet(s"$outputDir/$old_max_date")

      val users_items = new_users_items
        .union(old_users_items)
        .groupBy(col("uid"))
        .sum(new_users_items.columns.drop(1): _*)

      users_items
        .write
        .mode("overwrite")
        .parquet(s"$outputDir/$new_max_date")

    } else {
      new_users_items
        .write
        .mode("overwrite")
        .parquet(s"$outputDir/$new_max_date")
    }

  }
}