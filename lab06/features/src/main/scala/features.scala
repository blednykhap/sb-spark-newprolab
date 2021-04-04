import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object features {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Features Lab06 DE")
      //.master("yarn")
      //.config("spark.submit.deployMode", "cluster")
      .config("spark.driver.memory", "9g")
      .config("spark.driver.cores", "3")
      .config("spark.executor.instances", "6")
      .config("spark.executor.memory", "12g")
      .config("spark.executor.cores", "6")
      //.config("spark.sql.shuffle.partitions", "81")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    import spark.implicits._

    val weblogs = spark.read.json("/labs/laba03/weblogs.json")

    val parsed_logs = weblogs
      .select(col("uid"), explode(col("visits")).alias("visits"))
      .select(col("uid"), col("visits.timestamp").alias("timestamp"), col("visits.url").alias("url"))
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .select(col("uid"), col("domain"), col("timestamp"))

    val top_domains = parsed_logs
      .groupBy(col("domain"))
      .agg(count(lit(1)).alias("cn"))
      .sort(col("cn").desc)
      .filter("domain != 'null'")
      .limit(1000)

    val sorted_domain = top_domains
      .select("domain")
      .sort("domain")

    val sorted_logs = parsed_logs
      .join(sorted_domain, Seq("domain"), "inner")

    val users_domains = sorted_logs
      .select("uid", "domain")
      .groupBy("uid")
      .pivot("domain")
      .agg(count(lit(1)))
      .na.fill(0)

    val ecran_result = users_domains.select(col("uid"),
      array(users_domains.columns.drop(1).map(c => col(s"`$c`")):_*).alias("domain_features"))

    val timing_df = parsed_logs
      .withColumn("greg_date", to_timestamp(col("timestamp")/1000))
      .withColumn("web_day_mon", when(date_format(col("greg_date"), "u") === 1, 1).otherwise(0))
      .withColumn("web_day_tue", when(date_format(col("greg_date"), "u") === 2, 1).otherwise(0))
      .withColumn("web_day_wed", when(date_format(col("greg_date"), "u") === 3, 1).otherwise(0))
      .withColumn("web_day_thu", when(date_format(col("greg_date"), "u") === 4, 1).otherwise(0))
      .withColumn("web_day_fri", when(date_format(col("greg_date"), "u") === 5, 1).otherwise(0))
      .withColumn("web_day_sat", when(date_format(col("greg_date"), "u") === 6, 1).otherwise(0))
      .withColumn("web_day_sun", when(date_format(col("greg_date"), "u") === 7, 1).otherwise(0))
      .withColumn("web_hour_0", when(hour(col("greg_date")) === 0, 1).otherwise(0))
      .withColumn("web_hour_1", when(hour(col("greg_date")) === 1, 1).otherwise(0))
      .withColumn("web_hour_2", when(hour(col("greg_date")) === 2, 1).otherwise(0))
      .withColumn("web_hour_3", when(hour(col("greg_date")) === 3, 1).otherwise(0))
      .withColumn("web_hour_4", when(hour(col("greg_date")) === 4, 1).otherwise(0))
      .withColumn("web_hour_5", when(hour(col("greg_date")) === 5, 1).otherwise(0))
      .withColumn("web_hour_6", when(hour(col("greg_date")) === 6, 1).otherwise(0))
      .withColumn("web_hour_7", when(hour(col("greg_date")) === 7, 1).otherwise(0))
      .withColumn("web_hour_8", when(hour(col("greg_date")) === 8, 1).otherwise(0))
      .withColumn("web_hour_9", when(hour(col("greg_date")) === 9, 1).otherwise(0))
      .withColumn("web_hour_10", when(hour(col("greg_date")) === 10, 1).otherwise(0))
      .withColumn("web_hour_11", when(hour(col("greg_date")) === 11, 1).otherwise(0))
      .withColumn("web_hour_12", when(hour(col("greg_date")) === 12, 1).otherwise(0))
      .withColumn("web_hour_13", when(hour(col("greg_date")) === 13, 1).otherwise(0))
      .withColumn("web_hour_14", when(hour(col("greg_date")) === 14, 1).otherwise(0))
      .withColumn("web_hour_15", when(hour(col("greg_date")) === 15, 1).otherwise(0))
      .withColumn("web_hour_16", when(hour(col("greg_date")) === 16, 1).otherwise(0))
      .withColumn("web_hour_17", when(hour(col("greg_date")) === 17, 1).otherwise(0))
      .withColumn("web_hour_18", when(hour(col("greg_date")) === 18, 1).otherwise(0))
      .withColumn("web_hour_19", when(hour(col("greg_date")) === 19, 1).otherwise(0))
      .withColumn("web_hour_20", when(hour(col("greg_date")) === 20, 1).otherwise(0))
      .withColumn("web_hour_21", when(hour(col("greg_date")) === 21, 1).otherwise(0))
      .withColumn("web_hour_22", when(hour(col("greg_date")) === 22, 1).otherwise(0))
      .withColumn("web_hour_23", when(hour(col("greg_date")) === 23, 1).otherwise(0))
      .withColumn("visit_work_hours",
        when((hour(col("greg_date")) >= 9) && (hour(col("greg_date")) <= 17), 1).otherwise(0))
      .withColumn("visit_evening_hours",
        when((hour(col("greg_date")) >= 18) && (hour(col("greg_date")) <= 23), 1).otherwise(0))
    //   .drop("domain", "timestamp", "greg_date")

    val timing_agg = timing_df
      .groupBy("uid")
      .agg(sum(col("web_day_mon")).alias("web_day_mon"),sum(col("web_day_tue")).alias("web_day_tue"),
        sum(col("web_day_wed")).alias("web_day_wed"),sum(col("web_day_thu")).alias("web_day_thu"),
        sum(col("web_day_fri")).alias("web_day_fri"),sum(col("web_day_sat")).alias("web_day_sat"),
        sum(col("web_day_sun")).alias("web_day_sun"),sum(col("web_hour_0")).alias("web_hour_0"),
        sum(col("web_hour_1")).alias("web_hour_1"),sum(col("web_hour_2")).alias("web_hour_2"),
        sum(col("web_hour_3")).alias("web_hour_3"),sum(col("web_hour_4")).alias("web_hour_4"),
        sum(col("web_hour_5")).alias("web_hour_5"),sum(col("web_hour_6")).alias("web_hour_6"),
        sum(col("web_hour_7")).alias("web_hour_7"),sum(col("web_hour_8")).alias("web_hour_8"),
        sum(col("web_hour_9")).alias("web_hour_9"),sum(col("web_hour_10")).alias("web_hour_10"),
        sum(col("web_hour_11")).alias("web_hour_11"),sum(col("web_hour_12")).alias("web_hour_12"),
        sum(col("web_hour_13")).alias("web_hour_13"),sum(col("web_hour_14")).alias("web_hour_14"),
        sum(col("web_hour_15")).alias("web_hour_15"),sum(col("web_hour_16")).alias("web_hour_16"),
        sum(col("web_hour_17")).alias("web_hour_17"),sum(col("web_hour_18")).alias("web_hour_18"),
        sum(col("web_hour_19")).alias("web_hour_19"),sum(col("web_hour_20")).alias("web_hour_20"),
        sum(col("web_hour_21")).alias("web_hour_21"),sum(col("web_hour_22")).alias("web_hour_22"),
        sum(col("web_hour_23")).alias("web_hour_23"),
        sum(col("visit_work_hours")).alias("visit_work_hours"),
        sum(col("visit_evening_hours")).alias("visit_evening_hours"),
        count(lit(1)).alias("visit_count")
      )

    val timing_frac = timing_agg
      .withColumn("web_fraction_work_hours", col("visit_work_hours")/col("visit_count"))
      .withColumn("web_fraction_evening_hours", col("visit_evening_hours")/col("visit_count"))
      .drop("visit_work_hours", "visit_evening_hours", "visit_count")

    val weblog_futures = ecran_result.join(timing_frac, Seq("uid"), "inner")

    val users_items = spark.read.parquet("/user/andrey.blednykh2/users-items/20200429")

    val result_dataframe = users_items
      .join(weblog_futures, Seq("uid"), "full")
      .na.fill(0)

    result_dataframe
      .write
      .mode("overwrite")
      .parquet("/user/andrey.blednykh2/features")

    spark.stop()

  }
}
