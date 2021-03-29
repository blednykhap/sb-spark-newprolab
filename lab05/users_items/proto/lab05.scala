
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
    .appName("UsersItems Lab05 DE")
    .master("yarn")
    //  .config("spark.submit.deployMode", "cluster")
    .config("spark.driver.memory", "4g")
    .config("spark.driver.cores", "2")
    .config("spark.executor.instances", "10")
    .config("spark.executor.memory", "6g")
    .config("spark.executor.cores", "3")
    .getOrCreate()

spark.stop()

val updateTag = 0

val buyers = spark.read.json("/user/andrey.blednykh2/visits/buy/")

val visitors = spark.read.json("/user/andrey.blednykh2/visits/view/")

visits.show(2,200,true)

val users_items_buy = buyers
    .na.drop(Seq("uid"))
    .withColumn("new_column", 
                concat(lit("buy_"), regexp_replace(lower(col("item_id")), "[-| ]", "_")))
    .groupBy(col("uid"))
    .pivot(col("new_column"))
    .agg(count(lit(1)))
    .na.fill(0)

users_items_buy.show(2,200,true)

val users_items_view = visitors
    .na.drop(Seq("uid"))
    .withColumn("new_column", 
                concat(lit("view_"), regexp_replace(lower(col("item_id")), "[-| ]", "_")))
    .groupBy(col("uid"))
    .pivot(col("new_column"))
    .agg(count(lit(1)))
    .na.fill(0)

users_items_view.show(2,200,true)

val users_items = users_items_view
    .join(users_items_buy, Seq("uid"), "full")
    .na.fill(0)

users_items.show(10, 200, true)

users_items
    .write
    .mode("overwrite")
    .parquet("/user/andrey.blednykh2/storage/users_items")

val test = spark.read.parquet("/user/andrey.blednykh2/storage/users_items")

test.count
//29875

test.show(10,200,true)
