import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.functions.{col, concat, lit, lower, regexp_replace, when}
import shapeless.syntax.typeable.typeableOps

object data_mart {
  def main(args: Array[String]): Unit = {

    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! start program !!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    val spark = SparkSession.builder()
      .appName("DataMart Lab03 Andrey Blednykh")
      .master("yarn")
    //  .config("spark.submit.deployMode", "cluster")
      .config("spark.driver.memory", "4g")
      .config("spark.driver.cores", "2")
      .config("spark.executor.instances", "5")
      .config("spark.executor.memory", "2g")
      .config("spark.executor.cores", "2")
      .config("spark.cassandra.connection.host", "10.0.0.5")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.cassandra.output.consistency.level", "ANY")
      .config("spark.cassandra.input.consistency.level", "ONE")
      .config("es.nodes.wan.only", "true")
      .getOrCreate()

    import spark.implicits._

    println("******* clients (from cassandra) *******")

    val clientsOpts = Map("table" -> "clients","keyspace" -> "labdata")

    val cassandraDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(clientsOpts)
      .load()

    val clients = cassandraDF
      .select("uid", "gender", "age")
      .withColumn("age_cat",
        when(col("age") >= 18 and col("age") <= "24", lit("18-24"))
          .when(col("age") >= 25 and col("age") <= "34", lit("25-34"))
          .when(col("age") >= 35 and col("age") <= "44", lit("35-44"))
          .when(col("age") >= 45 and col("age") <= "54", lit("45-54"))
          .when(col("age") >= 55, lit(">=55"))
      )

    clients.show(2, 200, true)

 /* uid – уникальный идентификатор пользователя, string
    gender – пол пользователя, F или M - string
    age – возраст пользователя в годах, integer
    age_cat – категория возраста, одна из пяти: 18-24, 25-34, 35-44, 45-54, >=55 */

    println("******* logs of internet-shops visits (from elasticsearch) *******")

    val visitsOptions =
      Map(
        "es.nodes" -> "10.0.0.5:9200",
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true",
        "es.net.http.auth.user" -> "andrey.blednykh2",
        "es.net.http.auth.pass" -> "AkRjOhAK"
      )

    val elasticsearchDF = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(visitsOptions).load("visits*")

    val catShopResult = elasticsearchDF
      .withColumn("all_shop_cat",
        concat(lit("shop_"), regexp_replace(lower(col("category")), "[-,\\s]", "_")))
      .select("uid", "all_shop_cat")
      .na.drop(Seq("uid"))
      .groupBy("uid")
      .pivot("all_shop_cat")
      .agg(count("all_shop_cat"))
      .na.fill(0)

    catShopResult.show(2, 200, true)

 /* Из бэкенда интернет-магазина приходят отфильтрованные и обогащенные сообщения о просмотрах страниц товаров и покупках.
    Сообщения хранятся в Elasticsearch в формате json в следующем виде:
    uid – уникальный идентификатор пользователя, тот же, что и в базе с информацией о клиенте (в Cassandra), либо null,
    если в базе пользователей нет информации об этих посетителях магазина, string
    event_type – buy или view, соответственно покупка или просмотр товара, string
    category – категория товаров в магазине, string
    item_id – идентификатор товара, состоящий из категории и номера товара в категории, string
    item_price – цена товара, integer
    timestamp – unix epoch timestamp в миллисекундах

    Категории товаров берутся из логов посещения интернет-магазина. Чтобы сделать из категорий названия колонок,
    они приводятся к нижнему регистру, пробелы или тире заменяются на подчеркивание, к категории прибавляется приставка shop_.
    Например shop_everyday_jewelry.

    shop_cat1, ... , shop_catN

    В колонках категорий товаров должно быть число посещений соответствующих страниц, а в колонках категорий веб-сайтов -
    число посещений соответствующих веб-сайтов.
    */

    println("******* client logs (from hdfs) *******")

    val hdfsDF = spark.read.json("/labs/laba03/weblogs.json")

    val logs = hdfsDF
      .select($"uid", explode($"visits").alias("struct_data"))
      .select($"uid", $"struct_data.timestamp".as("timestamp"), $"struct_data.url".as("url"))
      .withColumn("trick1", regexp_replace(col("url"), "https://", "http://"))
      .withColumn("trick2", regexp_replace(col("trick1"), "http://http://", "http://"))
      .withColumn("webhost",
        regexp_extract(col("trick2"),"^(([^:\\/?#]+):)?(\\/\\/([^\\/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?", 4))
      .withColumn("domain", regexp_replace(col("webhost"), "^www.", ""))


    logs.show(2, 200, true)

    println("******* web-site category (from postgre) *******")

    val postgreDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.5:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "andrey_blednykh2")
      .option("password", "AkRjOhAK")
      .option("driver", "org.postgresql.Driver")
      .load()

    val catSite = postgreDF
      .select("domain", "category")
      .withColumn("fullcat", concat(lit("web_"), col("category")))

    catSite.show(2, 200, true)

/*    d. Информация о категориях веб-сайтов
    Эта информация хранится в базе данных PostgreSQL labdata таблице domain_cats:

    domain (только второго уровня), string
    category, string

    Категории веб-сайтов берутся из датасета категорий вебсайтов, и точно также из них создаются имена колонок.
    Например: web_arts_and_entertainment.
    После вычленения домена из URL нужно удалить из доменов "www."

    В колонках категорий товаров должно быть число посещений соответствующих страниц, а в колонках категорий веб-сайтов -
    число посещений соответствующих веб-сайтов.

    web_cat1, ... , web_catN
    */

    println("******* join logs with category *******")

    val visits = logs
      .join(catSite, Seq("domain"), "inner")
      .select("uid", "fullcat")
      .groupBy("uid")
      .pivot("fullcat")
      .agg(count("fullcat"))
      .na.fill(0)

    visits.show(2, 200, true)

    println("******* create datamart (to postgre) *******")

    val dmClient = clients.select("uid", "gender", "age_cat")
    println("print dmClient")
    dmClient.show(2, 200, true)

    val dmClientShop = dmClient
      .join(catShopResult, Seq("uid"), "left")

    println("print dmClientShop")
    dmClientShop.show(2, 200, true)

    val dm = dmClientShop
      .join(visits, Seq("uid"), "left")
      .na.fill(0)

    println("print final dm")
    dm.show(2, 200, true)

    dm.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.5:5432/andrey_blednykh2")
      .option("dbtable", "clients")
      .option("user", "andrey_blednykh2")
      .option("password", "AkRjOhAK")
      .option("driver", "org.postgresql.Driver")
      .mode("overwrite").save()

/*  uid, gender, age_cat, shop_cat1, ... , shop_catN, web_cat1, ... , web_catN
    shop_cat, web_cat – категории товаров и категории веб-сайтов.
      */

    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! end program !!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

  }
}
