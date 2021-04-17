package org.apache.spark.ml.feature.estimator

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.sql.functions.{col, collect_list, regexp_extract, regexp_replace}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class Url2DomainTransformer(override val uid: String) extends Transformer
  with DefaultParamsWritable with Params with HasInputCol with HasOutputCol
{
  def this() = this(Identifiable.randomUID("org.apache.spark.ml.feature.Url2DomainTransformer"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  final val labels: StringArrayParam = new StringArrayParam(this, "labels",
    "Optional array of labels specifying index-string mapping." +
      " If not provided or if empty, then metadata from inputCol is used instead.")

  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset.printSchema

    val result = dataset
      .withColumn("cleaning1", regexp_replace(col("url"), "https://", "http://"))
      .withColumn("cleaning2", regexp_replace(col("cleaning1"), "http://http://", "http://"))
      .withColumn("host", regexp_extract(col("cleaning2"),"^(([^:\\/?#]+):)?(\\/\\/([^\\/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?", 4))
      .withColumn("cleaning3", regexp_replace(col("host"), "^www.", ""))
      .withColumn("domain", regexp_replace(col("cleaning3"), "^\\.", ""))
      .drop("cleaning1", "cleaning2", "host", "cleaning3")
      .select("uid", "domain")
      .groupBy("uid")
      .agg(collect_list("domain").alias("domains"))
      .select("uid", "domains")

    result.printSchema

    val test = dataset.join(result, Seq("uid"), "inner")

    test.printSchema

    test

  }

  override def copy(extra: ParamMap): Url2DomainTransformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {

    val typeCandidates = List(StringType)
    SchemaUtils.checkColumnTypes(schema, $(inputCol), typeCandidates)
    SchemaUtils.appendColumn(schema, $(outputCol), new ArrayType(StringType, false))

  }

}

object Url2DomainTransformer extends DefaultParamsReadable[Url2DomainTransformer] {
  override def load(path: String): Url2DomainTransformer = super.load(path)
}