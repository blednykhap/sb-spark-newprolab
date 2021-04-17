package org.apache.spark.ml.feature.estimator

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.estimator.SklearnEstimatorModel.SklearnEstimatorModelWriter
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DefaultParamsReadable
import org.apache.spark.sql.functions.{col, regexp_replace, split, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.ml.linalg.SQLDataTypes
import java.io.PrintWriter

trait SklearnEstimatorParams extends Params {
  final val inputCol= new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")
  final val vocabSize = new Param[Int](this, "vocabSize", "vocabluarySize")
}

trait HasSklearnModel extends Params {
  final val sklearn_model = new Param[String](this, "sklearn_model", "Parameter that contains a serizlized sklearn model")
  final val model_path = new Param[String](this, "model_path", "Parameter that contains model path")
}

// Класс эстиматора назовите SklearnEstimator и унаследйте его от Estimator[SklearnEstimatorModel] и DefaultParamsWritable:
class SklearnEstimator(override val uid: String) extends Estimator[SklearnEstimatorModel]
  with DefaultParamsWritable with HasSklearnModel with SklearnEstimatorParams
{
  def setInputCol(value: String) = set(inputCol, value)

  def setOutputCol(value: String) = set(outputCol, value)

  def setVocabSize(value: Int) = set(vocabSize, value)

  def setModelPath(value: String) = set(model_path, value)

  def this() = this(Identifiable.randomUID("SklearnEstimator"))

  override def fit(dataset: Dataset[_]): SklearnEstimatorModel = {
    // Внутри данного метода необходимо вызывать обучение модели при помощи train.py. Используйте для этого rdd.pipe().
    // Файл train.py будет возвращать сериализованную модель в формате base64.
    // Данный метод fit возвращает SklearnEstimatorModel, поэтому инициализируйте данный объект,
    // где в качестве параметра будет приниматься модель в формате base64.
    val toDense = udf((v:MLVector) => v.toDense.toArray)
    val df = dataset.withColumn($(inputCol), toDense(col($(inputCol))))
      .select("uid", $(inputCol), "gender_age")

    val arrayDFColumn = df.select(
      df("uid") +: df("gender_age") +: (0 until $(vocabSize)).map(i => df($(inputCol))(i).alias(s"feature_$i")): _*
    )

    //spark.sparkContext.addFile("fit.py", true)
    val pipeRDD = arrayDFColumn.toJSON.rdd.pipe("./fit.py")
    val model = pipeRDD.collect.head
    set(sklearn_model, model)
    new PrintWriter($(model_path)) { write(pipeRDD.collect.head); close() }
    val newEstimator = new SklearnEstimatorModel(uid, model)
    newEstimator.set(inputCol, $(inputCol))
      .set(outputCol, $(outputCol))
      .set(sklearn_model, $(sklearn_model))
      .set(vocabSize, $(vocabSize))
      .set(model_path, $(model_path))
  }

  override def copy(extra: ParamMap): SklearnEstimator = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    // Определение выходной схемы данных
    val requiredSchema = StructField($(inputCol), SQLDataTypes.VectorType, false)
    val actualDataType = schema($(inputCol))
    require(actualDataType.equals(requiredSchema),
      s"Schema ${$(inputCol)} must be $requiredSchema but provided $actualDataType")
    schema.add(StructField($(outputCol), StringType, false))
  }

}

// Для возможности считывания пайплайна из hdfs в этом же файле определите объект SklearnEstimator,
// который наследуется от DefaultParamsReadable[SklearnEstimator]:
object SklearnEstimator extends DefaultParamsReadable[SklearnEstimator] {
  override def load(path: String): SklearnEstimator = super.load(path)
}

// Теперь необходимо создать класс SklearnEstimatorModel, объект которого возвращается в определенном выше методе fit.
// Данный класс отвечает за применение модели. Класс будет наследоваться от Model[SklearnEstimatorModel] и MLWritable
// (определите в этом же файле):
class SklearnEstimatorModel(override val uid: String, val model: String) extends Model[SklearnEstimatorModel]
  with MLWritable with SklearnEstimatorParams with HasSklearnModel with DefaultParamsReadable[this.type]
{
  //как видно выше, для инициализации объекта данного класса в качестве одного из параметров конструктора является
  // String-переменная model, это и есть модель в формате base64, которая была возвращена из train.py

  override def copy(extra: ParamMap): SklearnEstimatorModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // Внутри данного метода необходимо вызывать test.py для получения предсказаний. Используйте для этого rdd.pipe().
    // Внутри test.py используется обученная модель, которая хранится в переменной `model`. Поэтому перед вызовом
    // rdd.pipe() необходимо записать данное значение в файл и добавить его в spark-сессию при помощи sparkSession.sparkContext.addFile.
    // Данный метод возвращает DataFrame, поэтому полученные предсказания необходимо корректно преобразовать в DF.
    import dataset.sparkSession.implicits._
    val toDense = udf((v:MLVector) => v.toDense.toArray)
    val df = dataset.withColumn( $(inputCol), toDense(col( $(inputCol))))
      .select("uid",  $(inputCol))

    val arrayDFColumn = df.select(
      df("uid") +: (0 until $(vocabSize)).map(i => df($(inputCol))(i).alias(s"feature_$i")): _*
    )
    //spark.sparkContext.addFile("lab07.model", true)
    //spark.sparkContext.addFile("transformer.py", true)
    val pipeRDD = arrayDFColumn.toJSON.rdd.pipe("./transformer.py")

    pipeRDD.toDF.withColumn("value", split(regexp_replace(col("value"), "[)(]", ""), ","))
      .select(col("value").getItem(0).as("uid"), col("value").getItem(1).as($(outputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    // Определение выходной схемы данных
    val requiredSchema = StructField($(inputCol), SQLDataTypes.VectorType, false)
    val actualDataType = schema($(inputCol))
    require(actualDataType.equals(requiredSchema),
      s"Schema ${$(inputCol)} must be $requiredSchema but provided $actualDataType")
    schema.add(StructField($(outputCol), StringType, false))
  }

  override def write: MLWriter = new SklearnEstimatorModelWriter(this)
}

// Для корректной работы модели с hdfs необходимо в этом же файле определить объект SklearnEstimatorModel,
// унаследованный от MLReadable[SklearnEstimatorModel]:
object SklearnEstimatorModel extends MLReadable[SklearnEstimatorModel] {
  private[SklearnEstimatorModel]
  class SklearnEstimatorModelWriter(instance: SklearnEstimatorModel) extends MLWriter {

    private case class Data(model: String)

    override protected def saveImpl(path: String): Unit = {
      // В данном методе сохраняется значение модели в формате base64 на hdfs
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.model)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class SklearnEstimatorModelReader extends MLReader[SklearnEstimatorModel] {

    private val className = classOf[SklearnEstimatorModel].getName

    override def load(path: String): SklearnEstimatorModel = {
      // В данном методе считывается значение модели в формате base64 из hdfs
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("model")
        .head()
      val modelStr = data.getAs[String](0)
      val model = new SklearnEstimatorModel(metadata.uid, modelStr)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[SklearnEstimatorModel] = new SklearnEstimatorModelReader

  override def load(path: String): SklearnEstimatorModel = super.load(path)
}

