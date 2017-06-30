import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.linalg.Vectors

val spark = SparkSession
  .builder().master("local")
  .appName("Spark SQL basic example")
  .config("master", "spark://myhost:7077")
  .config("spark.default.parallelism", "7")
  .getOrCreate()

val sqlContext = spark.sqlContext

import sqlContext.implicits._

val mlpc = new MultilayerPerceptronClassifier()
  .setLayers(Array(23204, 10, 10, 2))
  .setBlockSize(128)
  .setFeaturesCol("bodyMatrix")
  .setLabelCol("unrelated")

val isUnrelated = udf((stance: String) =>  if (stance == "unrelated") 1 else 0)

val df = spark.read
  .parquet("/Users/warren/Desktop/programming_projects/news_analysis_spark/data/fakenews_processed.pqt")
  .withColumn("unrelated", isUnrelated($"stance"))
  .repartition(7)

val splits = df.randomSplit(Array(0.6, 0.4), seed = 1234L)
val train = splits(0)
val test = splits(1)

mlpc.fit(train).save("/Users/warren/Desktop/programming_projects/news_analysis_spark/data/neural_net")

