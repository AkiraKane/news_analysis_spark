import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.tuning.{TrainValidationSplit, ParamGridBuilder}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.functions.sum


val spark = SparkSession
  .builder().master("local")
  .appName("Spark SQL basic example")
  .config("master", "spark://myhost:7077")
  .config("spark.default.parallelism", "85")
  .getOrCreate()

val sqlContext = spark.sqlContext

import sqlContext.implicits._

val mlpc = new MultilayerPerceptronClassifier()
  .setBlockSize(128)
  .setFeaturesCol("features")
  .setLabelCol("unrelated")
  .setPredictionCol("rawPrediction")

val assembler = new VectorAssembler()
  .setInputCols(Array("bodyMatrix", "headlineMatrix"))
  .setOutputCol("features")

val paramGrid = new ParamGridBuilder()
  .addGrid(mlpc.layers, Array(Array(26109, 10, 10, 2)))
  .build()

val isUnrelated = udf((stance: String) => if (stance == "unrelated") 1 else 0)

val df = spark.read
  .parquet("s3://warren-datasets/news_datasets/fakenews_processed.pqt")
  .withColumn("unrelated", isUnrelated($"stance"))
  .repartition(85)

val dfWithFeatures = assembler.transform(df)

val splits = dfWithFeatures.randomSplit(Array(0.9, 0.1), seed = 1234L)
val train = splits(0)
val test = splits(1)

val trainValidationSplit = new TrainValidationSplit()
  .setEstimator(mlpc)
  .setEvaluator(new BinaryClassificationEvaluator().setLabelCol("unrelated"))
  .setEstimatorParamMaps(paramGrid)
  .setTrainRatio(.8)

val model = trainValidationSplit.fit(train)

val predMat = model.transform(test)

val correctPredUDF = udf((trueValue: Int, predValue: Int) => if (trueValue == predValue) 1 else 0 )
val equals = predMat.withColumn("equals", correctPredUDF($"unrelated", $"rawPrediction")).select("equals")
sum(equals)



val predictionAndLabels = predMat.map { case LabeledPoint(label, rawPrediction) =>
  (rawPrediction, label)
}
