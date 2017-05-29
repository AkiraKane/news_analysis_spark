import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import stanfordLemmatizer.StanfordLemmatizer
import org.apache.spark.ml.{Pipeline, PipelineModel}
import com.databricks.spark.csv
import org.apache.spark.ml.param.Param
import newsProcessor.NewsProcessor

val spark = SparkSession
  .builder().master("local")
  .appName("Spark SQL basic example")
  .config("master", "spark://myhost:7077")
  .getOrCreate()

val sqlContext = spark.sqlContext

import sqlContext.implicits._

val bodies = spark.read.format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/Users/warren/Desktop/programming_projects/news_analysis_spark/data/fakenews_bodies.csv")
  .withColumnRenamed("id", "bodyId")
val stances = spark.read.format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/Users/warren/Desktop/programming_projects/news_analysis_spark/data/fakenews_stances.csv")

val df = bodies.join(stances, $"bodyId" === $"id")
  .drop("Body Id")
  .na.drop().repartition(7)

val p = new NewsProcessor(df, "headline", "body")
p.mainPipe.write.overwrite()
  .save("/Users/warren/Desktop/programming_projects/news_analysis_spark/data/fakenews_processor")
p.mainPipe.transform(df).write
  .json("/Users/warren/Desktop/programming_projects/news_analysis_spark/data/fakenews_processed.json")