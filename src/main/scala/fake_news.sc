import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import stanfordLemmatizer.StanfordLemmatizer
import org.apache.spark.ml.{Pipeline, PipelineModel}
import com.databricks.spark.csv
import org.apache.spark.ml.param.Param

val spark = SparkSession
  .builder().master("local")
  .appName("Spark SQL basic example")
  .config("master", "spark://myhost:7077")
  .getOrCreate()

val sqlContext = spark.sqlContext

import sqlContext.implicits._

def pipeGen(inputCol: String, outputCol: String): Pipeline = {
  val extraStopwords = Array("say", "would", "one", "make", "like", "get", "go", "also",
    "could", "even", "use", "thing", "way", "see", "l", "var", "el", "")
    val sl = new StanfordLemmatizer()
      .setInputCol(inputCol)
      .setOutputCol("lemmatized")
  val tokenizer = new RegexTokenizer()
    .setInputCol("lemmatized")
    .setOutputCol("tokens")
  val stopWordRemover = new StopWordsRemover()
    .setInputCol("tokens")
    .setOutputCol("filteredTokens")
  val stopWordsRemover = stopWordRemover
    .setStopWords(stopWordRemover.getStopWords ++ extraStopwords)
  val countVectorizer = new CountVectorizer()
    .setMinDF(3)
    .setMinTF(2)
    .setInputCol("filteredTokens")
    .setOutputCol(outputCol)
  new Pipeline().setStages(Array(sl, tokenizer, stopWordRemover, countVectorizer))
}

class Processor(dataFrame: DataFrame, titleColName: String, bodyColName: String) {
  private val bodyPipe: Pipeline = pipeGen(bodyColName, bodyColName + "Matrix")
  private val titlePipe: Pipeline = pipeGen(titleColName, titleColName + "Matrix")
  val mainPipe: PipelineModel = new Pipeline().setStages(Array(bodyPipe, titlePipe)).fit(dataFrame)

  private def pipeGen(inputCol: String, outputCol: String): Pipeline = {
    val extraStopwords = Array("say", "would", "one", "make", "like", "get", "go", "also",
      "could", "even", "use", "thing", "way", "see", "l", "var", "el", "")
    val sl = new StanfordLemmatizer()
      .setInputCol(inputCol)
      .setOutputCol(inputCol + "Lemmatized")
    val tokenizer = new RegexTokenizer()
      .setInputCol(inputCol + "Lemmatized")
      .setOutputCol(inputCol + "Tokens")
    val stopWordRemover = new StopWordsRemover()
      .setInputCol(inputCol + "Tokens")
      .setOutputCol(inputCol + "FilteredTokens")
    val stopWordsRemover = stopWordRemover
      .setStopWords(stopWordRemover.getStopWords ++ extraStopwords)
    val countVectorizer = new CountVectorizer()
      .setMinDF(3)
      .setMinTF(2)
      .setInputCol(inputCol + "FilteredTokens")
      .setOutputCol(outputCol)
    new Pipeline().setStages(Array(sl, tokenizer, stopWordRemover, countVectorizer))
  }
}

val articlePipe = pipeGen("body", "articleMatrix")
val headlinePipe = pipeGen("headline", "headlineMatrix")

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

val p = new Processor(df, "headline", "body")
p.mainPipe.transform(df).describe()