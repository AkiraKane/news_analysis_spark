import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import edu.stanford.nlp.simple.Document
import org.apache.spark.sql.functions.udf
import stanfordLemmatizer.StanfordLemmatizer
import org.apache.spark.ml.Pipeline
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vector
import com.databricks.spark.csv
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.sql.types.StructType

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

val lemmatizer = udf((s: String) => {
  try {
    val doc = new Document(s)
    doc.sentences().map(_.lemmas()).map(_.mkString(" ")).mkString(" ")
  } catch {
    case e: Exception => ""
  }
})

val articlePipe = pipeGen("body", "articleMatrix")
val headlinePipe = pipeGen("headline", "headlineMatrix")

val bodies = spark.read.format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/Users/warren/Desktop/programming_projects/news_analysis_spark/data/fakenews_bodies.csv")
//  .withColumn("article", lemmatizer($"articleBody"))
//  .drop("articleBody")
  .withColumnRenamed("Body Id", "id")
val stances = spark.read.format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/Users/warren/Desktop/programming_projects/news_analysis_spark/data/fakenews_stances.csv")
//  .withColumn("hlem", lemmatizer($"Headline"))
//  .drop("Headline")
//  .withColumnRenamed("hlem", "headline")

//val df = bodies.join(stances, $"id" === $"Body Id")
//  .drop("Body Id")
//  .na.drop()
//  .show(false)

val p = articlePipe.fit(bodies)

p.transform(bodies).createOrReplaceTempView("p")
sqlContext.sql("""SELECT * FROM p WHERE id = 5""").first()