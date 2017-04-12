import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.functions._
import edu.stanford.nlp.simple.{Document, Sentence}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConversions._
import java.util.Properties

val spark = SparkSession
  .builder().master("local")
  .appName("Spark SQL basic example")
  .config("master", "spark://myhost:7077")
  .getOrCreate()

val sqlContext = spark.sqlContext

import sqlContext.implicits._

val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
val stopwords = new StopWordsRemover().setInputCol("words").setOutputCol("filteredWords")

val lemmatizer = udf((s: String) => {
  val doc = new Document(s)
  doc.sentences().map(_.lemmas()).map(_.mkString(" ")).mkString(" ")
})

val jezebel = spark.read.option("charset", "ascii").json("/Users/warren/Desktop/programming_projects/news_analysis_spark/data/jezebeltest.jsonl")
  .withColumn("id", $"_id".getField("$oid"))
  .withColumn("lemmatized", lemmatizer($"text"))
  .withColumn("textLower", regexp_replace($"lemmatized", """[\p{Punct}]|[^\x00-\x7F]|\s{2,}?""", ""))
  .drop("text")
  .withColumnRenamed("textLower", "text")
  .drop("_id")

val jezTokens = stopwords.transform(tokenizer.transform(jezebel))

jezTokens.printSchema()
jezTokens.select("filteredWords").head(1)