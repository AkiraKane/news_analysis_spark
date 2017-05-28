import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import edu.stanford.nlp.simple.Document
import org.apache.spark.sql.functions.udf
import stanfordLemmatizer.StanfordLemmatizer
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

val tokenizer = new RegexTokenizer()
  .setInputCol("wordsCleaned").setOutputCol("tokens")
val stopWordRemover = new StopWordsRemover()
  .setInputCol("tokens").setOutputCol("filteredTokens")
val stopwords = stopWordRemover.getStopWords ++ Array("say", "would", "one", "make", "like", "get", "go", "also",
  "could", "even", "use", "thing", "way", "see", "l", "var", "el")
val stopWordsRemover = stopWordRemover.setStopWords(stopwords)
val countVectorizer = new CountVectorizer()
  .setMinDF(3).setMinTF(2).setInputCol("filteredTokens").setOutputCol("features")
val stanfordLemmatizer = new StanfordLemmatizer()

val lemmatizer = udf((s: String) => {
  val doc = new Document(s)
  doc.sentences().map(_.lemmas()).map(_.mkString(" ")).mkString(" ")
})


