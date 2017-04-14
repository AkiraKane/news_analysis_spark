import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import edu.stanford.nlp.simple.Document
import org.apache.spark.sql.functions.udf
import scala.collection.JavaConversions._
import scala.collection.mutable

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
val stopwords = stopWordRemover.getStopWords ++ Array("say", "would", "one")
stopWordRemover.setStopWords(stopwords)
val countVectorizer = new CountVectorizer()
  .setMinDF(3).setMinTF(2).setInputCol("filteredTokens").setOutputCol("features")
val lda = new LDA().setK(20).setMaxIter(30)

val lemmatizer = udf((s: String) => {
  val doc = new Document(s)
  doc.sentences().map(_.lemmas()).map(_.mkString(" ")).mkString(" ")
})

val vox = spark.read.option("charset", "ascii")
  .json("/Users/warren/Desktop/programming_projects/news_analysis_spark/data/vox.jsonl")
  .withColumn("id", $"_id".getField("$oid"))
  .drop("_id")

val jezebel = spark.read.option("charset", "ascii")
  .json("/Users/warren/Desktop/programming_projects/news_analysis_spark/data/jezebel.jsonl")
  .withColumn("id", $"_id".getField("$oid"))
  .drop("_id")
  .sample(withReplacement = false, .2)

val regexString ="""[\p{Punct}]|[^\x00-\x7F]|\s{2,}?|advertisement|lrb|lcb|rcb|lsb|rsb|rrb"""
val news = jezebel.union(vox)
  .withColumn("textNoHttp", regexp_replace($"text", """http.*\s$""", ""))
  .withColumn("lemmatized", lemmatizer($"textNoHttp"))
  .drop("textNoHttp")
  .drop("text")
  .withColumn("wordsCleaned", regexp_replace($"lemmatized", regexString, ""))
  .drop("lemmatized")

val newsTokens = stopWordRemover.transform(tokenizer.transform(news))
  .drop("tokens")
  .drop("wordsCleaned")

val vectorized = countVectorizer.fit(newsTokens)
val vocab = vectorized.vocabulary
val model = lda.fit(vectorized.transform(newsTokens))

def topicAccessor(vocabulary: Array[String]) =
  udf((indices: mutable.WrappedArray[Int]) => indices.map(i => vocabulary(i)))

val topics = model.describeTopics()
  .drop("termWeights")
  .withColumn("terms", topicAccessor(vocab)($"termIndices"))
  .drop("termIndicies")

topics.show(false)