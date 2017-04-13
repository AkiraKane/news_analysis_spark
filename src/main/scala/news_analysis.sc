import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.{KMeans, LDA}
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import edu.stanford.nlp.simple.Document
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConversions._

val spark = SparkSession
  .builder().master("local")
  .appName("Spark SQL basic example")
  .config("master", "spark://myhost:7077")
  .getOrCreate()

val sqlContext = spark.sqlContext

import sqlContext.implicits._

val tokenizer = new RegexTokenizer().setInputCol("wordsCleaned").setOutputCol("tokens")
val stopwords = new StopWordsRemover().setInputCol("tokens").setOutputCol("filteredTokens")
val countVectorizer = new CountVectorizer().setMinDF(3).setMinTF(2).setInputCol("filteredTokens").setOutputCol("features")
val lda = new LDA().setK(5).setMaxIter(2)

val lemmatizer = udf((s: String) => {
  val doc = new Document(s)
  doc.sentences().map(_.lemmas()).map(_.mkString(" ")).mkString(" ")
})

val jezebel = spark.read.option("charset", "ascii").json("/Users/warren/Desktop/programming_projects/news_analysis_spark/data/jezebeltest.jsonl")
  .withColumn("id", $"_id".getField("$oid"))
  .drop("_id")
  .withColumn("lemmatized", lemmatizer($"text"))
  .drop("text")
  .withColumn("wordsCleaned", regexp_replace($"lemmatized", """[\p{Punct}]|[^\x00-\x7F]|\s{2,}?""", ""))
  .drop("lemmatized")

val jezTokens = stopwords.transform(tokenizer.transform(jezebel))
  .drop("tokens")
  .drop("wordsCleaned")

val vectorized = countVectorizer.fit(jezTokens)
val vocab = vectorized.vocabulary
val model = lda.fit(vectorized.transform(jezTokens))

def topicAccessor (vocabulary: Array[String]) = udf((indices: Array[Int]) => indices.map(i => vocabulary(i)))

val topics = model.describeTopics()
  .withColumn("terms", topicAccessor(vocab)($"termIndices"))
  .drop("termWeights")

topics.show(false)