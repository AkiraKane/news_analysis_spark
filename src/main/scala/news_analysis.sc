import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover, PCA, Normalizer}
import org.apache.spark.sql.functions._
import edu.stanford.nlp.simple.Document
import org.apache.spark.sql.functions.udf
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vector

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
val norm = new Normalizer().setInputCol("topicDistribution").setOutputCol("topicDistNorm")
val pca = new PCA().setInputCol("topicDistNorm").setOutputCol("topics2d").setK(2)
val lda = new LDA().setK(18).setMaxIter(60)
// optimal number thus far: 17

val lemmatizer = udf((s: String) => {
  val doc = new Document(s)
  doc.sentences().map(_.lemmas()).map(_.mkString(" ")).mkString(" ")
})

val maxTopic = udf((topicVector: Vector) => {
  val arr = topicVector.toArray
  arr.indexOf(arr.max)
})

val vox = spark.read.option("charset", "ascii")
  .json("s3://warren-datasets/vox.jsonl")
  .withColumn("org", lit("vox"))

val jezebel = spark.read.option("charset", "ascii")
  .json("s3://warren-datasets/jezebel.jsonl")
  .withColumn("org", lit("jezebel"))

val regexString ="""[\p{Punct}]|[^\x00-\x7F]|\s{2,}?|lrb|lcb|rcb|lsb|rsb|rrb"""
val news = jezebel.union(vox)
  .withColumn("textNoHttp", regexp_replace($"text", """http.*\s$|document[a-z]+|Advertisement""", ""))
  .withColumn("lemmatized", lemmatizer($"textNoHttp"))
  .drop("textNoHttp")
  .drop("text")
  .withColumn("wordsCleaned", regexp_replace($"lemmatized", regexString, ""))
  .drop("lemmatized").repartition(90)

val newsTokens = stopWordRemover.transform(tokenizer.transform(news))
  .drop("tokens")
  .drop("wordsCleaned")

val vectorizeFit = countVectorizer.fit(newsTokens)
val vectorizeTransform = vectorizeFit.transform(newsTokens).drop('filteredTokens)
val model = lda.fit(vectorizeTransform)
val modelTransform = model.transform(vectorizeTransform)
  .withColumn("maxTopic", maxTopic($"topicDistribution")).drop('features).drop('url).drop('author)
val normalized = norm.transform(modelTransform).drop('topicDistribution)
val pcaFit = pca.fit(normalized).transform(normalized)
pcaFit.write.mode("append").json("s3://warren-datasets/news_analysis.jsonl")
pcaFit.show(false)

def topicAccessor(vocabulary: Array[String]) =
  udf((indices: mutable.WrappedArray[Int]) => indices.map(i => vocabulary(i)))

val topics = model.describeTopics()
  .drop("termWeights")
  .withColumn("terms", topicAccessor(vectorizeFit.vocabulary)($"termIndices"))
  .drop("termIndices")
topics.write.mode("append").json("s3://warren-datasets/news_topics.json")
topics.show(false)