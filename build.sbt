name := "scala_news"

version := "1.0"

scalaVersion := "2.10.6"

resolvers += "Spark Packages Repository" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.10" % "2.1.0",
  "com.databricks" % "spark-csv_2.10" % "1.5.0",
  "org.apache.spark" % "spark-mllib_2.10" % "2.1.0",
  "com.github.nscala-time" %% "nscala-time" % "2.16.0"
)

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.7.0" withSources() withJavadoc()
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.7.0" classifier "models"
libraryDependencies += "databricks" % "spark-corenlp" % "0.2.0-s_2.11"