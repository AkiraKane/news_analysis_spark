/**
  * Created by warren on 5/27/17.
  */
package scala.stanfordLemmatizer
import edu.stanford.nlp.simple.Document
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import scala.collection.JavaConversions._

class StanfordLemmatizer(override val uid: String) extends UnaryTransformer[String, String, StanfordLemmatizer] {

  def this() = this(Identifiable.randomUID("StanfordLemmatizer"))

  override def outputDataType: DataType = StringType

  override def createTransformFunc: (String) => String = (docString: String) => {
    val doc = new Document(docString)
    doc.sentences().map(_.lemmas()).map(_.mkString(" ")).mkString(" ")
  }
}
