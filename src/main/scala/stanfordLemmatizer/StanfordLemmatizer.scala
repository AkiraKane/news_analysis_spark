/**
  * Created by warren on 5/27/17.
  */
package stanfordLemmatizer

import edu.stanford.nlp.simple.Document
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{DataType, StringType}

import scala.collection.JavaConversions._

class StanfordLemmatizer(override val uid: String) extends UnaryTransformer[String, String, StanfordLemmatizer] with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("StanfordLemmatizer"))

  override protected def outputDataType: DataType = StringType

  override protected def createTransformFunc: (String) => String = (docString: String) => {
    val regexString ="""[\p{Punct}]|[^\x00-\x7F]|\s{2,}?|\-?lrb\-?|\-?lcb\-?|\-?rcb\-?|\-?lsb\-?|\-?rsb\-?|\-?rrb\-?"""
    try {
      val doc = new Document(docString)
      doc.sentences().map(_.lemmas()).map(_.mkString(" ")).mkString(" ").replaceAll(regexString, "")
    } catch {
      case e: Exception => ""
    }
  }



  override protected def validateInputType(inputType: DataType): Unit = super.validateInputType(StringType)
}
