

import org.apache.spark.mllib.optimization.Gradient

def indexOfLargest(array: Seq[Int]) = {
  val result = array.foldLeft(-1,Int.MinValue,0) {
    case ((maxIndex, maxValue, currentIndex), currentValue) =>
      if(currentValue > maxValue) (currentIndex,currentValue,currentIndex+1)
      else (maxIndex,maxValue,currentIndex+1)
  }
  result._1
}

val test = Vector(1,2,34)

test indexOf test.max