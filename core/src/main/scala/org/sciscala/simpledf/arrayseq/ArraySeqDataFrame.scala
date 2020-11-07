package org.sciscala.simpledf.arrayseq

import org.sciscala.simpledf._
import scala.collection.immutable.ArraySeq

case class ArraySeqDataFrame(
    data: ArraySeq[Column[_]],
    index: ArraySeq[String],
    columns: ArraySeq[String]
) {
  val shape: (Int, Int) = (if (this.data.length > 0) this.data(0).size else 0, this.data.length)
}


