package org.sciscala.simpledf.arrayseq

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ListBuffer

import org.sciscala.simpledf.types._
import org.sciscala.simpledf.codecs._

object utils {
  val schema = Schema(
    Seq(
      Field("name", StringType, false),
      Field("speed", IntType, false),
      Field("stamina", IntType, false)
    )
  )

  val data = ArraySeq(
    ArraySeq(1, 4, 7, 10, 13, 16),
    ArraySeq(2, 5, 8, 11, 14, 17)
  )
  val index = ArraySeq(
    "viper",
    "sidewinder",
    "cobra",
    "python",
    "anaconda",
    "yellowbeard"
  )
  val cols = ArraySeq("speed", "stamina")
  val df = ArraySeqDataFrame(data, index, cols)
  val dfNoIndex = ArraySeqDataFrame(data, ArraySeq.empty[String], cols)
  val dfNoCols = ArraySeqDataFrame(data, index, ArraySeq.empty[String])
  val dfOnlyCols =
    ArraySeqDataFrame(ArraySeq(ArraySeq()), ArraySeq.empty[String], cols)
  val nullArraySeqDF = ArraySeqDataFrame(
    ArraySeq.empty[ArraySeq[Int]],
    ArraySeq.empty[String],
    ArraySeq.empty[String]
  )

  val serpents = ArraySeq(
    Serpent("viper", 1, 2),
    Serpent("sidewinder", 4, 5),
    Serpent("cobra", 7, 8),
    Serpent("python", 10, 11),
    Serpent("anaconda", 13, 14),
    Serpent("yellowbeard", 16, 17)
  )

  val serpentsNoIndex = ArraySeq(
    Serpent("0", 1, 2),
    Serpent("1", 4, 5),
    Serpent("2", 7, 8),
    Serpent("3", 10, 11),
    Serpent("4", 13, 14),
    Serpent("5", 16, 17)
  )

}
