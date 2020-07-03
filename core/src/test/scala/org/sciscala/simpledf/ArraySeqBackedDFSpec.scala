package org.sciscala.simpledf

import scala.collection.immutable.ArraySeq

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.collection.mutable.ListBuffer

class ArraySeqBackedDFSpec extends AnyFlatSpec with Matchers {

  case class Serpent(name: String, speed: Int, stamina: Int)

  implicit val arraySeqDFEncoder = new Encoder[ArraySeqBackedDF, Serpent] {

  import DataFrame.ops._

    override def encode(df: ArraySeqBackedDF): Seq[Serpent] = {
      var collector = new ListBuffer[Serpent]()
      for (i <- 1 to df.data.length) {
        val index = i - 1
        val name = if (df.index.indices.contains(index)) df.index(index) else index.toString
        val ndarr = df.data(index)
        collector += Serpent(
          name,
          ndarr.get[Int](0),
          ndarr.get[Int](1))
      }
      collector.toSeq
    }

  }

  val data = ArraySeq(
    NDArray(1, 2),
    NDArray(4, 5),
    NDArray(7, 8),
    NDArray(10, 11),
    NDArray(13, 14),
    NDArray(16, 17)
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
  val df = ArraySeqBackedDF(data, index, cols)
  val dfNoIndex = ArraySeqBackedDF(data, ArraySeq.empty[String], cols)
  val dfNoCols = ArraySeqBackedDF(data, index, ArraySeq.empty[String])
  val dfOnlyCols = ArraySeqBackedDF(
    ArraySeq.empty[NDArray[Int]],
    ArraySeq.empty[String],
    cols
  )
  val nullArraySeqDF = ArraySeqBackedDF(
    ArraySeq.empty[NDArray[Int]],
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

  "Size" should "equal 12" in {
    DataFrame[ArraySeqBackedDF].size(df) shouldEqual 12
  }

  "Shape" should "equal (6,2)" in {
    DataFrame[ArraySeqBackedDF].shape(df) shouldBe (6, 2)
  }

  "Head" should "return a Dataframe with the first 5 rows" in {
    val d = DataFrame[ArraySeqBackedDF].head(df)
    d.data shouldBe data.take(5)
    d.index shouldBe index.take(5)
    d.columns shouldBe cols.take(5)
  }

  "head(2)" should "return a Dataframe with the first 2 rows" in {
    val d = DataFrame[ArraySeqBackedDF].head(df, 2)
    d.data shouldBe data.take(2)
    d.index shouldBe index.take(2)
    d.columns shouldBe cols.take(2)
  }

  "Head of dfNoIndex" should "return a Dataframe with the first 5 rows and an empty index" in {
    val d = DataFrame[ArraySeqBackedDF].head(dfNoIndex)
    d.data shouldBe data.take(5)
    d.index shouldBe ArraySeq()
    d.columns shouldBe cols.take(5)
  }

  "Head of dfNoCols" should "return a Dataframe with the first 5 rows and a empty column names" in {
    val d = DataFrame[ArraySeqBackedDF].head(dfNoCols)
    d.data shouldBe data.take(5)
    d.index shouldBe index.take(5)
    d.columns shouldBe ArraySeq()
  }

  "iat(2,1)" should "equal 8" in {
    DataFrame[ArraySeqBackedDF].iat(df, 2, 1) shouldBe Some(8)
  }

  "iat(-2,1)" should "equal 14" in {
    DataFrame[ArraySeqBackedDF].iat(df, -2, 1) shouldBe Some(14)
  }

  "iat(2,-1)" should "equal 7" in {
    DataFrame[ArraySeqBackedDF].iat(df, 2, -1) shouldBe Some(8)
  }

  "iat(-2,-1)" should "equal 13" in {
    DataFrame[ArraySeqBackedDF].iat(df, -2, -2) shouldBe Some(13)
  }

  "at('viper', 'stamina')" should "equal 2" in {
    DataFrame[ArraySeqBackedDF].at(df, "viper", "stamina") shouldBe Some(2)
  }

  "at('viper', 'venom')" should "be None" in {
    DataFrame[ArraySeqBackedDF].at(df, "viper", "venom") shouldBe None
  }

  "loc('viper')" should "return df's first row" in {
    val row = DataFrame[ArraySeqBackedDF].loc(df, "viper") shouldBe
      ArraySeqBackedDF(ArraySeq(NDArray(1, 2)), ArraySeq("viper"), cols)
  }

  "loc('venom')" should "return 'null' dataframe" in {
    val row = DataFrame[ArraySeqBackedDF].loc(df, "venom") shouldBe nullArraySeqDF
  }

  "loc('viper', 'python')" should "return first and fourth rows" in {
    DataFrame[ArraySeqBackedDF].loc(df, Seq("viper", "python")) shouldBe
      ArraySeqBackedDF(
        ArraySeq(NDArray(1, 2), NDArray(10, 11)),
        ArraySeq("viper", "python"),
        cols
      )
  }

  "loc('viper', 'venom')" should "return a dataframe with only 'viper' elements" in {
    DataFrame[ArraySeqBackedDF].loc(df, Seq("viper", "venom")) shouldBe
      ArraySeqBackedDF(
        ArraySeq(NDArray(1, 2)),
        ArraySeq("viper"),
        cols
      )
  }

  "loc(true, false, false, true, false, false)" should "return first and fourth rows" in {
    DataFrame[ArraySeqBackedDF]
      .loc(df, Seq(true, false, false, true, false, false)) shouldBe
      ArraySeqBackedDF(
        ArraySeq(NDArray(1, 2), NDArray(10, 11)),
        ArraySeq("viper", "python"),
        cols
      )
  }

  "loc(false, false, false, false, false, false)" should "return a dataframe with no rows and only colNames" in {
    DataFrame[ArraySeqBackedDF]
      .loc(df, Seq(false, false, false, false, false, false)) shouldBe dfOnlyCols
  }

  "loc(true, true, true, true, true, true)" should "be the original dataframe" in {
    DataFrame[ArraySeqBackedDF]
      .loc(df, Seq(true, true, true, true, true, true)) shouldBe df
  }

  "Encoder" should "encode DataFrame" in {
    arraySeqDFEncoder.encode(df) shouldBe serpents
  }

  "Encoder" should "encode DataFrame with no index" in {
    arraySeqDFEncoder.encode(dfNoIndex) shouldBe serpentsNoIndex
  }

  "to" should "encode DataFrame" in {
    DataFrame[ArraySeqBackedDF].to(df) shouldBe serpents
  }

  "to" should "encode DataFrame with no index" in {
    DataFrame[ArraySeqBackedDF].to(dfNoIndex) shouldBe serpentsNoIndex
  }
}
