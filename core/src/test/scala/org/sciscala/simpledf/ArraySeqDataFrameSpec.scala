package org.sciscala.simpledf

import scala.collection.immutable.ArraySeq

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.collection.mutable.ListBuffer

import DataFrame.ops._

class ArraySeqDataFrameSpec extends AnyFlatSpec with Matchers {

  case class Serpent(name: String, speed: Int, stamina: Int)

  implicit val ArraySeqDFEncoder = new Encoder[ArraySeqDataFrame, Serpent] {

    override def encode(df: ArraySeqDataFrame): Seq[Serpent] = {
      (0 until df.data(0).length).map(i => {
        val idx = if (df.index.isEmpty) i.toString else df.index(i)
        val speed = df.data(0)(i).asInstanceOf[Int]
        val stamina = df.data(1)(i).asInstanceOf[Int]
        Serpent(idx, speed, stamina)
      })
    }

  }

  val data = ArraySeq(
    ArraySeq(1,4,7,10,13,16),
    ArraySeq(2,5,8,11,14,17)
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
  val dfOnlyCols = ArraySeqDataFrame(ArraySeq(ArraySeq()), ArraySeq.empty[String], cols)
  val nullArraySeqDF = ArraySeqDataFrame(ArraySeq.empty[ArraySeq[Int]], ArraySeq.empty[String], ArraySeq.empty[String])

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
    DataFrame[ArraySeqDataFrame].size(df) shouldEqual 12
  }

  "Shape" should "equal (6,2)" in {
    DataFrame[ArraySeqDataFrame].shape(df) shouldBe (6, 2)
  }

  "Head" should "return a Dataframe with the first 5 rows" in {
    val d = DataFrame[ArraySeqDataFrame].head(df)
    d.data shouldBe data.take(5)
    d.index shouldBe index.take(5)
    d.columns shouldBe cols.take(5)
  }

  "head(2)" should "return a Dataframe with the first 2 rows" in {
    val d = DataFrame[ArraySeqDataFrame].head(df, 2)
    d.data shouldBe data.take(2)
    d.index shouldBe index.take(2)
    d.columns shouldBe cols.take(2)
  }

  "Head of dfNoIndex" should "return a Dataframe with the first 5 rows and an empty index" in {
    val d = DataFrame[ArraySeqDataFrame].head(dfNoIndex)
    d.data shouldBe data.take(5)
    d.index shouldBe ArraySeq()
    d.columns shouldBe cols.take(5)
  }

  "Head of dfNoCols" should "return a Dataframe with the first 5 rows and a empty column names" in {
    val d = DataFrame[ArraySeqDataFrame].head(dfNoCols)
    d.data shouldBe data.take(5)
    d.index shouldBe index.take(5)
    d.columns shouldBe ArraySeq()
  }

  "Tail" should "return a Dataframe with the last 5 rows" in {
    val d = DataFrame[ArraySeqDataFrame].tail(df)
    d.data shouldBe data.takeRight(5)
    d.index shouldBe index.takeRight(5)
    d.columns shouldBe cols.takeRight(5)
  }

  "tail(2)" should "return a Dataframe with the last 2 rows" in {
    val d = DataFrame[ArraySeqDataFrame].tail(df, 2)
    d.data shouldBe data.takeRight(2)
    d.index shouldBe index.takeRight(2)
    d.columns shouldBe cols.takeRight(2)
  }

  "Tail of dfNoIndex" should "return a Dataframe with the last 5 rows and an empty index" in {
    val d = DataFrame[ArraySeqDataFrame].tail(dfNoIndex)
    d.data shouldBe data.takeRight(5)
    d.index shouldBe ArraySeq()
    d.columns shouldBe cols.takeRight(5)
  }

  "Tail of dfNoCols" should "return a Dataframe with the last 5 rows and a empty column names" in {
    val d = DataFrame[ArraySeqDataFrame].tail(dfNoCols)
    d.data shouldBe data.takeRight(5)
    d.index shouldBe index.takeRight(5)
    d.columns shouldBe ArraySeq()
  }

  "iat(2,1)" should "equal 8" in {
    DataFrame[ArraySeqDataFrame].iat(df, 2, 1) shouldBe Some(8)
  }

  "iat(-2,1)" should "equal 14" in {
    DataFrame[ArraySeqDataFrame].iat(df, -2, 1) shouldBe Some(14)
  }

  "iat(2,-1)" should "equal 7" in {
    DataFrame[ArraySeqDataFrame].iat(df, 2, -1) shouldBe Some(8)
  }

  "iat(-2,-1)" should "equal 13" in {
    DataFrame[ArraySeqDataFrame].iat(df, -2, -2) shouldBe Some(13)
  }

  "at('viper', 'stamina')" should "equal 2" in {
    DataFrame[ArraySeqDataFrame].at(df, "viper", "stamina") shouldBe Some(2)
  }

  "at('viper', 'venom')" should "be None" in {
    DataFrame[ArraySeqDataFrame].at(df, "viper", "venom") shouldBe None
  }

  "loc('viper')" should "return df's first row" in {
    DataFrame[ArraySeqDataFrame].loc(df, "viper") shouldBe
      ArraySeqDataFrame(ArraySeq(ArraySeq(1), ArraySeq(2)), ArraySeq("viper"), cols)
  }

  "loc('venom')" should "return 'null' dataframe" in {
    DataFrame[ArraySeqDataFrame].loc(df, "venom") shouldBe nullArraySeqDF
  }

  "loc('viper', 'python')" should "return first and fourth rows" in {
    DataFrame[ArraySeqDataFrame].loc(df, Seq("viper", "python")) shouldBe
      ArraySeqDataFrame(ArraySeq(ArraySeq(1, 10), ArraySeq(2, 11)), ArraySeq("viper", "python"), cols)
  }

  "loc('viper', 'venom')" should "return a dataframe with only 'viper' elements" in {
    DataFrame[ArraySeqDataFrame].loc(df, Seq("viper", "venom")) shouldBe
      ArraySeqDataFrame(ArraySeq(ArraySeq(1), ArraySeq(2)), ArraySeq("viper"), cols)
  }

  "loc(true, false, false, true, false, false)" should "return first and fourth rows" in {
    DataFrame[ArraySeqDataFrame]
      .loc(df, Seq(true, false, false, true, false, false)) shouldBe
      ArraySeqDataFrame(ArraySeq(ArraySeq(1, 10), ArraySeq(2, 11)), ArraySeq("viper", "python"), cols)
  }

  "loc(false, false, false, false, false, false)" should "return a dataframe with no rows and only colNames" in {
    DataFrame[ArraySeqDataFrame]
      .loc(df, Seq(false, false, false, false, false, false)) shouldBe dfOnlyCols
  }

  "loc(true, true, true, true, true, true)" should "be the original dataframe" in {
    DataFrame[ArraySeqDataFrame]
      .loc(df, Seq(true, true, true, true, true, true)) shouldBe df
  }

  "Encoder" should "encode DataFrame" in {
    ArraySeqDFEncoder.encode(df) shouldBe serpents
  }

  "Encoder" should "encode DataFrame with no index" in {
    ArraySeqDFEncoder.encode(dfNoIndex) shouldBe serpentsNoIndex
  }

  "to" should "encode DataFrame" in {
    DataFrame[ArraySeqDataFrame].to(df) shouldBe serpents
  }

  "to" should "encode DataFrame with no index" in {
    DataFrame[ArraySeqDataFrame].to(dfNoIndex) shouldBe serpentsNoIndex
  }

  "insert" should "adds a column at index `loc`" in {
    val newCol = ArraySeq(13,15,17,19,11,20)
    val newDF = DataFrame[ArraySeqDataFrame].insert[Column[Int]](df, 1, "sight", newCol, false).getOrElse(nullArraySeqDF)
    newDF.data(1) shouldBe newCol
   }
}
