package org.sciscala.simpledf.arrayseq

import java.nio.file.Paths

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ListBuffer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.sciscala.simpledf._
import org.sciscala.simpledf.types._
import org.sciscala.simpledf.codecs._
import org.sciscala.simpledf.arrayseq.utils._
import org.sciscala.simpledf.row.Row
import org.sciscala.simpledf.arrayseq.ArraySeqEncoder._
import org.sciscala.simpledf.arrayseq.ArraySeqDataFrame
import org.sciscala.simpledf.arrayseq.ArraySeqDataFrameReader

import org.sciscala.simpledf.DataFrame.ops._
import org.sciscala.simpledf.DataFrameReader.ops._

case class Serpent(name: String, speed: Int, stamina: Int)

class ArraySeqDataFrameSpec extends AnyFlatSpec with Matchers {

  implicit val arraySeqDFEncoder: Encoder[ArraySeqDataFrame, Serpent] = (df: ArraySeqDataFrame) => {
    df.data(0).indices.map(i => {
      val idx = if (df.index.isEmpty) i.toString else df.index(i)
      val speed = df.data(0)(i).asInstanceOf[Int]
      val stamina = df.data(1)(i).asInstanceOf[Int]
      Serpent(idx, speed, stamina)
    })
  }

  implicit val arraySeqDFDecoder: Decoder[Serpent, ArraySeqDataFrame] = (as: Seq[Serpent]) => {
    val cols = ArraySeq("speed", "stamina")
    val size = as.size
    var names = new Array[String](size)
    var speeds = new Array[Int](size)
    var staminas = new Array[Int](size)
    as.zipWithIndex.foreach(srpnt => {
      names(srpnt._2) = srpnt._1.name
      speeds(srpnt._2) = srpnt._1.speed
      staminas(srpnt._2) = srpnt._1.stamina
      ()
    })

    val index = ArraySeq.from(names)
    val data: ArraySeq[ArraySeq[_]] = ArraySeq(
      ArraySeq.from(speeds),
      ArraySeq.from(staminas)
    )
    ArraySeqDataFrame(data, index, cols)
  }

  "Size" should "equal 12" in {
    df.size shouldEqual 12
  }

  "Shape" should "equal (6,2)" in {
    df.shape shouldBe (6, 2)
  }

  "Head" should "return a Dataframe with the first 5 rows" in {
    val d = df.head()
    d.data shouldBe data.take(5)
    d.index shouldBe index.take(5)
    d.columns shouldBe cols.take(5)
  }

  "head(2)" should "return a Dataframe with the first 2 rows" in {
    val d = df.head(2)
    d.data shouldBe data.take(2)
    d.index shouldBe index.take(2)
    d.columns shouldBe cols.take(2)
  }

  "Head of dfNoIndex" should "return a Dataframe with the first 5 rows and an empty index" in {
    val d = dfNoIndex.head()
    d.data shouldBe data.take(5)
    d.index shouldBe ArraySeq()
    d.columns shouldBe cols.take(5)
  }

  "Head of dfNoCols" should "return a Dataframe with the first 5 rows and a empty column names" in {
    val d = dfNoCols.head()
    d.data shouldBe data.take(5)
    d.index shouldBe index.take(5)
    d.columns shouldBe ArraySeq()
  }

  "Tail" should "return a Dataframe with the last 5 rows" in {
    val d = df.tail()
    d.data shouldBe data.takeRight(5)
    d.index shouldBe index.takeRight(5)
    d.columns shouldBe cols.takeRight(5)
  }

  "tail(2)" should "return a Dataframe with the last 2 rows" in {
    val d = df.tail(2)
    d.data shouldBe data.takeRight(2)
    d.index shouldBe index.takeRight(2)
    d.columns shouldBe cols.takeRight(2)
  }

  "Tail of dfNoIndex" should "return a Dataframe with the last 5 rows and an empty index" in {
    val d = dfNoIndex.tail()
    d.data shouldBe data.takeRight(5)
    d.index shouldBe ArraySeq()
    d.columns shouldBe cols.takeRight(5)
  }

  "Tail of dfNoCols" should "return a Dataframe with the last 5 rows and a empty column names" in {
    val d = dfNoCols.tail()
    d.data shouldBe data.takeRight(5)
    d.index shouldBe index.takeRight(5)
    d.columns shouldBe ArraySeq()
  }

  "iat(2,1)" should "equal 8" in {
    df.iat(2, 1) shouldBe Some(8)
  }

  "iat(-2,1)" should "equal 14" in {
    df.iat(-2, 1) shouldBe Some(14)
  }

  "iat(2,-1)" should "equal 7" in {
    df.iat(2, -1) shouldBe Some(8)
  }

  "iat(-2,-1)" should "equal 13" in {
    df.iat(-2, -2) shouldBe Some(13)
  }

  "at('viper', 'stamina')" should "equal 2" in {
    df.at("viper", "stamina") shouldBe Some(2)
  }

  "at('viper', 'venom')" should "be None" in {
    df.at("viper", "venom") shouldBe None
  }

  "loc('viper')" should "return df's first row" in {
    df.loc("viper") shouldBe
      ArraySeqDataFrame(
        ArraySeq(ArraySeq(1), ArraySeq(2)),
        ArraySeq("viper"),
        cols
      )
  }

  "loc('venom')" should "return 'null' dataframe" in {
    df.loc("venom") shouldBe nullArraySeqDF
  }

  "loc('viper', 'python')" should "return first and fourth rows" in {
    df.loc(Seq("viper", "python")) shouldBe
      ArraySeqDataFrame(
        ArraySeq(ArraySeq(1, 10), ArraySeq(2, 11)),
        ArraySeq("viper", "python"),
        cols
      )
  }

  "loc('viper', 'venom')" should "return a dataframe with only 'viper' elements" in {
    df.loc(Seq("viper", "venom")) shouldBe
      ArraySeqDataFrame(
        ArraySeq(ArraySeq(1), ArraySeq(2)),
        ArraySeq("viper"),
        cols
      )
  }

  "loc(true, false, false, true, false, false)" should "return first and fourth rows" in {
    df.loc(Seq(true, false, false, true, false, false)) shouldBe
      ArraySeqDataFrame(
        ArraySeq(ArraySeq(1, 10), ArraySeq(2, 11)),
        ArraySeq("viper", "python"),
        cols
      )
  }

  "loc(false, false, false, false, false, false)" should "return a dataframe with no rows and only colNames" in {
    df.loc(Seq(false, false, false, false, false, false)) shouldBe dfOnlyCols
  }

  "loc(true, true, true, true, true, true)" should "be the original dataframe" in {
    df.loc(Seq(true, true, true, true, true, true)) shouldBe df
  }

  "Encoder" should "encode DataFrame" in {
    arraySeqDFEncoder.encode(df) shouldBe serpents
  }

  "Encoder" should "encode DataFrame with no index" in {
    arraySeqDFEncoder.encode(dfNoIndex) shouldBe serpentsNoIndex
  }

  "to" should "encode DataFrame" in {
    df.to shouldBe serpents
  }

  "to" should "encode DataFrame with no index" in {
    dfNoIndex.to shouldBe serpentsNoIndex
  }

  "insert" should "adds a column at index `loc`" in {
    val newCol = ArraySeq(13, 15, 17, 19, 11, 20)
    val newDF = df
      .insert[Column[Int]](1, "sight", newCol, false)
      .getOrElse(nullArraySeqDF)
    newDF.data(1) shouldBe newCol
  }

  "empty" should "return true if dataframe is empty" in {
    nullArraySeqDF.empty shouldBe true
  }

  "empty" should "return false if dataframe is not empty" in {
    df.empty shouldBe false
  }

  "get(name)" should "return the dataframe with the `name` label" in {
    //df.get[String](df, "speed", None) shouldBe ArraySeqDataFrame(ArraySeq(ArraySeq(1,4,7,10,13,16)), index, ArraySeq("speed"))
    df.get[String]("speed", None) shouldBe ArraySeqDataFrame(
      ArraySeq(ArraySeq(1, 4, 7, 10, 13, 16)),
      index,
      ArraySeq("speed")
    )
  }

  "get(unknown)" should "return the default value" in {
    df.get[String]("poison", None) shouldBe nullArraySeqDF
  }

  "items" should "return data in Array[(columnName, Column)] format" in {
    df.items shouldBe Array("speed" -> data(0), "stamina" -> data(1))
  }

  "items" should "return empty array for emptyDF" in {
    nullArraySeqDF.items shouldBe Array()
  }

  "iterrows" should "return sequence of tuples (index, Row) format" in {
    df.iterrows(rowEncoder(schema)) shouldBe Seq(
      ("viper", Row(Seq(1, 2), schema)),
      ("sidewinder", Row(Seq(4, 5), schema)),
      ("cobra", Row(Seq(7, 8), schema)),
      ("python", Row(Seq(10, 11), schema)),
      ("anaconda", Row(Seq(13, 14), schema)),
      ("yellowbeard", Row(Seq(16, 17), schema))
    )
  }

  "iterrows" should "return empty sequence of tuples for emptyDF" in {
    nullArraySeqDF.iterrows(rowEncoder(schema)) shouldBe Seq()
  }

  "decoder" should "return `Serpents` as a Dataframe" in {
    arraySeqDFDecoder.decode(serpents) shouldBe df
  }
}
