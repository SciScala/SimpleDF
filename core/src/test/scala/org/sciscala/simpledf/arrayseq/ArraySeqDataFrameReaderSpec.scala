package org.sciscala.simpledf.arrayseq

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.sciscala.simpledf.types._
import org.sciscala.simpledf.arrayseq.utils._

import java.nio.file.Paths;

import scala.collection.immutable.ArraySeq

class ArraySeqDataFrameReaderSpec extends AnyFlatSpec with Matchers {
  //TODO: Move reader tests to their own suite
  "DataFrameReader" should "read a CSV file from a String" in {
    val csv = """name,speed,stamina
viper,1,2
sidewinder,4,5
cobra,7,8
python,10,11
anaconda,13,14
yellowbeard,16,17""".stripMargin

    ArraySeqDataFrameReader.arrayseqDataFrameReader.readCSV(
      csv,
      schema,
      true,
      Some("name")
    ) shouldBe df
  }

  "DataFrameReader" should "read a CSV file without headers from a String" in {
    val csv = """viper,1,2
sidewinder,4,5
cobra,7,8
python,10,11
anaconda,13,14
yellowbeard,16,17""".stripMargin

    ArraySeqDataFrameReader.arrayseqDataFrameReader.readCSV(
      csv,
      schema,
      false,
      Some("name")
    ) shouldBe df
  }

  "DataFrameReader" should "read a CSV file from a File" in {
    ArraySeqDataFrameReader.arrayseqDataFrameReader.readCSV(
      Paths.get(getClass.getResource("/serpents.csv").getPath()),
      schema,
      true,
      Some("name")
    ) shouldBe df
  }

  "DataFrameReader" should "read a CSV file without headers from a File" in {
    ArraySeqDataFrameReader.arrayseqDataFrameReader.readCSV(
      Paths.get(getClass.getResource("/serpentsNoHeaders.csv").getPath()),
      schema,
      false,
      Some("name")
    ) shouldBe df
  }

  val fullData = ArraySeq(
    index,
    ArraySeq(1, 4, 7, 10, 13, 16),
    ArraySeq(2, 5, 8, 11, 14, 17)
  )

  val dfFullDataNoIndex = ArraySeqDataFrame(fullData, ArraySeq.empty[String], "name" +: cols)

  "DataFrameReader" should "read a CSV file from a File with no index" in {
    ArraySeqDataFrameReader.arrayseqDataFrameReader.readCSV(
      Paths.get(getClass.getResource("/serpents.csv").getPath()),
      schema,
      true,
      None
    ) shouldBe dfFullDataNoIndex
  }

  "DataFrameReader" should "read a CSV file with no headers and no index from a File" in {
    ArraySeqDataFrameReader.arrayseqDataFrameReader.readCSV(
      Paths.get(getClass.getResource("/serpentsNoHeaders.csv").getPath()),
      schema,
      false,
      None
    ) shouldBe dfFullDataNoIndex
  }
}
