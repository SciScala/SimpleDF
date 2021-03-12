package org.sciscala.simpledf.arrayseq

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.sciscala.simpledf.types._
import org.sciscala.simpledf.arrayseq.utils._

import java.nio.file.Paths;

import scala.collection.immutable.ArraySeq

class ArraySeqDataFrameReaderSpec extends AnyFlatSpec with Matchers {
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

  val dfFullDataNoIndex =
    ArraySeqDataFrame(fullData, ArraySeq.empty[String], "name" +: cols)

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

  "DataFrameReader" should "read a Json file with a simple Object from a String" in {

    val simpleObj = """{"omg": "123", "wtf": 12, "right": true}"""
    Json2ArraySeqDataFrame.processJsonString(simpleObj).data(0)(0) shouldBe jsonData(0)(0)
  }

  "DataFrameReader" should "read a Json file with a simple Array from a String" in {
    val simpleArr =
      """[{"omg": "123", "wtf": 12, "right": true}, {"omg": "245", "wtf": 15, "right": false}, {"omg": "678", "wtf": 10, "right": false}]"""

    Json2ArraySeqDataFrame.processJsonString(simpleArr) shouldBe jsonDf
  }

  "DataFrameReader" should "read a Json file with a nested object from a String" in {
    val dataString =
      """{"base": "USD","date": "2021-01-24","time_last_updated": 1611446401,"rates": {"USD": 1,"AED": 3.6725,"ARS": 86.363457,"AUD": 1.294731,"BGN": 1.606893,"BRL": 5.383967,"BSD": 1,"CAD": 1.268251,"CHF": 0.885517,"CLP": 717.616192,"CNY": 6.478288,"COP": 3471.118897,"CZK": 21.451045}}""".stripMargin
    val quotes = ArraySeq(1, 3.6725, 86.363457, 1.294731, 1.606893, 5.383967, 1, 1.268251, 0.885517, 717.616192, 6.478288, 3471.118897, 21.451045)
    val parsed = Json2ArraySeqDataFrame.processJsonString(dataString).data(0).map(_.asInstanceOf[Double])
    parsed shouldBe quotes
  }
}
