package org.sciscala.simpledf.arrayseq

import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sciscala.simpledf.Column
import org.sciscala.simpledf.types._
import org.sciscala.simpledf.arrayseq.utils._

import java.nio.file.Paths
import scala.collection.immutable.ArraySeq
import scala.reflect.{ClassTag, classTag}

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
      Paths.get(getClass.getResource("/serpents.csv").getPath),
      schema,
      true,
      Some("name")
    ) shouldBe df
  }

  "DataFrameReader" should "read a CSV file without headers from a File" in {
    ArraySeqDataFrameReader.arrayseqDataFrameReader.readCSV(
      Paths.get(getClass.getResource("/serpentsNoHeaders.csv").getPath),
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
      Paths.get(getClass.getResource("/serpents.csv").getPath),
      schema,
      true,
      None
    ) shouldBe dfFullDataNoIndex
  }

  "DataFrameReader" should "read a CSV file with no headers and no index from a File" in {
    ArraySeqDataFrameReader.arrayseqDataFrameReader.readCSV(
      Paths.get(getClass.getResource("/serpentsNoHeaders.csv").getPath),
      schema,
      false,
      None
    ) shouldBe dfFullDataNoIndex
  }

  "DataFrameReader" should "read a Json with a simple Object from a String" in {
    val simpleObj = """{"omg": "123", "wtf": 12, "right": true}"""
    ArraySeqDataFrameReader.arrayseqDataFrameReader.readJson(simpleObj).data.head.head shouldBe jsonData.head.head
  }

  "DataFrameReader" should "read a Json with a simple Array from a String" in {
    val simpleArr =
      """[{"omg": "123", "wtf": 12, "right": true}, {"omg": "245", "wtf": 15, "right": false}, {"omg": "678", "wtf": 10, "right": false}]"""

    val parsed = ArraySeqDataFrameReader.arrayseqDataFrameReader.readJson(simpleArr)

    parsed.data.head shouldBe jsonData.head
    parsed.data(1) shouldBe jsonData(1)
    parsed.data(2) shouldBe jsonData(2)
    parsed.data shouldBe jsonData
    parsed.columns shouldBe jsonCols
  }

  "DataFrameReader" should "read a Json with a nested object from a String" in {
    val dataString =
      """{"base": "USD","date": "2021-01-24","time_last_updated": 1611446401,"rates": {"USD": 1,"AED": 3.6725,"ARS": 86.363457,"AUD": 1.294731,"BGN": 1.606893,"BRL": 5.383967,"BSD": 1,"CAD": 1.268251,"CHF": 0.885517,"CLP": 717.616192,"CNY": 6.478288,"COP": 3471.118897,"CZK": 21.451045}}""".stripMargin
    val quotes = ArraySeq(1, 3.6725, 86.363457, 1.294731, 1.606893, 5.383967, 1, 1.268251, 0.885517, 717.616192, 6.478288, 3471.118897, 21.451045)
    val parsed = ArraySeqDataFrameReader.arrayseqDataFrameReader.readJson(dataString).data.head

    parsed shouldBe quotes
  }

  "DataFrameReader" should "read a Json with a simple Array from a file" in {
    val parsed = ArraySeqDataFrameReader.arrayseqDataFrameReader.readJson(Paths.get(getClass.getResource("/test.json").getPath))

    parsed.data.head shouldBe jsonData.head
    parsed.data(1) shouldBe jsonData(1)
    parsed.data(2) shouldBe jsonData(2)
    parsed.data shouldBe jsonData
    parsed.columns shouldBe jsonCols
  }

  "DataFrameReader" should "read a Json with a nested object from a file" in {
    val quotes = ArraySeq(1.0, 3.6725, 87.806529, 1.314093, 1.630231, 5.391292, 1.0, 1.278027, 0.903631, 734.282061, 6.472596, 3531.50652, 21.63737, 6.218152, 58.043863, 15.703007, 0.833808, 2.04351, 0.730873, 7.76729, 7.758122, 6.27993, 297.911099, 14164.926486, 3.300107, 72.975123, 130.199729, 105.484883, 1120.951784, 422.152477, 15.419596, 20.323743, 4.062151, 8.624232, 1.396817, 1.0, 3.638922, 48.137864, 160.546863, 3.760103, 6823.202154, 4.072388, 75.464191, 3.75, 8.457972, 1.337017, 30.109482, 7.113204, 27.990097, 27.828902, 42.391451, 14.992974)
    val parsed = ArraySeqDataFrameReader.arrayseqDataFrameReader.readJson(Paths.get(getClass.getResource("/USD.json").getPath)).data.head

    parsed shouldBe quotes
  }
}
