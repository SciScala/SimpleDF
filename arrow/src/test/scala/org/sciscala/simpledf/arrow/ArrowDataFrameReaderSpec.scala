package org.sciscala.simpledf.arrow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.sciscala.simpledf.arrow.utils._

import java.nio.file.Paths
import java.{math => jm}

import scala.collection.immutable.ArraySeq
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8
import org.apache.arrow.vector.VarCharVector

import org.sciscala.simpledf.DataFrame.ops._
import org.sciscala.simpledf.arrow.ArrowDataFrame._
import org.sciscala.simpledf.arrow.implicits._

class ArrowDataFrameReaderSpec extends AnyFlatSpec with Matchers {

  "DataFrameReader" should "read a CSV file from a String" in {
    val csv = """name,speed,stamina
viper,1,2
sidewinder,4,5
cobra,7,8
python,10,11
anaconda,13,14
yellowbeard,16,17""".stripMargin

    val root = ArrowDataFrameReader.arrowDataFrameReader.readCSV(
      csv,
      serpentSchema,
      true,
      Some("name")
    )
    root.data shouldEqual df.data
    root.columns shouldBe df.columns
    root.index shouldBe df.index
  }

  "DataFrameReader" should "read a CSV file without headers from a String" in {
    val csv = """viper,1,2
sidewinder,4,5
cobra,7,8
python,10,11
anaconda,13,14
yellowbeard,16,17""".stripMargin

    val root = ArrowDataFrameReader.arrowDataFrameReader.readCSV(
      csv,
      serpentSchema,
      false,
      Some("name")
    )
    root.data shouldEqual df.data
    root.columns shouldBe df.columns
    root.index shouldBe df.index
  }

  "DataFrameReader" should "read a CSV file from a File" in {
    val root = ArrowDataFrameReader.arrowDataFrameReader.readCSV(
      Paths.get(getClass.getResource("/serpents.csv").getPath()),
      serpentSchema,
      true,
      Some("name")
    )
    root.data shouldEqual df.data
    root.columns shouldBe df.columns
    root.index shouldBe df.index
  }

  "DataFrameReader" should "read a CSV file without headers from a File" in {
    val root = ArrowDataFrameReader.arrowDataFrameReader.readCSV(
      Paths.get(getClass.getResource("/serpentsNoHeaders.csv").getPath()),
      serpentSchema,
      false,
      Some("name")
    )
    root.columns shouldBe df.columns
    root.index shouldBe df.index
    root.data.contentToTSVString shouldEqual df.data.contentToTSVString()
  }

  "DataFrameReader" should "read a Json with a simple Object from a String" in {
    val simpleObj = """{"omg": "123", "wtf": 12, "right": true}"""
    val parsed = ArrowDataFrameReader.arrowDataFrameReader.readJson(simpleObj, Some(jsonSchema))
    val c01 = parsed.iat(0,1)
    val bd = new java.math.BigDecimal(12).setScale(2)
    c01 shouldBe Some(bd)
  }

  "DataFrameReader" should "read a Json with a simple Array from a String" in {
    val simpleArr = """[{"omg": "123", "wtf": 12, "right": true}, {"omg": "245", "wtf": 15, "right": false}, {"omg": "678", "wtf": 10, "right": false}]"""
    val parsed = ArrowDataFrameReader.arrowDataFrameReader.readJson(simpleArr, Some(jsonSchema))
    val c11 = parsed.iat(1,1)
    val c22 = parsed.iat(2,2)
    val bd = new java.math.BigDecimal(15).setScale(2)
    c11 shouldBe Some(bd)
    c22 shouldBe Some(false)
  }

  "DataFrameReader" should "read a Json with a nested Object from a String" in {
    val dataString =
      """{"base": "USD","date": "2021-01-24","time_last_updated": 1611446401,"rates": {"USD": 1,"AED": 3.6725,"ARS": 86.363457,"AUD": 1.294731,"BGN": 1.606893,"BRL": 5.383967,"BSD": 1,"CAD": 1.268251,"CHF": 0.885517,"CLP": 717.616192,"CNY": 6.478288,"COP": 3471.118897,"CZK": 21.451045}}""".stripMargin
    val quotes = ArraySeq(1, 3.6725, 86.363457, 1.294731, 1.606893, 5.383967, 1, 1.268251, 0.885517, 717.616192, 6.478288, 3471.118897, 21.451045)
    val parsed = ArrowDataFrameReader.arrowDataFrameReader.readJson(dataString, Some(nestedSchema))
    val c03 = parsed.iat(0,3)
    val c80 = parsed.iat(8,0)
    val bd1 = new java.math.BigDecimal(1611446401)//.setScale(4, jm.RoundingMode.HALF_UP)
    val bd2 = new java.math.BigDecimal(0.885517).setScale(6, jm.RoundingMode.HALF_UP)
    c03 shouldBe Some(bd1)
    c80 shouldBe Some(bd2)
  }

}
