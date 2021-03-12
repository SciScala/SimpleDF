package org.sciscala.simpledf.arrow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.sciscala.simpledf.arrow.utils._

import java.nio.file.Paths;

import scala.collection.immutable.ArraySeq
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8
import org.apache.arrow.vector.VarCharVector

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

  /*val fullData = ArraySeq(
    index,
    ArraySeq(1, 4, 7, 10, 13, 16),
    ArraySeq(2, 5, 8, 11, 14, 17)
  )

  val indexVector: VarCharVector = data.getVector()
    "viper",
    "sidewinder",
    "cobra",
    "python",
    "anaconda",
    "yellowbeard"

  val dfFullDataNoIndex = ArrowDataFrame(fullData, ArraySeq.empty[String])

  "DataFrameReader" should "read a CSV file from a File with no index" in {
    ArrowDataFrameReader.arrowDataFrameReader.readCSV(
      Paths.get(getClass.getResource("/serpents.csv").getPath()),
      serpentSchema,
      true,
      None
    ) shouldBe dfFullDataNoIndex
  }

  "DataFrameReader" should "read a CSV file with no headers and no index from a File" in {
    ArrowDataFrameReader.arrowDataFrameReader.readCSV(
      Paths.get(getClass.getResource("/serpentsNoHeaders.csv").getPath()),
      serpentSchema,
      false,
      None
    ) shouldBe dfFullDataNoIndex
  }*/
}
