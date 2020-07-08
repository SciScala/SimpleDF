package org.sciscala.simpledf

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.arrow.vector.{FieldVector, UInt4Vector, VectorSchemaRoot}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.ArrowType

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters._
import org.sciscala.simpledf.arrow._
import org.scalactic.Equality

class ArrowDataFrameSpec extends AnyFlatSpec with Matchers {

  case class Serpent(name: String, speed: Int, stamina: Int)

  implicit val schemaRootDataFrame = new Equality[VectorSchemaRoot] {
    override def areEqual(a: VectorSchemaRoot, b: Any): Boolean = 
      b match {
        case root: VectorSchemaRoot =>
          a.equals(root)
        case _ => false
      }
  }

  val intType = new FieldType(false, new ArrowType.Int(32, false), null)
  val speedField = new Field("speed", intType, null)
  val staminaField = new Field("stamina", intType, null)
  val schemaFields = List(speedField, staminaField)
  val schema: Schema = new Schema(schemaFields.asJava)
  val data = VectorSchemaRoot.create(schema, new RootAllocator())
  data.allocateNew()

  /*private def vectorizeSerpent(
      idx: Int,
      serpent: Serpent,
      schemaRoot: VectorSchemaRoot
  ): Unit = {
    schemaRoot
      .getVector("speed")
      .asInstanceOf[UInt4Vector]
      .setSafe(idx, serpent.speed)
    schemaRoot
      .getVector("stamina")
      .asInstanceOf[UInt4Vector]
      .setSafe(idx, serpent.stamina)
  }*/

  private def initData(): Unit = {
    val speedVector: UInt4Vector = data.getVector("speed").asInstanceOf[UInt4Vector]
    speedVector.allocateNew()
    speedVector.setSafe(0, 1) // setSafe checks we don't exceed initialCapacity
    speedVector.setSafe(1, 4)
    speedVector.setSafe(2, 7)
    speedVector.setSafe(3, 10)
    speedVector.setSafe(4, 13)
    speedVector.setSafe(5, 16)
    speedVector.setValueCount(6)

    val staminaVector: UInt4Vector =
      data.getVector("stamina").asInstanceOf[UInt4Vector]
    staminaVector.allocateNew()
    staminaVector.setSafe(0, 2) // setSafe checks we don't exceed initialCapacity
    staminaVector.setSafe(1, 5)
    staminaVector.setSafe(2, 8)
    staminaVector.setSafe(3, 11)
    staminaVector.setSafe(4, 14)
    staminaVector.setSafe(5, 17)
    staminaVector.setValueCount(6)
  }

  val index = ArraySeq(
    "viper",
    "sidewinder",
    "cobra",
    "python",
    "anaconda",
    "yellowbeard"
  )

  val cols = ArraySeq.from(schema.getFields().asScala.map(f => f.getName()))

  val serpents = ArraySeq(
    Serpent("viper", 1, 2),
    Serpent("sidewinder", 4, 5),
    Serpent("cobra", 7, 8),
    Serpent("python", 10, 11),
    Serpent("anaconda", 13, 14),
    Serpent("yellowbeard", 16, 17)
  )

  //serpents.zipWithIndex.foreach(t => vectorizeSerpent(t._2, t._1, data))
  data.setRowCount(6)

  val df = ArrowDataFrame(data, index, cols)

  val dfNoIndex = ArrowDataFrame(data, ArraySeq.empty[String], cols)
  val dfNoCols = ArrowDataFrame(data, index, ArraySeq.empty[String])
  val dfOnlyCols = ArrowDataFrame(
    VectorSchemaRoot.of(Array.empty[FieldVector]: _*),
    ArraySeq.empty[String],
    cols
  )
  val nullArraySeqDF = ArrowDataFrame(
    VectorSchemaRoot.of(Array.empty[FieldVector]: _*),
    ArraySeq.empty[String],
    ArraySeq.empty[String]
  )

  val serpentsNoIndex = ArraySeq(
    Serpent("0", 1, 2),
    Serpent("1", 4, 5),
    Serpent("2", 7, 8),
    Serpent("3", 10, 11),
    Serpent("4", 13, 14),
    Serpent("5", 16, 17)
  )

  initData()

  "Size" should "equal 12" in {
    DataFrame[ArrowDataFrame].size(df) shouldEqual 12
  }

  "Shape" should "equal (6,2)" in {
    DataFrame[ArrowDataFrame].shape(df) shouldBe (6, 2)
  }

  "Head" should "return a Dataframe with the first 5 rows" in {
    val d = DataFrame[ArrowDataFrame].head(df)
    d.data shouldEqual data.slice(0, 5)
    d.index shouldBe index.take(5)
    d.columns shouldBe cols.take(5)
  }

  "head(2)" should "return a Dataframe with the first 2 rows" in {
    val d = DataFrame[ArrowDataFrame].head(df, 2)
    d.data shouldEqual data.slice(0,2)
    d.index shouldBe index.take(2)
    d.columns shouldBe cols.take(2)
  }

  "Head of dfNoIndex" should "return a Dataframe with the first 5 rows and an empty index" in {
    val d = DataFrame[ArrowDataFrame].head(dfNoIndex)
    d.data shouldEqual data.slice(0, 5)
    d.index shouldBe ArraySeq()
    d.columns shouldBe cols.take(5)
  }

  "Head of dfNoCols" should "return a Dataframe with the first 5 rows and a empty column names" in {
    val d = DataFrame[ArrowDataFrame].head(dfNoCols)
    d.data shouldEqual data.slice(0, 5)
    d.index shouldBe index.take(5)
    d.columns shouldBe ArraySeq()
  }

  "iat(2,1)" should "equal 8" in {
    DataFrame[ArrowDataFrame].iat(df, 2, 1) shouldBe Some(8)
  }
}
