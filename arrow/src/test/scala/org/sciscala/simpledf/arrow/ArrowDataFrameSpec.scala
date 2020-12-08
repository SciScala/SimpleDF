package org.sciscala.simpledf.arrow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.arrow.vector.{FieldVector, UInt4Vector, VectorSchemaRoot}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.reader.FieldReader
import org.apache.arrow.vector.types.pojo._

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.{Seq => MSeq}
import scala.jdk.CollectionConverters._
import org.sciscala.simpledf.arrow._
import org.scalactic.Equality

import scala.collection.mutable.ListBuffer

import simulacrum._
import org.sciscala.simpledf._
import org.sciscala.simpledf.DataFrame.ops._
import org.sciscala.simpledf.codecs._
import org.sciscala.simpledf.arrow.ArrowDataFrame
import org.sciscala.simpledf.arrow.ArrowDataFrame._
import org.sciscala.simpledf.arrow.ArrowEncoder._
import org.sciscala.simpledf.arrow.implicits._

import scala.languageFeature.postfixOps

class ArrowDataFrameSpec extends AnyFlatSpec with Matchers {

  case class Serpent(name: String, speed: Int, stamina: Int)

  implicit val arrowDFEncoder = new Encoder[ArrowDataFrame, Serpent] {

    override def encode(df: ArrowDataFrame): Seq[Serpent] = {
      var collector = new ListBuffer[Serpent]()
      val fvs: MSeq[FieldReader] =
        df.data.getFieldVectors.asScala.map(fv => fv.getReader)
      for (i <- 1 to df.data.getRowCount) {
        val index = i - 1
        val name =
          if (df.index.indices.contains(index)) df.index(index)
          else index.toString
        collector += Serpent(
          name,
          { fvs(0).setPosition(index); fvs(0).readInteger() },
          { fvs(1).setPosition(index); fvs(1).readInteger() }
        )
      }
      collector.toSeq
    }

  }


  implicit val schemaRootDataFrame: Equality[VectorSchemaRoot] =
    (a: VectorSchemaRoot, b: Any) => b match {
      case root: VectorSchemaRoot =>
        a.equals(root)
      case _ => false
    }

  implicit val fieldVectorDataFrame: Equality[FieldVector] =
    (a: FieldVector, b: Any) => b match {
      case field: FieldVector =>
        a.getField.equals(field) &&
          a.asScala.zip(field.asScala).foldLeft(true)( (acc,c) => acc && (c._1 == c._2))
      case _ => false
    }

  val intType = new FieldType(false, new ArrowType.Int(32, false), null)
  val speedField = new Field("speed", intType, null)
  val staminaField = new Field("stamina", intType, null)
  val schemaFields = List(speedField, staminaField)
  val schema: Schema = new Schema(schemaFields.asJava)

  private def vectorizeSerpent(
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
  }

  private def serpents2DF(
      serpents: Seq[Serpent],
      schemaRoot: VectorSchemaRoot
  ): ArrowDataFrame = {
    val index = serpents.zipWithIndex
      .map(t => {
        vectorizeSerpent(t._2, t._1, schemaRoot)
        t._1.name
      })
    schemaRoot.setRowCount(serpents.length)

    ArrowDataFrame(schemaRoot, ArraySeq.from(index))
  }

  private def initData(data: VectorSchemaRoot): Unit = {
    data.clear()
    data.allocateNew()
    val speedVector: UInt4Vector =
      data.getVector("speed").asInstanceOf[UInt4Vector]
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
    staminaVector.setSafe(
      0,
      2
    ) // setSafe checks we don't exceed initialCapacity
    staminaVector.setSafe(1, 5)
    staminaVector.setSafe(2, 8)
    staminaVector.setSafe(3, 11)
    staminaVector.setSafe(4, 14)
    staminaVector.setSafe(5, 17)
    staminaVector.setValueCount(6)
    data.setRowCount(6)
  }

  val index: Seq[String] = ArraySeq(
    "viper",
    "sidewinder",
    "cobra",
    "python",
    "anaconda",
    "yellowbeard"
  )

  val cols: Seq[String] =
    ArraySeq.from(schema.getFields.asScala.map(f => f.getName))

  val serpents: Seq[Any] = List(
    Serpent("viper", 1, 2),
    Serpent("sidewinder", 4, 5),
    Serpent("cobra", 7, 8),
    Serpent("python", 10, 11),
    Serpent("anaconda", 13, 14),
    Serpent("yellowbeard", 16, 17)
  )

  //serpents.zipWithIndex.foreach(t => vectorizeSerpent(t._2, t._1, data))
  val data: VectorSchemaRoot =
    VectorSchemaRoot.create(schema, new RootAllocator())
  initData(data)
  val df = ArrowDataFrame(data, index)

  val dfNoIndex = ArrowDataFrame(data, ArraySeq.empty[String])

  val serpentsNoIndex = List(
    Serpent("0", 1, 2),
    Serpent("1", 4, 5),
    Serpent("2", 7, 8),
    Serpent("3", 10, 11),
    Serpent("4", 13, 14),
    Serpent("5", 16, 17)
  )

  "Size" should "equal 12" in {
    df.size shouldEqual 12
  }

  "Shape" should "equal (6,2)" in {
    df.shape shouldBe (6,2)
  }

  "Head" should "return a Dataframe with the first 5 rows" in {
    val d = df.head()
    d.data shouldEqual data.slice(0, 5)
    d.index shouldBe index.take(5)
    d.columns shouldBe cols.take(5)
  }

  "head(2)" should "return a Dataframe with the first 2 rows" in {
    val d = df.head(2)
    d.data shouldEqual data.slice(0, 2)
    d.index shouldBe index.take(2)
    d.columns shouldBe cols.take(2)
  }

  "Head of dfNoIndex" should "return a Dataframe with the first 5 rows and an empty index" in {
    val d = dfNoIndex.head()
    d.data shouldEqual data.slice(0, 5)
    d.index shouldBe ArraySeq()
    d.columns shouldBe cols.take(5)
  }

  "iat(2,1)" should "equal 8" in {
    df.iat(2,1) shouldBe Some(8)
  }

  "iat(-2,1)" should "equal 14" in {
    df.iat(-2,1) shouldBe Some(14)
  }

  "iat(2,-1)" should "equal 7" in {
    df.iat(2,-1) shouldBe Some(8)
  }

  "iat(-2,-1)" should "equal 13" in {
    df.iat(-2,-2) shouldBe Some(13)
  }

  "at('viper', 'stamina')" should "equal 2" in {
    df.at("viper", "stamina") shouldBe Some(2)
  }

  "at('viper', 'venom')" should "be None" in {
    df.at("viper", "venom") shouldBe None
  }

  "get(name)" should "return the dataframe with the `name` label" in {
    val schema: Schema = new Schema(List(speedField).asJava)
    val data: VectorSchemaRoot = VectorSchemaRoot.create(schema, new RootAllocator())
    data.allocateNew()
    val speedVector: UInt4Vector =
      data.getVector("speed").asInstanceOf[UInt4Vector]
    speedVector.allocateNew()
    speedVector.setSafe(0, 1) // setSafe checks we don't exceed initialCapacity
    speedVector.setSafe(1, 4)
    speedVector.setSafe(2, 7)
    speedVector.setSafe(3, 10)
    speedVector.setSafe(4, 13)
    speedVector.setSafe(5, 16)
    speedVector.setValueCount(6)
    data.setRowCount(6)

    val expected = ArrowDataFrame(data, index)
    val arrDF = df.get[String]("speed", None)

    arrDF.data.getFieldVectors.size() shouldBe 1
    val actual = arrDF.data.getVector(0)
    val expectedVector = expected.data.getVector("speed")
    actual === expectedVector
  }

  "get(unknown)" should "return the default value" in {
    df.get[String]("poison", None) shouldBe emptyInstance
  }

  "loc('viper')" should "return df's first row" in {
    data.clear()
    data.allocateNew()
    val expected = serpents2DF(Seq(Serpent("viper", 1, 2)), data)
    df.loc("viper") shouldBe expected
  }

  "loc('venom')" should "return 'null' dataframe" in {
    df.loc("venom") shouldEqual emptyInstance
  }

  "loc('viper', 'python')" should "return first and fourth rows" in {
    val cleanVSR: VectorSchemaRoot =
      VectorSchemaRoot.create(schema, new RootAllocator())
    val expected = serpents2DF(
      Seq(Serpent("viper", 1, 2), Serpent("python", 10, 11)),
      cleanVSR
    )
    initData(data)
    val wholeSet = ArrowDataFrame(data, index)
    val res = wholeSet.loc(Seq("viper", "python"))
    println(expected.data.toString())
    res.data shouldEqual expected.data
    res.index shouldBe expected.index
  }

  "loc('viper', 'venom')" should "return a dataframe with only 'viper' elements" in {
    data.clear()
    data.allocateNew()
    val df = serpents2DF(Seq(Serpent("viper", 1, 2)), data)
    val res = wholeSet.loc(Seq("viper", "venom"))
    res.data shouldEqual df.data
    res.index shouldBe df.index
  }

  "loc(true, false, false, true, false, false)" should "return first and fourth rows" in {
    val cleanVSR: VectorSchemaRoot =
      VectorSchemaRoot.create(schema, new RootAllocator())
    val expected = serpents2DF(
      Seq(Serpent("viper", 1, 2), Serpent("python", 10, 11)),
      cleanVSR
    )
    initData(data)
    val wholeSet = ArrowDataFrame(data, index)
    val res = wholeSet.loc(Seq(true, false, false, true, false, false))
    res.data shouldEqual expected.data
    res.index shouldBe expected.index
  }

  "loc(false, false, false, false, false, false)" should "return a dataframe with no rows and only colNames" in {
    initData(data)
    val wholeSet = ArrowDataFrame(data, index)
    val res = wholeSet.loc(Seq(false, false, false, false, false, false))
    res.data.getRowCount shouldEqual 0
    res.index.size shouldBe 0
  }

  "loc(true, true, true, true, true, true)" should "be the original dataframe" in {
    initData(data)
    val wholeSet = ArrowDataFrame(data, index)
    val res = wholeSet.loc(Seq(true, true, true, true, true, true))
    res.data shouldEqual df.data
    res.index shouldBe df.index
  }

  initData(data)
  val wholeSet = ArrowDataFrame(data, index)

  "Encoder" should "encode DataFrame" in {
    val res = arrowDFEncoder.encode(wholeSet)
    res shouldBe serpents
  }

  "Encoder" should "encode DataFrame with no index" in {
    arrowDFEncoder.encode(dfNoIndex) shouldBe serpentsNoIndex
  }

  "to" should "encode DataFrame" in {
    wholeSet.to[Serpent] shouldBe serpents
  }

  "to" should "encode DataFrame with no index" in {
    dfNoIndex.to[Serpent] shouldBe serpentsNoIndex
  }

  "empty" should "return true if dataframe is empty" in {
    emptyInstance.empty shouldBe true
  }

  "empty" should "return false if dataframe is not empty" in {
    df.empty shouldBe false
  }

  "insert" should "adds a column at index `loc`" in {
    val poisonField = new Field("poison", intType, null)
    val schemaFields = List(speedField, staminaField, poisonField)
    val schema: Schema = new Schema(schemaFields.asJava)
    val data: VectorSchemaRoot = VectorSchemaRoot.create(schema, new RootAllocator())
    val df = ArrowDataFrame(data, index)

    //df.data.allocateNew()
    val poisonVector: UInt4Vector =
      data.getVector("poison").asInstanceOf[UInt4Vector]
    poisonVector.setSafe(0, 1) // setSafe checks we don't exceed initialCapacity
    poisonVector.setSafe(1, 4)
    poisonVector.setSafe(2, 7)
    poisonVector.setSafe(3, 10)
    poisonVector.setSafe(4, 13)
    poisonVector.setSafe(5, 16)

    val newDF = df
      .insert[UInt4Vector](1, "poison", poisonVector, false)
      .getOrElse(emptyInstance)
    newDF.data.getVector("poison") shouldBe poisonVector
  }

  "items" should "return data in Array[(columnName, Column)] format" in {
    val speedVectorAsSeq = ArrowUtils.vectorAsSeq(df.shape._1,data.getVector("speed"))
    val staminaVectorAsSeq = ArrowUtils.vectorAsSeq(df.shape._1,data.getVector("stamina"))
    df.items shouldBe Array("speed" -> speedVectorAsSeq, "stamina" -> staminaVectorAsSeq)
  }

  "items" should "return empty array for emptyDF" in {
    emptyInstance.items shouldBe Array() 
  }

  "iterrows" should "return sequence of tuples (index, Row) format" in {
    import org.sciscala.simpledf.types._
    import org.sciscala.simpledf.row.Row
    val dfSchema = Schema(Seq(
      Field("name", StringType, false),
      Field("speed", IntType, false),
      Field("stamina", IntType, false)
    ))
    implicit val rowEncoder: Encoder[ArrowDataFrame, Row] = arrowRowEncoder(dfSchema)
    df.iterrows shouldBe Seq(
      ("viper",Row(Seq(1,2), dfSchema)),
      ("sidewinder",Row(Seq(4,5), dfSchema)),
      ("cobra",Row(Seq(7,8), dfSchema)),
      ("python",Row(Seq(10,11), dfSchema)),
      ("anaconda",Row(Seq(13,14), dfSchema)),
      ("yellowbeard",Row(Seq(16,17), dfSchema))
    )


  }

  "iterrows" should "return empty sequence of tuples for emptyDF" in {
    import org.sciscala.simpledf.types._
    import org.sciscala.simpledf.row.Row
    val dfSchema = Schema(Seq(
      Field("name", StringType, false),
      Field("speed", IntType, false),
      Field("stamina", IntType, false)
    ))
    implicit val rowEncoder: Encoder[ArrowDataFrame, Row] = arrowRowEncoder(dfSchema)
    emptyInstance.iterrows shouldBe Seq()
  }
}
