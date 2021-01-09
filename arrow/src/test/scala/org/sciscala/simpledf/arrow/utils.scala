package org.sciscala.simpledf.arrow

import org.sciscala.simpledf._
import org.sciscala.simpledf.types._
import org.sciscala.simpledf.codecs._

import org.apache.arrow.vector.{FieldVector, IntVector, UInt4Vector, VectorSchemaRoot}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.reader.FieldReader
import org.apache.arrow.vector.types.pojo.{
  Schema => ASchema,
  FieldType,
  ArrowType,
  Field => AField
}

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.{Seq => MSeq}
import scala.collection.mutable.ListBuffer

import scala.jdk.CollectionConverters._
import org.scalactic.Equality

object utils {
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

  val intType = new FieldType(false, new ArrowType.Int(32, true), null)
  val speedField = new AField("speed", intType, null)
  val staminaField = new AField("stamina", intType, null)
  val schemaFields = List(speedField, staminaField)
  val schema: ASchema = new ASchema(schemaFields.asJava)

  private def vectorizeSerpent(
      idx: Int,
      serpent: Serpent,
      schemaRoot: VectorSchemaRoot
  ): Unit = {
    schemaRoot
      .getVector("speed")
      .asInstanceOf[IntVector]
      .setSafe(idx, serpent.speed)
    schemaRoot
      .getVector("stamina")
      .asInstanceOf[IntVector]
      .setSafe(idx, serpent.stamina)
  }

  private[arrow] def serpents2DF(
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

  private[arrow] def initData(data: VectorSchemaRoot): Unit = {
    data.clear()
    data.allocateNew()
    val speedVector: IntVector =
      data.getVector("speed").asInstanceOf[IntVector]
    speedVector.allocateNew()
    speedVector.setSafe(0, 1) // setSafe checks we don't exceed initialCapacity
    speedVector.setSafe(1, 4)
    speedVector.setSafe(2, 7)
    speedVector.setSafe(3, 10)
    speedVector.setSafe(4, 13)
    speedVector.setSafe(5, 16)
    speedVector.setValueCount(6)

    val staminaVector: IntVector =
      data.getVector("stamina").asInstanceOf[IntVector]
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

  val serpentSchema = new Schema(
    Seq(
      Field("name", StringType, false),
      Field("speed", IntType, false),
      Field("stamina", IntType, false)
    )
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

}
