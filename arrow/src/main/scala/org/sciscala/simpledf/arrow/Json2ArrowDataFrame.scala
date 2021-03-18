package org.sciscala.simpledf.arrow

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.util.Text
import org.apache.arrow.vector.{
  BigIntVector,
  BitVector,
  DecimalVector,
  IntVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.sciscala.simpledf.arrow.ArrowDataFrame
import org.sciscala.simpledf.arrow.ArrowDataFrameReader.convertSchema
import org.sciscala.simpledf.types.{Schema, _}

import ujson.Value.Value
import ujson.{Arr, Bool, False, Null, Num, Obj, Str, True, read}

import java.{math => jm}

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.{
  ArrayBuffer,
  ArraySeq => MArraySeq,
  LinkedHashMap => MLinkedHashMap
}

object Json2ArrowDataFrame {

  private def processObj(
      obj: MLinkedHashMap[String, ujson.Value.Value],
      schema: Schema
  ): ArrowDataFrame = {
    var df: MArraySeq[MArraySeq[_]] =
      MArraySeq.empty[MArraySeq[_]]
    val src: mutable.Iterable[((String, Value), Int)] = obj.zipWithIndex
    val indexCol: Option[(String, Value)] =
      obj.find(t => t._2.getClass.getName == "ujson.Obj")

    def fillData(
        cell: ((String, Value), Int)
    ): Unit = {
      val idx = cell._2
      if (!df.isDefinedAt(idx)) {
        df = cell._1._2 match {
          case Str(value) => df.appended(MArraySeq.empty[String])
          case Num(value) => df.appended(MArraySeq.empty[Double])
          case False      => df.appended(MArraySeq.empty[Boolean])
          case True       => df.appended(MArraySeq.empty[Boolean])
          case Obj(value) => df.appended(MArraySeq.empty[String])
          case Arr(value) => df.appended(MArraySeq.empty[Array[String]])
          case ujson.Null => df.appended(MArraySeq.empty[String])
          case _          => df
        }
      }
      df.update(
        idx,
        cell._1._2 match {
          case Str(value) => df(idx).appended(value)
          case Obj(value) => df(idx).appended(value)
          case Arr(value) => df(idx).appended(value)
          case Num(value) => df(idx).appended(value)
          case bool: Bool => df(idx).appended(bool.value)
          case Null       => df(idx)
        }
      )
    }

    indexCol match {
      case None => {
        var cols: ArraySeq[String] =
          ArraySeq.from(src.map(_._1._1))
        src.foreach(fillData)
        val data = ArraySeq.from(
          df.map(arr => ArraySeq.from(arr.toIterable))
        )

        val root = buildVectorSchemaRoot(schema, None, cols zip data)
        ArrowDataFrame(root, ArraySeq.empty[String])
      }
      case Some((key, value)) => {
        val cols: ArraySeq[String] = key +: ArraySeq.from(src.map(_._1._1).filter( _ != key))
        val lm = value.value
          .asInstanceOf[MLinkedHashMap[String, ujson.Value.Value]]
        val index = ArraySeq.from(lm.keySet)
        val indexValues = ArraySeq.from(lm.values.map {
          case Str(value) => value
          case Obj(value) => value
          case Arr(value) => value
          case Num(value) => value
          case bool: Bool => bool.value
          case Null       => null
        })

        src
          .filter(tup => tup._1._1 != key)
          .foreach(fillData)
        val data = ArraySeq.from(
          indexValues +:
            df.map(arr =>
            ArraySeq.from(Seq.fill(index.size)(arr(0)))
          )
        )

        val root = buildVectorSchemaRoot(schema, None, cols zip data)
        ArrowDataFrame(root, index)
      }
    }
  }

  private def processArr(
      arr: ArrayBuffer[ujson.Obj],
      schema: Schema
  ): ArrowDataFrame = {
    var df: MArraySeq[MArraySeq[_]] =
      MArraySeq.empty[MArraySeq[_]]

    val lst = arr(0) //.value
    var cols: ArraySeq[String] =
      ArraySeq.from(lst.value.zipWithIndex.map(_._1._1))

    arr.foreach(obj =>
      obj.value.zipWithIndex.foreach(tup => {
        val idx = tup._2
        if (!df.isDefinedAt(idx)) df = tup._1._2 match {
          case Str(value) => df.appended(MArraySeq.empty[String])
          case Num(value) => df.appended(MArraySeq.empty[Double])
          case False      => df.appended(MArraySeq.empty[Boolean])
          case True       => df.appended(MArraySeq.empty[Boolean])
          //case Obj(value) => df.appended(MArraySeq.empty[String])
          //case Arr(value) => df.appended(MArraySeq.empty[Array[String]])
          case ujson.Null => df.appended(MArraySeq.empty[String])
          case _          => df
        }
        df.update(
          idx,
          df(idx).appended(tup._1._2 match {
            case Str(value) => value
            case Obj(value) => value
            case Arr(value) => value
            case Num(value) => value
            case bool: Bool => bool.value
            case Null       => null
          })
        )
      })
    )

    val data = ArraySeq.from(
      df.map(arry => ArraySeq.from(arry.toIterable))
    )

    val root = buildVectorSchemaRoot(schema, None, cols zip data)
    ArrowDataFrame(root, ArraySeq.empty[String])
  }

  private def buildVectorSchemaRoot(
      schema: Schema,
      indexColumnName: Option[String],
      as: ArraySeq[(String, ArraySeq[_])]
  ): VectorSchemaRoot = {
    val aschema = convertSchema(schema, indexColumnName)
    val data: VectorSchemaRoot = {
      VectorSchemaRoot.create(
        aschema,
        new RootAllocator()
      )
    }

    data.allocateNew()
    data.getFieldVectors
      .iterator()
      .forEachRemaining(fv => {
        val field = fv.getField
        val name = field.getName

        field.getFieldType.getType match {
          case i: ArrowType.Int if i.getBitWidth == 32 =>
            val v = data.getVector(name).asInstanceOf[IntVector]
            v.allocateNew()
            val ab = as.filter(p => p._1 == name).head._2
            ab.zipWithIndex
              .foreach(tpl => {
                v.setSafe(tpl._2, tpl._1.asInstanceOf[Int])
              })
            v.setValueCount(ab.size)

          case i: ArrowType.Int if i.getBitWidth == 64 =>
            val v = data.getVector(name).asInstanceOf[BigIntVector]
            v.allocateNew()
            val ab = as.filter(p => p._1 == name).head._2
            ab.zipWithIndex
              .foreach(tpl => {
                v.setSafe(tpl._2, tpl._1.asInstanceOf[Long])
              })
            v.setValueCount(ab.size)

          case i: ArrowType.Decimal =>
            val v = data.getVector(name).asInstanceOf[DecimalVector]
            v.allocateNew()
            val ab = as.filter(p => p._1 == name).head._2
            ab.zipWithIndex
              .foreach(tpl => {
                val mc = new jm.MathContext(i.getPrecision)
                var bd =
                  new java.math.BigDecimal(tpl._1.asInstanceOf[Double], mc)
                    .setScale(i.getScale, jm.RoundingMode.HALF_UP)
                v.setSafe(tpl._2, bd)
              })
            v.setValueCount(ab.size)

          case i: ArrowType.Bool =>
            val v = data.getVector(name).asInstanceOf[BitVector]
            v.allocateNew()
            val ab = as.filter(p => p._1 == name).head._2
            ab.zipWithIndex
              .foreach(tpl => {
                val b = if (tpl._1.asInstanceOf[Boolean]) 1 else 0
                v.setSafe(tpl._2, b)
              })
            v.setValueCount(ab.size)

          case i: ArrowType.Utf8 =>
            val ab = as.filter(p => p._1 == name).head._2
            val v = data.getVector(name).asInstanceOf[VarCharVector]
            v.allocateNew()
            ab.zipWithIndex
              .foreach(tpl => {
                v.setSafe(tpl._2, new Text(tpl._1.asInstanceOf[String]))
              })
            v.setValueCount(ab.size)

        }
      })
    data.setRowCount(as.head._2.size)
    data
  }

  def processJsonString(jsonString: String, schema: Schema): ArrowDataFrame =
    read(jsonString) match {
      case ujson.Arr(value) =>
        processArr(value.asInstanceOf[ArrayBuffer[ujson.Obj]], schema)
      case ujson.Obj(value) =>
        processObj(
          value.asInstanceOf[MLinkedHashMap[String, ujson.Value.Value]],
          schema
        )
      case _ => ArrowDataFrame.emptyInstance
    }

}
