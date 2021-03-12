package org.sciscala.simpledf.arrayseq

import ujson.Value.Value
import ujson.read

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.{
  ArrayBuffer,
  LinkedHashMap => MLinkedHashMap,
  ArraySeq => MArraySeq
}
import ujson.Str
import ujson.Obj
import ujson.Arr
import ujson.Num
import ujson.False
import ujson.True

object Json2ArraySeqDataFrame {

  def processObj(
      obj: MLinkedHashMap[String, ujson.Value.Value]
  ): ArraySeqDataFrame = {
    var df: MArraySeq[MArraySeq[_]] =
      MArraySeq.empty[MArraySeq[_]]
    val src = obj.zipWithIndex
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
      df.update(idx, df(idx).appended(cell._1._2.value))
    }

    indexCol match {
      case None => {
        var cols: ArraySeq[String] =
          ArraySeq.from(src.map(_._1._1))
        src.foreach(fillData)
        val data = ArraySeq.from(
          df.map(arr => ArraySeq.from(arr.toIterable))
        )
        ArraySeqDataFrame(data, ArraySeq.empty[String], cols)
      }
      case Some((key, value)) => {
        val cols: ArraySeq[String] =
          ArraySeq.from(src.map(_._1._1).filter(_ != key))
        val lm = value.value
          .asInstanceOf[MLinkedHashMap[String, ujson.Value.Value]]
        val index = ArraySeq.from(lm.keySet)
        val indexValues = ArraySeq.from(lm.values)
        src.filter(tup => tup._1._1 != key).foreach(fillData)
        val data = ArraySeq.from(
          indexValues +: df.map(arr =>
            ArraySeq.from(Seq.fill(index.size)(arr(0)))
          )
        )
        ArraySeqDataFrame(data, index, key +: cols)
      }
    }
  }

  def processArr(
      arr: ArrayBuffer[ujson.Obj]
  ): ArraySeqDataFrame = {
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
            case Obj(value) => df.appended(MArraySeq.empty[String])
            case Arr(value) => df.appended(MArraySeq.empty[Array[String]])
            case ujson.Null => df.appended(MArraySeq.empty[String])
            case _          => df
          }
        df.update(idx, df(idx).appended(tup._1._2.value))
      })
    )

    val data = ArraySeq.from(
      df.map(arr => ArraySeq.from(arr.toIterable))
    )

    ArraySeqDataFrame(data, ArraySeq.empty[String], cols)
  }

  def processJsonString(jsonString: String): ArraySeqDataFrame =
    read(jsonString) match {
      case ujson.Arr(value) =>
        processArr(value.asInstanceOf[ArrayBuffer[ujson.Obj]])
      case ujson.Obj(value) =>
        processObj(
          value.asInstanceOf[MLinkedHashMap[String, ujson.Value.Value]]
        )
      case _ => ArraySeqDataFrame.emptyInstance
    }
}
