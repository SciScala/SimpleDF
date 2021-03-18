package org.sciscala.simpledf.arrayseq

import ujson.Value.Value
import ujson.{Arr, Bool, False, Null, Num, Obj, Str, True, read}

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ArraySeq => MArraySeq, LinkedHashMap => MLinkedHashMap}

private[arrayseq] object Json2ArraySeqDataFrame {

  private def processObj(
      obj: MLinkedHashMap[String, ujson.Value.Value]
  ): ArraySeqDataFrame = {
    var df: MArraySeq[MArraySeq[_]] =
      MArraySeq.empty[MArraySeq[_]]
    val src: mutable.Iterable[((String, Value), Int)] = obj.zipWithIndex
    val indexCol: Option[(String, Value)] =
      obj.find(t => t._2.getClass.getName == "ujson.Obj")
    println(indexCol)

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
      df.update(idx, cell._1._2 match {
        case Str(value) => df(idx).appended(value)
        case Obj(value) => df(idx).appended(value)
        case Arr(value) => df(idx).appended(value)
        case Num(value) => df(idx).appended(value)
        case bool: Bool => df(idx).appended(bool)
        case Null => df(idx)
      })
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
          ArraySeq.from(src.map(_._1._1).dropRight(1))
        val lm = value.value
          .asInstanceOf[MLinkedHashMap[String, ujson.Value.Value]]
        val index = ArraySeq.from(lm.keySet)
        val indexValues = ArraySeq.from(lm.values.map {
          case Str(value) => value
          case Obj(value) => value
          case Arr(value) => value
          case Num(value) => value
          case bool: Bool => bool
          case Null => null
        })
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

  private def processArr(
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
            //case Obj(value) => df.appended(MArraySeq.empty[String])
            //case Arr(value) => df.appended(MArraySeq.empty[Array[String]])
            case ujson.Null => df.appended(MArraySeq.empty[String])
            case _          => df
          }
        df.update(idx, df(idx).appended(tup._1._2 match {
          case Str(value) => value
          case Obj(value) => value
          case Arr(value) => value
          case Num(value) => value
          case bool: Bool => bool.value
          case Null => null
        }))
      })
    )

    val data = ArraySeq.from(
      df.map(arry => ArraySeq.from(arry.toIterable))
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
