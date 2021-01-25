package org.sciscala.simpledf.arrayseq

import ujson.Value.Value
import ujson.read

import scala.collection.generic.DefaultSerializable
import scala.collection.mutable.ArrayBuffer
import scala.collection.{AbstractIterable, StrictOptimizedIterableOps, immutable, mutable}

object Json2ArraySeqDataFrame extends App {

  val simpleObj = """{"omg": "123", "wtf": 12, "right": true}"""
  val simpleArr =
    """[{"omg": "123", "wtf": 12, "right": true}, {"omg": "245", "wtf": 15, "right": false}, {"omg": "678", "wtf": 10, "right": false}]"""
  val dataString =
    """{"base": "USD","date": "2021-01-24","time_last_updated": 1611446401,"rates": {"USD": 1,"AED": 3.6725,"ARS": 86.363457,"AUD": 1.294731,"BGN": 1.606893,"BRL": 5.383967,"BSD": 1,"CAD": 1.268251,"CHF": 0.885517,"CLP": 717.616192,"CNY": 6.478288,"COP": 3471.118897,"CZK": 21.451045}}""".stripMargin

  def processObj(
      obj: mutable.LinkedHashMap[String, ujson.Value.Value],
  ): ArraySeqDataFrame = {
    val indexCol = obj.find(t => t._2.getClass.getName == "ujson.Obj")
    indexCol match {
      case None => {
        var df: mutable.ArraySeq[mutable.ArraySeq[_]] =
          mutable.ArraySeq.empty[mutable.ArraySeq[_]]

        val src = obj.zipWithIndex

        var cols: immutable.ArraySeq[String] =
          immutable.ArraySeq.from(src.map(_._1._1))

        src.foreach(tup => {
          val idx = tup._2
          if (!df.isDefinedAt(idx)) {
            df = df.appended(mutable.ArraySeq.empty[tup._1._2.type])
          }
          df.update(idx, df(idx).appended(tup._1._2.value))
        })

        val data = immutable.ArraySeq.from(
          df.map(arr => immutable.ArraySeq.from(arr.toIterable))
        )

        ArraySeqDataFrame(data, immutable.ArraySeq.empty[String], cols)
      }
      case Some(_) => ArraySeqDataFrame.emptyInstance
    }
  }

  def processArr(
      arr: mutable.ArrayBuffer[ujson.Obj]
  ): ArraySeqDataFrame = {
    var df: mutable.ArraySeq[mutable.ArraySeq[_]] =
      mutable.ArraySeq.empty[mutable.ArraySeq[_]]

    val lst = arr(0)//.value
    var cols: immutable.ArraySeq[String] =
      immutable.ArraySeq.from(lst.value.zipWithIndex.map(_._1._1))

    arr.foreach(obj => {
      val src = obj.value.zipWithIndex

      src.foreach(tup => {
        val idx = tup._2
        if (!df.isDefinedAt(idx)) {
          df = df.appended(mutable.ArraySeq.empty[tup._1._2.type])
        }
        df.update(idx, df(idx).appended(tup._1._2.value))
      })
    })

    val data = immutable.ArraySeq.from(
      df.map(arr => immutable.ArraySeq.from(arr.toIterable))
    )

    ArraySeqDataFrame(data, immutable.ArraySeq.empty[String], cols)
  }

  def processJsonString(jsonString: String): ArraySeqDataFrame =
    read(jsonString) match {
      case ujson.Arr(value) =>
        processArr(value.asInstanceOf[ArrayBuffer[ujson.Obj]])
      case ujson.Obj(value) =>
        processObj(
          value.asInstanceOf[mutable.LinkedHashMap[String, ujson.Value.Value]]
        )
    }

  val lm = processJsonString(simpleObj)
  println(lm)

  val lm1 = processJsonString(simpleArr)
  println(lm1)

  val data = processJsonString(dataString)
  println(data)

}
