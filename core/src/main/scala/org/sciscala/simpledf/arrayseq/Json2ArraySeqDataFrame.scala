package org.sciscala.simpledf.arrayseq

import ujson.Value.Value
import ujson.read

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap => MLinkedHashMap, ArraySeq => MArraySeq}

object Json2ArraySeqDataFrame extends App {

  val simpleObj = """{"omg": "123", "wtf": 12, "right": true}"""
  val simpleArr =
    """[{"omg": "123", "wtf": 12, "right": true}, {"omg": "245", "wtf": 15, "right": false}, {"omg": "678", "wtf": 10, "right": false}]"""
  val dataString =
    """{"base": "USD","date": "2021-01-24","time_last_updated": 1611446401,"rates": {"USD": 1,"AED": 3.6725,"ARS": 86.363457,"AUD": 1.294731,"BGN": 1.606893,"BRL": 5.383967,"BSD": 1,"CAD": 1.268251,"CHF": 0.885517,"CLP": 717.616192,"CNY": 6.478288,"COP": 3471.118897,"CZK": 21.451045}}""".stripMargin

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
        df = df.appended(MArraySeq.empty[cell._1._2.type])
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
          indexValues +: df.map(arr => ArraySeq.from(Seq.fill(index.size)(arr(0))))
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
        if (!df.isDefinedAt(idx)) df = df.appended(MArraySeq.empty[tup._1._2.type])
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
    }

  val lm = processJsonString(simpleObj)
  println(lm)

  val lm1 = processJsonString(simpleArr)
  println(lm1)

  val data = processJsonString(dataString)
  println(data)

}
