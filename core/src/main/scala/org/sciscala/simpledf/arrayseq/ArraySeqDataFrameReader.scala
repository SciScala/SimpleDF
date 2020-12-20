package org.sciscala.simpledf.arrayseq

import java.io.File
import java.io.FileReader
import java.io.Reader
import java.io.StringReader
import java.nio.file.Path

import scala.io.Source
import scala.io.BufferedSource

import org.sciscala.simpledf.DataFrameReader
import org.sciscala.simpledf.DataFrame
import org.sciscala.simpledf.codecs.Decoder

import com.opencsv.CSVReader
import com.opencsv.CSVReaderHeaderAware
import scala.reflect.{ClassTag, classTag}
import scala.jdk.CollectionConverters._
import org.sciscala.simpledf.types.Schema
import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.sciscala.simpledf.types._
import scala.collection.mutable.LinkedHashMap

object ArraySeqDataFrameReader {

  implicit val arrayseqDataFrameReader: DataFrameReader[ArraySeqDataFrame] =
    new DataFrameReader[ArraySeqDataFrame] {

      private def readFile(filepath: Path): BufferedSource =
        Source.fromFile(filepath.toFile())

      private def readFileLines(filepath: Path): Iterator[String] =
        readFile(filepath).getLines()

      private def readFileString(filepath: Path, headers: Boolean): String = {
        if (headers) readFileLines(filepath).toList.tail.mkString
        else readFile(filepath).mkString
      }

      override def readJson[A <: Serializable](filepath: Path)(
          implicit D: Decoder[A, ArraySeqDataFrame]
      ): ArraySeqDataFrame = ???

      private def processCSVWithHeaders[A](reader: Reader, schema: Schema, columns: ArraySeq[String]): Map[String, ArrayBuffer[_]] = {
        var as: Map[String, ArrayBuffer[_]] = LinkedHashMap.empty[String, ArrayBuffer[_]]
        val csvReader = new CSVReaderHeaderAware(reader)
        var tmp = csvReader.readMap()
        while (tmp != null) {
          schema.fields.map(f => {
            val colName = f.name
            as.update(colName, f.dtype match {
              case IntType =>
                as.getOrElse(colName, {
                  val ab = ArrayBuffer.empty[Int]
                  as += colName -> ab
                  ab
                }) :+ tmp.get(colName).toInt
              case LongType =>
                as.getOrElse(colName, {
                  val ab = ArrayBuffer.empty[Long]
                  as += colName -> ab
                  ab
                }) :+ tmp.get(colName).toLong
              case BooleanType =>
                as.getOrElse(colName, {
                  val ab = ArrayBuffer.empty[Boolean]
                  as += colName -> ab
                  ab
                }) :+ tmp.get(colName).toBoolean
              case StringType =>
                as.getOrElse(colName, {
                  val ab = ArrayBuffer.empty[String]
                  as += colName -> ab
                  ab
                }) :+ tmp.get(colName)
              // TODO add Double and BigDecimal
            })
          })
          tmp = csvReader.readMap()
        }
        as
      }

      private def processCSVPositions[A](reader: Reader, schema: Schema, columns: ArraySeq[String]): Map[String, ArrayBuffer[_]] = {
        var as: Map[String, ArrayBuffer[_]] = LinkedHashMap.empty[String, ArrayBuffer[_]]
        val csvReader = new CSVReader(reader)
        var tmp = csvReader.readNext()
        while (tmp != null) {
          schema.fields.zip(tmp)
          .map{ case (f,c) => {
            val colName = f.name
            as.update(colName, f.dtype match {
              case IntType =>
                as.getOrElse(colName, {
                  val ab = ArrayBuffer.empty[Int]
                  as += colName -> ab
                  ab
                }) :+ c.toInt
              case LongType =>
                as.getOrElse(colName, {
                  val ab = ArrayBuffer.empty[Long]
                  as += colName -> ab
                  ab
                }) :+ c.toLong
              case BooleanType =>
                as.getOrElse(colName, {
                  val ab = ArrayBuffer.empty[Boolean]
                  as += colName -> ab
                  ab
                }) :+ c.toBoolean
              case StringType =>
                as.getOrElse(colName, {
                  val ab = ArrayBuffer.empty[String]
                  as += colName -> ab
                  ab
                }) :+ c
              // TODO add Double and BigDecimal
            })
          }}
          tmp = csvReader.readNext()
        }
        as
      }

      private def processCSV(reader: Reader, schema: Schema, columns: ArraySeq[String], firstRowIsHeaders: Boolean, indexColumName: String): ArraySeqDataFrame = {
        val as: Map[String, ArrayBuffer[_]] =
          if (firstRowIsHeaders)
            processCSVWithHeaders(reader, schema, columns)
          else
            processCSVPositions(reader, schema, columns)
        val index = ArraySeq.from(as.getOrElse(indexColumName, ArrayBuffer.empty[String])).map(_.asInstanceOf[String])
        val data = ArraySeq.from(as.filter(m => m._1 != indexColumName).map(m => ArraySeq.from(m._2)))
        ArraySeqDataFrame(data, index, columns.filter(_ != indexColumName))
      }

      override def readCSV(
          csv: String,
          schema: Schema,
          firstRowIsHeaders: Boolean,
          indexColumnName: String = ""
      ): ArraySeqDataFrame = {
        val reader = new StringReader(csv)
        val columns = ArraySeq.from(schema.fieldNames)
        processCSV(reader, schema, columns, firstRowIsHeaders, indexColumnName)
      }

      override def readCSV(
          filepath: Path,
          schema: Schema,
          firstRowIsHeaders: Boolean,
          indexColumnName: String
      ): ArraySeqDataFrame = {
        val reader = new FileReader(filepath.toFile())
        val columns = ArraySeq.from(schema.fieldNames)
        processCSV(reader, schema, columns, firstRowIsHeaders, indexColumnName)
      }

      override def readParquet(filepath: Path): ArraySeqDataFrame = ???

    }

}
