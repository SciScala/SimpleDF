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
import java.nio.file.Files
import scala.collection.mutable

object ArraySeqDataFrameReader {

  implicit val arrayseqDataFrameReader: DataFrameReader[ArraySeqDataFrame] =
    new DataFrameReader[ArraySeqDataFrame] {

      private def readFile(filepath: Path): BufferedSource =
        Source.fromFile(filepath.toFile)

      private def readFileLines(filepath: Path): Iterator[String] =
        readFile(filepath).getLines()

      private def readFileString(filepath: Path, headers: Boolean): String = {
        if (headers) readFileLines(filepath).toList.tail.mkString
        else readFile(filepath).mkString
       }

      override def readJson(filepath: Path, schema: Option[Schema]): ArraySeqDataFrame = Json2ArraySeqDataFrame.processJsonString(readFileString(filepath, false))

      override def readJson(jsonString: String, schema: Option[Schema]): ArraySeqDataFrame = Json2ArraySeqDataFrame.processJsonString(jsonString)

      private def processCSVWithHeaders[A](reader: Reader, schema: Schema): mutable.Map[String, ArrayBuffer[_]] = {
        var as: mutable.Map[String, ArrayBuffer[_]] = mutable.LinkedHashMap.empty[String, ArrayBuffer[_]]
        val csvReader = new CSVReaderHeaderAware(reader)
        var tmp = csvReader.readMap()
        while (tmp != null) {
          schema.fields.foreach(f => {
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

      private def processCSVPositions[A](reader: Reader, schema: Schema): mutable.Map[String, ArrayBuffer[_]] = {
        var as: mutable.Map[String, ArrayBuffer[_]] = mutable.LinkedHashMap.empty[String, ArrayBuffer[_]]
        val csvReader = new CSVReader(reader)
        var tmp = csvReader.readNext()
        while (tmp != null) {
          schema.fields.zip(tmp)
          .foreach{ case (f,c) =>
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
          }
          tmp = csvReader.readNext()
        }
        as
      }

      private def processCSV(reader: Reader, schema: Schema, columns: ArraySeq[String],
                             firstRowIsHeaders: Boolean, indexColumName: Option[String]): ArraySeqDataFrame = {
        val as: mutable.Map[String, ArrayBuffer[_]] =
          if (firstRowIsHeaders)
            processCSVWithHeaders(reader, schema)
          else
            processCSVPositions(reader, schema)
        val indexCol = indexColumName.getOrElse("")
        val index = ArraySeq.from(as.getOrElse(indexCol, ArrayBuffer.empty[String])).map(_.asInstanceOf[String])
        val data = ArraySeq.from(as.filter(m => m._1 != indexCol).map(m => ArraySeq.from(m._2)))
        ArraySeqDataFrame(data, index, columns.filter(_ != indexCol))
      }

      override def readCSV(
          csv: String,
          schema: Schema,
          firstRowIsHeaders: Boolean,
          indexColumnName: Option[String]
      ): ArraySeqDataFrame = {
        val reader = new StringReader(csv)
        val columns = ArraySeq.from(schema.fieldNames)
        processCSV(reader, schema, columns, firstRowIsHeaders, indexColumnName)
      }

      override def readCSV(
          filepath: Path,
          schema: Schema,
          firstRowIsHeaders: Boolean,
          indexColumnName: Option[String]
      ): ArraySeqDataFrame = {
        val reader = new FileReader(filepath.toFile)
        val columns = ArraySeq.from(schema.fieldNames)
        processCSV(reader, schema, columns, firstRowIsHeaders, indexColumnName)
      }

      override def readParquet(filepath: Path): ArraySeqDataFrame = ???

    }

}
