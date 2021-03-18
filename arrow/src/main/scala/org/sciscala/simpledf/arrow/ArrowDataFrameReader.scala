package org.sciscala.simpledf.arrow

import com.opencsv.{CSVReader, CSVReaderHeaderAware}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType, Field => AField, Schema => ASchema}
import org.apache.arrow.vector.util.Text
import org.sciscala.simpledf.DataFrameReader
import org.sciscala.simpledf.codecs._
import org.sciscala.simpledf.types.{Schema, _}

import java.io._
import java.nio.file.Path
import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.jdk.CollectionConverters._

object ArrowDataFrameReader {

  def convertSchema(schema: Schema, indexColumnName: Option[String]): ASchema = {
    val fields: Seq[AField] = schema.fields
      .filter(_.name != indexColumnName.getOrElse(""))
      .map(f => {
        val aType = f.dtype match {
          case IntType =>
            new FieldType(false, new ArrowType.Int(32, true), null)
          case LongType =>
            new FieldType(false, new ArrowType.Int(64, true), null)
          case DoubleType =>
            new FieldType(false, new ArrowType.Decimal(f.floatingPointPrecision.getOrElse(5), f.floatingPointScale.getOrElse(2)), null)
          case BooleanType => new FieldType(false, new ArrowType.Bool(), null)
          case StringType  => new FieldType(false, new ArrowType.Utf8(), null)
        }
        new AField(f.name, aType, null)
      })
    new ASchema(fields.asJava)
  }

  implicit val arrowDataFrameReader: DataFrameReader[ArrowDataFrame] =
    new DataFrameReader[ArrowDataFrame] {

      override def readJson(filepath: Path, schema: Option[Schema]): ArrowDataFrame = ???

      override def readJson(jsonString: String, schema: Option[Schema]): ArrowDataFrame = schema match {
        case Some(sc) => Json2ArrowDataFrame.processJsonString(jsonString, sc)
        case None => ArrowDataFrame.emptyInstance
      }

      private def processCSVWithHeaders(
          reader: Reader,
          schema: Schema,
          indexColumnName: Option[String]
      ): (VectorSchemaRoot, ArrayBuffer[String]) = {
        val as: mutable.Map[String, ArrayBuffer[_]] =
          mutable.LinkedHashMap.empty[String, ArrayBuffer[_]]
        val csvReader = new CSVReaderHeaderAware(reader)
        var tmp = csvReader.readMap()

        while (tmp != null) {
          schema.fields.foreach(f => {
            val colName = f.name
            as.update(
              colName,
              f.dtype match {
                case IntType =>
                  as.getOrElse(colName, ArrayBuffer.empty[Int]) :+ tmp
                    .get(colName)
                    .toInt
                case LongType =>
                  as.getOrElse(colName, ArrayBuffer.empty[Long]) :+ tmp
                    .get(colName)
                    .toLong
                case BooleanType =>
                  as.getOrElse(colName, ArrayBuffer.empty[Boolean]) :+ tmp
                    .get(colName)
                    .toBoolean
                case StringType =>
                  as.getOrElse(colName, ArrayBuffer.empty[String]) :+ tmp.get(
                    colName
                  )
                // TODO add Double and BigDecimal
              }
            )
          })
          tmp = csvReader.readMap()
        }

        val data: VectorSchemaRoot = buildVectorSchemaRoot(schema, indexColumnName, as)
        val index = as.getOrElse(indexColumnName.getOrElse(""), ArrayBuffer.empty[String]).map(_.toString)
        (data, index)
      }

      private def processCSVPositions(
          reader: Reader,
          schema: Schema,
          columns: ArraySeq[String],
          indexColumName: Option[String]
      ): (VectorSchemaRoot, ArrayBuffer[String]) = {
        var as: mutable.Map[String, ArrayBuffer[_]] = mutable.LinkedHashMap.empty[String, ArrayBuffer[_]]
        val csvReader = new CSVReader(reader)
        var tmp = csvReader.readNext()
        while (tmp != null) {
          schema.fields.zip(tmp)
          .foreach{ case (f,c) => {
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

        val data: VectorSchemaRoot = buildVectorSchemaRoot(schema, indexColumName, as)
        val index = as.getOrElse(indexColumName.getOrElse(""), ArrayBuffer.empty[String]).map(_.toString)
        (data, index)
      }

      private def processCSV(
          reader: Reader,
          schema: Schema,
          columns: ArraySeq[String],
          isFirstRowHeaders: Boolean,
          indexColumName: Option[String]
      ): ArrowDataFrame = {
        val (data, index) = if (isFirstRowHeaders) processCSVWithHeaders(reader, schema, indexColumName)
        else processCSVPositions(reader, schema, columns, indexColumName)
        ArrowDataFrame(data, ArraySeq.from(index))
      }

      override def readCSV(
          filepath: Path,
          schema: Schema,
          isFirstRowHeaders: Boolean,
          indexColumnName: Option[String]
      ): ArrowDataFrame = {
        val reader = new FileReader(filepath.toFile)
        val columns = ArraySeq.from(schema.fieldNames)
        val (data, index) = if (isFirstRowHeaders) processCSVWithHeaders(reader, schema, indexColumnName)
        else processCSVPositions(reader, schema, columns, indexColumnName)
        ArrowDataFrame(data, ArraySeq.from(index))
      }

      override def readCSV(
          csv: String,
          schema: Schema,
          isFirstRowHeaders: Boolean,
          indexColumnName: Option[String]
      ): ArrowDataFrame = {
        val reader = new StringReader(csv)
        val columns = ArraySeq.from(schema.fieldNames)
        processCSV(reader, schema, columns, isFirstRowHeaders, indexColumnName)
      }

      override def readParquet(filepath: Path): ArrowDataFrame = ???

    }

  private def buildVectorSchemaRoot(schema: Schema, indexColumnName: Option[String], as: mutable.Map[String, ArrayBuffer[_]]): VectorSchemaRoot = {
    val data: VectorSchemaRoot =
      VectorSchemaRoot.create(
        convertSchema(schema, indexColumnName),
        new RootAllocator()
      )
    data.allocateNew()
    data
      .getFieldVectors
      .iterator()
      .forEachRemaining(fv => {
        val field = fv.getField
        val name = field.getName

        field.getFieldType.getType match {
          case i: ArrowType.Int if i.getBitWidth == 32 =>
            val v = data.getVector(name).asInstanceOf[IntVector]
            v.allocateNew()
            val ab = as.getOrElse(name, ArrayBuffer.empty[Int])
            ab.zipWithIndex
              .foreach(tpl => {
                v.setSafe(tpl._2, tpl._1.asInstanceOf[Int])
              })
            v.setValueCount(ab.size)

          case i: ArrowType.Int if i.getBitWidth == 64 =>
            val v = data.getVector(name).asInstanceOf[BigIntVector]
            v.allocateNew()
            val ab = as.getOrElse(name, ArrayBuffer.empty[Long])
            ab.zipWithIndex
              .foreach(tpl => {
                v.setSafe(tpl._2, tpl._1.asInstanceOf[Long])
              })
            v.setValueCount(ab.size)

          case i: ArrowType.Bool =>
            val v = data.getVector(name).asInstanceOf[BitVector]
            v.allocateNew()
            val ab = as.getOrElse(name, ArrayBuffer.empty[Boolean])
            ab.zipWithIndex
              .foreach(tpl => {
                val b = if (tpl._1.asInstanceOf[Boolean]) 1 else 0
                v.setSafe(tpl._2, b)
              })
            v.setValueCount(ab.size)

          case i: ArrowType.Utf8 =>
            val ab = as.getOrElse(name, ArrayBuffer.empty[String])
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
}

// required when using reflection, like `using` does
import scala.language.reflectiveCalls

object Control {
  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close()
    }
}
