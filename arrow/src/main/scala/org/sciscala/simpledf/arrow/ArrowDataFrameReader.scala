package org.sciscala.simpledf.arrow

import java.nio.file.Path
import java.io.File
import java.io.FileInputStream
import java.io.Reader
import java.io.StringReader

import org.apache.arrow.vector.ipc.SeekableReadChannel
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.FieldVector

import scala.jdk.CollectionConverters._
import scala.collection.mutable.LinkedHashMap
import org.sciscala.simpledf.{DataFrame, DataFrameReader, IsSupported}
import org.sciscala.simpledf.codecs._
import org.sciscala.simpledf.types.Schema
import com.opencsv.CSVReader
import com.opencsv.CSVReaderHeaderAware

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.apache.arrow.vector.types.pojo.{
  ArrowType,
  FieldType,
  Field => AField,
  Schema => ASchema
}
import org.sciscala.simpledf.types._
import org.apache.arrow.vector.{
  BigIntVector,
  BitVector,
  IntVector,
  VarCharVector
}
import org.apache.arrow.vector.util.Text

object ArrowDataFrameReader {

  def convertSchema(schema: Schema, indexColumnName: String): ASchema = {
    val fields: Seq[AField] = schema.fields
      .filter(_.name != indexColumnName)
      .map(f => {
        val aType = f.dtype match {
          case IntType =>
            new FieldType(false, new ArrowType.Int(32, true), null)
          case LongType =>
            new FieldType(false, new ArrowType.Int(64, true), null)
          case BooleanType => new FieldType(false, new ArrowType.Bool(), null)
          case StringType  => new FieldType(false, new ArrowType.Utf8(), null)
        }
        new AField(f.name, aType, null)
      })
    val its = fields.toIterable
    new ASchema(its.asJava)
  }

  private def read(filepath: Path): ArrowDataFrame = {
    val arrowFile: File = new File(filepath.toUri())
    val fileInputStream: FileInputStream = new FileInputStream(arrowFile)
    val seekableReadChannel: SeekableReadChannel = new SeekableReadChannel(
      fileInputStream.getChannel()
    )
    val arrowFileReader: ArrowFileReader = new ArrowFileReader(
      seekableReadChannel,
      new RootAllocator(Integer.MAX_VALUE)
    )
    val root: VectorSchemaRoot = arrowFileReader.getVectorSchemaRoot()
    /*
    val schema: Schema = root.getSchema()
    val arrowBlocks: List[ArrowBlock] = arrowFileReader.getRecordBlocks().asScala.toList
     */
    val fieldVectorNames: List[String] =
      root.getFieldVectors().asScala.toList.map(_.getName())
    ArrowDataFrame(root, fieldVectorNames)
  }

  implicit val arrowDataFrameReader: DataFrameReader[ArrowDataFrame] =
    new DataFrameReader[ArrowDataFrame] {

      override def readJson[A <: Serializable](filepath: Path)(
          implicit D: Decoder[A, ArrowDataFrame]
      ): ArrowDataFrame = ???

      private def processCSVWithHeaders(
          reader: Reader,
          schema: Schema,
          indexColumnName: String
      ): (VectorSchemaRoot, ArrayBuffer[String]) = {
        var as: Map[String, ArrayBuffer[_]] =
          LinkedHashMap.empty[String, ArrayBuffer[_]]
        val csvReader = new CSVReaderHeaderAware(reader)
        var tmp = csvReader.readMap()

        while (tmp != null) {
          schema.fields.map(f => {
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

        val data: VectorSchemaRoot =
          VectorSchemaRoot.create(
            convertSchema(schema, indexColumnName),
            new RootAllocator()
          )
        data.allocateNew()
        data
          .getFieldVectors()
          .iterator()
          .forEachRemaining(fv => {
            val field = fv.getField()
            val name = field.getName()

            field.getFieldType().getType() match {
              case i: ArrowType.Int if i.getBitWidth == 32 => {
                val v = data.getVector(name).asInstanceOf[IntVector]
                v.allocateNew()
                val ab = as.getOrElse(name, ArrayBuffer.empty[Int])
                ab.zipWithIndex
                  .foreach(tpl => {
                    v.setSafe(tpl._2, tpl._1.asInstanceOf[Int])
                  })
                v.setValueCount(ab.size)
              }
              case i: ArrowType.Int if i.getBitWidth == 64 => {
                val v = data.getVector(name).asInstanceOf[BigIntVector]
                v.allocateNew()
                val ab = as.getOrElse(name, ArrayBuffer.empty[Long])
                ab.zipWithIndex
                  .foreach(tpl => {
                    v.setSafe(tpl._2, tpl._1.asInstanceOf[Long])
                  })
                v.setValueCount(ab.size)
              }
              case i: ArrowType.Bool => {
                val v = data.getVector(name).asInstanceOf[BitVector]
                v.allocateNew()
                val ab = as.getOrElse(name, ArrayBuffer.empty[Boolean])
                ab.zipWithIndex
                  .foreach(tpl => {
                    val b = if (tpl._1.asInstanceOf[Boolean]) 1 else 0
                    v.setSafe(tpl._2, b)
                  })
                v.setValueCount(ab.size)
              }
              case i: ArrowType.Utf8 => {
                val ab = as.getOrElse(name, ArrayBuffer.empty[String])
                val v = data.getVector(name).asInstanceOf[VarCharVector]
                v.allocateNew()
                ab.zipWithIndex
                  .foreach(tpl => {
                    v.setSafe(tpl._2, new Text(tpl._1.asInstanceOf[String]))
                  })
                v.setValueCount(ab.size)
              }
            }
          })
        data.setRowCount(as.head._2.size)
        val index = as.getOrElse(indexColumnName, ArrayBuffer.empty[String]).map(_.toString)
        (data, index)
      }

      private def processCSV(
          reader: Reader,
          schema: Schema,
          columns: ArraySeq[String],
          isFirstRowHeaders: Boolean,
          indexColumName: String
      ): ArrowDataFrame = {
        val (data, index) =
          processCSVWithHeaders(reader, schema, indexColumName)
        //else
        //processCSVPositions(reader, schema, columns)
        /*val index = ArraySeq
          .from(as.getOrElse(indexColumName, ArrayBuffer.empty[String]))
          .map(_.asInstanceOf[String])*/
        ArrowDataFrame(data, ArraySeq.from(index))
      }

      override def readCSV(
          filepath: Path,
          schema: Schema,
          isFirstRowHeaders: Boolean,
          indexColumnName: String = ""
      ): ArrowDataFrame = ???

      override def readCSV(
          csv: String,
          schema: Schema,
          isFirstRowHeaders: Boolean,
          indexColumnName: String
      ): ArrowDataFrame = {
        val reader = new StringReader(csv)
        val columns = ArraySeq.from(schema.fieldNames)
        processCSV(reader, schema, columns, isFirstRowHeaders, indexColumnName)
      }

      override def readParquet(filepath: Path): ArrowDataFrame = ???

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
