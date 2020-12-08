package org.sciscala.simpledf.arrow

import java.nio.file.Path
import java.io.File
import java.io.FileInputStream

import org.apache.arrow.vector.ipc.SeekableReadChannel
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.ipc.message.ArrowBlock

import scala.jdk.CollectionConverters._
import org.apache.arrow.vector.FieldVector
import org.sciscala.simpledf.{DataFrame, DataFrameReader}
import org.sciscala.simpledf.codecs._
import scala.reflect.ClassTag

object ArrowDataFrameReader {

  private def read(filepath: Path): ArrowDataFrame = {
    val arrowFile: File = new File(filepath.toUri())
    val fileInputStream: FileInputStream  = new FileInputStream(arrowFile)
    val seekableReadChannel: SeekableReadChannel = new SeekableReadChannel(fileInputStream.getChannel())
    val arrowFileReader: ArrowFileReader = new ArrowFileReader(seekableReadChannel, new RootAllocator(Integer.MAX_VALUE))
    val root: VectorSchemaRoot = arrowFileReader.getVectorSchemaRoot()
    /*
    val schema: Schema = root.getSchema()
    val arrowBlocks: List[ArrowBlock] = arrowFileReader.getRecordBlocks().asScala.toList
     */
    val fieldVectorNames: List[String] = root.getFieldVectors().asScala.toList.map(_.getName())
    ArrowDataFrame(root, fieldVectorNames)
  }

  implicit val arrowDataFrameReader: DataFrameReader[ArrowDataFrame] = new DataFrameReader[ArrowDataFrame] {

    override def readJson[A <: Serializable](filepath: Path)(implicit D: Decoder[A,ArrowDataFrame]): ArrowDataFrame = ???

    override def readCSV[A <: Serializable : ClassTag](filepath: Path, headers: Boolean)(implicit D: Decoder[A,ArrowDataFrame]): ArrowDataFrame = ???

    override def readCSV[A <: Serializable : ClassTag](csv: String)(implicit D: Decoder[A,ArrowDataFrame]): ArrowDataFrame = ???

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
