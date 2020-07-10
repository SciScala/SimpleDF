package org.sciscala.simpledf

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.{ClassTag, classTag}

//import DataFrame.ops._

case class ArrowDataFrame(
  data: VectorSchemaRoot,
  index: Seq[String],
  //columns: ArraySeq[String]
) {
  val columns: ArraySeq[String] =
    ArraySeq.from(data.getSchema.getFields.asScala.map(f => f.getName))

  val shape: (Int, Int) =
      (this.data.getRowCount, this.data.getFieldVectors.size())

  val size: Int =
      this.data.getRowCount * this.data.getFieldVectors.size()
}

object arrow {

  val nullArrowDF = ArrowDataFrame(
    VectorSchemaRoot.of(Array.empty[FieldVector]: _*),
    ArraySeq.empty[String]
  )

  implicit val arrowDF: DataFrame[ArrowDataFrame] = new DataFrame[ArrowDataFrame] {

    override def data(df: ArrowDataFrame): Seq[Seq[_]] = {
      val fvs: java.util.List[FieldVector] = df.data.getFieldVectors
      val fvSeq: Seq[FieldVector] = fvs.toArray(Array.empty[FieldVector]).toSeq
      fvSeq.map(_.asScala.toSeq)
    }

    override def index(df: ArrowDataFrame): Seq[String] = df.index

    override def columns(df: ArrowDataFrame): Seq[String] = df.columns

    override def head(df: ArrowDataFrame, n: Int = 5): ArrowDataFrame = {
      val d = df.data.slice(0, n)
      ArrowDataFrame(d, df.index.take(n))
    }

    override def at[A](df: ArrowDataFrame, rowIdx: Label, colIdx: Label): Option[A] = {
      val row: Int = df.index.indexOf(rowIdx)
      if (row == -1) None
      else {
        val col = df.data.getVector(colIdx)
        if (col == null) None
        else Some(col.getObject(row).asInstanceOf[A])
      }
    }

    private def translateCoordinates(df: ArrowDataFrame, i: Coord, j: Coord): (Coord, Coord) = (i, j) match  {
      case (i1, j1) if (i1 < 0 && j1 < 0) =>
        (df.shape._1 + i, df.shape._2 + j)
      case (i1, j1) if (i1 < 0 && j1 <= df.shape._2) =>
        (df.shape._1 + i, j)
      case (i1, j1) if (i1 < df.shape._1 && j1 < 0) =>
        (i, df.shape._2 + j)
      case (_, _) =>
        (i, j)
    }

    override def iat[A](df: ArrowDataFrame, i: Coord, j: Coord): Option[A] = {
      val (i1, j1) = translateCoordinates(df, i, j)
      val l = df.data.getVector(j1)
      if (i1 <= l.getValueCount) {
        Option(l.getObject(i1).asInstanceOf[A])
      } else None
    }

    override def loc(df: ArrowDataFrame, index: Label): ArrowDataFrame = {
      val row = df.index.indexOf(index)
      if (row == -1) nullArrowDF
      else ArrowDataFrame(df.data.slice(row, row + 1), ArraySeq(df.index(row)))
    }

    override def loc[I: ClassTag: IsIndex](df: ArrowDataFrame, index: Seq[I]): ArrowDataFrame = {
      val tag = classTag[I].runtimeClass.getName
      val schema: Schema = df.data.getSchema
      val root = VectorSchemaRoot.create(schema, new RootAllocator())
      root.allocateNew()

      val newVectors: Seq[FieldVector] = schema.getFields.asScala.map(f => {
        val v = root.getVector(f.getName)
        v.allocateNew()
        v
      }).toSeq

      tag match {
        case "java.lang.String" => {
          copyVectorsByIndex(index, root, newVectors, df)
        }
        case "boolean" => {
          val filteredIdx: Seq[String] = index.zip(df.index).collect{ case b: (Boolean, String) if (b._1) => b._2}
          copyVectorsByIndex(filteredIdx, root, newVectors, df)
        }
      }
    }

    override def shape(df: ArrowDataFrame): (Int, Int) =
      df.shape

    override def size(df: ArrowDataFrame): Int =
      df.size

  }

  // TODO take into account field children
  private def copyVectorsByIndex[I: ClassTag : IsIndex](index: Seq[I], root: VectorSchemaRoot, newVectors: Seq[FieldVector], df: ArrowDataFrame) = {
    val indexes = index.zipWithIndex.foldLeft(ArraySeq.empty[String])((acc, cur) => {
      val rowIdx: Coord = df.index.indexOf(cur._1)
      if (rowIdx == -1) acc
      else {
        newVectors.zipWithIndex.foreach(fv => {
          val originalVector = df.data.getVector(fv._2)
          fv._1.copyFromSafe(rowIdx, cur._2, originalVector)
          fv._1.setValueCount(cur._2 + 1)
        })
        acc :+ cur._1.asInstanceOf[String]
      }
    })

    root.setRowCount(indexes.length)
    ArrowDataFrame(root, indexes)
  }
}
