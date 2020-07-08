package org.sciscala.simpledf

import org.apache.arrow.vector.VectorSchemaRoot

import scala.collection.immutable.ArraySeq
import scala.reflect.ClassTag
import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.Field

//import DataFrame.ops._

case class ArrowDataFrame(
  data: VectorSchemaRoot,
  index: ArraySeq[String],
  columns: ArraySeq[String]
) {
  val shape: (Int, Int) =
      (this.data.getRowCount, this.data.getFieldVectors.size())

  val size: Int =
      this.data.getRowCount * this.data.getFieldVectors.size()
}

object arrow {
  implicit val arrowDF: DataFrame[ArrowDataFrame] = new DataFrame[ArrowDataFrame] {

    override def data(df: ArrowDataFrame): Seq[Seq[_]] = {
      val fvs: java.util.List[FieldVector] = df.data.getFieldVectors()
      val fvSeq: Seq[FieldVector] = fvs.toArray(Array.empty[FieldVector]).toSeq
      fvSeq.map(_.asScala.toSeq)
    }

    override def index(df: ArrowDataFrame): Seq[String] = df.index

    override def columns(df: ArrowDataFrame): Seq[String] = df.columns

    override def head(df: ArrowDataFrame, n: Int = 5): ArrowDataFrame = {
      val d = df.data.slice(0, n)
      ArrowDataFrame(d, df.index.take(n), df.columns)
    }

    override def at[A](df: ArrowDataFrame, rowIdx: Label, colIdx: Label): Option[A] = {
      val row: Int = df.index.indexOf(rowIdx)
      Option(df.data.getVector(colIdx).getObject(row).asInstanceOf[A])
    }

    override def iat[A](df: ArrowDataFrame, i: Coord, j: Coord): Option[A] = {
      val l = df.data.getVector(j)
      if (i <= l.getValueCount) {
        Option(l.getObject(i).asInstanceOf[A])
      } else None
    }

    override def loc(df: ArrowDataFrame, index: Label): ArrowDataFrame = {
      val row = df.index.indexOf(index)
      ArrowDataFrame(df.data.slice(row), ArraySeq(df.index(row)), df.columns)
    }

    override def loc[I: ClassTag: IsIndex](df: ArrowDataFrame, index: Seq[I]): ArrowDataFrame = ???

    override def shape(df: ArrowDataFrame): (Int, Int) =
      df.shape

    override def size(df: ArrowDataFrame): Int =
      df.size

  }
}
