package org.sciscala.simpledf

import scala.collection.immutable.ArraySeq
import scala.reflect.{classTag, ClassTag}

import spire.math.Numeric
import simulacrum._

//case class NDArray[@specialized(Int, Long, Float, Double, Boolean) T](arr: ArraySeq[T])
case class NDArray[T: IsSupported](arr: ArraySeq[T]) {
  val size = arr.length
}
object NDArray {
  def apply[T: IsSupported](arr: ArraySeq[T]) =
    new NDArray[T](arr)

  def apply[T: ClassTag: IsSupported](values: T*) =
    new NDArray[T](ArraySeq(values: _*))
}

case class ArraySeqBackedDF(
    data: ArraySeq[NDArray[_]],
    index: ArraySeq[String],
    columns: ArraySeq[String]
) {
  def shape: (Int, Int) = (this.data.length, this.data(0).size)
}

@typeclass trait DataFrame[DFImpl] {
  import DataFrame._

  def head(df: DFImpl, n: Int = 5): DFImpl
  def at[A](df: DFImpl, rowIdx: Label, colIdx: Label): Option[A]
  def iat[A](df: DFImpl, i: Coord, j: Coord): Option[A]
  def loc(df: DFImpl, index: Label): DFImpl
  def loc[I: ClassTag: IsIndex](df: DFImpl, index: Seq[I]): DFImpl
  def shape(df: DFImpl): (Int, Int)
  def size(df: DFImpl): Int
}

object DataFrame {

  import DataFrame.ops._
  implicit def arraySeqDF = new DataFrame[ArraySeqBackedDF] {

    val nullArraySeqDF = ArraySeqBackedDF(
      ArraySeq.empty[NDArray[Int]],
      ArraySeq.empty[String],
      ArraySeq.empty[String]
    )

    override def head(
        df: ArraySeqBackedDF,
        n: Int = 5
    ): ArraySeqBackedDF =
      ArraySeqBackedDF(df.data.take(n), df.index.take(n), df.columns.take(n))

    override def at[A](
        df: ArraySeqBackedDF,
        rowIdx: Label,
        colIdx: Label
    ): Option[A] = {
      val row = df.index.indexOf(rowIdx)
      if (row == -1) None
      else {
        val col = df.columns.indexOf(colIdx)
        if (col == -1) None
        else Some(df.data(row).arr(col).asInstanceOf[A])
      }
    }

    override def iat[A](df: ArraySeqBackedDF, i: Coord, j: Coord): Option[A] =
      (i, j) match {
        case (i1, j1) if (i1 < 0 && j1 < 0) =>
          Some(df.data(df.shape._1 + i).arr(df.shape._2 + j).asInstanceOf[A])
        case (i1, j1) if (i1 < 0 && j1 <= df.shape._2) =>
          Some(df.data(df.shape._1 + i).arr(j).asInstanceOf[A])
        case (i1, j1) if (i1 < df.shape._1 && j1 < 0) =>
          Some(df.data(i).arr(df.shape._2 + j).asInstanceOf[A])
        case (i1, j1) if (i1 <= df.shape._1 && j1 <= df.shape._2) =>
          Some(df.data(i).arr(j).asInstanceOf[A])
        case (_, _) => None
      }

    override def loc(
        df: ArraySeqBackedDF,
        index: Label
    ): ArraySeqBackedDF = {
      val row = df.index.indexOf(index)
      if (row == -1) nullArraySeqDF
      else {
        val r = df.data(row)
        val i = df.index(row)
        ArraySeqBackedDF(ArraySeq(r), ArraySeq(i), df.columns)
      }
    }

    override def loc[I: ClassTag: IsIndex](
        df: ArraySeqBackedDF,
        index: Seq[I]
    ): ArraySeqBackedDF = {
      val tag = classTag[I].runtimeClass.getName
      println(tag)
      tag match {
        case "java.lang.String" => {
          val (rows, idxs) = index.foldLeft(
            (ArraySeq.empty[NDArray[_]], ArraySeq.empty[String])
          )((acc, cur) => {
            val rowIdx: Int = df.index.indexOf(cur)
            if (rowIdx == -1) acc
            else (acc._1 :+ df.data(rowIdx), acc._2 :+ cur.asInstanceOf[String])
          })
          ArraySeqBackedDF(rows, idxs, df.columns)
        }
        case "boolean" => {
          val filteredIdx = index.zip(df.index).collect{ case b: (Boolean, String) if (b._1) => b._2}
          val (rows, idxs) = filteredIdx.foldLeft(
            (ArraySeq.empty[NDArray[_]], ArraySeq.empty[String])
          )((acc, cur) => {
            val rowIdx: Int = df.index.indexOf(cur)
            if (rowIdx == -1) acc
            else (acc._1 :+ df.data(rowIdx), acc._2 :+ cur.asInstanceOf[String])
          })
          ArraySeqBackedDF(rows, idxs, df.columns)
        }
        case _ => nullArraySeqDF
      }
    }

    override def shape(df: ArraySeqBackedDF): (Int, Int) = df.shape

    override def size(df: ArraySeqBackedDF): Int =
      (df.data.length * df.data(0).size)
  }

}
