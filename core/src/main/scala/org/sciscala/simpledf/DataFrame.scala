package org.sciscala.simpledf

import scala.reflect.{ClassTag, classTag}
import spire.math.Numeric
import simulacrum._

import scala.collection.immutable.ArraySeq

case class ArraySeqDataFrame(
    data: ArraySeq[Column[_]],
    index: ArraySeq[String],
    columns: ArraySeq[String]
) {
  val shape: (Int, Int) =
    (if (this.data.length > 0) this.data(0).size else 0, this.data.length)
}

trait Error {
  val message: String
}
final case class InsertError(message: String) extends Error

@typeclass trait DataFrame[DFImpl] {

  def data(df: DFImpl): Seq[Column[_]]
  def index(df: DFImpl): Seq[String]
  def columns(df: DFImpl): Seq[String]
  def head(df: DFImpl, n: Int = 5): DFImpl
  def tail(df: DFImpl, n: Int = 5): DFImpl
  def at[A](df: DFImpl, rowIdx: Label, colIdx: Label): Option[A]
  def iat[A](df: DFImpl, i: Coord, j: Coord): Option[A]
  def loc(df: DFImpl, index: Label): DFImpl
  def loc[I: ClassTag: IsIndex](df: DFImpl, index: Seq[I]): DFImpl
  def shape(df: DFImpl): (Int, Int)
  def size(df: DFImpl): Int
  def to[A](df: DFImpl)(implicit E: Encoder[DFImpl, A]): Seq[A] =
    E.encode(df)
  def insert[A](
      df: DFImpl,
      loc: Int,
      col: Label,
      value: A,
      allow_duplicates: Boolean
  ): Either[Error, DFImpl]
  def empty(df: DFImpl): Boolean 
}

object DataFrame {

  implicit val ArraySeqDF: DataFrame[ArraySeqDataFrame] =
    new DataFrame[ArraySeqDataFrame] {

      val nullArraySeqDF: ArraySeqDataFrame = ArraySeqDataFrame(
        ArraySeq.empty[ArraySeq[Int]],
        ArraySeq.empty[String],
        ArraySeq.empty[String]
      )

      override def data(df: ArraySeqDataFrame): ArraySeq[Column[_]] = df.data

      override def index(df: ArraySeqDataFrame): ArraySeq[String] = df.index

      override def columns(df: ArraySeqDataFrame): ArraySeq[String] = df.columns

      override def head(
          df: ArraySeqDataFrame,
          n: Int = 5
      ): ArraySeqDataFrame =
        ArraySeqDataFrame(df.data.take(n), df.index.take(n), df.columns.take(n))

      override def tail(
          df: ArraySeqDataFrame,
          n: Int = 5
      ): ArraySeqDataFrame =
        ArraySeqDataFrame(
          df.data.takeRight(n),
          df.index.takeRight(n),
          df.columns.takeRight(n)
        )

      override def at[A](
          df: ArraySeqDataFrame,
          rowIdx: Label,
          colIdx: Label
      ): Option[A] = {
        val row = df.index.indexOf(rowIdx)
        if (row == -1) None
        else {
          val col = df.columns.indexOf(colIdx)
          if (col == -1) None
          else Some(df.data(col)(row).asInstanceOf[A])
        }
      }

      override def iat[A](
          df: ArraySeqDataFrame,
          row: Coord,
          col: Coord
      ): Option[A] =
        (row, col) match {
          case (i1, j1) if (i1 < 0 && j1 < 0) =>
            Some(df.data(df.shape._2 + col)(df.shape._1 + row).asInstanceOf[A])
          case (i1, j1) if (i1 < 0 && j1 < df.shape._2) =>
            Some(df.data(col)(df.shape._1 + row).asInstanceOf[A])
          case (i1, j1) if (i1 < df.shape._1 && j1 < 0) =>
            Some(df.data(df.shape._2 + col)(row).asInstanceOf[A])
          case (i1, j1) if (i1 < df.shape._1 && j1 < df.shape._2) =>
            Some(df.data(col)(row).asInstanceOf[A])
          case (_, _) => None
        }

      override def loc(
          df: ArraySeqDataFrame,
          index: Label
      ): ArraySeqDataFrame = {
        val row = df.index.indexOf(index)
        if (row == -1) nullArraySeqDF
        else {
          val r = df.data.map(_.zipWithIndex.filter(_._2 == row).map(_._1))
          val i = df.index(row)
          ArraySeqDataFrame(r, ArraySeq(i), df.columns)
        }
      }

      override def loc[I: ClassTag: IsIndex](
          df: ArraySeqDataFrame,
          index: Seq[I]
      ): ArraySeqDataFrame = {
        val tag = classTag[I].runtimeClass.getName
        tag match {
          case "java.lang.String" => {
            val filteredIdx = index.foldLeft(ArraySeq.empty[Int])((acc, cur) =>
              acc :+ df.index.indexOf(cur)
            )
            val cols = df.data.map(
              _.zipWithIndex
                .filter(tup => filteredIdx.contains(tup._2))
                .map(_._1)
            )
            val idxs = df.index.zipWithIndex.foldLeft(ArraySeq.empty[String])(
              (acc, cur) => {
                if (filteredIdx.contains(cur._2))
                  acc :+ cur._1.asInstanceOf[String]
                else
                  acc
              }
            )
            ArraySeqDataFrame(cols, idxs, df.columns)
          }
          case "boolean" => {
            val filteredIdx = index
              .zip(df.index)
              .collect {
                case b: (Boolean, String) if (b._1) => b._2
              }
              .map(df.index.indexOf(_))
            val cols =
              if (filteredIdx.isEmpty) ArraySeq(ArraySeq())
              else
                df.data.map(
                  _.zipWithIndex
                    .filter(tup => filteredIdx.contains(tup._2))
                    .map(_._1)
                )
            val idxs = df.index.zipWithIndex.foldLeft(ArraySeq.empty[String])(
              (acc, cur) => {
                if (filteredIdx.contains(cur._2))
                  acc :+ cur._1.asInstanceOf[String]
                else
                  acc
              }
            )
            ArraySeqDataFrame(cols, idxs, df.columns)
          }
          case _ => nullArraySeqDF
        }
      }

      override def shape(df: ArraySeqDataFrame): (Int, Int) = df.shape

      override def size(df: ArraySeqDataFrame): Int =
        (df.data.length * df.data(0).size)

      override def insert[A](
          df: ArraySeqDataFrame,
          loc: Coord,
          col: Label,
          value: A,
          allow_duplicates: Boolean
      ): Either[Error, ArraySeqDataFrame] =
        if (loc < 0) {
          Left(InsertError("Column index must be 0 or greater"))
        } else if (loc > df.data.size) {
          Left(InsertError("Column index is bigger than maximum size"))
        } else {
          val (h, t) = df.data.splitAt(loc)
          val (hc, tc) = df.columns.splitAt(loc)
          val n = (h :+ value.asInstanceOf[Column[_]]) ++ t
          val c = (hc :+ col) ++ tc
          Right(
            ArraySeqDataFrame(n, df.index, c)
          )
        }

      override def empty(df: ArraySeqDataFrame): Boolean = df.data.isEmpty

    }

}
