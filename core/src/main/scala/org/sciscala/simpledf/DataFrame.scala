package org.sciscala.simpledf

import scala.collection.immutable.ArraySeq
import scala.reflect.{classTag, ClassTag}

import spire.math.Numeric
import simulacrum._

case class ArraySeqBackedDF(
    data: ArraySeq[ArraySeq[_]],
    index: ArraySeq[String],
    columns: ArraySeq[String]
) {
  val shape: (Int, Int) = (this.data.length, if (this.data.length > 0) this.data(0).size else 0)
}

@typeclass trait DataFrame[DFImpl] {

  def data(df: DFImpl): Seq[Seq[_]]
  def index(df: DFImpl): Seq[String]
  def columns(df: DFImpl): Seq[String]
  def head(df: DFImpl, n: Int = 5): DFImpl
  def at[A](df: DFImpl, rowIdx: Label, colIdx: Label): Option[A]
  def iat[A](df: DFImpl, i: Coord, j: Coord): Option[A]
  def loc(df: DFImpl, index: Label): DFImpl
  def loc[I: ClassTag: IsIndex](df: DFImpl, index: Seq[I]): DFImpl
  def shape(df: DFImpl): (Int, Int)
  def size(df: DFImpl): Int
  def to[A](df: DFImpl)(implicit E: Encoder[DFImpl, A]): Seq[A] = E.encode(df)
}

object DataFrame {

  implicit val arraySeqDF: DataFrame[ArraySeqBackedDF] = new DataFrame[ArraySeqBackedDF] {

    val nullArraySeqDF: ArraySeqBackedDF = ArraySeqBackedDF(
      ArraySeq.empty[ArraySeq[Int]],
      ArraySeq.empty[String],
      ArraySeq.empty[String]
    )

    override def data(df: ArraySeqBackedDF): Seq[Seq[_]] = df.data

    override def index(df: ArraySeqBackedDF): Seq[String] = df.index

    override def columns(df: ArraySeqBackedDF): Seq[String] = df.columns

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
        else Some(df.data(row)(col).asInstanceOf[A])
      }
    }

    override def iat[A](df: ArraySeqBackedDF, i: Coord, j: Coord): Option[A] =
      (i, j) match {
        case (i1, j1) if (i1 < 0 && j1 < 0) =>
          Some(df.data(df.shape._1 + i)(df.shape._2 + j).asInstanceOf[A])
        case (i1, j1) if (i1 < 0 && j1 <= df.shape._2) =>
          Some(df.data(df.shape._1 + i)(j).asInstanceOf[A])
        case (i1, j1) if (i1 < df.shape._1 && j1 < 0) =>
          Some(df.data(i)(df.shape._2 + j).asInstanceOf[A])
        case (i1, j1) if (i1 <= df.shape._1 && j1 <= df.shape._2) =>
          Some(df.data(i)(j).asInstanceOf[A])
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
            (ArraySeq.empty[ArraySeq[_]], ArraySeq.empty[String])
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
            (ArraySeq.empty[ArraySeq[_]], ArraySeq.empty[String])
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
