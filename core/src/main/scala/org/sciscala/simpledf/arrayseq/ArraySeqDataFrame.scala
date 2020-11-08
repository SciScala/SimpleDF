package org.sciscala.simpledf.arrayseq

import org.sciscala.simpledf._
import org.sciscala.simpledf.arrayseq._
import org.sciscala.simpledf.error._
import org.sciscala.simpledf.row.Row
import scala.collection.immutable.ArraySeq
import scala.reflect.{ClassTag, classTag}

case class ArraySeqDataFrame( 
  data: ArraySeq[Column[_]], 
  index: ArraySeq[String], 
  columns: ArraySeq[String]) {
  val shape: (Int, Int) = (if (this.data.length > 0) this.data(0).size else 0, this.data.length)
}

object ArraySeqDataFrame {

  def emptyInstance: ArraySeqDataFrame = 
    ArraySeqDataFrame(
      data = ArraySeq.empty[ArraySeq[_]], 
      index = ArraySeq.empty[String], 
      columns = ArraySeq.empty[String]
    )

  def dfInstance: DataFrame[ArraySeqDataFrame] = {

    new DataFrame[ArraySeqDataFrame] {

      def at[A]( df: ArraySeqDataFrame, rowIdx: Label, colIdx: Label): Option[A] = {
        val row = df.index.indexOf(rowIdx)
        if (row == -1) None
        else {
          val col = df.columns.indexOf(colIdx)
          if (col == -1) None
          else Some(df.data(col)(row).asInstanceOf[A])
        }
      }

      def columns(df: ArraySeqDataFrame): ArraySeq[String] = df.columns

      def data(df: ArraySeqDataFrame): ArraySeq[Column[_]] = df.data

      def empty(df: ArraySeqDataFrame): Boolean = df.data.isEmpty

      def get[A](df: ArraySeqDataFrame, key: A, default: Option[ArraySeqDataFrame]): ArraySeqDataFrame = {
        val col = df.columns.indexOf(key)
        if (col == -1) default.getOrElse(emptyInstance)
        else ArraySeqDataFrame(ArraySeq(df.data(col)), df.index, ArraySeq(df.columns(col)))
      }

      def index(df: ArraySeqDataFrame): ArraySeq[String] = df.index

      def head(df: ArraySeqDataFrame, n: Int = 5): ArraySeqDataFrame = 
        ArraySeqDataFrame(
          data = df.data.take(n), 
          index = df.index.take(n), 
          columns = df.columns.take(n)
        )

      def iat[A](df: ArraySeqDataFrame, row: Coord, col: Coord): Option[A] =
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

      def insert[A]( 
        df: ArraySeqDataFrame,
        loc: Coord,
        col: Label,
        value: A,
        allow_duplicates: Boolean): Either[Error, ArraySeqDataFrame] =
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

      def loc( df: ArraySeqDataFrame, index: Label): ArraySeqDataFrame = {
        val row = df.index.indexOf(index)
        if (row == -1) 
          emptyInstance
        else {
          val r = df.data.map(_.zipWithIndex.filter(_._2 == row).map(_._1))
          val i = df.index(row)
          ArraySeqDataFrame(r, ArraySeq(i), df.columns)
        }
      }

      def loc[I: ClassTag: IsIndex]( df: ArraySeqDataFrame, index: Seq[I]): ArraySeqDataFrame = {
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
                case _ => emptyInstance
        }
      }

      def size(df: ArraySeqDataFrame): Int = (df.data.length * df.data(0).size)

      def shape(df: ArraySeqDataFrame): (Int, Int) = df.shape

      def tail( df: ArraySeqDataFrame, n: Int = 5): ArraySeqDataFrame = 
        ArraySeqDataFrame( 
          data = df.data.takeRight(n), 
          index = df.index.takeRight(n), 
          columns = df.columns.takeRight(n)
        )

      def items(df: ArraySeqDataFrame): Array[(String, Column[_])] = {
        for {
          index <- df.columns.indices.toArray
        } yield (df.columns(index), df.data(index))
      }

      def iterrows(df: ArraySeqDataFrame)(implicit encoder: Encoder[ArraySeqDataFrame, Row]): Seq[(String, Row)] = 
        encoder.encode(df).zip(index(df)).map(_.swap)

    }
  }

}


