package org.sciscala.simpledf

import org.sciscala.simpledf.error.Error
import org.sciscala.simpledf.row.Row
import scala.reflect.{ClassTag, classTag}
import spire.math.Numeric
import simulacrum._
import org.sciscala.simpledf.arrayseq.ArraySeqDataFrame


@typeclass trait DataFrame[DFImpl] { 

  def at[A](df: DFImpl, rowIdx: Label, colIdx: Label): Option[A]
  def columns(df: DFImpl): Seq[String]
  def data(df: DFImpl): Seq[Column[_]]
  def empty(df: DFImpl): Boolean
  def get[A](df: DFImpl, key: A, default: Option[DFImpl]): DFImpl
  def head(df: DFImpl, n: Int = 5): DFImpl
  def iat[A](df: DFImpl, i: Coord, j: Coord): Option[A]
  def index(df: DFImpl): Seq[String]
  def insert[A](
      df: DFImpl,
      loc: Int,
      col: Label,
      value: A,
      allow_duplicates: Boolean
  ): Either[Error, DFImpl]
  def loc(df: DFImpl, index: Label): DFImpl
  def loc[I: ClassTag: IsIndex](df: DFImpl, index: Seq[I]): DFImpl
  def shape(df: DFImpl): (Int, Int)
  def size(df: DFImpl): Int
  def tail(df: DFImpl, n: Int = 5): DFImpl
  def to[A](df: DFImpl)(implicit E: Encoder[DFImpl, A]): Seq[A] =
    E.encode(df)
  def items(df: DFImpl): Array[(String, Column[_])]
  def iterrows(df: DFImpl)(implicit encoder: Encoder[DFImpl, Row]): Seq[(String, Row)]
}

object DataFrame {
  def apply[A](f: => DataFrame[A]): DataFrame[A] = f

  implicit val arraySeqDataFrame: DataFrame[ArraySeqDataFrame] = apply(ArraySeqDataFrame.dfInstance)
}

