package org.sciscala.simpledf.row

import org.sciscala.simpledf._
import org.sciscala.simpledf.error._
import org.sciscala.simpledf.types._

case class Row(values: Seq[_] , schema: Schema) {

  val size: Int = schema.size

  def get[A](name: Label): Either[Error, A] = schema.indexOf(name).flatMap(get[A](_))

  def get[A](index: Coord): Either[Error, A] = {
    if (index >= size)
      Left(IndexOutOfBoundsError(s"Index $index is greater than length of row i.e $size"))
    else 
      schema.fields(index).dtype.matchType(values(index))
  }
}
