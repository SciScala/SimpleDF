package org.sciscala.simpledf.error

sealed trait Error {
  val message: String
}
case class InsertError(message: String) extends Error
case class NoSuchTypeError(message: String) extends Error
case class IndexOutOfBoundsError(message: String) extends Error
case class TypeDoesNotMatchError(message : String) extends Error
case class FieldNotFoundError(message: String) extends Error
