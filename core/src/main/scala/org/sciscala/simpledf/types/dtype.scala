package org.sciscala.simpledf.types

import org.sciscala.simpledf.error._

sealed trait dtype {
  val alias: String
  def matchType[A](value: Any): Either[Error, A]
}

object IntType extends dtype {
  val alias: String = "int"
  def matchType[Int](value: Any): Either[Error, Int] = {
    value match {
      case x: Int => Right(x)
      case _ => Left(TypeDoesNotMatchError(s"$value is not of type $alias"))
    }
  }
}

object LongType extends dtype {
  val alias: String = "long"
  def matchType[Long](value: Any): Either[Error, Long] = {
    value match {
      case x: Long => Right(x)
      case _ => Left(TypeDoesNotMatchError(s"$value is not of type $alias"))
    }
  }
}

object StringType extends dtype {
  val alias: String = "string"
  def matchType[String](value: Any): Either[Error, String] = {
    value match {
      case x: String => Right(x)
      case _ => Left(TypeDoesNotMatchError(s"$value is not of type $alias"))
    }
  }
}

object BooleanType extends dtype {
  val alias: String = "boolean"
  def matchType[Boolean](value: Any): Either[Error, Boolean] = {
    value match {
      case x: Boolean => Right(x)
      case _ => Left(TypeDoesNotMatchError(s"$value is not of type $alias"))
    }
  }
}

object dtype {

  def getdtype(tpe: String): Either[NoSuchTypeError, dtype] = {
    tpe match {
      case "int" => Right(IntType)
      case "long" => Right(LongType)
      case "string" => Right(StringType)
      case "boolean" => Right(BooleanType)
      case _ => Left(NoSuchTypeError(s"Cannot find $tpe among [int, long, string, boolean]"))
   }
  }

}
