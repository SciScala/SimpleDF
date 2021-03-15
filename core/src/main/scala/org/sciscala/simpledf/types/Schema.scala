package org.sciscala.simpledf.types

import org.sciscala.simpledf.types.dtype
import org.sciscala.simpledf.error._

case class Field(name: String, dtype: dtype, nullable: Boolean) 

case class Schema(fields: Seq[Field]) {

  def size: Int = fields.size

  def indexOf(name: String): Either[FieldNotFoundError, Int] = {
    fields.indexWhere(_.name == name) match {
      case -1 => Left(FieldNotFoundError(s"$name cannot be found under [${fieldNames.mkString(",")}]"))
      case other => Right(other)
    }
  }

  def fieldNames: Seq[String] = fields.map(_.name)

  def add(field: Field): Schema = Schema(fields :+ field)

  def treeString: String = fields.map(f => s"${f.name} : ${f.dtype.alias} (nullable : ${f.nullable})").mkString("\n")

  def printSchema(): Unit = println("root\n" + treeString.map("|-" + _))
}
