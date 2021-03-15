package org.sciscala.simpledf

import java.nio.file.Path
import simulacrum.typeclass
import org.sciscala.simpledf.codecs._
import scala.reflect.ClassTag
import org.sciscala.simpledf.types.Schema

@typeclass trait DataFrameReader[DFRImpl] {

  def readJson(filepath: Path): DFRImpl

  def readJson(jsonString: String): DFRImpl

  def readCSV(filepath: Path, schema: Schema, isFirstRowHeaders: Boolean, indexColumnName: Option[String]): DFRImpl

  def readCSV(csv: String, schema:Schema, isFirstRowHeaders: Boolean, indexColumnName: Option[String]): DFRImpl

  def readParquet(filepath: Path): DFRImpl

}
