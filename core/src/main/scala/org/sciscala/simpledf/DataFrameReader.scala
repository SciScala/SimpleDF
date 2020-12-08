package org.sciscala.simpledf

import java.nio.file.Path
import simulacrum.typeclass
import org.sciscala.simpledf.codecs._
import scala.reflect.ClassTag

@typeclass trait DataFrameReader[DFRImpl] {

  def readJson[A <: Serializable](filepath: Path)(implicit D: Decoder[A, DFRImpl]): DFRImpl

  def readCSV[A <: Serializable : ClassTag](filepath: Path, headers: Boolean)(implicit D: Decoder[A, DFRImpl]): DFRImpl

  def readCSV[A <: Serializable: ClassTag](csv: String)(implicit D: Decoder[A, DFRImpl]): DFRImpl

  def readParquet(filepath: Path): DFRImpl

}
