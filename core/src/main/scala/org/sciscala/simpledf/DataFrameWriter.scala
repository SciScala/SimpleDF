package org.sciscala.simpledf

import java.nio.file.Path

trait DataFrameWriter {

  def writeCSV(df: DataFrame[_], filepath: Path): DataFrame[_]

  def writeJson(df: DataFrame[_],filepath: Path): DataFrame[_]

  def writeParquet(df: DataFrame[_],filepath: Path): DataFrame[_]

}
