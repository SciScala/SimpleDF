package org.sciscala.simpledf.arrow

import org.sciscala.simpledf.row.Row
import org.sciscala.simpledf.types.Schema
import scala.jdk.CollectionConverters._
import org.sciscala.simpledf.types._
import org.sciscala.simpledf.codecs._

object ArrowEncoder {

  def arrowRowEncoder(schema: Schema) = new Encoder[ArrowDataFrame, Row] {
    def encode(df: ArrowDataFrame): Seq[Row] = {
      val rows = df.data.getRowCount
      val dataAsSeq = df.data.getFieldVectors.asScala.toSeq.map(ArrowUtils.vectorAsSeq(rows,_))
      for {
        row <- 0 until rows
        seq = dataAsSeq.map(_(row))
      } yield Row(seq, schema)
    }
  }
}
