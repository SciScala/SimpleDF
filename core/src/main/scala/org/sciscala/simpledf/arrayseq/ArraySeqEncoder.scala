package org.sciscala.simpledf.arrayseq

import org.sciscala.simpledf.types.Schema
import org.sciscala.simpledf.codecs.Encoder
import org.sciscala.simpledf.row.Row

object ArraySeqEncoder {

  def rowEncoder(schema: Schema): Encoder[ArraySeqDataFrame, Row] =  new Encoder[ArraySeqDataFrame, Row] {
    def encode(df: ArraySeqDataFrame): Seq[Row] = {
      val rows = df.shape._1
      for {
        row <- 0 until rows
        seq = df.data.map(_(row))
      } yield Row(seq, schema)
    }
  }

}
