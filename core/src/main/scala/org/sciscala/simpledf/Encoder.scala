package org.sciscala.simpledf

trait Encoder[DF, A] {
  def encode(df: DF): Seq[A]
}
