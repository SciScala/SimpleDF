package org.sciscala.simpledf.codecs

trait Encoder[DF, A] {
  def encode(df: DF): Seq[A]
}

trait Decoder[A <: Serializable, DF] {
  def decode(as: Seq[A]): DF
}
