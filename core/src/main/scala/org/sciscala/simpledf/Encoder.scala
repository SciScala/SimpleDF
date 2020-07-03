package org.sciscala.simpledf

import simulacrum._

trait Encoder[DF, A] {
  def encode(df: DF): Seq[A]
}
