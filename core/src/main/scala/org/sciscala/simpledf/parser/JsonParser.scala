package org.sciscala.simpledf.parser

import fastparse._, NoWhitespace._

sealed trait Val extends Any {
  def value: Any
  def apply(i: Int): Val = this.asInstanceOf[Arr].value(i)
  def apply(s: java.lang.String): Val =
    this.asInstanceOf[Obj].value.find(_._1 == s).get._2
}
case class Str(value: java.lang.String) extends AnyVal with Val
case class Obj(value: (java.lang.String, Val)*) extends AnyVal with Val
case class Arr(value: Val*) extends AnyVal with Val
case class Num(value: Double) extends AnyVal with Val
case object False extends Val {
  def value = false
}
case object True extends Val {
  def value = true
}
case object Null extends Val {
  def value = null
}

object JsonParser {
  def stringChars(c: Char) = c != '\"' && c != '\\'

  def space[_: P] = P(CharsWhileIn(" \r\n", 0))
  def digits[_: P] = P(CharsWhileIn("0-9"))
  def exponent[_: P] = P(CharIn("eE") ~ CharIn("+\\-").? ~ digits)
  def fractional[_: P] = P("." ~ digits)
  def integral[_: P] = P("0" | CharIn("1-9") ~ digits.?)

  def number[_: P] =
    P(CharIn("+\\-").? ~ integral ~ fractional.? ~ exponent.?).!.map(x =>
      Num(x.toDouble)
    )

  def `null`[_: P] = P("null").map(_ => Null)
  def `false`[_: P] = P("false").map(_ => False)
  def `true`[_: P] = P("true").map(_ => True)

  def hexDigit[_: P] = P(CharIn("0-9a-fA-F"))
  def unicodeEscape[_: P] = P("u" ~ hexDigit ~ hexDigit ~ hexDigit ~ hexDigit)
  def escape[_: P] = P("\\" ~ (CharIn("\"/\\\\bfnrt") | unicodeEscape))

  def strChars[_: P] = P(CharsWhile(stringChars))
  def string[_: P] =
    P(space ~ "\"" ~/ (strChars | escape).rep.! ~ "\"").map(Str)

  def array[_: P] =
    P("[" ~/ jsonExpr.rep(sep = ","./) ~ space ~ "]").map(Arr(_: _*))

  def pair[_: P] = P(string.map(_.value) ~/ ":" ~/ jsonExpr)

  def obj[_: P] =
    P("{" ~/ pair.rep(sep = ","./) ~ space ~ "}").map(Obj(_: _*))

  def jsonExpr[_: P]: P[Val] = P(
    space ~ (obj | array | string | `true` | `false` | `null` | number) ~ space
  )
}
