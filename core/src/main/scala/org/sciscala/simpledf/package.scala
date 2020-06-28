package org.sciscala

package object simpledf {

  import scala.collection.immutable.ArraySeq

  type ![A]  = A => Nothing
  type !![A] = ![![A]]

  trait Disjunction[T] {
    type or[S]  = Disjunction[T with ![S]]
    type create = ![T]
  }

  type Union[T] = {
    type or[S] = Disjunction[![T]]#or[S]
  }

  type Contains[S, T] = !![S] <:< T

  type Supported = Union[Int]#or[Long]#or[Float]#or[Double]#or[Boolean]#or[String]#create
  type IsSupported[T] = Contains[T, Supported]

  type Index = Union[Boolean]#or[String]#create
  type IsIndex[T] = Contains[T, Index]

  type Label = String
  type Coord = Int
}
