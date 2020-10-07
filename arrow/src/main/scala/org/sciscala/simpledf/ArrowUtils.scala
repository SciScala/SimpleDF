package org.sciscala.simpledf

import org.apache.arrow.vector._
import java.sql.Types._

object ArrowUtils {

  def vectorAsSeq(rowCount: Int, vector: FieldVector) : Seq[_] = {
    vector match {
      case v: BitVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: FixedSizeBinaryVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TinyIntVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: UInt1Vector => for (row <- 0 until rowCount) yield v.get(row)
      case v: UInt2Vector => for (row <- 0 until rowCount) yield v.get(row)
      case v: UInt4Vector => for (row <- 0 until rowCount) yield v.get(row)
      case v: UInt8Vector => for (row <- 0 until rowCount) yield v.get(row)
      case v: SmallIntVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: IntVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: BigIntVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: Float4Vector => for (row <- 0 until rowCount) yield v.get(row)
      case v: Float8Vector => for (row <- 0 until rowCount) yield v.get(row)
      case v: DecimalVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: VarCharVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: VarBinaryVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: DurationVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: DateDayVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: DateMilliVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: IntervalDayVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: IntervalYearVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeMicroVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeMilliVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeNanoVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeSecVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeStampMicroTZVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeStampMicroVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeStampMilliTZVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeStampMilliVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeStampNanoTZVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeStampNanoVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeStampSecTZVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeStampSecVector => for (row <- 0 until rowCount) yield v.get(row)
      case v: TimeStampVector => for (row <- 0 until rowCount) yield v.get(row)
    }
  }

}
