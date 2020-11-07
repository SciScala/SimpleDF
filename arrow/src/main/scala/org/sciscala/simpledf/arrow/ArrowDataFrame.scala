package org.sciscala.simpledf.arrow

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo._
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters._
import scala.reflect.{ClassTag, classTag}

import org.sciscala.simpledf._
import org.sciscala.simpledf.row.Row
import org.sciscala.simpledf.error._


case class ArrowDataFrame(
    data: VectorSchemaRoot,
    index: Seq[String]
) {
  val columns: ArraySeq[String] =
    ArraySeq.from(data.getSchema.getFields.asScala.map(f => f.getName))

  val shape: (Int, Int) =
    (this.data.getRowCount, this.data.getFieldVectors.size())

  val size: Int =
    this.data.getRowCount * this.data.getFieldVectors.size()
}

