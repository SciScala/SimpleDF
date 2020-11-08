package org.sciscala.simpledf.arrow

import org.sciscala.simpledf.DataFrame

object implicits {

  implicit val arrowDataFrame: DataFrame[ArrowDataFrame] = DataFrame.apply(ArrowDataFrame.dfInstance)

}
