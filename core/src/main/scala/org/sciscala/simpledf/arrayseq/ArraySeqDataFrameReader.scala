package org.sciscala.simpledf.arrayseq

import java.nio.file.Path

import scala.io.Source
import scala.io.BufferedSource

import org.sciscala.simpledf.DataFrameReader
import org.sciscala.simpledf.DataFrame
import org.sciscala.simpledf.codecs.Decoder
import com.opencsv.bean.CsvToBeanBuilder
import com.opencsv.CSVReader
import scala.reflect.{ClassTag, classTag}
import scala.jdk.CollectionConverters._
import java.io.StringReader

object ArraySeqDataFrameReader {

  implicit val arrayseqDataFrameReader: DataFrameReader[ArraySeqDataFrame] = new DataFrameReader[ArraySeqDataFrame] {

    private def readFile(filepath: Path): BufferedSource  =
      Source.fromFile(filepath.toFile())

    private def readFileLines(filepath: Path): Iterator[String] =
      readFile(filepath).getLines()

    private def readFileString(filepath: Path, headers: Boolean): String  = {
      if (headers) readFileLines(filepath).toList.tail.mkString
      else readFile(filepath).mkString
    }
//Map<String, String> values = new CSVReaderHeaderAware(new FileReader("yourfile.csv")).readMap();
/*
    ColumnPositionMappingStrategy strat = new ColumnPositionMappingStrategy();
    strat.setType(YourOrderBean.class);
    String[] columns = new String[] {"name", "orderNumber", "id"}; // the fields to bind to in your bean
    strat.setColumnMapping(columns);

    CsvToBean csv = new CsvToBean();
    List list = csv.parse(strat, yourReader);
    */
    override def readJson[A <: Serializable](filepath: Path)(implicit D: Decoder[A, ArraySeqDataFrame]): ArraySeqDataFrame =  ???

    override def readCSV[A <: Serializable : ClassTag](csv: String)(implicit D: Decoder[A, ArraySeqDataFrame]): ArraySeqDataFrame = {
      val as: Seq[A] = new CsvToBeanBuilder(new StringReader(csv))
        .withType(classTag[A].runtimeClass)
        .build
        .parse()
        .asScala
        .toSeq
        .map(_.asInstanceOf[A])
      D.decode(as)
    }

    override def readCSV[A <: Serializable : ClassTag](filepath: Path, headers: Boolean)(implicit D: Decoder[A, ArraySeqDataFrame]): ArraySeqDataFrame = {
      val as: Seq[A] = new CsvToBeanBuilder(readFile(filepath).reader())
        .withType(classTag[A].runtimeClass)
        .build
        .parse()
        .asScala
        .toSeq
        .map(_.asInstanceOf[A])
      D.decode(as)
    }

    override def readParquet(filepath: Path): ArraySeqDataFrame = ???


  }

}
